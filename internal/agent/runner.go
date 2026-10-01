package agent

import (
	"context"
	"errors"
	"log"
	"os"
	"time"
)

// Runner drives the management loop: poll the desired configuration with an
// ETag, validate and persist new versions, and report heartbeats at a fixed
// cadence. While the management plane is unreachable the agent keeps the last
// accepted configuration; when the server reports the collector disabled the
// runner stops applying configuration and returns nil after marking the local
// state.
type Runner struct {
	Client  *Client
	Store   Store
	Version string
	// Interval defaults to DefaultHeartbeatInterval.
	Interval time.Duration
	// Sources reports per-source operational status for heartbeats. It may be
	// nil while no component supervision is attached (COL-09).
	Sources func() []SourceStatus
	// Logger defaults to log.Default().
	Logger *log.Logger
}

func (r *Runner) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return DefaultHeartbeatInterval
}

func (r *Runner) logger() *log.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return log.Default()
}

// Run executes the management loop until ctx is cancelled or the collector is
// disabled on the management plane.
func (r *Runner) Run(ctx context.Context) error {
	state, err := r.Store.LoadState()
	if err != nil {
		return err
	}
	if state.Disabled {
		r.logger().Printf("agent %s is marked disabled locally; refusing to run", state.CollectorID)
		return nil
	}
	applied, err := r.Store.LoadConfig()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	version := r.Version
	if version == "" {
		version = "dev"
	}
	agentState := "running"
	diagnostic := ""
	tick := func() bool {
		cfg, changed, err := r.Client.FetchConfig(ctx, state.Credential, applied.Version)
		switch {
		case errors.Is(err, ErrDisabled):
			r.markDisabled(state)
			return false
		case errors.Is(err, ErrInvalidRemoteConfig):
			agentState = "error"
			diagnostic = truncateDiagnostic("rejecting fetched configuration: " + err.Error())
			r.logger().Printf("agent %s: %s", state.CollectorID, diagnostic)
		case err != nil:
			diagnostic = truncateDiagnostic("config poll failed; keeping last effective configuration: " + err.Error())
			r.logger().Printf("agent %s: %s", state.CollectorID, diagnostic)
		case changed:
			stored := StoredConfig{Version: cfg.Version, Configuration: cfg.Configuration, AppliedAt: time.Now().UTC()}
			if err := r.Store.SaveConfig(stored); err != nil {
				agentState = "error"
				diagnostic = truncateDiagnostic("rejecting fetched configuration: " + err.Error())
				r.logger().Printf("agent %s: %s", state.CollectorID, diagnostic)
			} else {
				applied = stored
				agentState = "running"
				diagnostic = ""
				r.logger().Printf("agent %s: applied configuration version %d", state.CollectorID, cfg.Version)
			}
		}
		beat := Heartbeat{
			Version:       version,
			ConfigVersion: applied.Version,
			State:         agentState,
			Sources:       []SourceStatus{},
			QueueDepth:    0,
			Diagnostic:    diagnostic,
		}
		if r.Sources != nil {
			if sources := r.Sources(); sources != nil {
				beat.Sources = sources
			}
		}
		if err := r.Client.SendHeartbeat(ctx, state.Credential, beat); err != nil {
			if errors.Is(err, ErrDisabled) {
				r.markDisabled(state)
				return false
			}
			r.logger().Printf("agent %s: heartbeat failed (offline; continuing with last effective configuration): %v", state.CollectorID, err)
		}
		return true
	}
	if !tick() {
		return nil
	}
	timer := time.NewTicker(r.interval())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if !tick() {
				return nil
			}
		}
	}
}

func (r *Runner) markDisabled(state State) {
	state.Disabled = true
	if err := r.Store.SaveState(state); err != nil {
		r.logger().Printf("agent %s: could not persist disabled state: %v", state.CollectorID, err)
	}
	r.logger().Printf("agent %s: collector disabled on the management plane; stopping management loop", state.CollectorID)
}

func truncateDiagnostic(value string) string {
	const limit = 2048
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
