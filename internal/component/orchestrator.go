package component

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Upgrade orchestration phases, persisted in
// <root>/components/<name>/orchestration.json so the state machine is
// queryable across restarts and by other processes (status command, future
// Management Agent).
const (
	PhaseIdle        = "idle"
	PhaseApplying    = "applying"
	PhaseObserving   = "observing"
	PhaseConfirming  = "confirming"
	PhaseRollingBack = "rolling-back"
	PhaseRecovering  = "recovering"
	PhaseFailed      = "failed"
)

// OrchestrationState is the durable record of the upgrade state machine.
type OrchestrationState struct {
	Component   string    `json:"component"`
	Phase       string    `json:"phase"`
	FromVersion string    `json:"from_version,omitempty"`
	ToVersion   string    `json:"to_version,omitempty"`
	// RolledBack marks a completed cycle whose upgrade did not stick.
	RolledBack bool   `json:"rolled_back,omitempty"`
	Error      string `json:"error,omitempty"`
	Updated    time.Time `json:"updated_at"`
}

const orchestrationFileName = "orchestration.json"

// ProcessManager abstracts process lifecycle control of the managed
// component. The production implementation drives the launcher's per-service
// controls; tests substitute a fake. This package deliberately grows no
// supervision loop of its own.
type ProcessManager interface {
	StopService(ctx context.Context) error
	StartService(ctx context.Context) error
	// Alive reports whether the component process is currently running.
	Alive(ctx context.Context) (bool, error)
}

// HealthChecker judges component health during the observation window; the
// production implementation polls the component's readiness endpoint.
type HealthChecker interface {
	Ready(ctx context.Context) error
}

// Orchestrator sequences the upgrade primitives into the design-baseline
// flow: stop the managed component, apply the signed package, start it, hold
// a health observation window, then confirm — or automatically roll back and
// observe the rollback. A rollback that itself stays unhealthy ends in the
// terminal "failed" phase with the component stopped: fail-closed, no
// oscillation.
type Orchestrator struct {
	Upgrader  *Upgrader
	Processes ProcessManager
	Health    HealthChecker

	// ObserveWindow is how long the new version must stay continuously
	// healthy before it is confirmed (design baseline: 5 minutes).
	ObserveWindow time.Duration
	// StartGrace is how long the component may take to become healthy after
	// (re)start before the observation counts it as failed.
	StartGrace time.Duration
	// PollInterval is the health/liveness poll cadence.
	PollInterval time.Duration
	// StopTimeout bounds each launcher stop/start wait.
	StopTimeout time.Duration
}

func (o *Orchestrator) withDefaults() {
	if o.ObserveWindow <= 0 {
		o.ObserveWindow = 5 * time.Minute
	}
	if o.StartGrace <= 0 {
		o.StartGrace = time.Minute
	}
	if o.StartGrace > o.ObserveWindow {
		o.StartGrace = o.ObserveWindow
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	if o.StopTimeout <= 0 {
		o.StopTimeout = 30 * time.Second
	}
}

// ErrOrchestrationBusy rejects a new upgrade while the journal of a previous
// one is still pending (e.g. the orchestrator died mid-window): the rollback
// target must be resolved by confirm/rollback before anything new is applied.
var ErrOrchestrationBusy = errors.New("an upgrade is pending confirmation; confirm or roll it back before upgrading again")

// RolledBackError reports that the upgrade was automatically rolled back
// after the observation window judged the new version unhealthy; the
// component is running its previous version again.
type RolledBackError struct {
	Component   string
	FromVersion string
	ToVersion   string
	Cause       error
}

func (e *RolledBackError) Error() string {
	return fmt.Sprintf("upgrade of %s to %s was rolled back to %s: %v", e.Component, e.ToVersion, e.FromVersion, e.Cause)
}

func (e *RolledBackError) Unwrap() error { return e.Cause }

// Upgrade runs the full orchestrated upgrade of the component to the package
// in packageDir. It returns nil only when the new version passed the
// observation window and was confirmed. A *RolledBackError means the
// component is healthy on its previous version; any other error leaves the
// state machine in the failed phase with the component stopped.
func (o *Orchestrator) Upgrade(ctx context.Context, packageDir, componentName string, instances []string) error {
	o.withDefaults()
	if o.Upgrader == nil || o.Processes == nil || o.Health == nil {
		return errors.New("orchestrator requires an upgrader, a process manager and a health checker")
	}
	manifest, err := LoadManifest(packageDir, o.Upgrader.Keyring)
	if err != nil {
		return err
	}
	if componentName == "" {
		componentName = manifest.Component
	}
	if manifest.Component != componentName {
		return fmt.Errorf("package is for component %q, not %q", manifest.Component, componentName)
	}
	if _, err := o.Upgrader.pending(componentName); err == nil {
		return ErrOrchestrationBusy
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	current, err := CurrentVersion(o.Upgrader.Root, componentName)
	if err != nil {
		return err
	}
	state := OrchestrationState{Component: componentName, FromVersion: current, ToVersion: manifest.Version}

	o.setPhase(&state, PhaseApplying, "")
	if err := o.Processes.StopService(ctx); err != nil {
		return o.fail(&state, fmt.Errorf("stop component for upgrade: %w", err))
	}
	if _, err := o.Upgrader.Apply(packageDir, componentName, instances); err != nil {
		// Apply only moves pointers at the very end, so the previous version
		// is still active; bring it back up before reporting the failure.
		startErr := o.Processes.StartService(ctx)
		return o.fail(&state, errors.Join(fmt.Errorf("apply package: %w", err), startErr))
	}
	if err := o.Processes.StartService(ctx); err != nil {
		return o.rollback(ctx, &state, instances, fmt.Errorf("start upgraded component: %w", err))
	}

	o.setPhase(&state, PhaseObserving, "")
	if err := o.observe(ctx); err != nil {
		return o.rollback(ctx, &state, instances, err)
	}

	o.setPhase(&state, PhaseConfirming, "")
	if err := o.Upgrader.Confirm(componentName, instances); err != nil {
		return o.rollback(ctx, &state, instances, fmt.Errorf("confirm upgrade: %w", err))
	}
	o.setPhase(&state, PhaseIdle, "")
	return nil
}

// rollback stops the unhealthy new version, switches back (state-format
// guarded), restarts the previous version and observes it. A healthy
// recovery yields a *RolledBackError; anything else ends failed with the
// component stopped.
func (o *Orchestrator) rollback(ctx context.Context, state *OrchestrationState, instances []string, cause error) error {
	o.setPhase(state, PhaseRollingBack, cause.Error())
	if err := o.Processes.StopService(ctx); err != nil {
		return o.fail(state, errors.Join(fmt.Errorf("stop unhealthy component: %w", err), cause))
	}
	if _, err := o.Upgrader.Rollback(state.Component, instances); err != nil {
		return o.fail(state, errors.Join(fmt.Errorf("automatic rollback: %w", err), cause))
	}
	if err := o.Processes.StartService(ctx); err != nil {
		return o.fail(state, errors.Join(fmt.Errorf("restart previous version: %w", err), cause))
	}
	o.setPhase(state, PhaseRecovering, cause.Error())
	if err := o.observe(ctx); err != nil {
		// Fail-closed: neither version is healthy, stop the component rather
		// than leave an unverified process running (or crash-looping under
		// the supervisor's backoff).
		stopErr := o.Processes.StopService(ctx)
		return o.fail(state, errors.Join(fmt.Errorf("previous version did not recover: %w", err), stopErr, cause))
	}
	o.setPhase(state, PhaseIdle, "")
	state.RolledBack = true
	state.Error = cause.Error()
	if err := o.write(state); err != nil {
		return err
	}
	return &RolledBackError{Component: state.Component, FromVersion: state.FromVersion, ToVersion: state.ToVersion, Cause: cause}
}

// observe holds the health gate: within StartGrace the component must become
// alive and ready, and it must stay so until ObserveWindow elapses. A dead
// process fails immediately at any point; a not-ready verdict after the
// grace period fails the window.
func (o *Orchestrator) observe(ctx context.Context) error {
	started := time.Now()
	graceEnd := started.Add(o.StartGrace)
	windowEnd := started.Add(o.ObserveWindow)
	for {
		alive, err := o.Processes.Alive(ctx)
		if err != nil {
			return fmt.Errorf("liveness check: %w", err)
		}
		if !alive {
			return errors.New("component process is not running")
		}
		readyErr := o.Health.Ready(ctx)
		now := time.Now()
		if readyErr != nil && now.After(graceEnd) {
			return fmt.Errorf("component did not become healthy within %s: %v", o.StartGrace, readyErr)
		}
		if !now.Before(windowEnd) {
			if readyErr != nil {
				return fmt.Errorf("component not healthy at the end of the observation window: %v", readyErr)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.PollInterval):
		}
	}
}

func (o *Orchestrator) setPhase(state *OrchestrationState, phase, failure string) {
	state.Phase = phase
	state.Error = failure
	_ = o.write(state)
}

func (o *Orchestrator) fail(state *OrchestrationState, err error) error {
	o.setPhase(state, PhaseFailed, err.Error())
	return err
}

func (o *Orchestrator) write(state *OrchestrationState) error {
	state.Updated = time.Now().UTC()
	return writeJSONAtomic(filepath.Join(componentDir(o.Upgrader.Root, state.Component), orchestrationFileName), state)
}

// Orchestration returns the persisted state machine record of a component, or
// nil when no orchestration ever ran.
func (u *Upgrader) Orchestration(componentName string) (*OrchestrationState, error) {
	var state OrchestrationState
	err := readJSON(filepath.Join(componentDir(u.Root, componentName), orchestrationFileName), &state)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if state.Component != componentName {
		return nil, fmt.Errorf("orchestration state of component %q is corrupt", componentName)
	}
	return &state, nil
}
