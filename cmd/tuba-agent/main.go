// tuba-agent is the TUBA Management Agent CLI. It enrolls a host against the
// management API with a one-time token, then runs the 30-second heartbeat and
// ETag configuration poll loop, keeping the last effective configuration
// locally so collection continues while the management plane is offline.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"tuba/product/internal/agent"
	"tuba/product/internal/lifecycle"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: tuba-agent <enroll|run|status> [flags]")
	}
	switch args[0] {
	case "enroll":
		return enroll(args[1:])
	case "run":
		return supervise(args[1:])
	case "status":
		return status(args[1:])
	default:
		return fmt.Errorf("unknown command %q: usage: tuba-agent <enroll|run|status> [flags]", args[0])
	}
}

func agentVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				revision := setting.Value
				if len(revision) > 12 {
					revision = revision[:12]
				}
				return revision
			}
		}
	}
	return "dev"
}

func enroll(args []string) error {
	fs := flag.NewFlagSet("enroll", flag.ContinueOnError)
	apiURL := fs.String("api-url", os.Getenv("TUBA_AGENT_API_URL"), "management API base URL (or TUBA_AGENT_API_URL)")
	stateDir := fs.String("state-dir", "", "agent state directory (credential and last effective config)")
	token := fs.String("enrollment-token", os.Getenv("TUBA_ENROLLMENT_TOKEN"), "one-time enrollment token (or TUBA_ENROLLMENT_TOKEN)")
	installID := fs.String("install-id", "", "stable installation identifier")
	hostname := fs.String("hostname", "", "reported hostname (defaults to os.Hostname)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || *token == "" || *installID == "" {
		return fmt.Errorf("enroll requires --state-dir, --enrollment-token and --install-id")
	}
	if *hostname == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("resolve hostname: %w", err)
		}
		*hostname = host
	}
	client := &agent.Client{BaseURL: *apiURL}
	registered, err := client.Enroll(context.Background(), *token, agent.Registration{
		InstallID:    *installID,
		Hostname:     *hostname,
		OS:           runtime.GOOS,
		Architecture: runtime.GOARCH,
		Version:      agentVersion(),
	})
	if err != nil {
		return err
	}
	store := agent.Store{Dir: *stateDir}
	if err := store.SaveState(agent.State{CollectorID: registered.CollectorID, Credential: registered.Credential, EnrolledAt: time.Now().UTC()}); err != nil {
		return fmt.Errorf("persist agent identity: %w", err)
	}
	log.Printf("enrolled as %s (heartbeat interval %ds); identity stored in %s", registered.CollectorID, registered.HeartbeatIntervalSeconds, *stateDir)
	return nil
}

func supervise(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	apiURL := fs.String("api-url", os.Getenv("TUBA_AGENT_API_URL"), "management API base URL (or TUBA_AGENT_API_URL)")
	stateDir := fs.String("state-dir", "", "agent state directory")
	interval := fs.Duration("heartbeat-interval", agent.DefaultHeartbeatInterval, "heartbeat/config poll interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" {
		return fmt.Errorf("run requires --state-dir")
	}
	runner := &agent.Runner{
		Client:   &agent.Client{BaseURL: *apiURL},
		Store:    agent.Store{Dir: *stateDir},
		Version:  agentVersion(),
		Interval: *interval,
	}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	log.Printf("management agent %s starting (heartbeat every %s)", agentVersion(), *interval)
	return runner.Run(ctx)
}

func status(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	stateDir := fs.String("state-dir", "", "agent state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" {
		return fmt.Errorf("status requires --state-dir")
	}
	store := agent.Store{Dir: *stateDir}
	state, err := store.LoadState()
	if err != nil {
		return fmt.Errorf("agent is not enrolled in %s: %w", *stateDir, err)
	}
	fmt.Printf("collector_id=%s enrolled_at=%s disabled=%t\n", state.CollectorID, state.EnrolledAt.Format(time.RFC3339), state.Disabled)
	cfg, err := store.LoadConfig()
	if err != nil {
		fmt.Println("effective_config=none")
		return nil
	}
	fmt.Printf("effective_config_version=%d applied_at=%s\n", cfg.Version, cfg.AppliedAt.Format(time.RFC3339))
	return nil
}
