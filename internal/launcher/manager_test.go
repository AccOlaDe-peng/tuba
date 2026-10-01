package launcher

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLauncherStopHelperProcess(t *testing.T) {
	if os.Getenv("TUBA_LAUNCHER_STOP_HELPER") != "1" {
		return
	}
	stateDir := os.Getenv("TUBA_LAUNCHER_STOP_STATE_DIR")
	store, err := newStateStore(stateDir, nil)
	if err != nil {
		os.Exit(81)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(stateDir, stopFileName)); err == nil {
			store.stop()
			os.Exit(0)
		}
		time.Sleep(10 * time.Millisecond)
	}
	os.Exit(82)
}

func TestStopRequestsManagedRunnerAndWaitsForExit(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(root, "tuba-services.json")
	manifest := map[string]any{
		"version": 1, "state_dir": "state", "log_dir": "logs",
		"services": []any{map[string]any{"name": "controlled-helper"}},
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(root, "state")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLauncherStopHelperProcess$")
	cmd.Env = append(os.Environ(), "TUBA_LAUNCHER_STOP_HELPER=1", "TUBA_LAUNCHER_STOP_STATE_DIR="+stateDir)
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		if cmd.Process != nil && processAlive(cmd.Process.Pid) {
			_ = cmd.Process.Kill()
			<-done
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, readErr := readState(stateDir)
		if readErr == nil && state.RunnerPID == cmd.Process.Pid {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("stop helper exited before publishing its managed state: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	state, err := readState(stateDir)
	if err != nil || state.RunnerPID != cmd.Process.Pid {
		t.Fatalf("helper did not publish managed runner state: state=%+v err=%v", state, err)
	}
	if err := Stop(manifestPath, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stop helper exited unsuccessfully: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop returned before its managed runner exited")
	}
	state, err = readState(stateDir)
	if err != nil || state.RunnerPID != 0 || state.RunnerIdentity != "" {
		t.Fatalf("stopped runner state was not persisted: state=%+v err=%v", state, err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, stopFileName)); !os.IsNotExist(err) {
		t.Fatalf("stop request was not removed after successful shutdown: %v", err)
	}
}

func TestLauncherHelperProcess(t *testing.T) {
	if os.Getenv("TUBA_LAUNCHER_HELPER_PROCESS") != "1" {
		return
	}
	os.Exit(7)
}

func TestLauncherBlockingHelperProcess(t *testing.T) {
	if os.Getenv("TUBA_LAUNCHER_BLOCKING_HELPER") != "1" {
		return
	}
	ctx, stop := signalNotifyContext()
	defer stop()
	<-ctx.Done()
	os.Exit(0)
}

func writeBlockingManifest(t *testing.T, root string, names ...string) string {
	t.Helper()
	services := make([]any, 0, len(names))
	for _, name := range names {
		services = append(services, map[string]any{
			"name":        name,
			"command":     os.Args[0],
			"args":        []string{"-test.run=^TestLauncherBlockingHelperProcess$"},
			"environment": map[string]string{"TUBA_LAUNCHER_BLOCKING_HELPER": "1"},
			"restart_min": "100ms",
			"restart_max": "300ms",
		})
	}
	manifest := map[string]any{"version": 1, "state_dir": "state", "log_dir": "logs", "services": services}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "tuba-services.json")
	if err := os.WriteFile(manifestPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

func waitForServiceState(t *testing.T, stateDir, name string, timeout time.Duration, match func(ServiceStatus) bool) ServiceStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state, err := readState(stateDir)
		if err == nil {
			if status, ok := state.Services[name]; ok && match(status) {
				return status
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	state, err := readState(stateDir)
	t.Fatalf("service %q did not reach the expected state: state=%+v err=%v", name, state, err)
	return ServiceStatus{}
}

func TestSingleServiceControlLifecycle(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha", "bravo")
	stateDir := filepath.Join(root, "state")
	done := make(chan error, 1)
	go func() { done <- Run(manifestPath) }()
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(stateDir, stopFileName), []byte("stop"), 0600)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("supervisor did not stop during cleanup")
		}
	})

	alpha := waitForServiceState(t, stateDir, "alpha", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" && s.PID > 0 })
	bravo := waitForServiceState(t, stateDir, "bravo", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" && s.PID > 0 })

	// Stop only alpha: it must stop, stay stopped, and bravo must not move.
	if err := StopServices(manifestPath, []string{"alpha"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if processAlive(alpha.PID) {
		t.Fatalf("stopped service alpha is still alive with pid %d", alpha.PID)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, err := readState(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		if got := state.Services["alpha"]; got.State != "stopped" {
			t.Fatalf("stopped service alpha was restarted: %+v", got)
		}
		if got := state.Services["bravo"]; got.State != "running" || got.PID != bravo.PID || got.Restarts != 0 {
			t.Fatalf("untargeted service bravo was disturbed: %+v", got)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Restart only bravo: new PID for bravo, alpha stays stopped, no crash budget consumed.
	if err := RestartServices(manifestPath, []string{"bravo"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	state, err := readState(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.Services["bravo"]; got.State != "running" || got.PID == bravo.PID || got.Restarts != 0 {
		t.Fatalf("bravo was not restarted cleanly: %+v (old pid %d)", got, bravo.PID)
	}
	if got := state.Services["alpha"]; got.State != "stopped" {
		t.Fatalf("restart of bravo disturbed stopped alpha: %+v", got)
	}

	// Restart of a stopped service starts it.
	if err := RestartServices(manifestPath, []string{"alpha"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	alphaRestarted := waitForServiceState(t, stateDir, "alpha", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" && s.PID > 0 })
	if alphaRestarted.PID == alpha.PID {
		t.Fatal("alpha restarted with its old pid")
	}

	// Stop then start again; the service returns with a fresh PID.
	if err := StopServices(manifestPath, []string{"alpha"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := StartServices(manifestPath, []string{"alpha"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	started := waitForServiceState(t, stateDir, "alpha", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" && s.PID > 0 })
	if started.PID == alphaRestarted.PID {
		t.Fatal("alpha started with its previous pid")
	}
}

func TestStoppedServiceSurvivesSupervisorRestart(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha", "bravo")
	stateDir := filepath.Join(root, "state")

	done := make(chan error, 1)
	go func() { done <- Run(manifestPath) }()
	waitForServiceState(t, stateDir, "alpha", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" })
	waitForServiceState(t, stateDir, "bravo", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" })
	if err := StopServices(manifestPath, []string{"alpha"}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, stopFileName), []byte("stop"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first supervisor did not stop")
	}
	if _, err := os.Stat(stopMarkerPath(stateDir, "alpha")); err != nil {
		t.Fatalf("stop marker must survive the supervisor: %v", err)
	}

	done = make(chan error, 1)
	go func() { done <- Run(manifestPath) }()
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(stateDir, stopFileName), []byte("stop"), 0600)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("second supervisor did not stop during cleanup")
		}
	})
	waitForServiceState(t, stateDir, "bravo", 5*time.Second, func(s ServiceStatus) bool { return s.State == "running" && s.PID > 0 })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		state, err := readState(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		if got := state.Services["alpha"]; got.State != "stopped" {
			t.Fatalf("stopped service was started by the new supervisor: %+v", got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSingleServiceControlRefusesLegacySupervisor(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha")
	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	identity, err := processIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	legacy := State{
		RunnerPID:      os.Getpid(),
		RunnerIdentity: identity,
		Started:        time.Now().UTC(),
		Services:       map[string]ServiceStatus{"alpha": {State: "running", PID: os.Getpid()}},
	}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, stateFileName), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		run  func() error
	}{
		{"stop", func() error { return StopServices(manifestPath, []string{"alpha"}, time.Second) }},
		{"start", func() error { return StartServices(manifestPath, []string{"alpha"}, time.Second) }},
		{"restart", func() error { return RestartServices(manifestPath, []string{"alpha"}, time.Second) }},
	}
	for _, operation := range operations {
		if err := operation.run(); err == nil || !strings.Contains(err.Error(), "does not support per-service control") {
			t.Fatalf("%s against a legacy supervisor must fail closed, got %v", operation.name, err)
		}
	}
	if _, err := os.Stat(stopMarkerPath(stateDir, "alpha")); !os.IsNotExist(err) {
		t.Fatalf("refused operation must not leave a stop marker behind: %v", err)
	}
}

func TestSingleServiceControlValidatesNames(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha")
	if err := StopServices(manifestPath, []string{"missing"}, time.Second); err == nil {
		t.Fatal("stop of an unknown service must fail")
	}
	if err := StartServices(manifestPath, []string{"missing"}, time.Second); err == nil {
		t.Fatal("start of an unknown service must fail")
	}
	if err := RestartServices(manifestPath, []string{"missing"}, time.Second); err == nil {
		t.Fatal("restart of an unknown service must fail")
	}
	if err := Status(manifestPath, []string{"missing"}); err == nil {
		t.Fatal("status of an unknown service must fail")
	}
}

func TestStatusFiltersServices(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha", "bravo")
	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := newStateStore(stateDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.setService("alpha", ServiceStatus{State: "running", PID: 111, LastChanged: time.Now().UTC()})
	store.setService("bravo", ServiceStatus{State: "stopped", LastChanged: time.Now().UTC()})

	var filtered bytes.Buffer
	if err := statusTo(&filtered, manifestPath, []string{"bravo"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(filtered.String(), "bravo") || strings.Contains(filtered.String(), "alpha") {
		t.Fatalf("filtered status must list only bravo: %q", filtered.String())
	}

	var full bytes.Buffer
	if err := statusTo(&full, manifestPath, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(full.String(), "alpha") || !strings.Contains(full.String(), "bravo") {
		t.Fatalf("unfiltered status must list every service: %q", full.String())
	}
}

func TestStatusFailsOnStaleState(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha", "bravo")
	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	writeState := func(t *testing.T, state State) {
		t.Helper()
		encoded, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stateDir, stateFileName), encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
	staleServices := map[string]ServiceStatus{
		"alpha": {State: "running", PID: 111, LastChanged: time.Now().UTC()},
		"bravo": {State: "running", PID: 222, LastChanged: time.Now().UTC()},
	}

	t.Run("state from another boot or reused pid", func(t *testing.T) {
		identity, err := processIdentity(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}
		writeState(t, State{
			RunnerPID:      os.Getpid(),
			RunnerIdentity: identity + "-from-another-boot",
			Started:        time.Now().UTC(),
			Services:       staleServices,
		})
		var output bytes.Buffer
		err = statusTo(&output, manifestPath, nil)
		if err == nil {
			t.Fatalf("status against a mismatched runner identity must fail, output=%q", output.String())
		}
		if !strings.Contains(output.String(), "no live supervisor") {
			t.Fatalf("status must report the missing supervisor, output=%q", output.String())
		}
		if strings.Contains(output.String(), "alpha") || strings.Contains(output.String(), "bravo") {
			t.Fatalf("stale per-service rows must not be printed, output=%q", output.String())
		}
	})

	t.Run("dead supervisor pid", func(t *testing.T) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLauncherHelperProcess$")
		cmd.Env = append(os.Environ(), "TUBA_LAUNCHER_HELPER_PROCESS=1")
		if err := cmd.Run(); err == nil {
			t.Fatal("helper process should have exited with a failure code")
		}
		deadPID := cmd.Process.Pid
		if processAlive(deadPID) {
			t.Fatalf("helper pid %d is still alive; cannot simulate a dead supervisor", deadPID)
		}
		writeState(t, State{
			RunnerPID:      deadPID,
			RunnerIdentity: "whatever-identity-the-dead-runner-had",
			Started:        time.Now().UTC(),
			Services:       staleServices,
		})
		var output bytes.Buffer
		err := statusTo(&output, manifestPath, nil)
		if err == nil {
			t.Fatalf("status against a dead supervisor pid must fail, output=%q", output.String())
		}
		if !strings.Contains(output.String(), "no live supervisor") {
			t.Fatalf("status must report the missing supervisor, output=%q", output.String())
		}
		if strings.Contains(output.String(), "alpha") || strings.Contains(output.String(), "bravo") {
			t.Fatalf("stale per-service rows must not be printed, output=%q", output.String())
		}
	})
}

func TestStatusReportsLiveSupervisor(t *testing.T) {
	root := t.TempDir()
	manifestPath := writeBlockingManifest(t, root, "alpha")
	stateDir := filepath.Join(root, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := newStateStore(stateDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.setService("alpha", ServiceStatus{State: "running", PID: 111, LastChanged: time.Now().UTC()})
	var output bytes.Buffer
	if err := statusTo(&output, manifestPath, nil); err != nil {
		t.Fatalf("status against a live supervisor must succeed: %v", err)
	}
	if !strings.Contains(output.String(), "TUBA launcher pid=") || !strings.Contains(output.String(), "alpha") {
		t.Fatalf("status must report the live supervisor and its services, output=%q", output.String())
	}
}

func TestSupervisorRestartsExitedServiceAndStops(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(root, "tuba-services.json")
	manifest := map[string]any{
		"version":   1,
		"state_dir": "state",
		"log_dir":   "logs",
		"services": []any{map[string]any{
			"name":        "unstable",
			"command":     os.Args[0],
			"args":        []string{"-test.run=^TestLauncherHelperProcess$"},
			"environment": map[string]string{"TUBA_LAUNCHER_HELPER_PROCESS": "1"},
			"restart_min": "100ms",
			"restart_max": "300ms",
		}},
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- Run(manifestPath) }()
	stateDir := filepath.Join(root, "state")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state, readErr := readState(stateDir)
		if readErr == nil && state.Services[loaded.Services[0].Name].Restarts >= 1 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("supervisor exited before restart verification: %v", err)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	state, err := readState(stateDir)
	if err != nil || state.Services[loaded.Services[0].Name].Restarts < 1 {
		t.Fatalf("service was not restarted: state=%+v err=%v", state, err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, stopFileName), []byte("stop"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not stop after stop request")
	}
	state, err = readState(stateDir)
	if err != nil || state.RunnerPID != 0 || state.Services[loaded.Services[0].Name].State != "stopped" {
		t.Fatalf("supervisor did not record stopped state: state=%+v err=%v", state, err)
	}
}
