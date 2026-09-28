package launcher

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
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
