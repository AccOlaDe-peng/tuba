//go:build windows

package launcher

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"testing"
	"time"
)

func TestLauncherGracefulStopHelper(t *testing.T) {
	if os.Getenv("TUBA_LAUNCHER_GRACEFUL_HELPER") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	readyPath := os.Getenv("TUBA_LAUNCHER_GRACEFUL_READY")
	stoppedPath := os.Getenv("TUBA_LAUNCHER_GRACEFUL_STOPPED")
	if err := os.WriteFile(readyPath, []byte("ready"), 0600); err != nil {
		os.Exit(91)
	}
	<-ctx.Done()
	if err := os.WriteFile(stoppedPath, []byte("stopped"), 0600); err != nil {
		os.Exit(92)
	}
}

func TestWindowsChildGracefulStopSendsCtrlBreak(t *testing.T) {
	directory := t.TempDir()
	readyPath := filepath.Join(directory, "ready")
	stoppedPath := filepath.Join(directory, "stopped")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLauncherGracefulStopHelper$")
	cmd.Env = append(os.Environ(),
		"TUBA_LAUNCHER_GRACEFUL_HELPER=1",
		"TUBA_LAUNCHER_GRACEFUL_READY="+readyPath,
		"TUBA_LAUNCHER_GRACEFUL_STOPPED="+stoppedPath,
	)
	configureChild(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitForFile := func(path string) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(path); err == nil {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}
	if !waitForFile(readyPath) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("child did not initialize its interrupt handler")
	}
	if err := terminateChild(cmd); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("send Ctrl-Break to child: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("child did not exit cleanly: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("child did not exit after Ctrl-Break")
	}
	if _, err := os.Stat(stoppedPath); err != nil {
		t.Fatalf("child did not run graceful shutdown handler: %v", err)
	}
}

func TestWindowsSupervisorGracefullyStopsManagedChild(t *testing.T) {
	root := t.TempDir()
	readyPath := filepath.Join(root, "ready")
	stoppedPath := filepath.Join(root, "stopped")
	manifestPath := filepath.Join(root, "tuba-services.json")
	manifest := map[string]any{
		"version": 1, "state_dir": "state", "log_dir": "logs",
		"services": []any{map[string]any{
			"name": "graceful-helper", "command": os.Args[0],
			"args": []string{"-test.run=^TestLauncherGracefulStopHelper$"},
			"environment": map[string]string{
				"TUBA_LAUNCHER_GRACEFUL_HELPER":  "1",
				"TUBA_LAUNCHER_GRACEFUL_READY":   readyPath,
				"TUBA_LAUNCHER_GRACEFUL_STOPPED": stoppedPath,
			},
		}},
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- Run(manifestPath) }()
	doneReceived := false
	requestStop := func() {
		t.Helper()
		_ = os.WriteFile(filepath.Join(root, "state", stopFileName), []byte("stop"), 0600)
	}
	t.Cleanup(func() {
		if doneReceived {
			return
		}
		requestStop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			state, stateErr := readState(filepath.Join(root, "state"))
			logBytes, _ := os.ReadFile(filepath.Join(root, "logs", "graceful-helper.log"))
			t.Errorf("supervisor did not finish during cleanup: state=%+v state_error=%v child_log=%s", state, stateErr, logBytes)
		}
	})
	waitForFile := func(path string) bool {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(path); err == nil {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return false
	}
	if !waitForFile(readyPath) {
		t.Fatal("supervised child did not become ready")
	}
	requestStop()
	select {
	case err := <-done:
		doneReceived = true
		if err != nil {
			t.Fatalf("supervisor returned an error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop after internal stop request")
	}
	if !waitForFile(stoppedPath) {
		t.Fatal("supervised child did not run its graceful shutdown handler")
	}
	state, err := readState(filepath.Join(root, "state"))
	if err != nil || state.RunnerPID != 0 || state.Services["graceful-helper"].State != "stopped" {
		t.Fatalf("supervisor did not persist stopped state: state=%+v err=%v", state, err)
	}
}
