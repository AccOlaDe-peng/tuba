package launcher

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProcessInstanceAliveRejectsReusedPIDIdentity(t *testing.T) {
	pid := os.Getpid()
	identity, err := processIdentity(pid)
	if err != nil {
		t.Fatalf("identify current test process: %v", err)
	}
	if identity == "" {
		t.Fatal("current process identity is empty")
	}
	if !processInstanceAlive(pid, identity) {
		t.Fatal("current process should match its saved identity")
	}
	if processInstanceAlive(pid, identity+"-from-another-boot") {
		t.Fatal("same PID with a different process identity must be treated as stale")
	}
	if processInstanceAlive(pid, "") {
		t.Fatal("legacy PID-only state must not be trusted after a reboot")
	}
}

func TestNewStateStorePersistsRunnerIdentity(t *testing.T) {
	store, err := newStateStore(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	state, err := readState(filepath.Dir(store.path))
	if err != nil {
		t.Fatal(err)
	}
	if state.RunnerPID != os.Getpid() || state.RunnerIdentity == "" || !processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
		t.Fatalf("runner process identity was not persisted correctly: %#v", state)
	}
}
