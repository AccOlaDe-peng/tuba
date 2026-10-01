package component

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeProcesses struct {
	mu    sync.Mutex
	alive bool
	log   []string
}

func (f *fakeProcesses) StopService(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive = false
	f.log = append(f.log, "stop")
	return nil
}

func (f *fakeProcesses) StartService(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive = true
	f.log = append(f.log, "start")
	return nil
}

func (f *fakeProcesses) Alive(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive, nil
}

// setAlive simulates a crash or recovery without going through stop/start.
func (f *fakeProcesses) setAlive(alive bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive = alive
}

func (f *fakeProcesses) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

func (f *fakeProcesses) startCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, entry := range f.log {
		if entry == "start" {
			count++
		}
	}
	return count
}

// scriptedHealth is ready once the supplied gate function says so.
type scriptedHealth struct {
	gate func() bool
}

func (h *scriptedHealth) Ready(context.Context) error {
	if h.gate() {
		return nil
	}
	return errors.New("not ready")
}

// crashOnFirstCheck simulates a process crash in the middle of the
// observation window: the first probe kills the component; the rollback's
// stop/start bookkeeping then restores aliveness.
type crashOnFirstCheck struct {
	pm   *fakeProcesses
	mu   sync.Mutex
	done bool
}

func (h *crashOnFirstCheck) Ready(context.Context) error {
	h.mu.Lock()
	if !h.done {
		h.done = true
		h.pm.setAlive(false)
	}
	h.mu.Unlock()
	return nil
}

func testOrchestrator(root string, ring Keyring, pm *fakeProcesses, health HealthChecker) *Orchestrator {
	return &Orchestrator{
		Upgrader:      &Upgrader{Root: root, Keyring: ring},
		Processes:     pm,
		Health:        health,
		ObserveWindow: 200 * time.Millisecond,
		StartGrace:    60 * time.Millisecond,
		PollInterval:  10 * time.Millisecond,
		StopTimeout:   time.Second,
	}
}

func TestOrchestratedUpgradeHappyPath(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	instances := []string{"src-a"}
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}

	pm := &fakeProcesses{alive: true}
	orch := testOrchestrator(root, ring, pm, &scriptedHealth{gate: func() bool { return true }})
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	if err := orch.Upgrade(context.Background(), v2, "", instances); err != nil {
		t.Fatal(err)
	}
	current, _ := CurrentVersion(root, "filebeat")
	if current != "8.19.1" {
		t.Fatalf("current = %q", current)
	}
	// Journal resolved and state machine back to idle.
	if _, err := upgrader.pending("filebeat"); err == nil {
		t.Fatal("pending journal must be resolved after confirm")
	}
	state, err := upgrader.Orchestration("filebeat")
	if err != nil || state == nil {
		t.Fatalf("orchestration state: %v", err)
	}
	if state.Phase != PhaseIdle || state.RolledBack || state.Error != "" {
		t.Fatalf("unexpected orchestration state: %#v", state)
	}
	if got := strings.Join(pm.calls(), ","); got != "stop,start" {
		t.Fatalf("process calls = %s", got)
	}
	if !pm.alive {
		t.Fatal("component must be running after a confirmed upgrade")
	}
}

func TestOrchestratedUpgradeRollsBackOnUnhealthy(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	instances := []string{"src-a"}
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}

	pm := &fakeProcesses{alive: true}
	// The new version never becomes ready; the rolled-back previous version
	// (the orchestrator's second start) does.
	health := &scriptedHealth{gate: func() bool { return pm.startCount() >= 2 }}
	orch := testOrchestrator(root, ring, pm, health)
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	err := orch.Upgrade(context.Background(), v2, "", instances)
	var rolledBack *RolledBackError
	if !errors.As(err, &rolledBack) {
		t.Fatalf("expected RolledBackError, got %v", err)
	}
	current, _ := CurrentVersion(root, "filebeat")
	if current != "8.19.0" {
		t.Fatalf("current after rollback = %q", current)
	}
	state, err := upgrader.Orchestration("filebeat")
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != PhaseIdle || !state.RolledBack {
		t.Fatalf("unexpected orchestration state: %#v", state)
	}
	if got := strings.Join(pm.calls(), ","); got != "stop,start,stop,start" {
		t.Fatalf("process calls = %s", got)
	}
	if !pm.alive {
		t.Fatal("previous version must be running after a recovered rollback")
	}
}

func TestOrchestratedUpgradeFailsClosedWhenRecoveryUnhealthy(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	instances := []string{"src-a"}
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}

	pm := &fakeProcesses{alive: true}
	// Neither the new nor the rolled-back version ever becomes healthy.
	orch := testOrchestrator(root, ring, pm, &scriptedHealth{gate: func() bool { return false }})
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	err := orch.Upgrade(context.Background(), v2, "", instances)
	if err == nil {
		t.Fatal("upgrade with unrecoverable health must fail")
	}
	var rolledBack *RolledBackError
	if errors.As(err, &rolledBack) {
		t.Fatal("an unrecovered rollback must not be reported as rolled back")
	}
	state, serr := upgrader.Orchestration("filebeat")
	if serr != nil {
		t.Fatal(serr)
	}
	if state.Phase != PhaseFailed || state.Error == "" {
		t.Fatalf("unexpected orchestration state: %#v", state)
	}
	// Fail-closed: the component is stopped and nothing keeps oscillating —
	// exactly one stop/start cycle for the upgrade plus one for the rollback.
	if got := strings.Join(pm.calls(), ","); got != "stop,start,stop,start,stop" {
		t.Fatalf("process calls = %s", got)
	}
	if pm.alive {
		t.Fatal("component must stay stopped in the failed phase")
	}
}

func TestOrchestratedUpgradeRollsBackOnProcessCrash(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	instances := []string{"src-a"}
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}

	pm := &fakeProcesses{alive: true}
	orch := testOrchestrator(root, ring, pm, &crashOnFirstCheck{pm: pm})
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	err := orch.Upgrade(context.Background(), v2, "", instances)
	var rolledBack *RolledBackError
	if !errors.As(err, &rolledBack) {
		t.Fatalf("expected RolledBackError after mid-window crash, got %v", err)
	}
	current, _ := CurrentVersion(root, "filebeat")
	if current != "8.19.0" {
		t.Fatalf("current after crash rollback = %q", current)
	}
}

func TestOrchestratedUpgradeRefusesPendingJournal(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", nil); err != nil {
		t.Fatal(err)
	}
	orch := testOrchestrator(root, ring, &fakeProcesses{}, &scriptedHealth{gate: func() bool { return true }})
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	if err := orch.Upgrade(context.Background(), v2, "", nil); !errors.Is(err, ErrOrchestrationBusy) {
		t.Fatalf("expected ErrOrchestrationBusy, got %v", err)
	}
}

func TestHTTPHealthChecker(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer bad.Close()

	checker := &HTTPHealthChecker{URL: ok.URL}
	if err := checker.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	checker = &HTTPHealthChecker{URL: bad.URL}
	if err := checker.Ready(context.Background()); err == nil {
		t.Fatal("503 readiness must fail the health check")
	}
}

func TestStatusIncludesOrchestration(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	instances := []string{"src-a"}
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}
	pm := &fakeProcesses{alive: true}
	orch := testOrchestrator(root, ring, pm, &scriptedHealth{gate: func() bool { return true }})
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	if err := orch.Upgrade(context.Background(), v2, "", instances); err != nil {
		t.Fatal(err)
	}
	status, err := upgrader.Status("filebeat")
	if err != nil {
		t.Fatal(err)
	}
	if status.Orchestration == nil || status.Orchestration.Phase != PhaseIdle || status.Orchestration.ToVersion != "8.19.1" {
		t.Fatalf("status must expose the orchestration state machine: %#v", status.Orchestration)
	}
}
