package agent

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"tuba/product/internal/component"
)

// fakePM is a controllable ProcessManager for supervision tests.
type fakePM struct {
	mu    sync.Mutex
	alive bool
	log   []string
}

func (f *fakePM) StopService(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive = false
	f.log = append(f.log, "stop")
	return nil
}

func (f *fakePM) StartService(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive = true
	f.log = append(f.log, "start")
	return nil
}

func (f *fakePM) Alive(context.Context) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.alive, nil
}

func (f *fakePM) startCount() int {
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

func (f *fakePM) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

type gateHealth struct{ ready func() bool }

func (h gateHealth) Ready(context.Context) error {
	if h.ready() {
		return nil
	}
	return errNotReady
}

var errNotReady = errString("not ready")

type errString string

func (e errString) Error() string { return string(e) }

// supervisionFixture builds a signed package repo and a supervisor root with
// filebeat 8.19.0 installed and confirmed.
type supervisionFixture struct {
	repoURL     string
	keyringPath string
	root        string
}

func newSupervisionFixture(t *testing.T) supervisionFixture {
	t.Helper()
	public, private, err := component.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	keyringPath := filepath.Join(t.TempDir(), "keyring.json")
	data, _ := json.Marshal(map[string]string{"test-key": hex.EncodeToString(public)})
	if err := os.WriteFile(keyringPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	sign := func(dir, version string) {
		t.Helper()
		if err := component.SignPackage(dir, "filebeat", version, component.StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", private); err != nil {
			t.Fatal(err)
		}
	}
	makePackage := func(dir, version, payload string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(dir, "bin"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "bin", "filebeat"), []byte(payload), 0755); err != nil {
			t.Fatal(err)
		}
		sign(dir, version)
	}

	// v1: installed and confirmed under the supervisor root.
	root := t.TempDir()
	v1 := t.TempDir()
	makePackage(v1, "8.19.0", "v1")
	keyring, err := component.LoadKeyring(keyringPath)
	if err != nil {
		t.Fatal(err)
	}
	upgrader := &component.Upgrader{Root: root, Keyring: keyring}
	if _, err := upgrader.Apply(v1, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", nil); err != nil {
		t.Fatal(err)
	}

	// v2: served by a release repository.
	repo := t.TempDir()
	v2 := filepath.Join(repo, "filebeat", "8.19.1")
	makePackage(v2, "8.19.1", "v2")
	server := httptest.NewServer(http.FileServer(http.Dir(repo)))
	t.Cleanup(server.Close)
	return supervisionFixture{repoURL: server.URL, keyringPath: keyringPath, root: root}
}

func (f supervisionFixture) spec(pm *fakePM, health component.HealthChecker) SupervisedComponent {
	return SupervisedComponent{
		Component:     "filebeat",
		Root:          f.root,
		KeyringPath:   f.keyringPath,
		RepoURL:       f.repoURL,
		Processes:     pm,
		Health:        health,
		ObserveWindow: 300 * time.Millisecond,
		StartGrace:    100 * time.Millisecond,
		PollInterval:  10 * time.Millisecond,
	}
}

func configBody(version int64, configuration string) (body, etag string) {
	etag = `"` + jsonNumber(version) + `"`
	return `{"collector_id":"col_0123456789abcdef0123456789abcdef","version":` + jsonNumber(version) + `,"configuration":` + configuration + `,"created_at":"2026-10-09T00:00:00Z"}`, etag
}

func jsonNumber(v int64) string {
	data, _ := json.Marshal(v)
	return string(data)
}

// waitForHeartbeat polls the captured heartbeats until one matches.
func waitForHeartbeat(t *testing.T, fake *fakeAPI, match func(Heartbeat) bool) Heartbeat {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, beat := range fake.beats() {
			if match(beat) {
				return beat
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no heartbeat matched in time")
	return Heartbeat{}
}

func findComponent(beat Heartbeat, name string) *ComponentStatus {
	for i := range beat.Components {
		if beat.Components[i].Component == name {
			return &beat.Components[i]
		}
	}
	return nil
}

func TestAgentDrivesOrchestratedUpgradeFromDirective(t *testing.T) {
	fixture := newSupervisionFixture(t)
	body1, etag1 := configBody(1, `{"notes":"initial"}`)
	fake := &fakeAPI{heartbeatCode: 204, configStatus: 200, configBody: body1, configETag: etag1}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)

	pm := &fakePM{alive: true}
	supervisor := NewSupervisor(nil)
	if err := supervisor.Supervise(fixture.spec(pm, gateHealth{ready: func() bool { return true }})); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: 50 * time.Millisecond, Supervisor: supervisor}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()

	// Baseline: heartbeats carry the real supervised component state.
	beat := waitForHeartbeat(t, fake, func(b Heartbeat) bool {
		c := findComponent(b, "filebeat")
		return c != nil && c.Version == "8.19.0" && c.State == "running"
	})
	if findComponent(beat, "filebeat").Phase != "" && findComponent(beat, "filebeat").Phase != "idle" {
		t.Fatalf("unexpected phase: %#v", findComponent(beat, "filebeat"))
	}

	// Publish the upgrade directive.
	body2, etag2 := configBody(2, `{"components":{"filebeat":{"desired_version":"8.19.1"}}}`)
	fake.mu.Lock()
	fake.configBody, fake.configETag = body2, etag2
	fake.mu.Unlock()

	// The upgrade completes and the heartbeat reports the confirmed version.
	beat = waitForHeartbeat(t, fake, func(b Heartbeat) bool {
		c := findComponent(b, "filebeat")
		return c != nil && c.Version == "8.19.1" && c.Phase == "idle"
	})
	component_ := findComponent(beat, "filebeat")
	if component_.State != "running" || component_.LastError != "" {
		t.Fatalf("confirmed component must be healthy: %#v", component_)
	}
	current, err := component.CurrentVersion(fixture.root, "filebeat")
	if err != nil || current != "8.19.1" {
		t.Fatalf("current = %q, %v", current, err)
	}
	// The downloaded package was verified and cached under the root.
	if _, err := component.LoadManifest(filepath.Join(fixture.root, "downloads", "filebeat-8.19.1"), mustKeyring(t, fixture.keyringPath)); err != nil {
		t.Fatalf("cached package: %v", err)
	}
}

func mustKeyring(t *testing.T, path string) component.Keyring {
	t.Helper()
	ring, err := component.LoadKeyring(path)
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

func TestAgentReportsAutoRollback(t *testing.T) {
	fixture := newSupervisionFixture(t)
	body1, etag1 := configBody(1, `{}`)
	fake := &fakeAPI{heartbeatCode: 204, configStatus: 200, configBody: body1, configETag: etag1}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)

	pm := &fakePM{alive: true}
	// The new version never becomes healthy; the rolled-back one (second
	// start) does.
	health := gateHealth{ready: func() bool { return pm.startCount() >= 2 }}
	supervisor := NewSupervisor(nil)
	if err := supervisor.Supervise(fixture.spec(pm, health)); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: 50 * time.Millisecond, Supervisor: supervisor}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()

	body2, etag2 := configBody(2, `{"components":{"filebeat":{"desired_version":"8.19.1"}}}`)
	fake.mu.Lock()
	fake.configBody, fake.configETag = body2, etag2
	fake.mu.Unlock()

	// The heartbeat eventually reports the rollback: old version restored,
	// error surfaced, phase back to idle.
	beat := waitForHeartbeat(t, fake, func(b Heartbeat) bool {
		c := findComponent(b, "filebeat")
		return c != nil && c.Version == "8.19.0" && c.Phase == "idle" && c.LastError != ""
	})
	component_ := findComponent(beat, "filebeat")
	if component_.State != "error" {
		t.Fatalf("a rolled-back upgrade must surface an error state: %#v", component_)
	}
	current, _ := component.CurrentVersion(fixture.root, "filebeat")
	if current != "8.19.0" {
		t.Fatalf("current after rollback = %q", current)
	}
}

func TestAgentRefusesUnknownComponentDirective(t *testing.T) {
	fixture := newSupervisionFixture(t)
	body1, etag1 := configBody(1, `{}`)
	fake := &fakeAPI{heartbeatCode: 204, configStatus: 200, configBody: body1, configETag: etag1}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)

	pm := &fakePM{alive: true}
	supervisor := NewSupervisor(nil)
	if err := supervisor.Supervise(fixture.spec(pm, gateHealth{ready: func() bool { return true }})); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: 50 * time.Millisecond, Supervisor: supervisor}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = runner.Run(ctx) }()

	body2, etag2 := configBody(2, `{"components":{"winlogbeat":{"desired_version":"8.19.1"}}}`)
	fake.mu.Lock()
	fake.configBody, fake.configETag = body2, etag2
	fake.mu.Unlock()

	// Fail closed: agent reports the rejected directive, filebeat untouched.
	beat := waitForHeartbeat(t, fake, func(b Heartbeat) bool {
		return b.State == "error" && b.ConfigVersion == 2
	})
	if beat.Diagnostic == "" || findComponent(beat, "filebeat") == nil {
		t.Fatalf("unexpected beat: %#v", beat)
	}
	current, _ := component.CurrentVersion(fixture.root, "filebeat")
	if current != "8.19.0" {
		t.Fatalf("current = %q", current)
	}
	if len(pm.calls()) != 0 {
		t.Fatalf("rejected directive must not touch the process manager: %v", pm.calls())
	}
}

func TestAgentDisabledKeepsComponentsRunning(t *testing.T) {
	fixture := newSupervisionFixture(t)
	body1, etag1 := configBody(1, `{}`)
	fake := &fakeAPI{heartbeatCode: 401, configStatus: 200, configBody: body1, configETag: etag1}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)

	pm := &fakePM{alive: true}
	supervisor := NewSupervisor(nil)
	if err := supervisor.Supervise(fixture.spec(pm, gateHealth{ready: func() bool { return true }})); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: 50 * time.Millisecond, Supervisor: supervisor}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadState()
	if err != nil || !state.Disabled {
		t.Fatalf("state = %+v, %v", state, err)
	}
	// Disabling stops the management loop; collection components keep
	// running (their source write access is revoked broker-side, COL-08).
	if len(pm.calls()) != 0 || !pm.alive {
		t.Fatalf("components must not be stopped on disable: calls=%v alive=%v", pm.calls(), pm.alive)
	}
}
