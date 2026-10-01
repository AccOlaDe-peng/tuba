package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

type fakeAPI struct {
	mu             sync.Mutex
	enrollStatus   int
	enrollBody     string
	heartbeatCalls []Heartbeat
	heartbeatCode  int
	configStatus   int
	configBody     string
	configETag     string
	etagsSeen      []string
	configCalls    int
}

func (f *fakeAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/collector/enroll", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.enrollStatus)
		_, _ = io.WriteString(w, f.enrollBody)
	})
	mux.HandleFunc("/api/v1/collector/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var beat Heartbeat
		_ = json.NewDecoder(r.Body).Decode(&beat)
		f.mu.Lock()
		f.heartbeatCalls = append(f.heartbeatCalls, beat)
		code := f.heartbeatCode
		f.mu.Unlock()
		w.WriteHeader(code)
	})
	mux.HandleFunc("/api/v1/collector/config", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.configCalls++
		f.etagsSeen = append(f.etagsSeen, r.Header.Get("If-None-Match"))
		status, body, etag := f.configStatus, f.configBody, f.configETag
		f.mu.Unlock()
		if status == http.StatusOK && etag != "" && r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	return mux
}

func (f *fakeAPI) beats() []Heartbeat {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Heartbeat(nil), f.heartbeatCalls...)
}

func newServer(t *testing.T, f *fakeAPI) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(f.handler())
	t.Cleanup(server.Close)
	return server
}

func TestEnrollStoresCredentialAndState(t *testing.T) {
	fake := &fakeAPI{enrollStatus: 201, enrollBody: `{"collector_id":"col_0123456789abcdef0123456789abcdef","collector_credential":"tuba_col_abc","heartbeat_interval_seconds":30}`}
	client := &Client{BaseURL: newServer(t, fake).URL}
	registered, err := client.Enroll(context.Background(), "tuba_enroll_xyz", Registration{
		InstallID: "install-1", Hostname: "host-1", OS: "linux", Architecture: "amd64", Version: "1.0.0",
	})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if registered.CollectorID != "col_0123456789abcdef0123456789abcdef" || registered.Credential != "tuba_col_abc" {
		t.Fatalf("unexpected identity %+v", registered)
	}
	store := Store{Dir: t.TempDir()}
	if err := store.SaveState(State{CollectorID: registered.CollectorID, Credential: registered.Credential, EnrolledAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	loaded, err := store.LoadState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if loaded.Credential != "tuba_col_abc" || loaded.Disabled {
		t.Fatalf("unexpected loaded state %+v", loaded)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(store.Dir, "agent-state.json"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("state file mode = %o, want 0600", info.Mode().Perm())
		}
	}
}

func TestEnrollUnauthorized(t *testing.T) {
	fake := &fakeAPI{enrollStatus: 401, enrollBody: `enrollment token is invalid`}
	client := &Client{BaseURL: newServer(t, fake).URL}
	_, err := client.Enroll(context.Background(), "tuba_enroll_bad", Registration{
		InstallID: "install-1", Hostname: "host-1", OS: "linux", Architecture: "amd64", Version: "1.0.0",
	})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}

func enrollState(t *testing.T, store Store) {
	t.Helper()
	if err := store.SaveState(State{CollectorID: "col_0123456789abcdef0123456789abcdef", Credential: "tuba_col_abc", EnrolledAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save state: %v", err)
	}
}

func runRunnerBriefly(t *testing.T, runner *Runner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunnerAppliesNewConfigAndReportsVersion(t *testing.T) {
	fake := &fakeAPI{
		heartbeatCode: 204,
		configStatus:  200,
		configBody:    `{"collector_id":"col_0123456789abcdef0123456789abcdef","version":7,"configuration":{"sources":[{"source_id":"src-1"}]},"created_at":"2026-10-02T00:00:00Z"}`,
		configETag:    `"7"`,
	}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Version: "1.2.3", Interval: 10 * time.Millisecond}
	runRunnerBriefly(t, runner)

	stored, err := store.LoadConfig()
	if err != nil {
		t.Fatalf("config was not persisted: %v", err)
	}
	if stored.Version != 7 {
		t.Fatalf("stored version = %d, want 7", stored.Version)
	}
	beats := fake.beats()
	if len(beats) < 2 {
		t.Fatalf("heartbeats = %d, want at least 2", len(beats))
	}
	for i, beat := range beats {
		if beat.ConfigVersion != 7 {
			t.Fatalf("heartbeat %d config_version = %d, want 7", i, beat.ConfigVersion)
		}
		if beat.State != "running" || beat.Version != "1.2.3" {
			t.Fatalf("heartbeat %d = %+v", i, beat)
		}
	}
	// The second poll must carry the applied version as an ETag and get 304.
	fake.mu.Lock()
	etags := append([]string(nil), fake.etagsSeen...)
	fake.mu.Unlock()
	if len(etags) < 2 || etags[0] != "" || etags[1] != `"7"` {
		t.Fatalf("etags seen = %v, want [\"\" \"\\\"7\\\"\"]", etags)
	}
}

func TestRunnerKeepsLastGoodConfigOffline(t *testing.T) {
	fake := &fakeAPI{heartbeatCode: 204, configStatus: 503, configBody: `unavailable`}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)
	if err := store.SaveConfig(StoredConfig{Version: 3, Configuration: json.RawMessage(`{"sources":[]}`), AppliedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: 10 * time.Millisecond}
	runRunnerBriefly(t, runner)

	beats := fake.beats()
	if len(beats) == 0 {
		t.Fatal("no heartbeats recorded")
	}
	for i, beat := range beats {
		if beat.ConfigVersion != 3 {
			t.Fatalf("heartbeat %d config_version = %d, want the last effective version 3", i, beat.ConfigVersion)
		}
		if beat.Diagnostic == "" {
			t.Fatalf("heartbeat %d must carry an offline diagnostic", i)
		}
	}
	stored, err := store.LoadConfig()
	if err != nil || stored.Version != 3 {
		t.Fatalf("last effective config must be retained offline, got %+v err=%v", stored, err)
	}
}

func TestRunnerStopsWhenDisabled(t *testing.T) {
	fake := &fakeAPI{heartbeatCode: 401, configStatus: 401}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("runner did not stop after disable")
	}
	state, err := store.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	if !state.Disabled {
		t.Fatal("disabled state was not persisted")
	}
	if _, err := store.LoadConfig(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled agent must not apply new config, err=%v", err)
	}
}

func TestRunnerRejectsForbiddenConfig(t *testing.T) {
	fake := &fakeAPI{
		heartbeatCode: 204,
		configStatus:  200,
		configBody:    `{"collector_id":"col_0123456789abcdef0123456789abcdef","version":4,"configuration":{"setup":{"command":"rm -rf /"}},"created_at":"2026-10-02T00:00:00Z"}`,
	}
	store := Store{Dir: t.TempDir()}
	enrollState(t, store)
	runner := &Runner{Client: &Client{BaseURL: newServer(t, fake).URL}, Store: store, Interval: 10 * time.Millisecond}
	runRunnerBriefly(t, runner)

	if _, err := store.LoadConfig(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("forbidden config must not be persisted, err=%v", err)
	}
	beats := fake.beats()
	if len(beats) == 0 {
		t.Fatal("no heartbeats recorded")
	}
	for i, beat := range beats {
		if beat.State != "error" || beat.ConfigVersion != 0 {
			t.Fatalf("heartbeat %d = %+v, want state=error with no applied version", i, beat)
		}
	}
}

func TestFetchConfigValidation(t *testing.T) {
	fake := &fakeAPI{configStatus: 200, configBody: `{"collector_id":"col_x","version":0,"configuration":{}}`}
	client := &Client{BaseURL: newServer(t, fake).URL}
	if _, _, err := client.FetchConfig(context.Background(), "tuba_col_abc", 0); err == nil {
		t.Fatal("non-positive config version must be rejected")
	}
}

func TestValidateRemoteConfiguration(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"plain object", `{"sources":[{"source_id":"src-1"}]}`, false},
		{"env reference", `{"kafka_env":"KAFKA_SASL_PASSWORD"}`, false},
		{"command directive", `{"command":["/bin/sh"]}`, true},
		{"nested script", `{"a":{"b":{"script":"x"}}}`, true},
		{"inline token", `{"kafka_token":"abc"}`, true},
		{"inline password", `{"password":"abc"}`, true},
		{"non-string env", `{"kafka_env":42}`, true},
		{"array directive", `{"steps":[{"exec":"true"}]}`, true},
		{"not an object", `[1,2]`, true},
		{"empty", ``, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRemoteConfiguration(json.RawMessage(tc.raw))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
