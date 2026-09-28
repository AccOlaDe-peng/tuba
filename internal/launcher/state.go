package launcher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const stateFileName = "launcher-state.json"

type State struct {
	RunnerPID      int                      `json:"runner_pid"`
	RunnerIdentity string                   `json:"runner_identity,omitempty"`
	Started        time.Time                `json:"started_at"`
	Updated        time.Time                `json:"updated_at"`
	Stopping       bool                     `json:"stopping"`
	Services       map[string]ServiceStatus `json:"services"`
}

type ServiceStatus struct {
	State       string    `json:"state"`
	PID         int       `json:"pid,omitempty"`
	Restarts    int       `json:"restarts"`
	LastExit    string    `json:"last_exit,omitempty"`
	LastChanged time.Time `json:"last_changed"`
}

type stateStore struct {
	mu   sync.Mutex
	path string
	data State
}

func newStateStore(path string, services []ServiceSpec) (*stateStore, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, fmt.Errorf("create launcher state directory: %w", err)
	}
	if err := securePrivateDirectory(path); err != nil {
		return nil, fmt.Errorf("secure launcher state directory: %w", err)
	}
	identity, err := processIdentity(os.Getpid())
	if err != nil || identity == "" {
		if err == nil {
			err = fmt.Errorf("process identity is empty")
		}
		return nil, fmt.Errorf("identify launcher process: %w", err)
	}
	data := State{RunnerPID: os.Getpid(), RunnerIdentity: identity, Started: time.Now().UTC(), Services: map[string]ServiceStatus{}}
	for _, service := range services {
		data.Services[service.Name] = ServiceStatus{State: "starting", LastChanged: time.Now().UTC()}
	}
	store := &stateStore{path: filepath.Join(path, stateFileName), data: data}
	if err := store.save(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *stateStore) setService(name string, status ServiceStatus) {
	s.mu.Lock()
	s.data.Services[name] = status
	s.data.Updated = time.Now().UTC()
	_ = s.writeLocked()
	s.mu.Unlock()
}

func (s *stateStore) setStopping() {
	s.mu.Lock()
	s.data.Stopping = true
	s.data.Updated = time.Now().UTC()
	_ = s.writeLocked()
	s.mu.Unlock()
}

func (s *stateStore) stop() {
	s.mu.Lock()
	s.data.Stopping = true
	s.data.RunnerPID = 0
	s.data.RunnerIdentity = ""
	s.data.Updated = time.Now().UTC()
	for name, status := range s.data.Services {
		status.State = "stopped"
		status.PID = 0
		status.LastChanged = s.data.Updated
		s.data.Services[name] = status
	}
	_ = s.writeLocked()
	s.mu.Unlock()
}

func (s *stateStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Updated = time.Now().UTC()
	return s.writeLocked()
}

func (s *stateStore) writeLocked() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	temp := s.path + ".tmp"
	if err := os.WriteFile(temp, append(data, '\n'), 0600); err != nil {
		return fmt.Errorf("write launcher state: %w", err)
	}
	_ = os.Chmod(temp, 0600)
	if err := os.Rename(temp, s.path); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("replace launcher state: %w", err)
	}
	return nil
}

func readState(stateDir string) (State, error) {
	data, err := os.ReadFile(filepath.Join(stateDir, stateFileName))
	if err != nil {
		return State{}, err
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, fmt.Errorf("decode launcher state: %w", err)
	}
	return state, nil
}

func processInstanceAlive(pid int, expectedIdentity string) bool {
	if !processAlive(pid) {
		return false
	}
	if expectedIdentity == "" {
		// A PID-only state from an older Launcher cannot distinguish a live old
		// supervisor from PID reuse after a reboot. Supported upgrades stop the
		// old supervisor before activating the new binary, so fail closed here
		// and require a fresh state carrying a process identity.
		return false
	}
	actualIdentity, err := processIdentity(pid)
	if err != nil || actualIdentity == "" {
		// An unavailable platform identity must not cause us to start a second
		// supervisor over a possibly live one.
		return true
	}
	return actualIdentity == expectedIdentity
}
