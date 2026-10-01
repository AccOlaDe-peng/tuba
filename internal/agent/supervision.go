package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"tuba/product/internal/component"
	"tuba/product/internal/launcher"
)

// SupervisedComponent wires one managed component (COL-09) into the agent.
// All fields are local agent configuration: the management plane may say
// WHICH version a component should run, but only the local spec decides how
// that component is stopped, started and health-checked.
type SupervisedComponent struct {
	Component        string
	Root             string // supervisor root (components/, data/, downloads/)
	Instances        []string
	KeyringPath      string
	RepoURL          string // release repository base URL for fetches
	LauncherManifest string
	Service          string
	HealthURL        string

	// Timing overrides; zero means the orchestrator defaults (5m/1m/2s).
	ObserveWindow time.Duration
	StartGrace    time.Duration
	PollInterval  time.Duration

	// Test hooks; nil constructs the production adapters.
	Processes  component.ProcessManager
	Health     component.HealthChecker
	Downloader *component.Downloader
}

// upgradeDirective is the per-component section of a remote configuration:
// {"components": {"filebeat": {"desired_version": "8.19.1"}}}. It passes
// ValidateRemoteConfiguration by construction (no executable directives, no
// secret-shaped keys).
type upgradeDirective struct {
	DesiredVersion string `json:"desired_version"`
}

// Supervisor connects the management loop to the component supervisor: it
// applies component upgrade directives from accepted configurations via the
// orchestrator (fetch with verify-before-download, then the full
// stop→apply→observe→confirm/rollback sequence) and reports per-component
// status for heartbeats.
type Supervisor struct {
	Logger *log.Logger

	mu       sync.Mutex
	specs    map[string]*SupervisedComponent
	inFlight map[string]bool
	lastErr  map[string]string
}

func NewSupervisor(logger *log.Logger) *Supervisor {
	if logger == nil {
		logger = log.Default()
	}
	return &Supervisor{Logger: logger, specs: map[string]*SupervisedComponent{}, inFlight: map[string]bool{}, lastErr: map[string]string{}}
}

// Supervise registers a managed component. Registration is local topology;
// unknown components named by remote directives are refused.
func (s *Supervisor) Supervise(spec SupervisedComponent) error {
	if !componentNamePattern.MatchString(spec.Component) {
		return fmt.Errorf("invalid component name %q", spec.Component)
	}
	if spec.Root == "" || spec.KeyringPath == "" {
		return errors.New("supervised component requires a root and a keyring path")
	}
	if spec.Processes == nil && (spec.LauncherManifest == "" || spec.Service == "") {
		return fmt.Errorf("component %q requires launcher manifest and service (or an injected process manager)", spec.Component)
	}
	if spec.Health == nil && spec.HealthURL == "" {
		return fmt.Errorf("component %q requires a health URL (or an injected health checker)", spec.Component)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specs[spec.Component] = &spec
	return nil
}

var componentNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
var componentVersionPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)

// ApplyConfig applies the component upgrade directives of a newly accepted
// configuration. Unknown components and malformed versions are refused
// (fail-closed) and reported as an error to the caller; valid directives for
// components already at the desired version are no-ops. Upgrades run
// asynchronously so the heartbeat cadence is not blocked by the observation
// window.
func (s *Supervisor) ApplyConfig(ctx context.Context, raw json.RawMessage) error {
	var parsed struct {
		Components map[string]upgradeDirective `json:"components"`
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("decode component directives: %w", err)
	}
	var problems []string
	for name, directive := range parsed.Components {
		spec, ok := s.spec(name)
		if !ok {
			problems = append(problems, fmt.Sprintf("unknown component %q (not supervised locally)", name))
			continue
		}
		if !componentVersionPattern.MatchString(directive.DesiredVersion) {
			problems = append(problems, fmt.Sprintf("component %q has an invalid desired version %q", name, directive.DesiredVersion))
			continue
		}
		current, err := component.CurrentVersion(spec.Root, spec.Component)
		if err != nil {
			problems = append(problems, fmt.Sprintf("component %q: %v", name, err))
			continue
		}
		if current == directive.DesiredVersion {
			continue
		}
		if !s.startUpgrade(name) {
			continue // an upgrade is already running; its result is reported by phase
		}
		go s.upgrade(ctx, spec, directive.DesiredVersion)
	}
	if len(problems) > 0 {
		return errors.New(joinSemicolon(problems))
	}
	return nil
}

func (s *Supervisor) spec(name string) (*SupervisedComponent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.specs[name]
	return spec, ok
}

func (s *Supervisor) startUpgrade(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight[name] {
		return false
	}
	s.inFlight[name] = true
	return true
}

func (s *Supervisor) finishUpgrade(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, name)
	if err != nil {
		s.lastErr[name] = truncateDiagnostic(err.Error())
	} else {
		delete(s.lastErr, name)
	}
}

// upgrade fetches the package (verify-before-download) and runs the
// orchestrated upgrade. A disabled management plane never interrupts an
// in-flight upgrade: the orchestration is local and health-guarded, and
// disabling stops NEW configuration, not a half-finished version switch.
func (s *Supervisor) upgrade(ctx context.Context, spec *SupervisedComponent, version string) {
	err := s.doUpgrade(ctx, spec, version)
	s.finishUpgrade(spec.Component, err)
	if err != nil {
		s.Logger.Printf("component %s upgrade to %s failed: %v", spec.Component, version, err)
		return
	}
	s.Logger.Printf("component %s upgraded to %s and confirmed", spec.Component, version)
}

func (s *Supervisor) doUpgrade(ctx context.Context, spec *SupervisedComponent, version string) error {
	keyring, err := component.LoadKeyring(spec.KeyringPath)
	if err != nil {
		return err
	}
	if spec.RepoURL == "" {
		return fmt.Errorf("component %q has no release repository URL", spec.Component)
	}
	downloader := spec.Downloader
	if downloader == nil {
		downloader = &component.Downloader{Keyring: keyring}
	}
	// Reuse an already-downloaded copy only if it still passes full
	// signature verification; otherwise fetch fresh.
	packageDir, err := componentPackageCache(spec.Root, spec.Component, version)
	if err != nil {
		return err
	}
	if _, err := component.LoadManifest(packageDir, keyring); err != nil {
		if _, err := downloader.Fetch(ctx, spec.RepoURL, spec.Component, version, packageDir); err != nil {
			return fmt.Errorf("fetch %s %s: %w", spec.Component, version, err)
		}
	}
	processes := spec.Processes
	if processes == nil {
		processes = &component.LauncherProcessManager{ManifestPath: spec.LauncherManifest, Service: spec.Service}
	}
	health := spec.Health
	if health == nil {
		health = &component.HTTPHealthChecker{URL: spec.HealthURL}
	}
	orchestrator := &component.Orchestrator{
		Upgrader:      &component.Upgrader{Root: spec.Root, Keyring: keyring},
		Processes:     processes,
		Health:        health,
		ObserveWindow: spec.ObserveWindow,
		StartGrace:    spec.StartGrace,
		PollInterval:  spec.PollInterval,
	}
	return orchestrator.Upgrade(ctx, packageDir, spec.Component, spec.Instances)
}

func componentPackageCache(root, componentName, version string) (string, error) {
	if !componentNamePattern.MatchString(componentName) || !componentVersionPattern.MatchString(version) {
		return "", fmt.Errorf("invalid component %q or version %q", componentName, version)
	}
	return filepath.Join(root, "downloads", componentName+"-"+version), nil
}

// ComponentStatus reports the per-component summary carried in heartbeats:
// current version from the supervisor pointer, upgrade phase from the
// orchestration journal, and process state/restarts from the launcher.
// Unreadable pieces degrade to an error state for that component rather than
// being silently omitted — a heartbeat must not present unknowns as healthy.
func (s *Supervisor) ComponentStatus() []ComponentStatus {
	s.mu.Lock()
	specs := make([]*SupervisedComponent, 0, len(s.specs))
	for _, spec := range s.specs {
		specs = append(specs, spec)
	}
	lastErr := make(map[string]string, len(s.lastErr))
	for name, text := range s.lastErr {
		lastErr[name] = text
	}
	s.mu.Unlock()

	statuses := make([]ComponentStatus, 0, len(specs))
	for _, spec := range specs {
		status := ComponentStatus{Component: spec.Component, State: "error"}
		current, err := component.CurrentVersion(spec.Root, spec.Component)
		if err == nil {
			status.Version = current
		} else {
			status.LastError = truncateDiagnostic(err.Error())
		}
		if orchestration, err := (&component.Upgrader{Root: spec.Root}).Orchestration(spec.Component); err == nil && orchestration != nil {
			status.Phase = orchestration.Phase
		}
		if err == nil {
			status.State, status.Restarts = spec.processState()
		}
		if text, ok := lastErr[spec.Component]; ok {
			status.LastError = text
			if status.State == "running" {
				status.State = "error"
			}
		}
		statuses = append(statuses, status)
	}
	return statuses
}

// processState maps the supervised process state to the heartbeat enum.
func (spec *SupervisedComponent) processState() (string, int64) {
	if spec.Processes != nil {
		alive, err := spec.Processes.Alive(context.Background())
		if err != nil {
			return "error", 0
		}
		if !alive {
			return "paused", 0
		}
		return "running", 0
	}
	service, err := launcher.QueryService(spec.LauncherManifest, spec.Service)
	if err != nil {
		return "error", 0
	}
	switch service.State {
	case "running":
		return "running", int64(service.Restarts)
	case "stopped":
		return "paused", int64(service.Restarts)
	default:
		return "error", int64(service.Restarts)
	}
}

func joinSemicolon(parts []string) string {
	out := ""
	for i, part := range parts {
		if i > 0 {
			out += "; "
		}
		out += part
	}
	return out
}
