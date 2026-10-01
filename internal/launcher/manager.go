package launcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	stopFileName        = "stop.request"
	stopMarkerSuffix    = ".stop-request"
	restartMarkerSuffix = ".restart-request"
	maxLogTailBytes     = 4 << 20
	maxLogTailLines     = 1000
	controlPollInterval = 200 * time.Millisecond
)

// Validate checks the manifest, referenced executables, environment file, and
// platform-specific secret permissions without starting any service process.
func Validate(manifestPath string) error {
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	fmt.Printf("TUBA launcher manifest valid services=%d\n", len(manifest.Services))
	return nil
}

func Start(manifestPath string) error {
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(manifest.StateDir, 0700); err != nil {
		return fmt.Errorf("create launcher state directory: %w", err)
	}
	if err := securePrivateDirectory(manifest.StateDir); err != nil {
		return fmt.Errorf("secure launcher state directory: %w", err)
	}
	if state, err := readState(manifest.StateDir); err == nil && state.RunnerPID > 0 && processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
		return fmt.Errorf("TUBA launcher is already running with pid %d", state.RunnerPID)
	}
	_ = os.Remove(filepath.Join(manifest.StateDir, stopFileName))
	_ = os.Remove(filepath.Join(manifest.StateDir, stateFileName))
	if err := os.MkdirAll(manifest.LogDir, 0700); err != nil {
		return fmt.Errorf("create launcher log directory: %w", err)
	}
	if err := securePrivateDirectory(manifest.LogDir); err != nil {
		return fmt.Errorf("secure launcher log directory: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve launcher executable: %w", err)
	}
	runnerLogPath := filepath.Join(manifest.LogDir, "launcher.log")
	if err := rotateExistingLogIfNeeded(runnerLogPath, serviceLogMaxBytes, serviceLogBackups); err != nil {
		return fmt.Errorf("rotate launcher log: %w", err)
	}
	runnerLog, err := os.OpenFile(runnerLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open launcher log: %w", err)
	}
	defer runnerLog.Close()
	cmd := exec.Command(executable, "run", "--manifest", manifest.path)
	cmd.Stdout, cmd.Stderr = runnerLog, runnerLog
	configureDetached(cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start launcher supervisor: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if state, err := readState(manifest.StateDir); err == nil &&
			state.RunnerPID == cmd.Process.Pid && state.RunnerIdentity != "" &&
			processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
			fmt.Printf("TUBA launcher started pid=%d services=%d\n", cmd.Process.Pid, len(manifest.Services))
			return nil
		}
		if !processAlive(cmd.Process.Pid) {
			return errors.New("launcher supervisor exited during startup; inspect launcher.log")
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timed out waiting for launcher supervisor; inspect launcher.log")
}

func Stop(manifestPath string, timeout time.Duration) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	state, err := readState(manifest.StateDir)
	if err != nil || state.RunnerPID <= 0 || !processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
		fmt.Println("TUBA launcher is not running")
		return nil
	}
	stopPath := filepath.Join(manifest.StateDir, stopFileName)
	if err := os.WriteFile(stopPath, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
		return fmt.Errorf("request launcher stop: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
			_ = os.Remove(stopPath)
			fmt.Println("TUBA launcher stopped")
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("launcher pid %d did not stop within %s", state.RunnerPID, timeout)
}

// stopMarkerPath is the persistent desired-state file for a single service:
// while it exists the supervisor keeps that service stopped, across supervisor
// and host restarts, until `start --service` removes it. Service names are
// restricted to a filename-safe pattern by manifest validation.
func stopMarkerPath(stateDir, name string) string {
	return filepath.Join(stateDir, name+stopMarkerSuffix)
}

// restartMarkerPath is a transient request consumed and deleted by the
// running supervisor; a stale copy left by a crashed supervisor is removed on
// startup and never honored.
func restartMarkerPath(stateDir, name string) string {
	return filepath.Join(stateDir, name+restartMarkerSuffix)
}

func resolveServiceNames(manifest *Manifest, names []string) ([]string, error) {
	seen := map[string]bool{}
	resolved := make([]string, 0, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		if !manifest.hasService(name) {
			return nil, fmt.Errorf("unknown service %q", name)
		}
		resolved = append(resolved, name)
	}
	if len(resolved) == 0 {
		return nil, errors.New("at least one --service NAME is required")
	}
	return resolved, nil
}

// requireServiceControl fails closed when the running supervisor predates
// per-service control: it would ignore request files now, while a future
// upgraded supervisor would honor them later — neither surprise is acceptable.
func requireServiceControl(state State) error {
	if !state.ServiceControl {
		return fmt.Errorf("the running launcher supervisor (pid %d) does not support per-service control; restart the launcher main process with the updated binary before using --service", state.RunnerPID)
	}
	return nil
}

func supervisorRunning(manifest *Manifest) (State, bool) {
	state, err := readState(manifest.StateDir)
	if err != nil || state.RunnerPID <= 0 || !processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
		return State{}, false
	}
	return state, true
}

// StopServices marks the named services as desired-stopped. The marker files
// persist, so the services stay stopped across supervisor and host restarts
// until StartServices removes the markers. Other services are unaffected.
func StopServices(manifestPath string, names []string, timeout time.Duration) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	names, err = resolveServiceNames(manifest, names)
	if err != nil {
		return err
	}
	state, running := supervisorRunning(manifest)
	if running {
		if err := requireServiceControl(state); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(manifest.StateDir, 0700); err != nil {
		return fmt.Errorf("create launcher state directory: %w", err)
	}
	for _, name := range names {
		marker := stopMarkerPath(manifest.StateDir, name)
		if err := os.WriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
			return fmt.Errorf("request stop of service %q: %w", name, err)
		}
	}
	if !running {
		fmt.Printf("TUBA launcher is not running; %s will stay stopped when it starts\n", strings.Join(names, ", "))
		return nil
	}
	return waitForServiceStates(manifest, names, timeout, "stopped", func(_ string, got ServiceStatus) bool {
		return got.State == "stopped"
	})
}

// StartServices removes the persistent stop markers so the supervisor (now or
// after its next start) runs the named services again. It never starts the
// launcher itself; with the supervisor down it only updates desired state.
func StartServices(manifestPath string, names []string, timeout time.Duration) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	names, err = resolveServiceNames(manifest, names)
	if err != nil {
		return err
	}
	state, running := supervisorRunning(manifest)
	if running {
		if err := requireServiceControl(state); err != nil {
			return err
		}
	}
	for _, name := range names {
		if err := os.Remove(stopMarkerPath(manifest.StateDir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("clear stop request of service %q: %w", name, err)
		}
	}
	if !running {
		fmt.Printf("TUBA launcher is not running; %s will start with it\n", strings.Join(names, ", "))
		return nil
	}
	return waitForServiceStates(manifest, names, timeout, "running", func(_ string, got ServiceStatus) bool {
		return got.State == "running" && got.PID > 0
	})
}

// RestartServices restarts only the named services. A stopped service is
// started; a running service is gracefully terminated and immediately
// restarted by the supervisor without consuming its crash-restart budget.
// Other services keep their PIDs and restart counters.
func RestartServices(manifestPath string, names []string, timeout time.Duration) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	names, err = resolveServiceNames(manifest, names)
	if err != nil {
		return err
	}
	state, running := supervisorRunning(manifest)
	if !running {
		return errors.New("TUBA launcher is not running; restart of individual services requires a running supervisor")
	}
	if err := requireServiceControl(state); err != nil {
		return err
	}
	oldPIDs := map[string]int{}
	for _, name := range names {
		oldPIDs[name] = state.Services[name].PID
		if _, err := os.Stat(stopMarkerPath(manifest.StateDir, name)); err == nil {
			// Restart of a deliberately stopped service is a start.
			if err := os.Remove(stopMarkerPath(manifest.StateDir, name)); err != nil {
				return fmt.Errorf("clear stop request of service %q: %w", name, err)
			}
			continue
		}
		marker := restartMarkerPath(manifest.StateDir, name)
		if err := os.WriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600); err != nil {
			return fmt.Errorf("request restart of service %q: %w", name, err)
		}
	}
	return waitForServiceStates(manifest, names, timeout, "restarted", func(name string, got ServiceStatus) bool {
		return got.State == "running" && got.PID > 0 && got.PID != oldPIDs[name]
	})
}

func waitForServiceStates(manifest *Manifest, names []string, timeout time.Duration, goal string, reached func(name string, status ServiceStatus) bool) error {
	deadline := time.Now().Add(timeout)
	pending := map[string]bool{}
	for _, name := range names {
		pending[name] = true
	}
	for time.Now().Before(deadline) {
		state, err := readState(manifest.StateDir)
		if err == nil {
			for name := range pending {
				status, ok := state.Services[name]
				if ok && reached(name, status) {
					delete(pending, name)
				}
			}
		}
		if len(pending) == 0 {
			fmt.Printf("%s: %s\n", goal, strings.Join(names, ", "))
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	remaining := make([]string, 0, len(pending))
	for name := range pending {
		remaining = append(remaining, name)
	}
	sort.Strings(remaining)
	return fmt.Errorf("timed out waiting for %s to reach %q within %s", strings.Join(remaining, ", "), goal, timeout)
}

func Restart(manifestPath string) error {
	if err := Stop(manifestPath, 30*time.Second); err != nil {
		return err
	}
	return Start(manifestPath)
}

func Status(manifestPath string, services []string) error {
	return statusTo(os.Stdout, manifestPath, services)
}

func statusTo(w io.Writer, manifestPath string, services []string) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	if len(services) > 0 {
		if services, err = resolveServiceNames(manifest, services); err != nil {
			return err
		}
	}
	state, err := readState(manifest.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(w, "TUBA launcher is not running")
			return nil
		}
		return err
	}
	if state.RunnerPID <= 0 {
		fmt.Fprintln(w, "TUBA launcher is stopped")
	} else if !processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
		fmt.Fprintln(w, "TUBA launcher is stopped (stale state file)")
	} else {
		fmt.Fprintf(w, "TUBA launcher pid=%d started=%s stopping=%t\n", state.RunnerPID, state.Started.Format(time.RFC3339), state.Stopping)
	}
	for _, service := range manifest.Services {
		if len(services) > 0 && !containsName(services, service.Name) {
			continue
		}
		status := state.Services[service.Name]
		fmt.Fprintf(w, "%-24s %-10s pid=%-8d restarts=%d", service.Name, status.State, status.PID, status.Restarts)
		if status.LastExit != "" {
			fmt.Fprintf(w, " last_exit=%s", status.LastExit)
		}
		fmt.Fprintln(w)
	}
	return nil
}

func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func Logs(manifestPath, service string, tail int) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	if service != "launcher" && !manifest.hasService(service) {
		return fmt.Errorf("unknown service %q", service)
	}
	path := filepath.Join(manifest.LogDir, service+".log")
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if tail <= 0 {
		_, err = io.Copy(os.Stdout, file)
		return err
	}
	if tail > maxLogTailLines {
		tail = maxLogTailLines
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	start := info.Size() - maxLogTailBytes
	if start < 0 {
		start = 0
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxLogTailBytes))
	if err != nil {
		return err
	}
	if start > 0 {
		if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
			data = data[newline+1:]
		}
	}
	lines := strings.Split(strings.TrimRight(string(data), "\r\n"), "\n")
	if len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}
	for _, line := range lines {
		fmt.Println(line)
	}
	return nil
}

func Run(manifestPath string) error {
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(manifest.LogDir, 0700); err != nil {
		return fmt.Errorf("create launcher log directory: %w", err)
	}
	if err := securePrivateDirectory(manifest.LogDir); err != nil {
		return fmt.Errorf("secure launcher log directory: %w", err)
	}
	store, err := newStateStore(manifest.StateDir, manifest.Services)
	if err != nil {
		return err
	}
	defer store.stop()
	_ = os.Remove(filepath.Join(manifest.StateDir, stopFileName))
	// Restart requests are transient: a leftover from a crashed supervisor must
	// not restart a service that was never asked to restart in this lifetime.
	// Stop markers are persistent desired state and are deliberately kept.
	for _, service := range manifest.Services {
		_ = os.Remove(restartMarkerPath(manifest.StateDir, service.Name))
	}
	ctx, cancel := signalContext()
	defer cancel()
	var workers sync.WaitGroup
	controllers := make(map[string]*serviceController, len(manifest.Services))
	for _, service := range manifest.Services {
		service := service
		controller := newServiceController(manifest.StateDir, service.Name)
		controllers[service.Name] = controller
		workers.Add(1)
		go func() {
			defer workers.Done()
			runService(ctx, manifest.LogDir, service, store, controller)
		}()
	}
	stopPoll := time.NewTicker(controlPollInterval)
	defer stopPoll.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			store.setStopping()
		case <-stopPoll.C:
			if _, err := os.Stat(filepath.Join(manifest.StateDir, stopFileName)); err == nil {
				store.setStopping()
				cancel()
				continue
			}
			for _, service := range manifest.Services {
				controller := controllers[service.Name]
				if _, err := os.Stat(restartMarkerPath(manifest.StateDir, service.Name)); err == nil {
					_ = os.Remove(restartMarkerPath(manifest.StateDir, service.Name))
					controller.requestRestart()
				}
				if _, err := os.Stat(stopMarkerPath(manifest.StateDir, service.Name)); err == nil {
					controller.requestStop()
				}
			}
		}
	}
	cancel()
	workers.Wait()
	return nil
}

// serviceController passes per-service control requests from the supervisor's
// poll loop to the service's runService goroutine.
type serviceController struct {
	stopPath string
	wake     chan struct{}

	mu      sync.Mutex
	cancel  context.CancelFunc
	restart bool
}

func newServiceController(stateDir, name string) *serviceController {
	return &serviceController{
		stopPath: stopMarkerPath(stateDir, name),
		wake:     make(chan struct{}, 1),
	}
}

func (c *serviceController) stopMarkerExists() bool {
	_, err := os.Stat(c.stopPath)
	return err == nil
}

// requestStop cancels the running child (if any) and interrupts a backoff
// wait so the stop takes effect within one poll interval.
func (c *serviceController) requestStop() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.signalWake()
}

// requestRestart asks runService to restart the service promptly without
// charging its crash-restart counter.
func (c *serviceController) requestRestart() {
	c.mu.Lock()
	c.restart = true
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.signalWake()
}

func (c *serviceController) setCancel(cancel context.CancelFunc) {
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
}

func (c *serviceController) takeRestart() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	restart := c.restart
	c.restart = false
	return restart
}

func (c *serviceController) signalWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// waitPaused blocks while the service is desired-stopped, returning false if
// the supervisor is shutting down.
func (c *serviceController) waitPaused(ctx context.Context) bool {
	ticker := time.NewTicker(controlPollInterval)
	defer ticker.Stop()
	for {
		if !c.stopMarkerExists() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-c.wake:
		case <-ticker.C:
		}
	}
}

// waitRetry sleeps for delay, returning early (true) when a control request
// arrives and false when the supervisor is shutting down.
func (c *serviceController) waitRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-c.wake:
		return true
	case <-timer.C:
		return true
	}
}

func runService(ctx context.Context, logDir string, service ServiceSpec, state *stateStore, control *serviceController) {
	restarts := 0
	delay := service.restartMinDelay
	for ctx.Err() == nil {
		if control.stopMarkerExists() {
			state.setService(service.Name, ServiceStatus{State: "stopped", Restarts: restarts, LastChanged: time.Now().UTC()})
			if !control.waitPaused(ctx) {
				return
			}
			delay = service.restartMinDelay
			continue
		}
		logFile, err := openRollingLog(filepath.Join(logDir, service.Name+".log"), serviceLogMaxBytes, serviceLogBackups)
		if err != nil {
			state.setService(service.Name, ServiceStatus{State: "failed", Restarts: restarts, LastExit: err.Error(), LastChanged: time.Now().UTC()})
			return
		}
		childCtx, cancelChild := context.WithCancel(ctx)
		control.setCancel(cancelChild)
		control.takeRestart()
		cmd := exec.CommandContext(childCtx, service.commandPath, service.Args...)
		cmd.Dir = service.workingPath
		cmd.Env = childEnvironment(service.resolvedEnv)
		cmd.Stdout, cmd.Stderr = logFile, logFile
		configureChild(cmd)
		cmd.Cancel = func() error { return terminateChild(cmd) }
		cmd.WaitDelay = 10 * time.Second
		startErr := cmd.Start()
		if startErr == nil {
			state.setService(service.Name, ServiceStatus{State: "running", PID: cmd.Process.Pid, Restarts: restarts, LastChanged: time.Now().UTC()})
			err = cmd.Wait()
		} else {
			err = startErr
		}
		control.setCancel(nil)
		cancelChild()
		_ = logFile.Close()
		if ctx.Err() != nil {
			state.setService(service.Name, ServiceStatus{State: "stopping", Restarts: restarts, LastChanged: time.Now().UTC()})
			return
		}
		if control.takeRestart() {
			delay = service.restartMinDelay
			continue
		}
		if control.stopMarkerExists() {
			continue
		}
		restarts++
		exitText := "exited successfully"
		if err != nil {
			exitText = err.Error()
		}
		state.setService(service.Name, ServiceStatus{State: "backoff", Restarts: restarts, LastExit: exitText, LastChanged: time.Now().UTC()})
		if !control.waitRetry(ctx, delay) {
			return
		}
		if control.takeRestart() || control.stopMarkerExists() {
			delay = service.restartMinDelay
			continue
		}
		delay *= 2
		if delay > service.restartMaxDelay {
			delay = service.restartMaxDelay
		}
	}
}

func childEnvironment(explicit []string) []string {
	allowed := map[string]bool{
		"PATH": true, "HOME": true, "USER": true, "LOGNAME": true,
		"TMPDIR": true, "TMP": true, "TEMP": true, "LANG": true, "LC_ALL": true,
		"SYSTEMROOT": true, "WINDIR": true, "USERPROFILE": true, "APPDATA": true,
		"LOCALAPPDATA": true, "HOMEDRIVE": true, "HOMEPATH": true, "PATHEXT": true, "COMSPEC": true,
	}
	values := map[string]string{}
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok && allowed[strings.ToUpper(key)] {
			values[key] = value
		}
	}
	for _, entry := range explicit {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	for _, key := range keys {
		env = append(env, key+"="+values[key])
	}
	return env
}

func (m *Manifest) hasService(name string) bool {
	for _, service := range m.Services {
		if service.Name == name {
			return true
		}
	}
	return false
}

func signalContext() (context.Context, context.CancelFunc) {
	return signalNotifyContext()
}
