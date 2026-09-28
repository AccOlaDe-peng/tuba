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
	stopFileName    = "stop.request"
	maxLogTailBytes = 4 << 20
	maxLogTailLines = 1000
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

func Restart(manifestPath string) error {
	if err := Stop(manifestPath, 30*time.Second); err != nil {
		return err
	}
	return Start(manifestPath)
}

func Status(manifestPath string) error {
	manifest, err := LoadManifestForControl(manifestPath)
	if err != nil {
		return err
	}
	state, err := readState(manifest.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("TUBA launcher is not running")
			return nil
		}
		return err
	}
	if state.RunnerPID <= 0 {
		fmt.Println("TUBA launcher is stopped")
		return nil
	}
	if !processInstanceAlive(state.RunnerPID, state.RunnerIdentity) {
		fmt.Println("TUBA launcher is stopped (stale state file)")
		return nil
	}
	fmt.Printf("TUBA launcher pid=%d started=%s stopping=%t\n", state.RunnerPID, state.Started.Format(time.RFC3339), state.Stopping)
	for _, service := range manifest.Services {
		status := state.Services[service.Name]
		fmt.Printf("%-24s %-10s pid=%-8d restarts=%d", service.Name, status.State, status.PID, status.Restarts)
		if status.LastExit != "" {
			fmt.Printf(" last_exit=%s", status.LastExit)
		}
		fmt.Println()
	}
	return nil
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
	ctx, cancel := signalContext()
	defer cancel()
	var workers sync.WaitGroup
	for _, service := range manifest.Services {
		service := service
		workers.Add(1)
		go func() {
			defer workers.Done()
			runService(ctx, manifest.LogDir, service, store)
		}()
	}
	stopPoll := time.NewTicker(200 * time.Millisecond)
	defer stopPoll.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			store.setStopping()
		case <-stopPoll.C:
			if _, err := os.Stat(filepath.Join(manifest.StateDir, stopFileName)); err == nil {
				store.setStopping()
				cancel()
			}
		}
	}
	cancel()
	workers.Wait()
	return nil
}

func runService(ctx context.Context, logDir string, service ServiceSpec, state *stateStore) {
	restarts := 0
	delay := service.restartMinDelay
	for ctx.Err() == nil {
		logFile, err := openRollingLog(filepath.Join(logDir, service.Name+".log"), serviceLogMaxBytes, serviceLogBackups)
		if err != nil {
			state.setService(service.Name, ServiceStatus{State: "failed", Restarts: restarts, LastExit: err.Error(), LastChanged: time.Now().UTC()})
			return
		}
		cmd := exec.CommandContext(ctx, service.commandPath, service.Args...)
		cmd.Dir = service.workingPath
		cmd.Env = childEnvironment(service.resolvedEnv)
		cmd.Stdout, cmd.Stderr = logFile, logFile
		configureChild(cmd)
		cmd.Cancel = func() error { return terminateChild(cmd) }
		cmd.WaitDelay = 10 * time.Second
		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			restarts++
			state.setService(service.Name, ServiceStatus{State: "backoff", Restarts: restarts, LastExit: err.Error(), LastChanged: time.Now().UTC()})
		} else {
			state.setService(service.Name, ServiceStatus{State: "running", PID: cmd.Process.Pid, Restarts: restarts, LastChanged: time.Now().UTC()})
			err := cmd.Wait()
			_ = logFile.Close()
			if ctx.Err() != nil {
				state.setService(service.Name, ServiceStatus{State: "stopping", Restarts: restarts, LastChanged: time.Now().UTC()})
				return
			}
			restarts++
			exitText := "exited successfully"
			if err != nil {
				exitText = err.Error()
			}
			state.setService(service.Name, ServiceStatus{State: "backoff", Restarts: restarts, LastExit: exitText, LastChanged: time.Now().UTC()})
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
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
