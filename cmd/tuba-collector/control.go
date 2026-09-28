package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type runtimeStatus struct {
	PID         int       `json:"pid"`
	State       string    `json:"state"`
	StartedAt   time.Time `json:"started_at"`
	Heartbeat   time.Time `json:"heartbeat"`
	SourceCount int       `json:"source_count"`
}

func controlPaths(spool string) (lock, status, stop, log string) {
	return spool + ".lock", spool + ".status.json", spool + ".stop", spool + ".log"
}

func startCollector(configPath string) error {
	c, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.Spool), 0700); err != nil {
		return err
	}
	_, statusPath, stopPath, logPath := controlPaths(c.Spool)
	if collectorProcessAlive(c.Spool) {
		return errors.New("collector is already running")
	}
	_ = os.Remove(stopPath)
	_ = os.Remove(statusPath)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "run", "--config", configPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	cmd.Env = append(os.Environ(), "TUBA_COLLECTOR_CONFIG="+configPath)
	configureDetached(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	_ = cmd.Process.Release()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if statusFresh(statusPath, 15*time.Second) {
			fmt.Printf("collector started (pid %d)\n", readStatus(statusPath).PID)
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("collector did not report healthy startup; inspect logs with: tuba-collector logs --config %s", configPath)
}

func stopCollector(configPath string) error {
	c, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	_, statusPath, stopPath, _ := controlPaths(c.Spool)
	if !collectorProcessAlive(c.Spool) {
		fmt.Println("collector is stopped")
		return nil
	}
	if err := os.WriteFile(stopPath, []byte("stop\n"), 0600); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if s, err := readStatusFile(statusPath); err == nil && s.State == "stopped" {
			fmt.Println("collector stopped")
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return errors.New("collector did not stop within 30 seconds; buffered data remains in the SQLite spool")
}

func showStatus(configPath string) error {
	c, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	_, statusPath, _, _ := controlPaths(c.Spool)
	s, err := readStatusFile(statusPath)
	if collectorProcessAlive(c.Spool) && (err != nil || s.State != "running" || time.Since(s.Heartbeat) > 15*time.Second) {
		fmt.Printf("collector process %d is alive but its heartbeat is stale\n", lockPID(c.Spool))
		return nil
	}
	if err != nil || s.State != "running" || time.Since(s.Heartbeat) > 15*time.Second {
		fmt.Println("collector is stopped")
		return nil
	}
	fmt.Printf("collector is running (pid %d, sources %d, heartbeat %s)\n", s.PID, s.SourceCount, s.Heartbeat.UTC().Format(time.RFC3339))
	return nil
}

func showLogs(configPath string) error {
	c, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	_, _, _, path := controlPaths(c.Spool)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("no collector log yet")
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(os.Stdout, f)
	return err
}

func readStatus(path string) runtimeStatus { s, _ := readStatusFile(path); return s }
func readStatusFile(path string) (runtimeStatus, error) {
	var s runtimeStatus
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	err = json.Unmarshal(b, &s)
	return s, err
}
func statusFresh(path string, maxAge time.Duration) bool {
	s, err := readStatusFile(path)
	return err == nil && s.State == "running" && time.Since(s.Heartbeat) <= maxAge
}

func collectorProcessAlive(spool string) bool {
	_, statusPath, _, _ := controlPaths(spool)
	if statusFresh(statusPath, 15*time.Second) {
		return true
	}
	return processAlive(lockPID(spool))
}

func lockPID(spool string) int {
	lock, _, _, _ := controlPaths(spool)
	b, err := os.ReadFile(lock)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0
	}
	return pid
}

func writeStatus(path string, s runtimeStatus) {
	b, err := json.Marshal(s)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, b, 0600)
}
func heartbeat(path string, pid int, started time.Time, sources int, ctxDone <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	update := func(state string) {
		writeStatus(path, runtimeStatus{PID: pid, State: state, StartedAt: started, Heartbeat: time.Now().UTC(), SourceCount: sources})
	}
	update("running")
	for {
		select {
		case <-ctxDone:
			update("stopped")
			return
		case <-ticker.C:
			update("running")
		}
	}
}
func watchStopFile(ctxDone <-chan struct{}, cancel context.CancelFunc, path string) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctxDone:
			return
		case <-ticker.C:
			if _, err := os.Stat(path); err == nil {
				_ = os.Remove(path)
				cancel()
				return
			}
		}
	}
}
