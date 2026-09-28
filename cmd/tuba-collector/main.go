package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"tuba/product/internal/collector"
	"tuba/product/internal/lifecycle"
)

type config struct {
	Endpoint     string         `json:"endpoint"`
	Spool        string         `json:"spool_path"`
	PollInterval string         `json:"poll_interval"`
	Sources      []sourceConfig `json:"sources"`
}
type sourceConfig struct {
	collector.ZeekSource
	APIKey    string `json:"api_key"`
	APIKeyEnv string `json:"api_key_env"`
	AllowDrop bool   `json:"allow_drop"`
}

func main() {
	args := os.Args[1:]
	command := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command = args[0]
		args = args[1:]
	}
	configPath, err := parseConfigPath(args)
	fatalIf(err)
	switch command {
	case "run":
		fatalIf(runCollector(configPath))
	case "start":
		fatalIf(startCollector(configPath))
	case "stop":
		fatalIf(stopCollector(configPath))
	case "restart":
		fatalIf(stopCollector(configPath))
		fatalIf(startCollector(configPath))
	case "status":
		fatalIf(showStatus(configPath))
	case "logs":
		fatalIf(showLogs(configPath))
	case "help", "--help", "-h":
		printUsage()
	default:
		fatalIf(fmt.Errorf("unknown command %q", command))
	}
}

func parseConfigPath(args []string) (string, error) {
	path := os.Getenv("TUBA_COLLECTOR_CONFIG")
	if path == "" {
		path = "collector.json"
	}
	for i := 0; i < len(args); i++ {
		if args[i] != "--config" {
			return "", fmt.Errorf("unknown argument %q", args[i])
		}
		if i+1 >= len(args) {
			return "", errors.New("--config requires a file path")
		}
		path = args[i+1]
		i++
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func loadConfig(path string) (config, error) {
	var c config
	raw, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(raw, &c); err != nil {
		return c, err
	}
	if !strings.HasPrefix(c.Endpoint, "http://") && !strings.HasPrefix(c.Endpoint, "https://") {
		return c, errors.New("endpoint must be an absolute HTTP(S) URL")
	}
	if c.Spool == "" || len(c.Sources) == 0 {
		return c, errors.New("spool_path and at least one source are required")
	}
	if !filepath.IsAbs(c.Spool) {
		c.Spool = filepath.Join(filepath.Dir(path), c.Spool)
	}
	return c, nil
}

func runCollector(configPath string) error {
	c, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.Spool), 0700); err != nil {
		return err
	}
	lock, statusPath, stopPath, _ := controlPaths(c.Spool)
	lockFile, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			owner, readErr := os.ReadFile(lock)
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(owner)))
			if readErr != nil || parseErr != nil {
				return errors.New("collector process lock exists but its owner cannot be verified")
			}
			if processAlive(pid) || statusFresh(statusPath, 15*time.Second) {
				return errors.New("collector is already running")
			}
			if removeErr := os.Remove(lock); removeErr != nil {
				return fmt.Errorf("cannot remove stale process lock: %w", removeErr)
			}
			lockFile, err = os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		}
	}
	if err != nil {
		return fmt.Errorf("cannot acquire collector process lock: %w", err)
	}
	_, _ = fmt.Fprintf(lockFile, "%d\n", os.Getpid())
	_ = lockFile.Close()
	defer os.Remove(lock)
	start := time.Now().UTC()
	defer writeStatus(statusPath, runtimeStatus{PID: os.Getpid(), State: "stopped", StartedAt: start, Heartbeat: time.Now().UTC(), SourceCount: len(c.Sources)})
	if err := validateConfig(&c); err != nil {
		return err
	}
	store, err := collector.OpenStore(c.Spool)
	if err != nil {
		return err
	}
	defer store.Close()
	creds := make(map[string]collector.Credential, len(c.Sources))
	for _, s := range c.Sources {
		key := s.APIKey
		if s.APIKeyEnv != "" {
			key = os.Getenv(s.APIKeyEnv)
		}
		creds[s.ID] = collector.Credential{APIKey: key}
	}
	interval := time.Second
	if c.PollInterval != "" {
		interval, err = time.ParseDuration(c.PollInterval)
		if err != nil {
			return err
		}
		if interval < 100*time.Millisecond {
			return errors.New("poll_interval must be at least 100ms")
		}
	}
	ctx, cancel := lifecycle.NotifyContext(context.Background())
	defer cancel()
	go watchStopFile(ctx.Done(), cancel, stopPath)
	go heartbeat(statusPath, os.Getpid(), start, len(c.Sources), ctx.Done())
	go (&collector.Sender{Store: store, Endpoint: c.Endpoint, Sources: creds}).Run(ctx)
	log.Printf("tuba-collector started with %d source(s)", len(c.Sources))
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		for _, source := range c.Sources {
			if err := source.Poll(ctx, store); err != nil && ctx.Err() == nil {
				log.Printf("source %s paused: %v", source.ID, err)
			}
		}
		select {
		case <-ctx.Done():
			log.Print("tuba-collector stopped")
			return nil
		case <-tick.C:
		}
	}
}

func validateConfig(c *config) error {
	seen := map[string]bool{}
	for i := range c.Sources {
		s := &c.Sources[i]
		if s.ID == "" || s.ContextID == "" || s.StreamID == "" || s.Pattern == "" {
			return fmt.Errorf("source %d is missing source_instance_id, source_context_id, stream_id, or path_glob", i)
		}
		if seen[s.ID] {
			return fmt.Errorf("duplicate source_instance_id %s", s.ID)
		}
		seen[s.ID] = true
		if err := s.Filter.Validate(); err != nil {
			return fmt.Errorf("source %s filter: %w", s.ID, err)
		}
		if s.Filter.Mode == "drop" && !s.AllowDrop {
			return fmt.Errorf("source %s uses drop filtering without explicit allow_drop: true", s.ID)
		}
		if s.APIKeyEnv != "" {
			s.APIKey = os.Getenv(s.APIKeyEnv)
		}
		if s.APIKey == "" {
			return fmt.Errorf("source %s has no API key configured", s.ID)
		}
	}
	return nil
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func printUsage() { fmt.Println("tuba-collector <start|stop|restart|status|logs|run> [--config PATH]") }
