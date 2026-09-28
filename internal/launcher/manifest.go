package launcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"tuba/product/internal/config"
)

const maxManifestBytes = 1 << 20

var (
	serviceNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)
	envReference       = regexp.MustCompile(`^\$\{([A-Z_][A-Z0-9_]*)\}$`)
	sensitiveName      = regexp.MustCompile(`(?i)(PASSWORD|TOKEN|SECRET|API_KEY|PRIVATE_KEY|DATABASE_URL)$`)
	sensitiveArgument  = regexp.MustCompile(`(?i)(password|passwd|token|secret|api[_-]?key|private[_-]?key)`)
)

type Manifest struct {
	Version         int           `json:"version"`
	StateDir        string        `json:"state_dir"`
	LogDir          string        `json:"log_dir"`
	EnvironmentFile string        `json:"environment_file,omitempty"`
	Services        []ServiceSpec `json:"services"`
	path            string
	fileEnv         map[string]string
}

type ServiceSpec struct {
	Name            string            `json:"name"`
	Command         string            `json:"command"`
	Args            []string          `json:"args,omitempty"`
	WorkingDir      string            `json:"working_dir,omitempty"`
	Environment     map[string]string `json:"environment,omitempty"`
	RestartMin      string            `json:"restart_min,omitempty"`
	RestartMax      string            `json:"restart_max,omitempty"`
	restartMinDelay time.Duration
	restartMaxDelay time.Duration
	commandPath     string
	workingPath     string
	resolvedEnv     []string
}

func LoadManifest(path string) (*Manifest, error) {
	return loadManifest(path, true)
}

func LoadManifestForControl(path string) (*Manifest, error) {
	return loadManifest(path, false)
}

func loadManifest(path string, resolveServices bool) (*Manifest, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest path: %w", err)
	}
	file, err := os.Open(absPath)
	if err != nil {
		return nil, fmt.Errorf("open launcher manifest: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read launcher manifest: %w", err)
	}
	if len(data) > maxManifestBytes {
		return nil, errors.New("launcher manifest exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode launcher manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("launcher manifest must contain exactly one JSON object")
	}
	manifest.path = absPath
	if err := manifest.validate(resolveServices); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (m *Manifest) validate(resolveServices bool) error {
	if m.Version != 1 {
		return fmt.Errorf("unsupported launcher manifest version %d", m.Version)
	}
	if len(m.Services) == 0 || len(m.Services) > 128 {
		return errors.New("launcher manifest must define between 1 and 128 services")
	}
	base := filepath.Dir(m.path)
	var err error
	if m.StateDir, err = resolvePath(base, m.StateDir, "state_dir"); err != nil {
		return err
	}
	if m.LogDir, err = resolvePath(base, m.LogDir, "log_dir"); err != nil {
		return err
	}
	m.fileEnv = map[string]string{}
	if resolveServices && m.EnvironmentFile != "" {
		m.EnvironmentFile, err = resolvePath(base, m.EnvironmentFile, "environment_file")
		if err != nil {
			return err
		}
		m.fileEnv, err = loadEnvironmentFile(m.EnvironmentFile)
		if err != nil {
			return err
		}
	}
	seen := make(map[string]bool, len(m.Services))
	for i := range m.Services {
		s := &m.Services[i]
		if !serviceNamePattern.MatchString(s.Name) || seen[s.Name] {
			return fmt.Errorf("service %d has an invalid or duplicate name", i)
		}
		seen[s.Name] = true
		if !resolveServices {
			continue
		}
		if strings.TrimSpace(s.Command) == "" {
			return fmt.Errorf("service %q has no command", s.Name)
		}
		for _, arg := range s.Args {
			if sensitiveArgument.MatchString(arg) {
				return fmt.Errorf("service %q must receive credentials through environment references, not command arguments", s.Name)
			}
		}
		if filepath.IsAbs(s.Command) {
			s.commandPath = filepath.Clean(s.Command)
		} else {
			s.commandPath = filepath.Clean(filepath.Join(base, s.Command))
		}
		commandInfo, statErr := os.Stat(s.commandPath)
		if statErr != nil || commandInfo.IsDir() {
			return fmt.Errorf("service %q command is missing or not a file", s.Name)
		}
		if s.WorkingDir == "" {
			s.workingPath = filepath.Dir(s.commandPath)
		} else if s.workingPath, err = resolvePath(base, s.WorkingDir, "working_dir"); err != nil {
			return fmt.Errorf("service %q: %w", s.Name, err)
		}
		workInfo, statErr := os.Stat(s.workingPath)
		if statErr != nil || !workInfo.IsDir() {
			return fmt.Errorf("service %q working_dir is missing or not a directory", s.Name)
		}
		if s.restartMinDelay, err = parseRestartDelay(s.RestartMin, time.Second); err != nil {
			return fmt.Errorf("service %q restart_min: %w", s.Name, err)
		}
		if s.restartMaxDelay, err = parseRestartDelay(s.RestartMax, 30*time.Second); err != nil {
			return fmt.Errorf("service %q restart_max: %w", s.Name, err)
		}
		if s.restartMaxDelay < s.restartMinDelay || s.restartMaxDelay > 10*time.Minute {
			return fmt.Errorf("service %q restart_max must be between restart_min and 10m", s.Name)
		}
		s.resolvedEnv = make([]string, 0, len(s.Environment))
		resolvedEnvironment := make(map[string]string, len(s.Environment))
		for key, raw := range s.Environment {
			if !envNamePattern.MatchString(key) {
				return fmt.Errorf("service %q has an invalid environment variable name", s.Name)
			}
			if sensitiveName.MatchString(key) && !envReference.MatchString(raw) {
				return fmt.Errorf("service %q sensitive environment value %s must use an exact ${ENV_NAME} reference", s.Name, key)
			}
			value := raw
			if match := envReference.FindStringSubmatch(raw); len(match) == 2 {
				if fileValue, ok := m.fileEnv[match[1]]; ok {
					value = fileValue
				} else if processValue, ok := os.LookupEnv(match[1]); ok {
					value = processValue
				} else {
					return fmt.Errorf("service %q requires environment variable %s", s.Name, match[1])
				}
			}
			s.resolvedEnv = append(s.resolvedEnv, key+"="+value)
			resolvedEnvironment[key] = value
		}
		for listenerVariable, address := range resolvedEnvironment {
			if !isListenerEnvironmentName(listenerVariable) {
				continue
			}
			if err := config.ValidateListenerAddress(listenerVariable, address, resolvedEnvironment["TUBA_ALLOW_NON_LOOPBACK_LISTEN"]); err != nil {
				return fmt.Errorf("service %q listener configuration: %w", s.Name, err)
			}
		}
	}
	return nil
}

func isListenerEnvironmentName(name string) bool {
	return name == "HTTP_LISTEN" || name == "API_LISTEN" || name == "WEB_LISTEN" || name == "METRICS_LISTEN" || strings.HasSuffix(name, "_METRICS_LISTEN")
}

func loadEnvironmentFile(path string) (map[string]string, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("environment_file is missing or not a regular file")
	}
	if err := checkSecretFilePermissions(path, info); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("environment_file could not be read")
	}
	if len(data) > 64<<10 {
		return nil, errors.New("environment_file exceeds 64 KiB")
	}
	values := map[string]string{}
	for lineNumber, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || !envNamePattern.MatchString(key) || strings.ContainsAny(value, "\x00\r\n") {
			return nil, fmt.Errorf("environment_file has an invalid entry at line %d", lineNumber+1)
		}
		if _, duplicate := values[key]; duplicate {
			return nil, fmt.Errorf("environment_file repeats variable %s", key)
		}
		values[key] = strings.TrimSpace(value)
	}
	return values, nil
}

var envNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

func parseRestartDelay(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	delay, err := time.ParseDuration(value)
	if err != nil || delay < 100*time.Millisecond || delay > 10*time.Minute {
		return 0, errors.New("must be a duration between 100ms and 10m")
	}
	return delay, nil
}

func resolvePath(base, value, field string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if !filepath.IsAbs(value) {
		value = filepath.Join(base, value)
	}
	return filepath.Abs(filepath.Clean(value))
}
