package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Store persists the agent identity and the last effective configuration in a
// local state directory. Both files are written 0600: the credential is a
// bearer secret and the configuration may reference internal endpoints. While
// the management plane is offline the agent keeps operating from the last
// configuration stored here.
type Store struct {
	Dir string
}

// State is the enrolled identity plus the locally observed agent status.
type State struct {
	CollectorID string    `json:"collector_id"`
	Credential  string    `json:"credential"`
	EnrolledAt  time.Time `json:"enrolled_at"`
	Disabled    bool      `json:"disabled,omitempty"`
}

// StoredConfig is the last configuration the agent accepted, with its version.
type StoredConfig struct {
	Version       int64           `json:"version"`
	Configuration json.RawMessage `json:"configuration"`
	AppliedAt     time.Time       `json:"applied_at"`
}

func (s Store) statePath() string  { return filepath.Join(s.Dir, "agent-state.json") }
func (s Store) configPath() string { return filepath.Join(s.Dir, "config.current.json") }

// LoadState returns the enrolled identity. os.ErrNotExist is returned wrapped
// when the agent has not enrolled yet.
func (s Store) LoadState() (State, error) {
	var st State
	raw, err := os.ReadFile(s.statePath())
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, fmt.Errorf("decode agent state: %w", err)
	}
	if !strings.HasPrefix(st.CollectorID, "col_") || !strings.HasPrefix(st.Credential, "tuba_col_") {
		return st, errors.New("agent state file is malformed")
	}
	return st, nil
}

func (s Store) SaveState(st State) error {
	if !strings.HasPrefix(st.CollectorID, "col_") || !strings.HasPrefix(st.Credential, "tuba_col_") {
		return errors.New("refusing to persist a malformed agent identity")
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writePrivateFile(s.statePath(), raw)
}

// LoadConfig returns the last accepted configuration, or os.ErrNotExist when
// none has ever been applied.
func (s Store) LoadConfig() (StoredConfig, error) {
	var cfg StoredConfig
	raw, err := os.ReadFile(s.configPath())
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("decode stored config: %w", err)
	}
	if cfg.Version <= 0 || len(cfg.Configuration) == 0 {
		return cfg, errors.New("stored config file is malformed")
	}
	return cfg, nil
}

func (s Store) SaveConfig(cfg StoredConfig) error {
	if cfg.Version <= 0 {
		return errors.New("refusing to persist a config with a non-positive version")
	}
	if err := ValidateRemoteConfiguration(cfg.Configuration); err != nil {
		return fmt.Errorf("refusing to persist config: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return writePrivateFile(s.configPath(), raw)
}

func writePrivateFile(path string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, content, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// ValidateRemoteConfiguration is the agent-side second check for management
// configuration: it must be a JSON object within the size cap and must not
// contain executable directives or inline secrets. The server applies the
// same rules (internal/control.containsForbiddenRemoteConfig); the agent
// re-checks because the control-plane design requires both sides to validate.
func ValidateRemoteConfiguration(raw json.RawMessage) error {
	if len(raw) == 0 || len(raw) > maxConfigBytes {
		return fmt.Errorf("remote configuration must be 1..%d bytes", maxConfigBytes)
	}
	var obj map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&obj); err != nil || obj == nil {
		return errors.New("remote configuration must be a JSON object")
	}
	if containsForbiddenDirective(obj) {
		return errors.New("remote configuration contains executable directives or inline secrets")
	}
	return nil
}

func containsForbiddenDirective(value any) bool {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			name := strings.ToLower(strings.TrimSpace(key))
			normalized := strings.NewReplacer("_", "", "-", "").Replace(name)
			if normalized == "command" || normalized == "commands" || normalized == "shell" || normalized == "exec" || normalized == "executable" || normalized == "script" {
				return true
			}
			if strings.HasSuffix(name, "_env") {
				// Environment references are allowed only as plain variable names.
				ref, ok := child.(string)
				if !ok || !validEnvironmentReference(ref) {
					return true
				}
				continue
			}
			if strings.Contains(normalized, "password") || strings.Contains(normalized, "secret") || strings.Contains(normalized, "token") || normalized == "apikey" || normalized == "accesskey" || normalized == "credential" || normalized == "credentials" || normalized == "privatekey" || normalized == "authorization" || normalized == "bearer" {
				return true
			}
			if containsForbiddenDirective(child) {
				return true
			}
		}
	case []any:
		for _, child := range node {
			if containsForbiddenDirective(child) {
				return true
			}
		}
	}
	return false
}

func validEnvironmentReference(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i, r := range value {
		if i == 0 {
			if !(r == '_' || r >= 'A' && r <= 'Z') {
				return false
			}
		} else if !(r == '_' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
