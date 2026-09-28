package collector

import (
	"encoding/json"
	"fmt"
	"strings"
)

type FilterPolicy struct {
	Version    string            `json:"version"`
	Mode       string            `json:"mode"` // keep, shadow, drop
	DropEquals map[string]string `json:"drop_equals"`
}

func (p FilterPolicy) Validate() error {
	if p.Mode != "keep" && p.Mode != "shadow" && p.Mode != "drop" {
		return fmt.Errorf("filter mode must be keep, shadow, or drop")
	}
	if p.Version == "" {
		return fmt.Errorf("filter version is required")
	}
	if len(p.DropEquals) > 256 {
		return fmt.Errorf("filter rule may contain at most 256 equality clauses")
	}
	for path := range p.DropEquals {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("filter field path cannot be empty")
		}
	}
	return nil
}

// Evaluate implements a deliberately small, deterministic equality-only rule set.
func (p FilterPolicy) Evaluate(payload []byte) (drop bool, reason string, err error) {
	if err := p.Validate(); err != nil {
		return false, "", err
	}
	if len(p.DropEquals) == 0 {
		return false, "", nil
	}
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return false, "", err
	}
	for path, want := range p.DropEquals {
		value := any(event)
		for _, part := range strings.Split(path, ".") {
			m, ok := value.(map[string]any)
			if !ok {
				value = nil
				break
			}
			value = m[part]
		}
		if got, ok := value.(string); ok && got == want {
			return p.Mode == "drop", "match:" + path + "=" + want, nil
		}
	}
	return false, "", nil
}
