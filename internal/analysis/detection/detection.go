// Package detection is the top layer of the analysis module DAG
// (feature <- baseline <- detection). A Detector evaluates one bounded
// feature window, optionally against a baseline model, and emits explained
// findings. Concrete detectors are F05 scope; this package carries only the
// framework types. It may import feature and baseline but must not import
// registry.
package detection

import (
	"errors"
	"time"

	"tuba/product/internal/analysis/baseline"
	"tuba/product/internal/analysis/feature"
)

var (
	ErrEmptyFindingRef = errors.New("finding id, entity id, rule id, and rule version are required")
	ErrInvalidScore    = errors.New("finding score must be within [0, 1]")
)

// Finding is one explained detection output for one entity.
type Finding struct {
	ID          string    `json:"id"`
	EntityID    string    `json:"entity_id"`
	RuleID      string    `json:"rule_id"`
	RuleVersion string    `json:"rule_version"`
	Severity    string    `json:"severity"`
	Score       float64   `json:"score"`
	Evidence    []string  `json:"evidence"`
	At          time.Time `json:"at"`
}

// Validate rejects malformed findings fail-closed.
func (f Finding) Validate() error {
	if f.ID == "" || f.EntityID == "" || f.RuleID == "" || f.RuleVersion == "" {
		return ErrEmptyFindingRef
	}
	if f.Score < 0 || f.Score > 1 {
		return ErrInvalidScore
	}
	return nil
}

// Detector evaluates one bounded feature window. A nil model means the
// detector is purely rule-based; a non-ready baseline is passed through as
// declared (cold-start handling is the detector's contract, never silent).
// Implementations must be deterministic over the same window and values.
type Detector interface {
	Detect(window feature.Window, values feature.Values, model *baseline.Model) ([]Finding, error)
}
