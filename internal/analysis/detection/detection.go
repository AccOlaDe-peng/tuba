// Package detection is the top layer of the analysis module DAG
// (feature <- baseline <- detection). A Detector evaluates one closed,
// versioned feature record, optionally against a baseline model, and emits
// explained findings. The concrete first-phase detectors (failure-then-
// success, short-window failure burst, statistical baseline deviation) live
// in detectors.go; they are the reference/diagnostic mirror of the
// authoritative Python implementation in tuba_analysis/detection.py and have
// no worker, topic, or database wiring. It may import feature and baseline
// but must not import registry.
package detection

import (
	"errors"
	"time"

	"tuba/product/internal/analysis/baseline"
	"tuba/product/internal/analysis/feature"
)

var (
	ErrEmptyFindingRef    = errors.New("finding id, entity id, rule id, and rule version are required")
	ErrMissingGeneration  = errors.New("finding requires a generation (business-key part)")
	ErrInvalidRevision    = errors.New("invalid finding revision lifecycle operation")
	ErrInvalidScore       = errors.New("finding score must be within [0, 1]")
	ErrMissingExplanation = errors.New("finding requires a human-readable explanation")
	ErrMissingThreshold   = errors.New("finding requires the applied threshold")
	ErrMissingFeatures    = errors.New("finding requires contributing features")
	ErrMissingEvidence    = errors.New("finding requires input references")
)

// Finding is one explained detection output for one entity. Every finding
// carries the five mandatory elements of the design baseline (§6): a
// human-readable explanation, the applied threshold, the contributing
// features, the rule/model version, and input references (evidence event ids
// plus the bounded window). ModelID/ModelVersion are set by the statistical
// baseline detector and empty for pure rules. Generation (F06) is a
// mandatory part of the finding business key: a rule upgrade on a new
// generation never overwrites the old generation's findings.
type Finding struct {
	ID           string             `json:"id"`
	EntityID     string             `json:"entity_id"`
	RuleID       string             `json:"rule_id"`
	RuleVersion  string             `json:"rule_version"`
	Generation   string             `json:"generation"`
	Severity     string             `json:"severity"`
	Score        float64            `json:"score"`
	Explanation  string             `json:"explanation"`
	ReasonCodes  []string           `json:"reason_codes"`
	Threshold    string             `json:"threshold"`
	Features     map[string]float64 `json:"features"`
	ModelID      string             `json:"model_id,omitempty"`
	ModelVersion string             `json:"model_version,omitempty"`
	Evidence     []string           `json:"evidence"`
	Window       feature.Window     `json:"window"`
	At           time.Time          `json:"at"`
}

// Validate rejects malformed findings fail-closed, including any finding
// missing one of the five mandatory output elements.
func (f Finding) Validate() error {
	if f.ID == "" || f.EntityID == "" || f.RuleID == "" || f.RuleVersion == "" {
		return ErrEmptyFindingRef
	}
	if f.Generation == "" {
		return ErrMissingGeneration
	}
	if f.Score < 0 || f.Score > 1 {
		return ErrInvalidScore
	}
	if f.Explanation == "" || len(f.ReasonCodes) == 0 {
		return ErrMissingExplanation
	}
	if f.Threshold == "" {
		return ErrMissingThreshold
	}
	if len(f.Features) == 0 {
		return ErrMissingFeatures
	}
	if len(f.Evidence) == 0 {
		return ErrMissingEvidence
	}
	return nil
}

// Detector evaluates one closed, versioned feature record. A nil model means
// the detector is purely rule-based; a non-ready baseline is passed through
// as declared (cold-start handling is the detector's contract, never
// silent). Implementations must be deterministic over the same record.
type Detector interface {
	Detect(record feature.Record, model *baseline.Model) ([]Finding, error)
}
