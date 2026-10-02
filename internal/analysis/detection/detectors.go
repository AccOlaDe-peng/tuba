// Reference/diagnostic mirror of the F05 detection scenarios. The
// authoritative production implementation is Python tuba_analysis/
// detection.py (design baseline §6: the only official scheduling entry is
// the Python analysis worker). These detectors mirror the same definitions
// over closed feature records — failure-then-success, short-window failure
// burst (both cold-start-allowed deterministic rules), and statistical
// baseline deviation (z-score / 3-sigma over moments.v1 statistics, which
// never scores a non-ready baseline) — so diagnostics reproduce the exact
// production verdicts. There is no worker, topic, or database wiring here.
package detection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"

	"tuba/product/internal/analysis/baseline"
	"tuba/product/internal/analysis/feature"
)

const (
	// RuleFailureThenSuccess identifies the failure-burst-then-success rule.
	RuleFailureThenSuccess = "auth.failure-then-success"
	// RuleFailureBurst identifies the short-window failure concentration rule.
	RuleFailureBurst = "auth.failure-burst"
	// RuleBaselineDeviation identifies the statistical baseline scenario.
	RuleBaselineDeviation = "auth.baseline-deviation"
	// RuleVersionV1 is the immutable first-phase rule version.
	RuleVersionV1 = "1.0.0"
	// DefaultZThreshold is the 3-sigma deviation threshold.
	DefaultZThreshold = 3.0
)

var (
	ErrInvalidDetectorConfig  = errors.New("invalid detector configuration")
	ErrBaselineNotReady       = errors.New("baseline model is not ready; statistical detection is skipped, never fabricated")
	ErrFeatureVersionMismatch = errors.New("feature version mismatch between record and model")
	ErrMissingTrainedFeature  = errors.New("feature record misses a trained feature")
	ErrEmptyStatistics        = errors.New("ready baseline requires trained feature statistics")
)

func findingID(parts ...string) string {
	sum := sha256.Sum256([]byte(joinParts(parts)))
	return "anom:" + hex.EncodeToString(sum[:])
}

func joinParts(parts []string) string {
	out := parts[0]
	for _, part := range parts[1:] {
		out += "|" + part
	}
	return out
}

// FailureThenSuccessDetector fires when the window contains at least one
// failure-then-success sequence after at least MinFailures failures
// (cold-start allowed deterministic rule).
type FailureThenSuccessDetector struct {
	MinFailures int
	Severity    string
	Generation  string
}

// Detect implements Detector; the model is ignored (pure rule).
func (d FailureThenSuccessDetector) Detect(record feature.Record, _ *baseline.Model) ([]Finding, error) {
	if d.MinFailures < 1 {
		return nil, fmt.Errorf("%w: min failures must be positive", ErrInvalidDetectorConfig)
	}
	sequences := record.Values[feature.FeatureAuthFailureThenSuccessCount]
	failures := record.Values[feature.FeatureAuthFailureCount]
	if sequences < 1 || failures < float64(d.MinFailures) {
		return nil, nil
	}
	finding := Finding{
		ID:          findingID(record.EntityID, record.Window.Start.UTC().Format("2006-01-02T15:04:05Z07:00"), RuleFailureThenSuccess+"@"+RuleVersionV1, generationOr(d.Generation)),
		EntityID:    record.EntityID,
		RuleID:      RuleFailureThenSuccess,
		RuleVersion: RuleVersionV1,
		Generation:  generationOr(d.Generation),
		Severity:    severityOr(d.Severity, "high"),
		Score:       1.0,
		Explanation: fmt.Sprintf("%.0f failed logins followed by a successful login in the window", failures),
		ReasonCodes: []string{"AUTH_FAILURE_BURST_THEN_SUCCESS"},
		Threshold:   fmt.Sprintf("failure_count >= %d and failure_then_success_count >= 1", d.MinFailures),
		Features: map[string]float64{
			feature.FeatureAuthFailureCount:            failures,
			feature.FeatureAuthFailureThenSuccessCount: sequences,
		},
		Evidence: record.Inputs,
		Window:   record.Window,
		At:       record.Window.End,
	}
	return []Finding{finding}, finding.Validate()
}

// FailureBurstDetector fires when the window failure count reaches the
// threshold (cold-start allowed deterministic rule).
type FailureBurstDetector struct {
	Threshold int
	Severity  string
	// Generation is the finding business-key generation (F06); empty defaults to DefaultGeneration.
	Generation string
}

// Detect implements Detector; the model is ignored (pure rule).
func (d FailureBurstDetector) Detect(record feature.Record, _ *baseline.Model) ([]Finding, error) {
	if d.Threshold < 1 {
		return nil, fmt.Errorf("%w: threshold must be positive", ErrInvalidDetectorConfig)
	}
	failures := record.Values[feature.FeatureAuthFailureCount]
	if failures < float64(d.Threshold) {
		return nil, nil
	}
	finding := Finding{
		ID:          findingID(record.EntityID, record.Window.Start.UTC().Format("2006-01-02T15:04:05Z07:00"), RuleFailureBurst+"@"+RuleVersionV1, generationOr(d.Generation)),
		EntityID:    record.EntityID,
		RuleID:      RuleFailureBurst,
		RuleVersion: RuleVersionV1,
		Generation:  generationOr(d.Generation),
		Severity:    severityOr(d.Severity, "medium"),
		Score:       1.0,
		Explanation: fmt.Sprintf("%.0f failed logins within %s (threshold %d)", failures, record.Window.End.Sub(record.Window.Start), d.Threshold),
		ReasonCodes: []string{"AUTH_FAILURE_BURST"},
		Threshold:   fmt.Sprintf("failure_count >= %d per %s", d.Threshold, record.Window.End.Sub(record.Window.Start)),
		Features: map[string]float64{
			feature.FeatureAuthFailureCount: failures,
			feature.FeatureAuthFailureRate:  record.Values[feature.FeatureAuthFailureRate],
		},
		Evidence: record.Inputs,
		Window:   record.Window,
		At:       record.Window.End,
	}
	return []Finding{finding}, finding.Validate()
}

// BaselineDeviationDetector scores a closed window against the trained
// moments.v1 statistics of a ready baseline model (z-score / 3-sigma). A
// baseline that is not ready never scores: Detect fails closed with
// ErrBaselineNotReady instead of fabricating a verdict.
type BaselineDeviationDetector struct {
	Stats      baseline.Statistics
	ZThreshold float64
	Severity   string
	// Generation is the finding business-key generation (F06); empty defaults to DefaultGeneration.
	Generation string
}

// Detect implements Detector. The model must be non-nil and ready.
func (d BaselineDeviationDetector) Detect(record feature.Record, model *baseline.Model) ([]Finding, error) {
	if d.ZThreshold <= 0 || math.IsNaN(d.ZThreshold) || math.IsInf(d.ZThreshold, 0) {
		return nil, fmt.Errorf("%w: z threshold must be a positive finite number", ErrInvalidDetectorConfig)
	}
	if model == nil || model.Status != baseline.StatusReady {
		return nil, ErrBaselineNotReady
	}
	if model.FeatureVersion != record.FeatureVersion {
		return nil, fmt.Errorf("%w: record %s, model %s", ErrFeatureVersionMismatch, record.FeatureVersion, model.FeatureVersion)
	}
	if len(d.Stats.FeatureStats) == 0 {
		return nil, ErrEmptyStatistics
	}
	names := make([]string, 0, len(d.Stats.FeatureStats))
	for name := range d.Stats.FeatureStats {
		names = append(names, name)
	}
	sort.Strings(names)
	breaches := map[string]float64{}
	worst := 0.0
	for _, name := range names {
		observed, ok := record.Values[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrMissingTrainedFeature, name)
		}
		stats := d.Stats.FeatureStats[name]
		var z float64
		switch {
		case stats.Std == 0 && observed == stats.Mean:
			z = 0
		case stats.Std == 0:
			z = math.Inf(1)
		default:
			z = math.Abs(observed-stats.Mean) / stats.Std
		}
		if z >= d.ZThreshold {
			breaches[name] = z
			if z > worst || math.IsInf(z, 1) && !math.IsInf(worst, 1) {
				worst = z
			}
		}
	}
	if len(breaches) == 0 {
		return nil, nil
	}
	score := 1.0
	if !math.IsInf(worst, 1) {
		score = math.Min(1.0, worst/(2*d.ZThreshold))
	}
	finding := Finding{
		ID: findingID(
			record.EntityID,
			record.Window.Start.UTC().Format("2006-01-02T15:04:05Z07:00"),
			RuleBaselineDeviation+"@"+RuleVersionV1,
			generationOr(d.Generation),
			model.ID+"@"+model.Version,
		),
		EntityID:     record.EntityID,
		RuleID:       RuleBaselineDeviation,
		RuleVersion:  RuleVersionV1,
		Generation:   generationOr(d.Generation),
		Severity:     severityOr(d.Severity, "medium"),
		Score:        score,
		Explanation:  deviationExplanation(breaches, d.Stats, model),
		ReasonCodes:  []string{"AUTH_BASELINE_DEVIATION"},
		Threshold:    fmt.Sprintf("abs_z >= %g", d.ZThreshold),
		Features:     breaches,
		ModelID:      model.ID,
		ModelVersion: model.Version,
		Evidence:     record.Inputs,
		Window:       record.Window,
		At:           record.Window.End,
	}
	return []Finding{finding}, finding.Validate()
}

func deviationExplanation(breaches map[string]float64, stats baseline.Statistics, model *baseline.Model) string {
	names := make([]string, 0, len(breaches))
	for name := range breaches {
		names = append(names, name)
	}
	sort.Strings(names)
	out := fmt.Sprintf("feature values deviate from baseline %s@%s:", model.ID, model.Version)
	for _, name := range names {
		z := breaches[name]
		zText := "inf"
		if !math.IsInf(z, 1) {
			zText = fmt.Sprintf("%.2f", z)
		}
		out += fmt.Sprintf(" %s (|z|=%s, mean=%g, std=%g);", name, zText, stats.FeatureStats[name].Mean, stats.FeatureStats[name].Std)
	}
	return out
}

func generationOr(value string) string {
	if value == "" {
		return DefaultGeneration
	}
	return value
}

func severityOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
