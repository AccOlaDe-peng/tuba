// Reference/diagnostic mirror of the F04 baseline training pure core. The
// authoritative production implementation is Python tuba_analysis/baseline.py
// (design baseline §6: the only official scheduling entry is the Python
// analysis worker). This file mirrors the same definitions — training sample
// selection (closed, quality-qualified, same feature version/generation,
// window ended at or before the deadline watermark), the minimum-sample
// policy (14 complete days AND >= 100 samples, otherwise cold_start), and
// deterministic moments.v1 statistics/evaluation — so diagnostics reproduce
// the exact training decision of the production worker. There is no worker,
// topic, or database wiring here.
package baseline

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	// DefaultMinCompleteDays is the required number of complete UTC days of
	// persisted feature samples (design baseline §6).
	DefaultMinCompleteDays = 14
	// DefaultMinSamples is the required minimum number of persisted feature
	// samples for a training run.
	DefaultMinSamples = 100
	// StatisticsAlgorithm identifies the deterministic statistics definition.
	StatisticsAlgorithm = "moments.v1"

	// QualityQualified marks samples eligible for training.
	QualityQualified = "qualified"
	// QualityPartial marks samples excluded from training.
	QualityPartial = "partial"

	// ReasonInsufficientSamples means the sample count is below the policy.
	ReasonInsufficientSamples = "insufficient_samples"
	// ReasonInsufficientCompleteDays means the observation window is short.
	ReasonInsufficientCompleteDays = "insufficient_complete_days"
	// ReasonOK means the policy is satisfied and training may proceed.
	ReasonOK = "ok"
)

var (
	ErrInvalidSample = errors.New("invalid feature sample")
	ErrInvalidPolicy = errors.New("invalid training policy")
	ErrEmptyInput    = errors.New("training requires at least one sample")
)

// Sample is one persisted, closed, windowed feature record (training input).
type Sample struct {
	EntityID       string
	FeatureVersion string
	Generation     string
	WindowStart    time.Time
	WindowEnd      time.Time
	Revision       int
	Quality        string
	Values         map[string]float64
	Inputs         []string
}

// Validate enforces the sample contract fail-closed.
func (s Sample) Validate() error {
	if s.EntityID == "" || s.FeatureVersion == "" || s.Generation == "" {
		return fmt.Errorf("%w: entity id, feature version, and generation are required", ErrInvalidSample)
	}
	if s.Revision < 1 {
		return fmt.Errorf("%w: revision must be >= 1", ErrInvalidSample)
	}
	if s.Quality != QualityQualified && s.Quality != QualityPartial {
		return fmt.Errorf("%w: unknown quality %q", ErrInvalidSample, s.Quality)
	}
	if !s.WindowEnd.After(s.WindowStart) {
		return fmt.Errorf("%w: window end must be after its start", ErrInvalidSample)
	}
	return nil
}

// TrainingPolicy is the minimum-sample policy; a run that cannot meet it
// stays cold_start (only deterministic rules that explicitly allow cold
// start may run).
type TrainingPolicy struct {
	MinCompleteDays int
	MinSamples      int
}

// DefaultTrainingPolicy returns the design-baseline policy.
func DefaultTrainingPolicy() TrainingPolicy {
	return TrainingPolicy{MinCompleteDays: DefaultMinCompleteDays, MinSamples: DefaultMinSamples}
}

// Validate enforces positive bounds.
func (p TrainingPolicy) Validate() error {
	if p.MinCompleteDays < 1 || p.MinSamples < 1 {
		return ErrInvalidPolicy
	}
	return nil
}

// SelectSamples computes the training input set of one run, fail-closed:
// only samples of the same feature version/generation that are
// quality-qualified and whose window ended at or before the deadline are
// eligible; the latest revision of each (entity, window) business key wins;
// the result is sorted deterministically by (window start, entity id).
func SelectSamples(samples []Sample, deadline time.Time, featureVersion, generation string) ([]Sample, error) {
	type key struct {
		entity string
		start  time.Time
	}
	latest := map[key]Sample{}
	for _, sample := range samples {
		if err := sample.Validate(); err != nil {
			return nil, err
		}
		if sample.FeatureVersion != featureVersion || sample.Generation != generation {
			continue
		}
		if sample.Quality != QualityQualified {
			continue
		}
		if sample.WindowEnd.After(deadline) {
			continue
		}
		k := key{sample.EntityID, sample.WindowStart}
		if existing, ok := latest[k]; !ok || sample.Revision > existing.Revision {
			latest[k] = sample
		}
	}
	out := make([]Sample, 0, len(latest))
	for _, sample := range latest {
		out = append(out, sample)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].WindowStart.Equal(out[j].WindowStart) {
			return out[i].WindowStart.Before(out[j].WindowStart)
		}
		return out[i].EntityID < out[j].EntityID
	})
	return out, nil
}

// CompleteDays counts distinct UTC calendar days with samples whose entire
// [00:00Z, next 00:00Z) span ends at or before the deadline; the final
// partial day never counts.
func CompleteDays(samples []Sample, deadline time.Time) int {
	days := map[time.Time]bool{}
	for _, sample := range samples {
		start := sample.WindowStart.UTC()
		day := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
		days[day] = true
	}
	count := 0
	for day := range days {
		if !day.AddDate(0, 0, 1).After(deadline) {
			count++
		}
	}
	return count
}

// Decision is the trainable vs cold_start outcome with an auditable reason.
type Decision struct {
	Trainable    bool
	Reason       string
	SampleCount  int
	CompleteDays int
}

// Decide applies the training policy.
func Decide(samples []Sample, deadline time.Time, policy TrainingPolicy) (Decision, error) {
	if err := policy.Validate(); err != nil {
		return Decision{}, err
	}
	days := CompleteDays(samples, deadline)
	decision := Decision{SampleCount: len(samples), CompleteDays: days}
	switch {
	case len(samples) < policy.MinSamples:
		decision.Reason = ReasonInsufficientSamples
	case days < policy.MinCompleteDays:
		decision.Reason = ReasonInsufficientCompleteDays
	default:
		decision.Trainable = true
		decision.Reason = ReasonOK
	}
	return decision, nil
}

// FeatureStats holds the population moments of one feature.
type FeatureStats struct {
	Count int
	Mean  float64
	Std   float64
	Min   float64
	Max   float64
}

// Statistics is the deterministic trained content of a model version.
type Statistics struct {
	Algorithm    string
	FeatureStats map[string]FeatureStats
}

// Train computes deterministic per-feature population statistics.
func Train(samples []Sample) (Statistics, error) {
	if len(samples) == 0 {
		return Statistics{}, ErrEmptyInput
	}
	series := map[string][]float64{}
	for _, sample := range samples {
		for name, value := range sample.Values {
			series[name] = append(series[name], value)
		}
	}
	stats := Statistics{Algorithm: StatisticsAlgorithm, FeatureStats: map[string]FeatureStats{}}
	for name, values := range series {
		var sum float64
		for _, value := range values {
			sum += value
		}
		mean := sum / float64(len(values))
		var squared float64
		min, max := values[0], values[0]
		for _, value := range values {
			squared += (value - mean) * (value - mean)
			if value < min {
				min = value
			}
			if value > max {
				max = value
			}
		}
		stats.FeatureStats[name] = FeatureStats{
			Count: len(values),
			Mean:  mean,
			Std:   math.Sqrt(squared / float64(len(values))),
			Min:   min,
			Max:   max,
		}
	}
	return stats, nil
}

// FeatureEval is the in-sample fit of one feature.
type FeatureEval struct {
	MaxAbsZ           float64
	Within3SigmaRatio float64
}

// Evaluation is published with the model version (design baseline §6:
// publication carries training cutoff, sample range, evaluation metrics).
type Evaluation struct {
	Algorithm   string
	SampleCount int
	EntityCount int
	PerFeature  map[string]FeatureEval
}

// Evaluate computes deterministic in-sample fit metrics: the maximum
// absolute z-score of the training samples and the share within 3 standard
// deviations (a zero-variance feature scores an equal value as z=0).
func Evaluate(stats Statistics, samples []Sample) (Evaluation, error) {
	if len(stats.FeatureStats) == 0 {
		return Evaluation{}, fmt.Errorf("%w: trained feature statistics", ErrEmptyInput)
	}
	if len(samples) == 0 {
		return Evaluation{}, ErrEmptyInput
	}
	eval := Evaluation{
		Algorithm:   stats.Algorithm,
		SampleCount: len(samples),
		EntityCount: len(distinctEntities(samples)),
		PerFeature:  map[string]FeatureEval{},
	}
	names := make([]string, 0, len(stats.FeatureStats))
	for name := range stats.FeatureStats {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := stats.FeatureStats[name]
		var maxZ float64
		within := 0
		observed := 0
		for _, sample := range samples {
			value, ok := sample.Values[name]
			if !ok {
				continue
			}
			observed++
			var z float64
			if entry.Std == 0 {
				if value != entry.Mean {
					z = math.Inf(1)
				}
			} else {
				z = math.Abs(value-entry.Mean) / entry.Std
			}
			if z > maxZ {
				maxZ = z
			}
			if z <= 3.0 {
				within++
			}
		}
		if observed == 0 {
			return Evaluation{}, fmt.Errorf("%w: evaluation sample misses feature %q", ErrEmptyInput, name)
		}
		eval.PerFeature[name] = FeatureEval{
			MaxAbsZ:           maxZ,
			Within3SigmaRatio: float64(within) / float64(observed),
		}
	}
	return eval, nil
}

func distinctEntities(samples []Sample) map[string]bool {
	seen := map[string]bool{}
	for _, sample := range samples {
		seen[sample.EntityID] = true
	}
	return seen
}
