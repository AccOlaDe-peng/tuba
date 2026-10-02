package baseline

import (
	"math"
	"testing"
	"time"
)

var testDeadline = time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)

func makeSample(entity string, day int, revision int, quality string, attempts float64) Sample {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, day)
	return Sample{
		EntityID:       entity,
		FeatureVersion: "1.0.0",
		Generation:     "g1",
		WindowStart:    start,
		WindowEnd:      start.Add(10 * time.Minute),
		Revision:       revision,
		Quality:        quality,
		Values: map[string]float64{
			"auth.attempt.count": attempts,
			"auth.failure.count": attempts / 2,
			"auth.failure.rate":  0.5,
		},
		Inputs: []string{"evt"},
	}
}

func TestSelectSamplesFiltersAndRevision(t *testing.T) {
	wrongVersion := makeSample("ent:a", 1, 1, QualityQualified, 4)
	wrongVersion.FeatureVersion = "2.0.0"
	wrongGeneration := makeSample("ent:a", 2, 1, QualityQualified, 4)
	wrongGeneration.Generation = "g2"
	afterDeadline := makeSample("ent:a", 44, 1, QualityQualified, 4)
	old := makeSample("ent:a", 0, 1, QualityQualified, 4)
	corrected := makeSample("ent:a", 0, 2, QualityQualified, 9)
	samples := []Sample{
		old,
		wrongVersion,
		wrongGeneration,
		makeSample("ent:a", 3, 1, QualityPartial, 4),
		afterDeadline,
		corrected,
	}
	selected, err := SelectSamples(samples, testDeadline, "1.0.0", "g1")
	if err != nil {
		t.Fatalf("SelectSamples: %v", err)
	}
	if len(selected) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(selected))
	}
	if selected[0].Revision != 2 || selected[0].Values["auth.attempt.count"] != 9 {
		t.Fatalf("latest revision must win: %+v", selected[0])
	}
}

func TestSelectSamplesDeadlineBoundary(t *testing.T) {
	edge := makeSample("ent:a", 0, 1, QualityQualified, 1)
	edge.WindowStart = testDeadline.Add(-10 * time.Minute)
	edge.WindowEnd = testDeadline
	selected, err := SelectSamples([]Sample{edge}, testDeadline, "1.0.0", "g1")
	if err != nil || len(selected) != 1 {
		t.Fatalf("window ending exactly at the deadline must be selected: %v %d", err, len(selected))
	}
	later := makeSample("ent:a", 0, 1, QualityQualified, 1)
	later.WindowStart = testDeadline
	later.WindowEnd = testDeadline.Add(10 * time.Minute)
	selected, err = SelectSamples([]Sample{later}, testDeadline, "1.0.0", "g1")
	if err != nil {
		t.Fatalf("SelectSamples: %v", err)
	}
	if len(selected) != 0 {
		t.Fatalf("window ending after the deadline must be excluded")
	}
}

func TestSelectSamplesRejectsInvalid(t *testing.T) {
	bad := makeSample("ent:a", 0, 0, QualityQualified, 4)
	if _, err := SelectSamples([]Sample{bad}, testDeadline, "1.0.0", "g1"); err == nil {
		t.Fatal("invalid sample must be rejected fail-closed")
	}
}

func TestDecidePolicy(t *testing.T) {
	policy := DefaultTrainingPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("default policy: %v", err)
	}
	// 30 days of data but only 30 samples -> cold_start (samples).
	sparse := make([]Sample, 0, 30)
	for day := 0; day < 30; day++ {
		sparse = append(sparse, makeSample("ent:a", day, 1, QualityQualified, 4))
	}
	decision, err := Decide(sparse, testDeadline, policy)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if decision.Trainable || decision.Reason != ReasonInsufficientSamples {
		t.Fatalf("expected insufficient_samples cold_start, got %+v", decision)
	}
	// >= 100 samples inside 2 days -> cold_start (complete days).
	dense := make([]Sample, 0, 120)
	for i := 0; i < 120; i++ {
		sample := makeSample("ent:a", i%2, 1, QualityQualified, 4)
		sample.WindowStart = sample.WindowStart.Add(time.Duration(i) * time.Minute)
		sample.WindowEnd = sample.WindowStart.Add(10 * time.Minute)
		dense = append(dense, sample)
	}
	decision, err = Decide(dense, testDeadline, policy)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if decision.Trainable || decision.Reason != ReasonInsufficientCompleteDays {
		t.Fatalf("expected insufficient_complete_days cold_start, got %+v", decision)
	}
	// 15 days x 8 samples = 120 samples, 15 complete days -> trainable.
	full := make([]Sample, 0, 120)
	for day := 0; day < 15; day++ {
		for slot := 0; slot < 8; slot++ {
			sample := makeSample("ent:a", day, 1, QualityQualified, 4)
			sample.WindowStart = sample.WindowStart.Add(time.Duration(slot) * time.Hour)
			sample.WindowEnd = sample.WindowStart.Add(10 * time.Minute)
			full = append(full, sample)
		}
	}
	decision, err = Decide(full, testDeadline, policy)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if !decision.Trainable || decision.Reason != ReasonOK || decision.CompleteDays != 15 {
		t.Fatalf("expected trainable with 15 complete days, got %+v", decision)
	}
}

// Golden vector shared with python/tests/test_baseline.py: attempts 2/4/6
// over three windows -> mean 4, std sqrt(8/3), min 2, max 6.
func TestTrainGoldenVector(t *testing.T) {
	samples := []Sample{
		makeSample("ent:a", 0, 1, QualityQualified, 2),
		makeSample("ent:a", 1, 1, QualityQualified, 4),
		makeSample("ent:b", 2, 1, QualityQualified, 6),
	}
	stats, err := Train(samples)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	if stats.Algorithm != StatisticsAlgorithm {
		t.Fatalf("algorithm: %q", stats.Algorithm)
	}
	attempt := stats.FeatureStats["auth.attempt.count"]
	if attempt.Count != 3 || attempt.Mean != 4 || attempt.Min != 2 || attempt.Max != 6 {
		t.Fatalf("golden stats mismatch: %+v", attempt)
	}
	if math.Abs(attempt.Std-math.Sqrt(8.0/3.0)) > 1e-12 {
		t.Fatalf("golden std mismatch: %v", attempt.Std)
	}
	if _, err := Train(nil); err == nil {
		t.Fatal("empty training input must be rejected")
	}
}

func TestEvaluateGoldenVector(t *testing.T) {
	samples := []Sample{
		makeSample("ent:a", 0, 1, QualityQualified, 2),
		makeSample("ent:a", 1, 1, QualityQualified, 4),
		makeSample("ent:b", 2, 1, QualityQualified, 6),
	}
	stats, err := Train(samples)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	eval, err := Evaluate(stats, samples)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if eval.SampleCount != 3 || eval.EntityCount != 2 {
		t.Fatalf("evaluation counts: %+v", eval)
	}
	attempt := eval.PerFeature["auth.attempt.count"]
	if math.Abs(attempt.MaxAbsZ-2.0/math.Sqrt(8.0/3.0)) > 1e-12 {
		t.Fatalf("golden max_abs_z mismatch: %v", attempt.MaxAbsZ)
	}
	if attempt.Within3SigmaRatio != 1.0 {
		t.Fatalf("within ratio: %v", attempt.Within3SigmaRatio)
	}
	// Zero variance: equal values score z=0.
	flat := []Sample{
		makeSample("ent:a", 0, 1, QualityQualified, 5),
		makeSample("ent:a", 1, 1, QualityQualified, 5),
	}
	flatStats, err := Train(flat)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	flatEval, err := Evaluate(flatStats, flat)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if flatEval.PerFeature["auth.attempt.count"].MaxAbsZ != 0 {
		t.Fatalf("zero variance must score z=0: %+v", flatEval.PerFeature["auth.attempt.count"])
	}
}
