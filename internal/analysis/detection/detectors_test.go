package detection

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"tuba/product/internal/analysis/baseline"
	"tuba/product/internal/analysis/feature"
)

// goldenRecord mirrors the F03 golden vector: window
// [2026-10-12T00:00Z, 00:10Z), 7 contributions — attempts=7, failures=4,
// rate=4/7, devices=2, ips=2, failure-then-success=2. The Python tests in
// tests/test_detection.py lock the same verdicts.
func goldenRecord(t *testing.T) feature.Record {
	t.Helper()
	start := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	window := feature.Window{Start: start, End: start.Add(10 * time.Minute), AllowedLateness: 10 * time.Minute}
	return feature.Record{
		EntityID:       "ent:u1",
		Window:         window,
		FeatureVersion: feature.AuthFeatureVersionV1,
		Revision:       1,
		Values: feature.Values{
			feature.FeatureAuthAttemptCount:            7,
			feature.FeatureAuthFailureCount:            4,
			feature.FeatureAuthFailureRate:             4.0 / 7.0,
			feature.FeatureAuthSourceDeviceCount:       2,
			feature.FeatureAuthSourceIPCount:           2,
			feature.FeatureAuthFailureThenSuccessCount: 2,
		},
		Inputs: []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7"},
	}
}

func assertFiveElements(t *testing.T, finding Finding, requireModel bool) {
	t.Helper()
	if err := finding.Validate(); err != nil {
		t.Fatalf("finding misses a mandatory element: %v", err)
	}
	if requireModel && (finding.ModelID == "" || finding.ModelVersion == "") {
		t.Fatal("statistical finding requires the model id and version")
	}
}

func TestFailureThenSuccessDetector(t *testing.T) {
	record := goldenRecord(t)
	fires, err := FailureThenSuccessDetector{MinFailures: 3}.Detect(record, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(fires) != 1 {
		t.Fatalf("expected one finding, got %d", len(fires))
	}
	assertFiveElements(t, fires[0], false)
	if fires[0].RuleID != RuleFailureThenSuccess || fires[0].RuleVersion != RuleVersionV1 {
		t.Fatalf("rule identity: %+v", fires[0])
	}
	quiet, err := FailureThenSuccessDetector{MinFailures: 5}.Detect(record, nil)
	if err != nil || len(quiet) != 0 {
		t.Fatalf("below threshold must not fire: findings=%d err=%v", len(quiet), err)
	}
	if _, err := (FailureThenSuccessDetector{MinFailures: 0}).Detect(record, nil); err == nil {
		t.Fatal("invalid config must fail closed")
	}
}

func TestFailureBurstDetector(t *testing.T) {
	record := goldenRecord(t)
	fires, err := FailureBurstDetector{Threshold: 4}.Detect(record, nil)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(fires) != 1 {
		t.Fatalf("expected one finding, got %d", len(fires))
	}
	assertFiveElements(t, fires[0], false)
	quiet, err := FailureBurstDetector{Threshold: 5}.Detect(record, nil)
	if err != nil || len(quiet) != 0 {
		t.Fatalf("below threshold must not fire: findings=%d err=%v", len(quiet), err)
	}
	if _, err := (FailureBurstDetector{Threshold: 0}).Detect(record, nil); err == nil {
		t.Fatal("invalid config must fail closed")
	}
}

func baselineStats() baseline.Statistics {
	return baseline.Statistics{
		Algorithm: baseline.StatisticsAlgorithm,
		FeatureStats: map[string]baseline.FeatureStats{
			feature.FeatureAuthFailureCount: {Count: 100, Mean: 4, Std: 2, Min: 0, Max: 9},
			feature.FeatureAuthAttemptCount: {Count: 100, Mean: 7, Std: 0.5, Min: 5, Max: 9},
		},
	}
}

func readyModel(status baseline.Status) *baseline.Model {
	return &baseline.Model{
		ID:             "auth-baseline",
		Version:        "1.0.0",
		FeatureID:      "auth.window",
		FeatureVersion: feature.AuthFeatureVersionV1,
		Status:         status,
		Samples:        100,
		TrainedAt:      time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC),
	}
}

func TestBaselineDeviationDetector(t *testing.T) {
	record := goldenRecord(t)
	model := readyModel(baseline.StatusReady)

	// At the mean: no finding, but scoring happened without error.
	quiet, err := BaselineDeviationDetector{Stats: baselineStats(), ZThreshold: DefaultZThreshold}.Detect(record, model)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(quiet) != 0 {
		t.Fatalf("in-baseline record must not fire: %d", len(quiet))
	}

	// failures=10 -> |z| = |10-4|/2 = 3 -> fires at exactly 3 sigma.
	deviated := record
	deviated.Values = feature.Values{
		feature.FeatureAuthFailureCount: 10,
		feature.FeatureAuthAttemptCount: 7,
	}
	fires, err := BaselineDeviationDetector{Stats: baselineStats(), ZThreshold: DefaultZThreshold}.Detect(deviated, model)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if len(fires) != 1 {
		t.Fatalf("expected one finding, got %d", len(fires))
	}
	finding := fires[0]
	assertFiveElements(t, finding, true)
	if finding.ModelID != "auth-baseline" || finding.ModelVersion != "1.0.0" {
		t.Fatalf("model reference missing: %+v", finding)
	}
	if finding.Features[feature.FeatureAuthFailureCount] != 3.0 {
		t.Fatalf("contributing feature z-score: %+v", finding.Features)
	}
	if finding.Score != 0.5 {
		t.Fatalf("score: %v", finding.Score)
	}

	// Zero variance: equal value is z=0, any deviation is infinite.
	zeroVar := baseline.Statistics{
		Algorithm: baseline.StatisticsAlgorithm,
		FeatureStats: map[string]baseline.FeatureStats{
			feature.FeatureAuthSourceDeviceCount: {Count: 100, Mean: 2, Std: 0, Min: 2, Max: 2},
		},
	}
	zeroRecord := record
	zeroRecord.Values = feature.Values{feature.FeatureAuthSourceDeviceCount: 3}
	infFire, err := BaselineDeviationDetector{Stats: zeroVar, ZThreshold: DefaultZThreshold}.Detect(zeroRecord, model)
	if err != nil || len(infFire) != 1 {
		t.Fatalf("zero-variance deviation must fire: findings=%d err=%v", len(infFire), err)
	}
	if !math.IsInf(infFire[0].Features[feature.FeatureAuthSourceDeviceCount], 1) || infFire[0].Score != 1.0 {
		t.Fatalf("zero-variance z semantics: %+v", infFire[0])
	}
}

func TestBaselineDeviationNeverScoresColdStart(t *testing.T) {
	record := goldenRecord(t)
	detector := BaselineDeviationDetector{Stats: baselineStats(), ZThreshold: DefaultZThreshold}
	for _, status := range []baseline.Status{baseline.StatusColdStart, baseline.StatusTraining, baseline.StatusRetired} {
		if _, err := detector.Detect(record, readyModel(status)); !errors.Is(err, ErrBaselineNotReady) {
			t.Fatalf("status %s must fail closed: %v", status, err)
		}
	}
	if _, err := detector.Detect(record, nil); !errors.Is(err, ErrBaselineNotReady) {
		t.Fatalf("nil model must fail closed: %v", err)
	}
	mismatched := readyModel(baseline.StatusReady)
	mismatched.FeatureVersion = "2.0.0"
	if _, err := detector.Detect(record, mismatched); !errors.Is(err, ErrFeatureVersionMismatch) {
		t.Fatalf("version mismatch must fail closed: %v", err)
	}
	if _, err := (BaselineDeviationDetector{Stats: baseline.Statistics{}, ZThreshold: 3}).Detect(record, readyModel(baseline.StatusReady)); !errors.Is(err, ErrEmptyStatistics) {
		t.Fatalf("empty statistics must fail closed: %v", err)
	}
	missing := record
	missing.Values = feature.Values{feature.FeatureAuthAttemptCount: 7}
	if _, err := detector.Detect(missing, readyModel(baseline.StatusReady)); !errors.Is(err, ErrMissingTrainedFeature) {
		t.Fatalf("missing trained feature must fail closed: %v", err)
	}
	if _, err := (BaselineDeviationDetector{Stats: baselineStats(), ZThreshold: 0}).Detect(record, readyModel(baseline.StatusReady)); err == nil {
		t.Fatal("invalid z threshold must fail closed")
	}
}

func TestFindingDeterminismAndCompleteness(t *testing.T) {
	record := goldenRecord(t)
	first, err := FailureBurstDetector{Threshold: 4}.Detect(record, nil)
	if err != nil || len(first) != 1 {
		t.Fatalf("detect: %v %d", err, len(first))
	}
	second, err := FailureBurstDetector{Threshold: 4}.Detect(record, nil)
	if err != nil || !reflect.DeepEqual(first[0], second[0]) {
		t.Fatalf("same record must yield the same finding: %v", err)
	}
	later := record
	later.Window.Start = later.Window.Start.Add(10 * time.Minute)
	later.Window.End = later.Window.End.Add(10 * time.Minute)
	third, err := FailureBurstDetector{Threshold: 4}.Detect(later, nil)
	if err != nil || len(third) != 1 {
		t.Fatalf("detect: %v %d", err, len(third))
	}
	if first[0].ID == third[0].ID {
		t.Fatal("different windows must not share a finding id")
	}

	// The five-element contract is fail-closed in Validate.
	broken := first[0]
	broken.Explanation = ""
	if err := broken.Validate(); !errors.Is(err, ErrMissingExplanation) {
		t.Fatalf("missing explanation must fail validation: %v", err)
	}
	broken = first[0]
	broken.Threshold = ""
	if err := broken.Validate(); !errors.Is(err, ErrMissingThreshold) {
		t.Fatalf("missing threshold must fail validation: %v", err)
	}
	broken = first[0]
	broken.Features = nil
	if err := broken.Validate(); !errors.Is(err, ErrMissingFeatures) {
		t.Fatalf("missing features must fail validation: %v", err)
	}
	broken = first[0]
	broken.Evidence = nil
	if err := broken.Validate(); !errors.Is(err, ErrMissingEvidence) {
		t.Fatalf("missing input references must fail validation: %v", err)
	}
}
