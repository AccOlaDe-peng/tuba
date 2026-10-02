package control

import (
	"strings"
	"testing"
	"time"

	"tuba/product/internal/auth"
)

func TestFeedbackLabelMapping(t *testing.T) {
	cases := map[string]string{
		"true_positive":  "confirmed",
		"false_positive": "false_positive",
		"inconclusive":   "inconclusive",
		"false_negative": "",
		"unknown":        "",
	}
	for in, want := range cases {
		if got := feedbackLabel(in); got != want {
			t.Errorf("feedbackLabel(%q)=%q want %q", in, got, want)
		}
	}
}

func TestValidateAutoCaseFinding(t *testing.T) {
	validEntity := "ent:" + strings.Repeat("ab", 32)
	bucket := time.Date(2026, 10, 12, 14, 0, 0, 0, time.UTC)
	valid := AutoCaseFindingInput{EntityID: validEntity, PolicyID: "auth.failure-burst", BucketStart: bucket, AnomalyID: "anom-1", Severity: "high"}
	if err := validateAutoCaseFinding(valid); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	reject := []AutoCaseFindingInput{
		// Entity must be an R-series entity id (ent:+sha256).
		{EntityID: "user-1", PolicyID: "p", BucketStart: bucket, AnomalyID: "a", Severity: "high"},
		{EntityID: "ent:" + strings.Repeat("AB", 32), PolicyID: "p", BucketStart: bucket, AnomalyID: "a", Severity: "high"},
		// Policy id bounds.
		{EntityID: validEntity, PolicyID: "", BucketStart: bucket, AnomalyID: "a", Severity: "high"},
		{EntityID: validEntity, PolicyID: strings.Repeat("p", 257), BucketStart: bucket, AnomalyID: "a", Severity: "high"},
		// Bucket must be UTC and hour-aligned (the time_bucket contract).
		{EntityID: validEntity, PolicyID: "p", BucketStart: time.Time{}, AnomalyID: "a", Severity: "high"},
		{EntityID: validEntity, PolicyID: "p", BucketStart: bucket.Add(7 * time.Minute), AnomalyID: "a", Severity: "high"},
		{EntityID: validEntity, PolicyID: "p", BucketStart: time.Date(2026, 10, 12, 14, 0, 0, 0, time.FixedZone("x", 3600)), AnomalyID: "a", Severity: "high"},
		// Anomaly bounds.
		{EntityID: validEntity, PolicyID: "p", BucketStart: bucket, AnomalyID: "", Severity: "high"},
		{EntityID: validEntity, PolicyID: "p", BucketStart: bucket, AnomalyID: strings.Repeat("a", 257), Severity: "high"},
		// Severity enum.
		{EntityID: validEntity, PolicyID: "p", BucketStart: bucket, AnomalyID: "a", Severity: "p0"},
		// Title bound.
		{EntityID: validEntity, PolicyID: "p", BucketStart: bucket, AnomalyID: "a", Severity: "high", Title: strings.Repeat("t", 301)},
	}
	for i, in := range reject {
		if err := validateAutoCaseFinding(in); err == nil {
			t.Errorf("case %d: invalid input accepted: %+v", i, in)
		}
	}
}

func TestAutoCaseDisabledFailsClosed(t *testing.T) {
	// The default-off gate must reject before any validation or DB access:
	// a nil store pool would panic if the gate did not short-circuit.
	s := &Store{}
	in := AutoCaseFindingInput{EntityID: "ent:" + strings.Repeat("ab", 32), PolicyID: "p", BucketStart: time.Date(2026, 10, 12, 14, 0, 0, 0, time.UTC), AnomalyID: "a", Severity: "high"}
	_, _, err := s.AutoCaseForFinding(t.Context(), auth.Principal{Subject: "subject", Organization: "tenant_a"}, false, in, "req")
	if err == nil || err != ErrAutoCaseDisabled {
		t.Fatalf("disabled auto case not fail-closed: %v", err)
	}
}
