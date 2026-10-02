package main

import (
	"strings"
	"testing"
)

// The diagnostic marking is part of the F08 contract: help text, startup log
// and machine-readable output must all mark this CLI as diagnostic-only and
// point at the Python tuba-analysis-worker as the authoritative entry.
func TestDiagnosticMarking(t *testing.T) {
	for _, marker := range []string{diagnosticBanner, diagnosticPurpose} {
		if !strings.Contains(strings.ToLower(marker), "diagnostic") {
			t.Fatalf("diagnostic marker missing from %q", marker)
		}
	}
	if !strings.Contains(diagnosticBanner, "tuba-analysis-worker") {
		t.Fatal("banner must name the authoritative scheduling entry")
	}
	out := summary("2026-10-12T00:00:00Z", "2026-10-12T01:00:00Z", 3, 1, false)
	if out["purpose"] != diagnosticPurpose {
		t.Fatalf("summary output must be marked diagnostic: %v", out)
	}
}
