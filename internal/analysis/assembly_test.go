package analysis_test

import (
	"os/exec"
	"strings"
	"testing"
)

// F08 double-emission guard: the Python tuba-analysis-worker is the single
// authoritative analysis scheduling/emission entry. The Go detection,
// feature and baseline packages are diagnostic/reference mirrors; this test
// asserts at the assembly layer that no binary is wired to produce analysis
// results from them:
//
//   - the legacy diagnostic package internal/detection may be linked only by
//     cmd/tuba-detect-auth (the diagnostic CLI);
//   - no cmd binary may link the reference mirrors
//     internal/analysis/{feature,baseline,detection};
//   - the result-consuming path (analysisworker, sink) must never import the
//     producer-side detection packages — it consumes envelopes only.
func TestNoDoubleEmissionAssembly(t *testing.T) {
	const diagnosticCLI = "tuba/product/cmd/tuba-detect-auth"
	producerPackages := []string{
		"tuba/product/internal/detection",
		"tuba/product/internal/analysis/detection",
		"tuba/product/internal/analysis/feature",
		"tuba/product/internal/analysis/baseline",
	}

	out, err := exec.Command("go", "list", "-f", `{{.ImportPath}} {{join .Deps " "}}`, "tuba/product/cmd/...").Output()
	if err != nil {
		t.Fatalf("go list ./cmd/...: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		binary := fields[0]
		deps := fields[1:]
		for _, producer := range producerPackages {
			if !contains(deps, producer) {
				continue
			}
			if producer == "tuba/product/internal/detection" && binary == diagnosticCLI {
				continue // the diagnostic CLI is the sole permitted link
			}
			t.Fatalf("%s links producer-side package %s; only the Python tuba-analysis-worker may emit analysis results", binary, producer)
		}
	}

	for _, consumer := range []string{"tuba/product/internal/analysisworker", "tuba/product/internal/sink"} {
		for _, producer := range producerPackages {
			if contains(moduleImports(t, consumer), producer) {
				t.Fatalf("%s must consume envelopes only, not import %s", consumer, producer)
			}
		}
	}
}
