package analysis_test

import (
	"os/exec"
	"strings"
	"testing"
)

// moduleImports lists the imports of a package via the go tool so the module
// DAG (feature <- baseline <- detection; registry composes from outside) is
// enforced by the test suite, not by convention.
func moduleImports(t *testing.T, pkg string) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-f", `{{range .Imports}}{{.}}{{"\n"}}{{end}}`, pkg).Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pkg, err)
	}
	var imports []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			imports = append(imports, line)
		}
	}
	return imports
}

func contains(imports []string, pkg string) bool {
	for _, imp := range imports {
		if imp == pkg {
			return true
		}
	}
	return false
}

func TestAnalysisModuleBoundaries(t *testing.T) {
	const (
		feature   = "tuba/product/internal/analysis/feature"
		baseline  = "tuba/product/internal/analysis/baseline"
		detection = "tuba/product/internal/analysis/detection"
		registry  = "tuba/product/internal/analysis/registry"
	)
	featureImports := moduleImports(t, feature)
	for _, forbidden := range []string{baseline, detection, registry} {
		if contains(featureImports, forbidden) {
			t.Fatalf("feature must not import %s", forbidden)
		}
	}
	baselineImports := moduleImports(t, baseline)
	if !contains(baselineImports, feature) {
		t.Fatal("baseline must declare its dependency on feature")
	}
	for _, forbidden := range []string{detection, registry} {
		if contains(baselineImports, forbidden) {
			t.Fatalf("baseline must not import %s", forbidden)
		}
	}
	detectionImports := moduleImports(t, detection)
	for _, required := range []string{feature, baseline} {
		if !contains(detectionImports, required) {
			t.Fatalf("detection must declare its dependency on %s", required)
		}
	}
	if contains(detectionImports, registry) {
		t.Fatal("detection must not import registry")
	}
}
