package control

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// verifyReleaseBundle is the only thing standing between a manifest and the
// asset bytes the normalizer, indexer and UIM actually load, so these tests pin
// the rejections rather than the happy path alone. Each one names the production
// change that would make it fail.

const (
	testReleaseID = "test-release-1.0.0"
	testVersion   = "1.0.0"
)

type bundledAsset struct {
	id      string
	kind    string
	relPath string
	deps    []string
}

// standardAssets mirrors the shape of a real release bundle: one asset per
// required kind, joined by the same acyclic dependency graph.
func standardAssets() []bundledAsset {
	return []bundledAsset{
		{"asset-routing", "routing", "routing/domains.json", nil},
		{"asset-uim", "uim", "uim/uim.json", []string{"asset-routing"}},
		{"asset-dip", "dip", "dip/dip.json", []string{"asset-uim"}},
		{"asset-es", "es_mapping", "elasticsearch/templates.json", []string{"asset-uim"}},
		{"asset-entities", "entity_rules", "entities/rules.json", []string{"asset-uim"}},
		{"asset-model", "data_model", "model/datasets.json", []string{"asset-routing"}},
		{"asset-analysis", "analysis_rules", "analysis/rules.json", []string{"asset-entities", "asset-model"}},
	}
}

// writeBundle lays the bundle out under root and returns a manifest whose hashes
// match the bytes just written.
func writeBundle(t *testing.T, root string, assets []bundledAsset) ReleaseManifest {
	t.Helper()
	base := filepath.Join(root, testReleaseID)
	m := ReleaseManifest{
		SchemaVersion: "1.0.0",
		ReleaseID:     testReleaseID,
		Version:       testVersion,
		Compatibility: map[string]string{
			"raw_contract":             "1",
			"uim_contract":             "1",
			"analysis_result_contract": "2",
			"generation":               "g1",
		},
	}
	for _, a := range assets {
		target := filepath.Join(base, filepath.FromSlash(a.relPath))
		if e := os.MkdirAll(filepath.Dir(target), 0o755); e != nil {
			t.Fatal(e)
		}
		content := []byte("content for " + a.id + "\n")
		if e := os.WriteFile(target, content, 0o644); e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(content)
		m.Assets = append(m.Assets, ReleaseAsset{
			ID:           a.id,
			Kind:         a.kind,
			Version:      testVersion,
			Path:         a.relPath,
			SHA256:       hex.EncodeToString(sum[:]),
			Dependencies: a.deps,
		})
	}
	return m
}

func bundlePath(root, rel string) string {
	return filepath.Join(root, testReleaseID, filepath.FromSlash(rel))
}

func TestVerifyReleaseBundleAcceptsValidBundle(t *testing.T) {
	root := t.TempDir()
	if e := verifyReleaseBundle(root, writeBundle(t, root, standardAssets())); e != nil {
		t.Fatalf("valid bundle rejected: %v", e)
	}
}

// A bundle whose bytes change after its hashes were recorded must not advance;
// production change: dropping the comparison of the file digest against a.SHA256.
func TestVerifyReleaseBundleRejectsTamperedAsset(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	if e := os.WriteFile(bundlePath(root, "dip/dip.json"), []byte("tampered\n"), 0o644); e != nil {
		t.Fatal(e)
	}
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "SHA-256 mismatch") {
		t.Fatalf("want SHA-256 mismatch, got %v", e)
	}
}

// production change: removing the ".." segment check on a.Path.
func TestVerifyReleaseBundleRejectsTraversalPath(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	m.Assets[0].Path = "../outside.json"
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "unsafe release asset path") {
		t.Fatalf("want unsafe release asset path, got %v", e)
	}
}

// A bundle entry that resolves outside the bundle must not be accepted: this is
// what stops a release from reading arbitrary files on the host. The recorded
// path looks harmless, so only resolving it reveals the escape.
//
// The outside file is given the exact content the manifest expects, so a
// matching hash cannot mask a broken containment check. If containment were not
// enforced the bundle would be accepted outright.
//
// Platform note: on Linux filepath.EvalSymlinks resolves the link and the
// containment comparison rejects it as "resolves outside release bundle". Go's
// EvalSymlinks does not resolve Windows directory junctions and returns an error
// instead, so on Windows the escape is rejected one step earlier as "asset
// unavailable". Both outcomes are fail-closed, so the assertion accepts either;
// the property under test is that the bundle is not accepted. Covering the
// containment comparison itself therefore requires a Linux run.
func TestVerifyReleaseBundleRejectsEscapingAsset(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())

	outside := t.TempDir()
	if e := os.WriteFile(filepath.Join(outside, "templates.json"), []byte("content for asset-es\n"), 0o644); e != nil {
		t.Fatal(e)
	}
	dir := bundlePath(root, "elasticsearch")
	if e := os.RemoveAll(dir); e != nil {
		t.Fatal(e)
	}
	// os.Symlink needs SeCreateSymbolicLinkPrivilege on Windows, which a plain
	// developer account does not hold; a directory junction needs no privilege.
	if e := os.Symlink(outside, dir); e != nil {
		if junction := exec.Command("cmd", "/c", "mklink", "/J", dir, outside).Run(); junction != nil {
			t.Skipf("no directory link available (symlink: %v; junction: %v)", e, junction)
		}
	}
	e := verifyReleaseBundle(root, m)
	if e == nil {
		t.Fatal("escaping asset accepted; the containment check did not run")
	}
	if !strings.Contains(e.Error(), "resolves outside release bundle") &&
		!strings.Contains(e.Error(), "unavailable") {
		t.Fatalf("rejected for an unrelated reason: %v", e)
	}
}

// production change: dropping the DFS cycle detection.
func TestVerifyReleaseBundleRejectsDependencyCycle(t *testing.T) {
	root := t.TempDir()
	assets := standardAssets()
	// routing -> analysis -> model -> routing closes the loop.
	assets[0].deps = []string{"asset-analysis"}
	m := writeBundle(t, root, assets)
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "cycle") {
		t.Fatalf("want cycle rejection, got %v", e)
	}
}

// production change: dropping the membership check against known asset IDs.
func TestVerifyReleaseBundleRejectsUnknownDependency(t *testing.T) {
	root := t.TempDir()
	assets := standardAssets()
	assets[0].deps = []string{"asset-absent"}
	m := writeBundle(t, root, assets)
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "unknown dependency") {
		t.Fatalf("want unknown dependency, got %v", e)
	}
}

// production change: relaxing len(m.Assets) != 7.
func TestVerifyReleaseBundleRejectsMissingAssetKind(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets()[:6])
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "seven") {
		t.Fatalf("want seven-asset rejection, got %v", e)
	}
}

// production change: dropping the duplicate-kind check, which would let a
// bundle ship two competing definitions of the same asset kind.
func TestVerifyReleaseBundleRejectsDuplicateKind(t *testing.T) {
	root := t.TempDir()
	assets := standardAssets()
	assets[1].kind = "routing"
	m := writeBundle(t, root, assets)
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "duplicate release asset kind") {
		t.Fatalf("want duplicate kind rejection, got %v", e)
	}
}

// production change: dropping the duplicate-ID check on ids[a.ID].
func TestVerifyReleaseBundleRejectsDuplicateAssetID(t *testing.T) {
	root := t.TempDir()
	assets := standardAssets()
	assets[1].id = assets[0].id
	m := writeBundle(t, root, assets)
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "invalid or duplicate release asset") {
		t.Fatalf("want duplicate id rejection, got %v", e)
	}
}

// production change: dropping the IsRegular check, which would let a directory
// or device node pass as an asset.
func TestVerifyReleaseBundleRejectsNonRegularAsset(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	target := bundlePath(root, "dip/dip.json")
	if e := os.Remove(target); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(target, 0o755); e != nil {
		t.Fatal(e)
	}
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "not a regular file") {
		t.Fatalf("want regular-file rejection, got %v", e)
	}
}

// production change: accepting an asset hash that is not 64 lowercase hex.
func TestVerifyReleaseBundleRejectsMalformedHash(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	m.Assets[0].SHA256 = strings.ToUpper(m.Assets[0].SHA256)
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "invalid release asset hash") {
		t.Fatalf("want hash-format rejection, got %v", e)
	}
}

// production change: relaxing the raw_contract clause of the compatibility check.
func TestVerifyReleaseBundleRejectsIncompatibleContract(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	m.Compatibility["raw_contract"] = "2"
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "compatibility") {
		t.Fatalf("want compatibility rejection, got %v", e)
	}
}

// production change: relaxing the len(m.Compatibility) != 4 clause. A bundle that
// omits a contract declaration is not the same as one that declares it loosely,
// so the arity check has to stand on its own.
func TestVerifyReleaseBundleRejectsMissingCompatibilityKey(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	delete(m.Compatibility, "generation")
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "compatibility") {
		t.Fatalf("want missing-key rejection, got %v", e)
	}
}

// production change: dropping the releaseIDValid guard on the generation, which
// would let a generation name like ".." through.
func TestVerifyReleaseBundleRejectsInvalidGeneration(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	m.Compatibility["generation"] = ".."
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "compatibility") {
		t.Fatalf("want generation rejection, got %v", e)
	}
}

// production change: dropping the releaseIDValid guard, which would let ".."
// select the release root's parent as the bundle directory.
func TestVerifyReleaseBundleRejectsUnsafeReleaseID(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	m.ReleaseID = ".."
	e := verifyReleaseBundle(root, m)
	if e == nil || !strings.Contains(e.Error(), "invalid release ID") {
		t.Fatalf("want release ID rejection, got %v", e)
	}
}

// production change: removing the empty-root guard, which would make an
// unconfigured TUBA_RELEASE_ROOT resolve to the process working directory.
func TestVerifyReleaseBundleRejectsUnconfiguredRoot(t *testing.T) {
	root := t.TempDir()
	m := writeBundle(t, root, standardAssets())
	e := verifyReleaseBundle("", m)
	if e == nil || !strings.Contains(e.Error(), "TUBA_RELEASE_ROOT") {
		t.Fatalf("want unconfigured-root rejection, got %v", e)
	}
}
