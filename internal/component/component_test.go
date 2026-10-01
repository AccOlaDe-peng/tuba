package component

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func testKeyring(t *testing.T) (Keyring, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return Keyring{"test-key": public}, private
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// makePackage writes a signed component package into a fresh directory.
func makePackage(t *testing.T, componentName, version string, format StateFormat, files map[string]string, keyID string, key ed25519.PrivateKey) string {
	t.Helper()
	dir := t.TempDir()
	manifest := &Manifest{
		SchemaVersion: 1,
		Component:     componentName,
		Version:       version,
		OS:            runtime.GOOS,
		Architecture:  runtime.GOARCH,
		StateFormat:   format,
		Files:         map[string]string{},
	}
	for rel, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0755); err != nil {
			t.Fatal(err)
		}
		manifest.Files[rel] = sha256Hex([]byte(content))
	}
	signature, err := SignManifest(manifest, keyID, key)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Signature = signature
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, manifestFileName), data, 0644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func defaultFormat() StateFormat { return StateFormat{Version: 2, MinReadable: 1} }

func TestLoadManifestVerifiesSignatureAndFiles(t *testing.T) {
	ring, key := testKeyring(t)
	dir := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "binary-bytes"}, "test-key", key)
	manifest, err := LoadManifest(dir, ring)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Component != "filebeat" || manifest.Version != "8.19.0" {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
}

func TestLoadManifestRejectsTampering(t *testing.T) {
	ring, key := testKeyring(t)

	t.Run("tampered file content", func(t *testing.T) {
		dir := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "original"}, "test-key", key)
		manifest, err := LoadManifest(dir, ring)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "bin", "filebeat"), []byte("trojan"), 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyPackageFiles(dir, manifest); err == nil || !strings.Contains(err.Error(), "SHA-256") {
			t.Fatalf("tampered file must be rejected, got %v", err)
		}
	})

	t.Run("signature does not match edited manifest", func(t *testing.T) {
		dir := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "original"}, "test-key", key)
		// Edit the manifest after signing with a still-valid change.
		path := filepath.Join(dir, manifestFileName)
		data, _ := os.ReadFile(path)
		data = []byte(strings.Replace(string(data), `"version": "8.19.0"`, `"version": "8.19.9"`, 1))
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadManifest(dir, ring); err == nil || !strings.Contains(err.Error(), "signature") {
			t.Fatalf("edited manifest must fail signature verification, got %v", err)
		}
	})

	t.Run("untrusted key id", func(t *testing.T) {
		_, otherKey := testKeyring(t)
		dir := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "original"}, "attacker-key", otherKey)
		if _, err := LoadManifest(dir, ring); err == nil || !strings.Contains(err.Error(), "untrusted key") {
			t.Fatalf("unknown signer must be rejected, got %v", err)
		}
	})

	t.Run("wrong os", func(t *testing.T) {
		dir := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "original"}, "test-key", key)
		path := filepath.Join(dir, manifestFileName)
		data, _ := os.ReadFile(path)
		// Re-sign so only the OS mismatch can fail the load.
		var manifest Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			t.Fatal(err)
		}
		manifest.OS = "plan9"
		if runtime.GOOS == "plan9" {
			manifest.OS = "windows"
		}
		signature, err := SignManifest(&manifest, "test-key", key)
		if err != nil {
			t.Fatal(err)
		}
		manifest.Signature = signature
		out, _ := json.MarshalIndent(manifest, "", "  ")
		if err := os.WriteFile(path, out, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadManifest(dir, ring); err == nil || !strings.Contains(err.Error(), "host is") {
			t.Fatalf("wrong-OS package must be rejected, got %v", err)
		}
	})

	t.Run("path escape in file list", func(t *testing.T) {
		manifest := &Manifest{
			SchemaVersion: 1, Component: "filebeat", Version: "8.19.0",
			OS: runtime.GOOS, Architecture: runtime.GOARCH,
			StateFormat: defaultFormat(),
			Files:       map[string]string{"../evil": sha256Hex([]byte("x"))},
		}
		signature, err := SignManifest(manifest, "test-key", key)
		if err != nil {
			t.Fatal(err)
		}
		manifest.Signature = signature
		dir := t.TempDir()
		data, _ := json.Marshal(manifest)
		if err := os.WriteFile(filepath.Join(dir, manifestFileName), data, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadManifest(dir, ring); err == nil || !strings.Contains(err.Error(), "unsafe path") {
			t.Fatalf("escaping file path must be rejected, got %v", err)
		}
	})

	t.Run("extra file not in manifest", func(t *testing.T) {
		dir := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "original"}, "test-key", key)
		manifest, err := LoadManifest(dir, ring)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "smuggled"), []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyPackageFiles(dir, manifest); err == nil || !strings.Contains(err.Error(), "not in the signed manifest") {
			t.Fatalf("extra file must be rejected, got %v", err)
		}
	})
}

func TestApplyConfirmAndStatus(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}

	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	result, err := upgrader.Apply(v1, "", []string{"src-a"})
	if err != nil {
		t.Fatal(err)
	}
	if result.FromVersion != "" || result.ToVersion != "8.19.0" || result.PreviousSaved {
		t.Fatalf("unexpected first-install result: %#v", result)
	}
	current, err := CurrentVersion(root, "filebeat")
	if err != nil || current != "8.19.0" {
		t.Fatalf("current = %q, %v", current, err)
	}
	// The staged file really landed; on Unix its executable bit is preserved.
	info, err := os.Stat(filepath.Join(versionDir(root, "filebeat", "8.19.0"), "bin", "filebeat"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0100 == 0 {
		t.Fatal("installed file lost its executable bit")
	}

	// A second apply on top of an unconfirmed upgrade is refused.
	v2 := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	if _, err := upgrader.Apply(v2, "filebeat", nil); !errors.Is(err, ErrUpgradePending) {
		t.Fatalf("second apply must fail with ErrUpgradePending, got %v", err)
	}

	if err := upgrader.Confirm("filebeat", []string{"src-a"}); err != nil {
		t.Fatal(err)
	}
	format, err := readStateFormat(root, "filebeat", "src-a")
	if err != nil || format != 2 {
		t.Fatalf("state format = %d, %v", format, err)
	}
	status, err := upgrader.Status("filebeat")
	if err != nil {
		t.Fatal(err)
	}
	if status.Current != "8.19.0" || status.PendingUpgrade || len(status.Installed) != 1 {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestUpgradeAndCompatibleRollback(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}
	instances := []string{"src-a"}

	v1 := makePackage(t, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}

	// v2 writes the same format the instances are stamped at.
	v2 := makePackage(t, "filebeat", "8.19.1", StateFormat{Version: 2, MinReadable: 1}, map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	if _, err := upgrader.Apply(v2, "", instances); err != nil {
		t.Fatal(err)
	}
	result, err := upgrader.Rollback("filebeat", instances)
	if err != nil {
		t.Fatal(err)
	}
	if result.ToVersion != "8.19.0" {
		t.Fatalf("rollback target = %q", result.ToVersion)
	}
	current, _ := CurrentVersion(root, "filebeat")
	if current != "8.19.0" {
		t.Fatalf("current after rollback = %q", current)
	}
	// The failed version stays installed for forensics.
	status, err := upgrader.Status("filebeat")
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Installed) != 2 || status.Previous != "8.19.1" {
		t.Fatalf("unexpected status after rollback: %#v", status)
	}
}

func TestRollbackRefusedAfterFormatMigration(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}
	instances := []string{"src-a"}

	v1 := makePackage(t, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}
	// v2 migrates the data to format 3 and can read format 2.
	v2 := makePackage(t, "filebeat", "8.20.0", StateFormat{Version: 3, MinReadable: 2}, map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	if _, err := upgrader.Apply(v2, "", instances); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", instances); err != nil {
		t.Fatal(err)
	}
	format, err := readStateFormat(root, "filebeat", "src-a")
	if err != nil || format != 3 {
		t.Fatalf("state format after migration = %d, %v", format, err)
	}
	// Rolling the binary back to v1 (writes 2) over data at format 3 must fail.
	_, err = upgrader.Rollback("filebeat", instances)
	if err == nil || !strings.Contains(err.Error(), "newer than the format") {
		t.Fatalf("rollback over migrated data must be refused, got %v", err)
	}
	current, _ := CurrentVersion(root, "filebeat")
	if current != "8.20.0" {
		t.Fatalf("current must stay at the working version, got %q", current)
	}
}

func TestStateFormatGates(t *testing.T) {
	ring, key := testKeyring(t)
	instances := []string{"src-a"}

	t.Run("data newer than binary", func(t *testing.T) {
		root := t.TempDir()
		upgrader := &Upgrader{Root: root, Keyring: ring}
		if err := StampStateFormat(root, "filebeat", "src-a", 5); err != nil {
			t.Fatal(err)
		}
		pkg := makePackage(t, "filebeat", "8.19.0", StateFormat{Version: 3, MinReadable: 2}, map[string]string{"bin/filebeat": "x"}, "test-key", key)
		if _, err := upgrader.Apply(pkg, "", instances); err == nil || !strings.Contains(err.Error(), "newer than the format") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("data older than readable", func(t *testing.T) {
		root := t.TempDir()
		upgrader := &Upgrader{Root: root, Keyring: ring}
		if err := StampStateFormat(root, "filebeat", "src-a", 1); err != nil {
			t.Fatal(err)
		}
		pkg := makePackage(t, "filebeat", "8.19.0", StateFormat{Version: 3, MinReadable: 2}, map[string]string{"bin/filebeat": "x"}, "test-key", key)
		if _, err := upgrader.Apply(pkg, "", instances); err == nil || !strings.Contains(err.Error(), "older than the minimum") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("unstamped non-empty data dir", func(t *testing.T) {
		root := t.TempDir()
		upgrader := &Upgrader{Root: root, Keyring: ring}
		dir := instanceDataDir(root, "src-a")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "registry.dat"), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		pkg := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "x"}, "test-key", key)
		if _, err := upgrader.Apply(pkg, "", instances); err == nil || !strings.Contains(err.Error(), "no state format stamp") {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("stamp refuses regression", func(t *testing.T) {
		root := t.TempDir()
		if err := StampStateFormat(root, "filebeat", "src-a", 3); err != nil {
			t.Fatal(err)
		}
		if err := StampStateFormat(root, "filebeat", "src-a", 2); err == nil {
			t.Fatal("stamping an older format over newer data must fail")
		}
	})
}

func TestImmutableVersionsAndComponentMismatch(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}

	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", nil); err != nil {
		t.Fatal(err)
	}
	// Re-installing the same version directory is refused even after confirm.
	if _, err := upgrader.Apply(v1, "", nil); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("reinstalling a version must be refused, got %v", err)
	}
	// A package for another component must not be applied under this name.
	other := makePackage(t, "winlogbeat", "8.19.0", defaultFormat(), map[string]string{"bin/winlogbeat": "x"}, "test-key", key)
	if _, err := upgrader.Apply(other, "filebeat", nil); err == nil || !strings.Contains(err.Error(), "not") {
		t.Fatalf("component mismatch must be refused, got %v", err)
	}
}

func TestRollbackWithoutTarget(t *testing.T) {
	ring, _ := testKeyring(t)
	upgrader := &Upgrader{Root: t.TempDir(), Keyring: ring}
	if _, err := upgrader.Rollback("filebeat", nil); !errors.Is(err, ErrNothingToRollback) {
		t.Fatalf("got %v", err)
	}
}

func TestInstanceLock(t *testing.T) {
	root := t.TempDir()

	lock, err := AcquireInstanceLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if !lock.Held() {
		t.Fatal("lock should be held after acquire")
	}
	if _, err := AcquireInstanceLock(root); err == nil || !strings.Contains(err.Error(), "another management agent") {
		t.Fatalf("second acquire must fail, got %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	if lock.Held() {
		t.Fatal("lock should not be held after release")
	}

	lock, err = AcquireInstanceLock(root)
	if err != nil {
		t.Fatalf("re-acquire after release: %v", err)
	}

	// A stale lock (live PID but an identity from another boot) is replaced.
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	stale := `{"pid":` + strconv.Itoa(os.Getpid()) + `,"identity":"from-another-boot","acquired_at":"2026-01-01T00:00:00Z"}`
	if err := os.WriteFile(filepath.Join(root, lockFileName), []byte(stale), 0600); err != nil {
		t.Fatal(err)
	}
	lock, err = AcquireInstanceLock(root)
	if err != nil {
		t.Fatalf("stale lock must be replaceable: %v", err)
	}
	if !lock.Held() {
		t.Fatal("lock should be held after stale replacement")
	}
}

func TestInstanceLockCorruptReplacement(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, lockFileName), []byte("{not json"), 0600); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireInstanceLock(root)
	if err != nil {
		t.Fatalf("corrupt lock must be replaceable: %v", err)
	}
	if !lock.Held() {
		t.Fatal("lock should be held after corrupt replacement")
	}
}
