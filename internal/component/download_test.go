package component

import (
	"context"
	"crypto/subtle"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSignPackageRoundTrip(t *testing.T) {
	ring, key := testKeyring(t)
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "filebeat"), []byte("payload"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := SignPackage(dir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, "", "", "test-key", key); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(dir, ring)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 1 || manifest.Files["bin/filebeat"] != sha256Hex([]byte("payload")) {
		t.Fatalf("unexpected files: %#v", manifest.Files)
	}
	// Re-signing in place is refused.
	if err := SignPackage(dir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, "", "", "test-key", key); err == nil {
		t.Fatal("re-signing a signed package must be refused")
	}
}

func TestSigningKeyFileLifecycle(t *testing.T) {
	_, private, err := GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "test.ed25519.key")
	if err := SavePrivateKey(path, private); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		info, _ := os.Stat(path)
		if info.Mode().Perm() != 0600 {
			t.Fatalf("key file mode = %o", info.Mode().Perm())
		}
	}
	loaded, err := LoadPrivateKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if subtle.ConstantTimeCompare([]byte(loaded), []byte(private)) != 1 {
		t.Fatal("loaded key does not match the generated private key")
	}
	// Never overwrite an existing key.
	if err := SavePrivateKey(path, private); err == nil {
		t.Fatal("overwriting a key file must be refused")
	}
	// On Unix a permissive key file is rejected.
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadPrivateKey(path); err == nil {
			t.Fatal("a group-readable key file must be rejected")
		}
	}
}

// serveRepo hosts dir as a release repository tree.
func serveRepo(t *testing.T, dir string) string {
	t.Helper()
	server := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(server.Close)
	return server.URL
}

// makeRepoPackage creates an unsigned package inside repo/<component>/<version>/.
func makeRepoPackage(t *testing.T, repo, componentName, version string, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(repo, componentName, version)
	for rel, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestDownloadHappyPath(t *testing.T) {
	ring, key := testKeyring(t)
	repo := t.TempDir()
	pkgDir := makeRepoPackage(t, repo, "filebeat", "8.19.0", map[string]string{"bin/filebeat": "payload", "README.txt": "docs"})
	if err := SignPackage(pkgDir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", key); err != nil {
		t.Fatal(err)
	}
	url := serveRepo(t, repo)
	dest := filepath.Join(t.TempDir(), "downloaded")
	downloader := &Downloader{Keyring: ring}
	manifest, err := downloader.Fetch(context.Background(), url, "filebeat", "8.19.0", dest)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 2 {
		t.Fatalf("manifest files: %#v", manifest.Files)
	}
	content, err := os.ReadFile(filepath.Join(dest, "bin", "filebeat"))
	if err != nil || string(content) != "payload" {
		t.Fatalf("downloaded content = %q, %v", content, err)
	}
	// Downloaded package is loadable and passes full verification.
	if _, err := LoadManifest(dest, ring); err != nil {
		t.Fatal(err)
	}
	// Re-fetch over an existing destination is refused.
	if _, err := downloader.Fetch(context.Background(), url, "filebeat", "8.19.0", dest); err == nil {
		t.Fatal("fetch over an existing destination must be refused")
	}
}

func TestDownloadRejectsBadSignature(t *testing.T) {
	ring, _ := testKeyring(t)
	_, otherKey := testKeyring(t)
	repo := t.TempDir()
	pkgDir := makeRepoPackage(t, repo, "filebeat", "8.19.0", map[string]string{"bin/filebeat": "payload"})
	if err := SignPackage(pkgDir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", otherKey); err != nil {
		t.Fatal(err)
	}
	url := serveRepo(t, repo)
	parent := t.TempDir()
	downloader := &Downloader{Keyring: ring}
	if _, err := downloader.Fetch(context.Background(), url, "filebeat", "8.19.0", filepath.Join(parent, "dest")); err == nil {
		t.Fatal("a package signed by the wrong key must be rejected")
	}
	assertCleanParent(t, parent)
}

func TestDownloadRejectsHashMismatch(t *testing.T) {
	ring, key := testKeyring(t)
	repo := t.TempDir()
	pkgDir := makeRepoPackage(t, repo, "filebeat", "8.19.0", map[string]string{"bin/filebeat": "payload"})
	if err := SignPackage(pkgDir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", key); err != nil {
		t.Fatal(err)
	}
	url := serveRepo(t, repo)
	// Corrupt the payload after signing: the signature still verifies, so the
	// tamper can only be caught by the per-file hash.
	if err := os.WriteFile(filepath.Join(pkgDir, "bin", "filebeat"), []byte("trojan"), 0644); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	downloader := &Downloader{Keyring: ring}
	_, err := downloader.Fetch(context.Background(), url, "filebeat", "8.19.0", filepath.Join(parent, "dest"))
	if err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("hash mismatch must be rejected, got %v", err)
	}
	assertCleanParent(t, parent)
}

// assertCleanParent checks a failed download left neither the destination nor
// a staging directory behind.
func assertCleanParent(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		t.Fatalf("failed download left %s behind", entry.Name())
	}
}

func TestDownloadCleansUpTruncatedBody(t *testing.T) {
	ring, key := testKeyring(t)
	repo := t.TempDir()
	pkgDir := makeRepoPackage(t, repo, "filebeat", "8.19.0", map[string]string{"bin/filebeat": "0123456789abcdef"})
	if err := SignPackage(pkgDir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", key); err != nil {
		t.Fatal(err)
	}
	// Server truncates the payload mid-body.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, manifestFileName) {
			http.ServeFile(w, r, filepath.Join(pkgDir, manifestFileName))
			return
		}
		w.Header().Set("Content-Length", "16")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0123")) // fewer bytes than announced
	}))
	defer server.Close()
	parent := t.TempDir()
	downloader := &Downloader{Keyring: ring}
	if _, err := downloader.Fetch(context.Background(), server.URL, "filebeat", "8.19.0", filepath.Join(parent, "dest")); err == nil {
		t.Fatal("a truncated download must fail")
	}
	assertCleanParent(t, parent)
}

func TestDownloadRateLimit(t *testing.T) {
	ring, key := testKeyring(t)
	repo := t.TempDir()
	payload := strings.Repeat("x", 256<<10)
	pkgDir := makeRepoPackage(t, repo, "filebeat", "8.19.0", map[string]string{"bin/filebeat": payload})
	if err := SignPackage(pkgDir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", key); err != nil {
		t.Fatal(err)
	}
	url := serveRepo(t, repo)
	dest := filepath.Join(t.TempDir(), "downloaded")
	downloader := &Downloader{Keyring: ring, RateLimitBytesPerSecond: 128 << 10}
	started := time.Now()
	if _, err := downloader.Fetch(context.Background(), url, "filebeat", "8.19.0", dest); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	// 256 KiB at 128 KiB/s budgets 2s; allow generous scheduling slack.
	if elapsed < 1500*time.Millisecond {
		t.Fatalf("rate limit not applied: 256 KiB at 128 KiB/s took %s", elapsed)
	}
}

func TestDownloadRejectsManifestMismatch(t *testing.T) {
	ring, key := testKeyring(t)
	repo := t.TempDir()
	pkgDir := makeRepoPackage(t, repo, "filebeat", "8.19.0", map[string]string{"bin/filebeat": "payload"})
	if err := SignPackage(pkgDir, "filebeat", "8.19.0", StateFormat{Version: 2, MinReadable: 1}, runtime.GOOS, runtime.GOARCH, "test-key", key); err != nil {
		t.Fatal(err)
	}
	url := serveRepo(t, repo)
	downloader := &Downloader{Keyring: ring}
	// Ask for a different version than the manifest declares.
	if _, err := downloader.Fetch(context.Background(), url, "filebeat", "8.19.0"+".1", filepath.Join(t.TempDir(), "dest")); err == nil {
		t.Fatal("a manifest naming another component/version must be rejected")
	}
}
