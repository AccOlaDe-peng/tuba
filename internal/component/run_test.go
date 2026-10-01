package component

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveCurrentBinary(t *testing.T) {
	ring, key := testKeyring(t)
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}
	bin := "bin/agent"
	if runtime.GOOS == "windows" {
		bin = "bin/agent.exe"
	}
	pkg := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{bin: "payload"}, "test-key", key)
	if _, err := upgrader.Apply(pkg, "", nil); err != nil {
		t.Fatal(err)
	}

	dir, binary, err := ResolveCurrentBinary(root, "filebeat", bin)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(binary, filepath.FromSlash("8.19.0/"+bin)) || !strings.HasSuffix(dir, "8.19.0") {
		t.Fatalf("resolved %q in %q", binary, dir)
	}

	// Path escape and unknown component fail closed.
	if _, _, err := ResolveCurrentBinary(root, "filebeat", "../outside"); err == nil {
		t.Fatal("path escape must be rejected")
	}
	if _, _, err := ResolveCurrentBinary(root, "winlogbeat", bin); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("expected ErrNoActiveVersion, got %v", err)
	}
}

func TestResolveArgs(t *testing.T) {
	dir := filepath.Join("/root", "components", "filebeat", "8.19.0")
	if runtime.GOOS == "windows" {
		dir = `C:\tuba\components\filebeat\8.19.0`
	}
	got := ResolveArgs(dir, []string{"-c", "filebeat.yml", "--strict.perms=false", "-e"})
	if filepath.IsAbs(got[1]) != true || !strings.HasPrefix(filepath.ToSlash(got[1]), filepath.ToSlash(dir)) {
		t.Fatalf("config arg not resolved against version dir: %v", got)
	}
	if got[2] != "--strict.perms=false" || got[3] != "-e" {
		t.Fatalf("non-config args must pass through: %v", got)
	}
}
