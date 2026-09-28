package launcher

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRollingLogRotatesAndBoundsBackups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.log")
	log, err := openRollingLog(path, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	for _, value := range []string{"123456", "abcdef", "ABCDEF", "GHIJKL"} {
		if _, err := log.Write([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"worker.log":   "GHIJKL",
		"worker.log.1": "ABCDEF",
		"worker.log.2": "abcdef",
	} {
		got, err := os.ReadFile(filepath.Join(filepath.Dir(path), name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("rotation retained more than the configured backups: %v", err)
	}
}

func TestRollingLogRotatesOversizedExistingFileBeforeAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.log")
	if err := os.WriteFile(path, []byte("existing-data"), 0600); err != nil {
		t.Fatal(err)
	}
	log, err := openRollingLog(path, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	active, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if string(active) != "new" || string(backup) != "existing-data" {
		t.Fatalf("unexpected active/backup contents: active=%q backup=%q", active, backup)
	}
}

func TestRotateExistingLogIfNeeded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "launcher.log")
	if err := os.WriteFile(path, []byte("exact-limit"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := rotateExistingLogIfNeeded(path, int64(len("exact-limit")), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("oversized launcher log remained active before it is reopened: %v", err)
	}
	backup, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if string(backup) != "exact-limit" {
		t.Fatalf("launcher log was not rotated: backup=%q", backup)
	}
}
