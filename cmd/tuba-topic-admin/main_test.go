package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindContractPathFromWorkspaceAncestor(t *testing.T) {
	path, err := findContractPath("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("resolved contract path does not exist: %s: %v", path, err)
	}
}

func TestFindContractPathUsesExplicitPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.json")
	if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := findContractPath(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != path {
		t.Fatalf("explicit path changed: got %q want %q", got, path)
	}
}
