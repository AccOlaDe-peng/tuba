package component

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// On-disk layout under the supervisor root, following the unified layout in
// COLLECTOR-DESIGN §6:
//
//	<root>/components/<name>/<version>/        one immutable directory per version
//	<root>/components/<name>/current.json      atomic pointer to the active version
//	<root>/components/<name>/previous.json     atomic pointer to the rollback target
//	<root>/components/<name>/upgrade-pending.json  journal of an unconfirmed upgrade
//	<root>/data/<instance>/state-format.json   per-instance registry/data format stamp
//
// The pointers are small JSON files rather than symlinks or junctions so the
// same commands and semantics work identically on Windows and Unix without
// symlink privileges. Pointer writes are tmp+rename atomic and 0600.
const (
	componentsDirName   = "components"
	currentPointerName  = "current.json"
	previousPointerName = "previous.json"
	pendingJournalName  = "upgrade-pending.json"
)

// pointer records which version of a component is active (or next in line for
// rollback).
type pointer struct {
	Component string    `json:"component"`
	Version   string    `json:"version"`
	Updated   time.Time `json:"updated_at"`
}

// pendingUpgrade journals an applied-but-unconfirmed upgrade so a crash
// between the pointer switch and the health confirmation cannot be mistaken
// for a committed state.
type pendingUpgrade struct {
	Component   string    `json:"component"`
	FromVersion string    `json:"from_version"`
	ToVersion   string    `json:"to_version"`
	AppliedAt   time.Time `json:"applied_at"`
}

func componentDir(root, name string) string {
	return filepath.Join(root, componentsDirName, name)
}

func versionDir(root, name, version string) string {
	return filepath.Join(componentDir(root, name), version)
}

func readJSON(path string, out any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return nil
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temp := path + ".tmp"
	if err := os.WriteFile(temp, append(data, '\n'), 0600); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	_ = os.Chmod(temp, 0600)
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("replace %s: %w", filepath.Base(path), err)
	}
	return nil
}

func readPointer(root, name, pointerName string) (pointer, error) {
	var p pointer
	err := readJSON(filepath.Join(componentDir(root, name), pointerName), &p)
	if err != nil {
		return pointer{}, err
	}
	if p.Component != name || !versionPattern.MatchString(p.Version) {
		return pointer{}, fmt.Errorf("%s of component %q is corrupt", pointerName, name)
	}
	return p, nil
}

func writePointer(root, name, pointerName, version string) error {
	return writeJSONAtomic(filepath.Join(componentDir(root, name), pointerName), pointer{
		Component: name,
		Version:   version,
		Updated:   time.Now().UTC(),
	})
}

// CurrentVersion returns the active version of a component, or "" if the
// component has never been installed.
func CurrentVersion(root, name string) (string, error) {
	p, err := readPointer(root, name, currentPointerName)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return p.Version, nil
}
