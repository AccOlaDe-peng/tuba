package component

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Upgrader applies signed component packages under a supervisor root. It only
// ever touches the on-disk version layout: the caller is responsible for
// stopping the component (via the launcher's single-service controls) before
// Apply switches the pointer, and for starting it again for the health
// observation window before Confirm.
type Upgrader struct {
	Root    string
	Keyring Keyring
}

// ApplyResult describes a completed pointer switch awaiting health
// confirmation.
type ApplyResult struct {
	Component     string
	FromVersion   string // "" on first install
	ToVersion     string
	PreviousSaved bool
}

// ErrUpgradePending is returned when an earlier upgrade was never confirmed
// or rolled back; applying a second one on top would lose the rollback target.
var ErrUpgradePending = errors.New("an upgrade is pending confirmation; confirm or roll it back first")

// ErrNothingToRollback is returned when there is no pending upgrade and no
// previous version pointer.
var ErrNothingToRollback = errors.New("no pending upgrade or previous version to roll back to")

// Apply stages, verifies, and installs the package in packageDir, then
// atomically switches the component's current pointer to the new version. The
// previous current version is kept as the rollback target. The upgrade is
// journaled as pending until Confirm; Rollback switches back, but only if the
// instance data directories are still readable by the previous version.
//
// instances are the data instances of this component whose state format must
// be compatible with the new version; pass nil only for stateless components.
func (u *Upgrader) Apply(packageDir, componentName string, instances []string) (*ApplyResult, error) {
	if u.Keyring == nil {
		return nil, errors.New("upgrader has no keyring")
	}
	if err := os.MkdirAll(u.Root, 0755); err != nil {
		return nil, fmt.Errorf("create supervisor root: %w", err)
	}
	manifest, err := LoadManifest(packageDir, u.Keyring)
	if err != nil {
		return nil, err
	}
	if componentName == "" {
		componentName = manifest.Component
	}
	if manifest.Component != componentName {
		return nil, fmt.Errorf("package is for component %q, not %q", manifest.Component, componentName)
	}
	if _, err := u.pending(componentName); err == nil {
		return nil, ErrUpgradePending
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	size, err := verifyPackageFiles(packageDir, manifest)
	if err != nil {
		return nil, err
	}
	for _, instance := range instances {
		if err := checkUpgradeCompatibility(u.Root, instance, manifest); err != nil {
			return nil, err
		}
	}
	free, err := freeSpace(u.Root)
	if err != nil {
		return nil, fmt.Errorf("check free space: %w", err)
	}
	if free < uint64(size) {
		return nil, fmt.Errorf("insufficient free space for component %q %s: need %d bytes, have %d", componentName, manifest.Version, size, free)
	}
	target := versionDir(u.Root, componentName, manifest.Version)
	if _, err := os.Stat(target); err == nil {
		return nil, fmt.Errorf("component %q version %s is already installed; versions are immutable", componentName, manifest.Version)
	}
	if err := u.stage(packageDir, manifest, target); err != nil {
		return nil, err
	}
	current, err := CurrentVersion(u.Root, componentName)
	if err != nil {
		return nil, err
	}
	if current == manifest.Version {
		return nil, fmt.Errorf("component %q version %s is already active", componentName, manifest.Version)
	}
	if current != "" {
		if err := writePointer(u.Root, componentName, previousPointerName, current); err != nil {
			return nil, err
		}
	}
	if err := writePointer(u.Root, componentName, currentPointerName, manifest.Version); err != nil {
		return nil, err
	}
	journal := pendingUpgrade{
		Component:   componentName,
		FromVersion: current,
		ToVersion:   manifest.Version,
		AppliedAt:   time.Now().UTC(),
	}
	if err := writeJSONAtomic(filepath.Join(componentDir(u.Root, componentName), pendingJournalName), journal); err != nil {
		return nil, err
	}
	return &ApplyResult{
		Component:     componentName,
		FromVersion:   current,
		ToVersion:     manifest.Version,
		PreviousSaved: current != "",
	}, nil
}

// stage copies the verified package into a staging directory on the target
// filesystem and renames it into place, so a version directory never appears
// half-written. The staged copy is hash-verified again after the copy: the
// pre-staging verification protects the install decision, this one protects
// what actually lands on disk.
func (u *Upgrader) stage(packageDir string, manifest *Manifest, target string) error {
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return fmt.Errorf("create component directory: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".staging-")
	if err != nil {
		return fmt.Errorf("create staging directory: %w", err)
	}
	defer os.RemoveAll(staging)
	for _, rel := range manifest.sortedFiles() {
		src := filepath.Join(packageDir, filepath.FromSlash(rel))
		dst := filepath.Join(staging, filepath.FromSlash(rel))
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("stage %s: %w", rel, err)
		}
	}
	manifestData, err := os.ReadFile(manifest.path)
	if err != nil {
		return fmt.Errorf("read package manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(staging, manifestFileName), manifestData, 0644); err != nil {
		return fmt.Errorf("stage manifest: %w", err)
	}
	if _, err := verifyPackageFiles(staging, manifest); err != nil {
		return fmt.Errorf("verify staged copy: %w", err)
	}
	if err := os.Rename(staging, target); err != nil {
		return fmt.Errorf("install component version: %w", err)
	}
	return nil
}

// verifyPackageFiles checks that the package contains exactly the files the
// manifest lists, each with a matching SHA-256, and returns the total size.
// Extra files are rejected: a package must not smuggle content past the signed
// file set.
func verifyPackageFiles(dir string, manifest *Manifest) (int64, error) {
	var total int64
	seen := map[string]bool{}
	for _, rel := range manifest.sortedFiles() {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		digest, size, err := hashFile(path)
		if err != nil {
			return 0, fmt.Errorf("hash package file %s: %w", rel, err)
		}
		if digest != manifest.Files[rel] {
			return 0, fmt.Errorf("package file %s does not match its signed SHA-256", rel)
		}
		seen[rel] = true
		total += size
	}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == manifestFileName {
			return nil
		}
		if !seen[rel] {
			return fmt.Errorf("package contains file %s that is not in the signed manifest", rel)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hasher.Sum(nil)), size, nil
}

func copyFile(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil || !info.Mode().IsRegular() {
		return fmt.Errorf("not a regular file: %s", src)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm()&0777)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// Confirm commits a pending upgrade after the health observation window has
// passed, and stamps the instance data directories with the new version's
// state format — a healthy component may have migrated them already.
func (u *Upgrader) Confirm(componentName string, instances []string) error {
	journal, err := u.pending(componentName)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("no pending upgrade to confirm")
	}
	if err != nil {
		return err
	}
	current, err := CurrentVersion(u.Root, componentName)
	if err != nil {
		return err
	}
	if current != journal.ToVersion {
		return fmt.Errorf("component %q current pointer moved from %s to %s during the observation window; refusing to confirm", componentName, journal.ToVersion, current)
	}
	manifest, err := u.installedManifest(componentName, journal.ToVersion)
	if err != nil {
		return err
	}
	for _, instance := range instances {
		if err := StampStateFormat(u.Root, componentName, instance, manifest.StateFormat.Version); err != nil {
			return err
		}
	}
	return os.Remove(filepath.Join(componentDir(u.Root, componentName), pendingJournalName))
}

// Rollback switches the component back to the version it ran before the
// pending (or last confirmed) upgrade. It fails closed when the instance data
// has already been migrated to a format the previous version cannot read —
// blindly swapping the executable while keeping an incompatible data
// directory is exactly what the design forbids.
func (u *Upgrader) Rollback(componentName string, instances []string) (*ApplyResult, error) {
	journal, journalErr := u.pending(componentName)
	target := ""
	if journalErr == nil {
		target = journal.FromVersion
	} else if errors.Is(journalErr, os.ErrNotExist) {
		previous, err := readPointer(u.Root, componentName, previousPointerName)
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNothingToRollback
		}
		if err != nil {
			return nil, err
		}
		target = previous.Version
	} else {
		return nil, journalErr
	}
	if target == "" {
		return nil, ErrNothingToRollback
	}
	current, err := CurrentVersion(u.Root, componentName)
	if err != nil {
		return nil, err
	}
	if current == target {
		return nil, fmt.Errorf("component %q already runs version %s", componentName, target)
	}
	manifest, err := u.installedManifest(componentName, target)
	if err != nil {
		return nil, fmt.Errorf("rollback target %s is not installed: %w", target, err)
	}
	for _, instance := range instances {
		format, err := readStateFormat(u.Root, componentName, instance)
		if errors.Is(err, errNoStateStamp) {
			empty, statErr := directoryEmpty(instanceDataDir(u.Root, instance))
			if statErr != nil {
				return nil, statErr
			}
			if !empty {
				return nil, fmt.Errorf("instance %q has data but no state format stamp; refusing to roll back over unknown state", instance)
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := requireReadable(instance, format, manifest.StateFormat); err != nil {
			return nil, fmt.Errorf("rollback refused: %w", err)
		}
	}
	// Keep the failed version installed for forensics; only pointers move.
	if err := writePointer(u.Root, componentName, previousPointerName, current); err != nil {
		return nil, err
	}
	if err := writePointer(u.Root, componentName, currentPointerName, target); err != nil {
		return nil, err
	}
	_ = os.Remove(filepath.Join(componentDir(u.Root, componentName), pendingJournalName))
	return &ApplyResult{Component: componentName, FromVersion: current, ToVersion: target, PreviousSaved: true}, nil
}

func (u *Upgrader) pending(componentName string) (pendingUpgrade, error) {
	var journal pendingUpgrade
	err := readJSON(filepath.Join(componentDir(u.Root, componentName), pendingJournalName), &journal)
	if err != nil {
		return pendingUpgrade{}, err
	}
	if journal.Component != componentName || !versionPattern.MatchString(journal.FromVersion) && journal.FromVersion != "" || !versionPattern.MatchString(journal.ToVersion) {
		return pendingUpgrade{}, fmt.Errorf("pending upgrade journal of component %q is corrupt", componentName)
	}
	return journal, nil
}

// installedManifest loads the manifest of an already-installed version. It is
// re-verified against the keyring: an installed directory is evidence of past
// verification, not a trust exemption.
func (u *Upgrader) installedManifest(componentName, version string) (*Manifest, error) {
	return LoadManifest(versionDir(u.Root, componentName, version), u.Keyring)
}

// Status reports the installed versions and pointer state of a component.
type Status struct {
	Component       string   `json:"component"`
	Current         string   `json:"current,omitempty"`
	Previous        string   `json:"previous,omitempty"`
	PendingUpgrade  bool     `json:"pending_upgrade"`
	PendingFrom     string   `json:"pending_from,omitempty"`
	PendingTo       string   `json:"pending_to,omitempty"`
	Installed       []string `json:"installed"`
	// Orchestration is the persisted upgrade state machine record, when any
	// orchestrated upgrade ever ran for this component.
	Orchestration *OrchestrationState `json:"orchestration,omitempty"`
}

func (u *Upgrader) Status(componentName string) (*Status, error) {
	status := &Status{Component: componentName}
	current, err := CurrentVersion(u.Root, componentName)
	if err != nil {
		return nil, err
	}
	status.Current = current
	if previous, err := readPointer(u.Root, componentName, previousPointerName); err == nil {
		status.Previous = previous.Version
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if journal, err := u.pending(componentName); err == nil {
		status.PendingUpgrade = true
		status.PendingFrom = journal.FromVersion
		status.PendingTo = journal.ToVersion
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	entries, err := os.ReadDir(componentDir(u.Root, componentName))
	if errors.Is(err, os.ErrNotExist) {
		return status, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".staging-") {
			status.Installed = append(status.Installed, entry.Name())
		}
	}
	orchestration, err := u.Orchestration(componentName)
	if err != nil {
		return nil, err
	}
	status.Orchestration = orchestration
	return status, nil
}
