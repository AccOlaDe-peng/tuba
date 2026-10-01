package component

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const stateFormatFileName = "state-format.json"

// stateFormatStamp is the per-instance record of which on-disk format version
// the registry/data directory currently holds. The component writes it at
// first install and whenever it migrates its data.
type stateFormatStamp struct {
	Component string `json:"component"`
	Instance  string `json:"instance"`
	Version   int    `json:"format_version"`
}

// StampStateFormat records the on-disk format version of an instance's data
// directory. It is called at first install and after a confirmed upgrade that
// migrated the data. It refuses to stamp a format below the recorded one:
// format migration is forward-only, so a silent regression would mean the
// directory was replaced behind the supervisor's back.
func StampStateFormat(root, componentName, instance string, formatVersion int) error {
	if !namePattern.MatchString(instance) {
		return fmt.Errorf("invalid instance name %q", instance)
	}
	if formatVersion < 1 {
		return fmt.Errorf("invalid state format version %d", formatVersion)
	}
	dir := instanceDataDir(root, instance)
	existing, err := readStateFormat(root, componentName, instance)
	if err == nil && formatVersion < existing {
		return fmt.Errorf("instance %q data is already at format %d; refusing to stamp older format %d", instance, existing, formatVersion)
	}
	if err != nil && !errors.Is(err, errNoStateStamp) {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create instance data directory: %w", err)
	}
	return writeJSONAtomic(filepath.Join(dir, stateFormatFileName), stateFormatStamp{
		Component: componentName,
		Instance:  instance,
		Version:   formatVersion,
	})
}

var errNoStateStamp = errors.New("no state format stamp")

func instanceDataDir(root, instance string) string {
	return filepath.Join(root, "data", instance)
}

// readStateFormat returns the stamped format version of an instance's data
// directory. Missing stamp yields errNoStateStamp; a corrupt or mismatched
// stamp is a hard error (fail closed).
func readStateFormat(root, componentName, instance string) (int, error) {
	var stamp stateFormatStamp
	err := readJSON(filepath.Join(instanceDataDir(root, instance), stateFormatFileName), &stamp)
	if errors.Is(err, os.ErrNotExist) {
		return 0, errNoStateStamp
	}
	if err != nil {
		return 0, err
	}
	if stamp.Component != componentName || stamp.Instance != instance || stamp.Version < 1 {
		return 0, fmt.Errorf("state format stamp of instance %q is corrupt or belongs to another component", instance)
	}
	return stamp.Version, nil
}

// checkUpgradeCompatibility verifies that a component version described by m
// may run against the instance's data directory:
//   - a stamped format F must satisfy m.MinReadable <= F <= m.Version
//     (older than readable → needs explicit migration tooling, never silent;
//     newer than written → the binary cannot read its own future);
//   - a missing stamp is only acceptable for a fresh, empty data directory —
//     an unstamped non-empty directory is fail-closed because its format is
//     unknown.
func checkUpgradeCompatibility(root, instance string, m *Manifest) error {
	format, err := readStateFormat(root, m.Component, instance)
	if errors.Is(err, errNoStateStamp) {
		empty, statErr := directoryEmpty(instanceDataDir(root, instance))
		if statErr != nil {
			return statErr
		}
		if !empty {
			return fmt.Errorf("instance %q has data but no state format stamp; refusing to guess its format", instance)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return requireReadable(instance, format, m.StateFormat)
}

func requireReadable(instance string, format int, target StateFormat) error {
	if format < target.MinReadable {
		return fmt.Errorf("instance %q data is format %d, older than the minimum %d readable by this version", instance, format, target.MinReadable)
	}
	if format > target.Version {
		return fmt.Errorf("instance %q data is format %d, newer than the format %d this version writes; refusing to open newer state with an older binary", instance, format, target.Version)
	}
	return nil
}

func directoryEmpty(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect instance data directory: %w", err)
	}
	return len(entries) == 0, nil
}
