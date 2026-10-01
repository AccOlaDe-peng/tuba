package component

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveCurrentBinary resolves the executable of the currently active
// component version: <root>/components/<name>/<current>/<relBin>. Launcher
// manifests point at this stable entry (tuba-component run) so a version
// switch plus a service restart picks up the new binary. Relative config
// arguments (see ResolveArgs) are resolved against the same version
// directory, keeping the whole invocation version-consistent.
func ResolveCurrentBinary(root, componentName, relBin string) (dir string, binary string, err error) {
	if err := validatePackagePath(relBin); err != nil {
		return "", "", err
	}
	version, err := CurrentVersion(root, componentName)
	if err != nil {
		return "", "", err
	}
	if version == "" {
		return "", "", fmt.Errorf("component %q: %w", componentName, ErrNoActiveVersion)
	}
	dir = versionDir(root, componentName, version)
	binary = filepath.Join(dir, filepath.FromSlash(relBin))
	info, err := os.Stat(binary)
	if err != nil || info.IsDir() {
		return "", "", fmt.Errorf("component %q version %s binary %s is missing", componentName, version, relBin)
	}
	return dir, binary, nil
}

// ResolveArgs rewrites relative file arguments against the active version
// directory, so a manifest can say "-c filebeat.yml" and always get the
// config shipped inside the signed package. Arguments that are absolute
// paths, flags without values, or non-path-looking values pass through
// unchanged; only the value of "-c"/"--c" (the Beat config flag) is resolved.
func ResolveArgs(versionDirPath string, args []string) []string {
	out := make([]string, 0, len(args))
	for i, arg := range args {
		if i > 0 && (args[i-1] == "-c" || args[i-1] == "--c") && !filepath.IsAbs(arg) && !strings.Contains(arg, "://") {
			out = append(out, filepath.Join(versionDirPath, filepath.FromSlash(arg)))
			continue
		}
		out = append(out, arg)
	}
	return out
}

// ErrNoActiveVersion reports a component with no current pointer.
var ErrNoActiveVersion = errors.New("component has no active version")
