package launcher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MergeManifest appends services introduced by a release while retaining every
// configured service and top-level setting from the installed manifest. The
// merged candidate is validated against the new release before it is written.
func MergeManifest(existingPath, previousDefaultsPath, defaultsPath, outputPath, currentPath, releasePath string) error {
	existing, err := readManifestForMerge(existingPath)
	if err != nil {
		return fmt.Errorf("read installed manifest: %w", err)
	}
	defaults, err := readManifestForMerge(defaultsPath)
	if err != nil {
		return fmt.Errorf("read release defaults: %w", err)
	}
	previousDefaults, err := readManifestForMerge(previousDefaultsPath)
	if err != nil {
		return fmt.Errorf("read installed release defaults: %w", err)
	}
	if existing.Version != defaults.Version || previousDefaults.Version != defaults.Version {
		return fmt.Errorf("cannot merge launcher manifest versions %d, %d, and %d", existing.Version, previousDefaults.Version, defaults.Version)
	}
	merged := *existing
	merged.Services = append([]ServiceSpec(nil), existing.Services...)
	seen := make(map[string]bool, len(merged.Services))
	previouslyShipped := make(map[string]bool, len(previousDefaults.Services))
	for _, service := range merged.Services {
		seen[service.Name] = true
	}
	for _, service := range previousDefaults.Services {
		previouslyShipped[service.Name] = true
	}
	for _, service := range defaults.Services {
		if !seen[service.Name] && !previouslyShipped[service.Name] {
			merged.Services = append(merged.Services, service)
			seen[service.Name] = true
		}
	}

	outputPath, err = filepath.Abs(outputPath)
	if err != nil {
		return fmt.Errorf("resolve candidate manifest path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0750); err != nil {
		return fmt.Errorf("create candidate directory: %w", err)
	}
	validationPath := outputPath + ".validate"
	defer os.Remove(validationPath)
	validation := merged
	validation.Services = append([]ServiceSpec(nil), merged.Services...)
	for i := range validation.Services {
		service := &validation.Services[i]
		if service.Command, err = remapReleasePath(service.Command, currentPath, releasePath); err != nil {
			return fmt.Errorf("service %q command: %w", service.Name, err)
		}
		if service.WorkingDir != "" {
			if service.WorkingDir, err = remapReleasePath(service.WorkingDir, currentPath, releasePath); err != nil {
				return fmt.Errorf("service %q working_dir: %w", service.Name, err)
			}
		}
	}
	if err := writeManifest(validationPath, &validation); err != nil {
		return fmt.Errorf("write validation manifest: %w", err)
	}
	if _, err := LoadManifest(validationPath); err != nil {
		return fmt.Errorf("merged manifest does not validate against new release: %w", err)
	}

	tempPath := outputPath + ".tmp"
	defer os.Remove(tempPath)
	if err := writeManifest(tempPath, &merged); err != nil {
		return fmt.Errorf("write merged manifest: %w", err)
	}
	if err := os.Rename(tempPath, outputPath); err != nil {
		return fmt.Errorf("activate candidate manifest: %w", err)
	}
	return nil
}

func readManifestForMerge(path string) (*Manifest, error) {
	return LoadManifestForControl(path)
}

func writeManifest(path string, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func remapReleasePath(value, currentPath, releasePath string) (string, error) {
	if value == "" || !filepath.IsAbs(value) {
		return value, nil
	}
	current, err := filepath.Abs(currentPath)
	if err != nil {
		return "", err
	}
	valueAbs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(current, valueAbs)
	if err != nil {
		return "", err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return value, nil
	}
	if relative == "." {
		return filepath.Clean(releasePath), nil
	}
	return filepath.Join(releasePath, relative), nil
}
