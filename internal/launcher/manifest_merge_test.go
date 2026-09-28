package launcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMergeManifestPreservesOverridesAndAddsNewServices(t *testing.T) {
	root := t.TempDir()
	current := filepath.Join(root, "current")
	release := filepath.Join(root, "release")
	config := filepath.Join(root, "config")
	for _, dir := range []string{current, release, config} {
		if err := os.MkdirAll(filepath.Join(dir, "bin"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{filepath.Join(current, "bin", "worker"), filepath.Join(release, "bin", "worker"), filepath.Join(release, "bin", "web")} {
		if err := os.WriteFile(path, []byte("placeholder executable"), 0755); err != nil {
			t.Fatal(err)
		}
	}
	existingPath := filepath.Join(config, "tuba-services.json")
	previousDefaultsPath := filepath.Join(config, "previous-defaults.json")
	defaultsPath := filepath.Join(release, "defaults.json")
	outputPath := filepath.Join(config, "candidate.json")
	existing := `{"version":1,"state_dir":"` + filepath.ToSlash(filepath.Join(root, "state")) + `","log_dir":"` + filepath.ToSlash(filepath.Join(root, "logs")) + `","services":[{"name":"worker","command":"` + filepath.ToSlash(filepath.Join(current, "bin", "worker")) + `","environment":{"MODE":"custom","HTTP_LISTEN":"127.0.0.1:18080"},"restart_min":"2s"}]}`
	previousDefaults := `{"version":1,"state_dir":"` + filepath.ToSlash(filepath.Join(root, "previous-state")) + `","log_dir":"` + filepath.ToSlash(filepath.Join(root, "previous-logs")) + `","services":[{"name":"worker","command":"` + filepath.ToSlash(filepath.Join(current, "bin", "worker")) + `"},{"name":"removed-by-admin","command":"` + filepath.ToSlash(filepath.Join(current, "bin", "worker")) + `"}]}`
	defaults := `{"version":1,"state_dir":"` + filepath.ToSlash(filepath.Join(root, "default-state")) + `","log_dir":"` + filepath.ToSlash(filepath.Join(root, "default-logs")) + `","services":[{"name":"worker","command":"` + filepath.ToSlash(filepath.Join(release, "bin", "worker")) + `","environment":{"MODE":"default"}},{"name":"removed-by-admin","command":"` + filepath.ToSlash(filepath.Join(release, "bin", "worker")) + `"},{"name":"web","command":"` + filepath.ToSlash(filepath.Join(current, "bin", "web")) + `","working_dir":"` + filepath.ToSlash(current) + `","environment":{"WEB_LISTEN":"127.0.0.1:8088"}}]}`
	if err := os.WriteFile(existingPath, []byte(existing), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(previousDefaultsPath, []byte(previousDefaults), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(defaultsPath, []byte(defaults), 0600); err != nil {
		t.Fatal(err)
	}
	if err := MergeManifest(existingPath, previousDefaultsPath, defaultsPath, outputPath, current, release); err != nil {
		t.Fatal(err)
	}
	merged, err := LoadManifestForControl(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(merged.Services); got != 2 {
		t.Fatalf("merged service count = %d, want 2 (retained worker, newly introduced web)", got)
	}
	worker := merged.Services[0]
	if worker.Environment["MODE"] != "custom" || worker.Environment["HTTP_LISTEN"] != "127.0.0.1:18080" || worker.RestartMin != "2s" {
		t.Fatalf("existing overrides changed: %#v", worker)
	}
	if merged.StateDir != filepath.Join(root, "state") {
		t.Fatalf("existing state_dir changed: %q", merged.StateDir)
	}
	web := merged.Services[1]
	if web.Name != "web" || filepath.Clean(web.Command) != filepath.Join(current, "bin", "web") || filepath.Clean(web.WorkingDir) != current {
		t.Fatalf("new service defaults were not retained: %#v", web)
	}
	for _, service := range merged.Services {
		if service.Name == "removed-by-admin" {
			t.Fatal("service removed from the installed manifest was reintroduced")
		}
	}
}

func TestMergeManifestLeavesNoCandidateWhenNewServiceCannotValidate(t *testing.T) {
	root := t.TempDir()
	current := filepath.Join(root, "current")
	release := filepath.Join(root, "release")
	config := filepath.Join(root, "config")
	for _, dir := range []string{current, release, config} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	old := `{"version":1,"state_dir":"` + filepath.ToSlash(filepath.Join(root, "state")) + `","log_dir":"` + filepath.ToSlash(filepath.Join(root, "logs")) + `","services":[{"name":"old","command":"` + filepath.ToSlash(filepath.Join(current, "old")) + `"}]}`
	defaults := `{"version":1,"state_dir":"` + filepath.ToSlash(filepath.Join(root, "ignored-state")) + `","log_dir":"` + filepath.ToSlash(filepath.Join(root, "ignored-logs")) + `","services":[{"name":"new","command":"` + filepath.ToSlash(filepath.Join(current, "missing")) + `"}]}`
	existingPath, previousDefaultsPath, defaultsPath, outputPath := filepath.Join(config, "old.json"), filepath.Join(config, "previous-defaults.json"), filepath.Join(config, "defaults.json"), filepath.Join(config, "candidate.json")
	previousDefaults := old
	if err := os.WriteFile(existingPath, []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	originalManifest, err := os.ReadFile(existingPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(previousDefaultsPath, []byte(previousDefaults), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(defaultsPath, []byte(defaults), 0600); err != nil {
		t.Fatal(err)
	}
	err = MergeManifest(existingPath, previousDefaultsPath, defaultsPath, outputPath, current, release)
	if err == nil || !strings.Contains(err.Error(), "does not validate") {
		t.Fatalf("MergeManifest error = %v, want validation failure", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("candidate should not be created on validation failure, stat error = %v", err)
	}
	currentManifest, err := os.ReadFile(existingPath)
	if err != nil || string(currentManifest) != string(originalManifest) {
		t.Fatalf("installed manifest changed after rejected migration: err=%v", err)
	}
}
