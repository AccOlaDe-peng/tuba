package launcher

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLoadManifestResolvesSecretReferenceOnlyInMemory(t *testing.T) {
	root := t.TempDir()
	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0700); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(binDir, "service")
	if err := os.WriteFile(command, []byte("binary"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TUBA_TEST_PASSWORD", "secret-value")
	manifestPath := filepath.Join(root, "tuba-services.json")
	body := `{"version":1,"state_dir":"state","log_dir":"logs","services":[{"name":"worker","command":"bin/service","environment":{"DATABASE_URL":"${TUBA_TEST_PASSWORD}","MODE":"validation"}}]}`
	if err := os.WriteFile(manifestPath, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(manifest.Services[0].resolvedEnv, "\n"); !strings.Contains(got, "DATABASE_URL=secret-value") {
		t.Fatalf("secret reference was not resolved for child: %q", got)
	}
	stored, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(stored), "secret-value") {
		t.Fatal("resolved secret was written into the manifest")
	}
}

func TestExampleServiceManifestsReferenceAvailableEnvironmentKeys(t *testing.T) {
	root := filepath.Join("..", "..", "deploy", "launcher")
	environmentFile, err := os.ReadFile(filepath.Join(root, "tuba.env.example"))
	if err != nil {
		t.Fatal(err)
	}
	available := map[string]bool{}
	for _, line := range strings.Split(string(environmentFile), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("invalid environment template entry %q", line)
		}
		available[strings.TrimSpace(key)] = true
	}

	for _, name := range []string{"tuba-services.linux.example.json", "tuba-services.windows.example.json"} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		var manifest Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		for _, service := range manifest.Services {
			for key, value := range service.Environment {
				if sensitiveName.MatchString(key) && !envReference.MatchString(value) {
					t.Errorf("%s service %s embeds a sensitive value for %s", name, service.Name, key)
				}
				if match := envReference.FindStringSubmatch(value); len(match) == 2 && !available[match[1]] {
					t.Errorf("%s service %s references missing environment key %s", name, service.Name, match[1])
				}
			}
		}
	}
}

func TestLoadManifestRejectsInlineSensitiveValue(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "service"), []byte("binary"), 0600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "tuba-services.json")
	body := `{"version":1,"state_dir":"state","log_dir":"logs","services":[{"name":"worker","command":"service","environment":{"ES_API_KEY":"literal-secret"}}]}`
	if err := os.WriteFile(manifestPath, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(manifestPath); err == nil {
		t.Fatal("inline API key was accepted")
	}
}

func TestLoadManifestValidatesListenerAddressesBeforeActivation(t *testing.T) {
	tests := []struct {
		name        string
		environment map[string]string
		wantError   bool
	}{
		{name: "wildcard requires explicit opt-in", environment: map[string]string{"HTTP_LISTEN": ":8080"}, wantError: true},
		{name: "wildcard allowed with opt-in", environment: map[string]string{"HTTP_LISTEN": ":8080", "TUBA_ALLOW_NON_LOOPBACK_LISTEN": "true"}},
		{name: "web wildcard requires explicit opt-in", environment: map[string]string{"WEB_LISTEN": ":8088"}, wantError: true},
		{name: "web wildcard allowed with opt-in", environment: map[string]string{"WEB_LISTEN": ":8088", "TUBA_ALLOW_NON_LOOPBACK_LISTEN": "true"}},
		{name: "external IP requires opt-in", environment: map[string]string{"API_LISTEN": "192.0.2.10:8788"}, wantError: true},
		{name: "external IP allowed with opt-in", environment: map[string]string{"API_LISTEN": "192.0.2.10:8788", "TUBA_ALLOW_NON_LOOPBACK_LISTEN": "true"}},
		{name: "DNS listener remains invalid", environment: map[string]string{"API_LISTEN": "api.internal:8788", "TUBA_ALLOW_NON_LOOPBACK_LISTEN": "true"}, wantError: true},
		{name: "invalid opt-in is rejected", environment: map[string]string{"API_LISTEN": "127.0.0.1:8788", "TUBA_ALLOW_NON_LOOPBACK_LISTEN": "TRUE"}, wantError: true},
		{name: "worker metrics wildcard requires opt-in", environment: map[string]string{"RAW_INDEXER_METRICS_LISTEN": ":19095"}, wantError: true},
		{name: "worker metrics wildcard allowed with opt-in", environment: map[string]string{"RAW_INDEXER_METRICS_LISTEN": ":19095", "TUBA_ALLOW_NON_LOOPBACK_LISTEN": "true"}},
		{name: "Python metrics wildcard requires opt-in", environment: map[string]string{"METRICS_LISTEN": ":9090"}, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			command := filepath.Join(root, "service")
			if err := os.WriteFile(command, []byte("binary"), 0600); err != nil {
				t.Fatal(err)
			}
			manifest := map[string]any{
				"version":   1,
				"state_dir": "state",
				"log_dir":   "logs",
				"services": []any{map[string]any{
					"name":        "listener",
					"command":     command,
					"environment": tt.environment,
				}},
			}
			body, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(root, "tuba-services.json")
			if err := os.WriteFile(manifestPath, body, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = LoadManifest(manifestPath)
			if (err != nil) != tt.wantError {
				t.Fatalf("LoadManifest error=%v, wantError=%t", err, tt.wantError)
			}
		})
	}
}

func TestLoadManifestReadsRestrictedEnvironmentFile(t *testing.T) {
	root := t.TempDir()
	if err := securePrivateDirectory(root); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(root, "service")
	if err := os.WriteFile(command, []byte("binary"), 0600); err != nil {
		t.Fatal(err)
	}
	environmentFile := filepath.Join(root, "tuba.env")
	if err := os.WriteFile(environmentFile, []byte("TUBA_TEST_SECRET=left=right\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(environmentFile, 0600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "tuba-services.json")
	body := `{"version":1,"state_dir":"state","log_dir":"logs","environment_file":"tuba.env","services":[{"name":"worker","command":"service","environment":{"ES_API_KEY":"${TUBA_TEST_SECRET}"}}]}`
	if err := os.WriteFile(manifestPath, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := manifest.Services[0].resolvedEnv[0]; got != "ES_API_KEY=left=right" {
		t.Fatalf("unexpected resolved environment entry %q", got)
	}
}

func TestControlManifestDoesNotRequireRuntimeCredentialsOrBinaries(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(root, "tuba-services.json")
	body := `{"version":1,"state_dir":"state","log_dir":"logs","environment_file":"missing.env","services":[{"name":"worker","command":"missing-binary","environment":{"ES_API_KEY":"${NOT_AVAILABLE}"}}]}`
	if err := os.WriteFile(manifestPath, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifestForControl(manifestPath); err != nil {
		t.Fatalf("status/stop config unnecessarily required runtime-only files: %v", err)
	}
}

func TestChildEnvironmentOnlyInheritsSafeBaseAndExplicitValues(t *testing.T) {
	t.Setenv("TUBA_UNDECLARED_PASSWORD", "must-not-reach-child")
	env := childEnvironment([]string{"TUBA_CONFIGURED_PASSWORD=declared-secret"})
	if slices.Contains(env, "TUBA_UNDECLARED_PASSWORD=must-not-reach-child") {
		t.Fatal("child inherited an undeclared parent secret")
	}
	if !slices.Contains(env, "TUBA_CONFIGURED_PASSWORD=declared-secret") {
		t.Fatal("explicit manifest environment was not passed to child")
	}
}
