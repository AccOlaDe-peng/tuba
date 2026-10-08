package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSystemLoginSettings(t *testing.T) {
	file := filepath.Join(t.TempDir(), "auth.json")
	t.Setenv("TUBA_AUTH_CONFIG", file)
	t.Setenv("TUBA_PUBLIC_ORIGIN", "")
	t.Setenv("TUBA_AUTH_COOKIE_SECURE", "")
	t.Setenv("TUBA_AUTH_SESSION_TTL", "")
	if err := os.WriteFile(file, []byte(`{"public_origin":"https://tuba.example:8443","cookie_secure":true,"session_ttl":"2h"}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, ttl, err := SystemLoginSettings()
	if err != nil || !cfg.CookieSecure || ttl != 2*time.Hour || cfg.PublicOrigin != "https://tuba.example:8443" {
		t.Fatal(cfg, ttl, err)
	}
	t.Setenv("TUBA_AUTH_SESSION_TTL", "25h")
	if _, _, err = SystemLoginSettings(); err == nil {
		t.Fatal("unbounded session lifetime accepted")
	}
	t.Setenv("TUBA_AUTH_SESSION_TTL", "")
	t.Setenv("TUBA_PUBLIC_ORIGIN", "http://tuba.example")
	if _, _, err = SystemLoginSettings(); err == nil {
		t.Fatal("secure cookies allowed on HTTP origin")
	}
	t.Setenv("TUBA_AUTH_COOKIE_SECURE", "false")
	if _, _, err = SystemLoginSettings(); err != nil {
		t.Fatal("local HTTP configuration", err)
	}
	t.Setenv("TUBA_PUBLIC_ORIGIN", "https://tuba.example/path")
	if _, _, err = SystemLoginSettings(); err == nil {
		t.Fatal("non-origin URL accepted")
	}
}
