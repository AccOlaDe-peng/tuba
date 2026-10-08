package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

type LoginSettings struct {
	PublicOrigin string `json:"public_origin"`
	SessionTTL   string `json:"session_ttl"`
	CookieSecure bool   `json:"cookie_secure"`
}

// The non-secret file is read at API start so a single-service Launcher restart
// can update login settings without restarting the entire data plane.
func SystemLoginSettings() (LoginSettings, time.Duration, error) {
	cfg := LoginSettings{CookieSecure: true, SessionTTL: "8h"}
	path := os.Getenv("TUBA_AUTH_CONFIG")
	if path == "" {
		path = "/etc/tuba/auth.json"
	}
	data, err := os.ReadFile(path)
	if err == nil {
		if len(data) > 4096 {
			return cfg, 0, errors.New("login configuration too large")
		}
		if err = json.Unmarshal(data, &cfg); err != nil {
			return cfg, 0, errors.New("invalid login configuration")
		}
	} else if !os.IsNotExist(err) {
		return cfg, 0, errors.New("cannot read login configuration")
	}
	if v := os.Getenv("TUBA_PUBLIC_ORIGIN"); v != "" {
		cfg.PublicOrigin = v
	}
	if v := os.Getenv("TUBA_AUTH_SESSION_TTL"); v != "" {
		cfg.SessionTTL = v
	}
	if v := os.Getenv("TUBA_AUTH_COOKIE_SECURE"); v != "" {
		if v != "true" && v != "false" {
			return cfg, 0, errors.New("TUBA_AUTH_COOKIE_SECURE must be true or false")
		}
		cfg.CookieSecure = v == "true"
	}
	lifetime, err := time.ParseDuration(cfg.SessionTTL)
	if err != nil || lifetime < time.Minute || lifetime > 24*time.Hour {
		return cfg, 0, errors.New("session TTL must be between 1m and 24h")
	}
	if cfg.PublicOrigin != "" {
		u, err := url.Parse(cfg.PublicOrigin)
		if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
			return cfg, 0, fmt.Errorf("invalid public login origin")
		}
		if cfg.CookieSecure && u.Scheme != "https" {
			return cfg, 0, errors.New("secure login cookies require an HTTPS public origin")
		}
	}
	return cfg, lifetime, nil
}
