// Package agent implements the TUBA Management Agent client: enrollment,
// 30-second heartbeats, ETag-based configuration polling and offline
// retention of the last effective configuration. It talks to the collector
// management endpoints served by tuba-api (see internal/api/collectors.go).
//
// The agent never accepts arbitrary commands, executables or inline secrets
// from the management plane; remote configuration is validated again on the
// agent side even though the server already rejects such content.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrUnauthorized means the enrollment token or collector credential was
	// rejected (401): unknown, expired, consumed or disabled.
	ErrUnauthorized = errors.New("agent credential rejected by management API")
	// ErrDisabled means the collector has been disabled on the server; the
	// agent must stop applying configuration and shut down.
	ErrDisabled = errors.New("collector is disabled on the management plane")
	// ErrInvalidRemoteConfig means the server delivered a configuration that
	// failed agent-side validation. Unlike a transient poll failure this is a
	// definitive rejection: the configuration is never persisted.
	ErrInvalidRemoteConfig = errors.New("remote configuration failed agent-side validation")
)

const (
	// DefaultHeartbeatInterval is the management-plane cadence fixed by the
	// collector control-plane design (30 seconds).
	DefaultHeartbeatInterval = 30 * time.Second

	maxConfigBytes       = 64 << 10
	maxEnrollmentBody    = 8 << 10
	maxResponseBodyBytes = 1 << 20
)

// Registration is the agent self-description sent once at enrollment.
type Registration struct {
	InstallID    string `json:"install_id"`
	Hostname     string `json:"hostname"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Version      string `json:"version"`
}

// Registered is the server-issued identity returned by enrollment. The
// credential is shown exactly once and must be persisted by the caller.
type Registered struct {
	CollectorID              string `json:"collector_id"`
	Credential               string `json:"collector_credential"`
	HeartbeatIntervalSeconds int    `json:"heartbeat_interval_seconds"`
}

// Heartbeat mirrors control.CollectorHeartbeat on the server side. It carries
// operational metadata only; event payloads must never be sent.
type Heartbeat struct {
	Version        string         `json:"version"`
	ConfigVersion  int64          `json:"config_version"`
	State          string         `json:"state"`
	Sources        []SourceStatus `json:"sources"`
	QueueDepth     int64          `json:"queue_depth"`
	OldestQueuedAt *time.Time     `json:"oldest_queued_at,omitempty"`
	Diagnostic     string         `json:"diagnostic,omitempty"`
}

// SourceStatus is the per-source operational summary included in heartbeats.
type SourceStatus struct {
	SourceID   string `json:"source_id"`
	State      string `json:"state"`
	EventsRead int64  `json:"events_read"`
	EventsSent int64  `json:"events_sent"`
	EventsDrop int64  `json:"events_drop"`
	LastError  string `json:"last_error,omitempty"`
}

// Config is a versioned management configuration as served by the API.
type Config struct {
	CollectorID   string          `json:"collector_id"`
	Version       int64           `json:"version"`
	Configuration json.RawMessage `json:"configuration"`
	CreatedAt     time.Time       `json:"created_at"`
}

// Client calls the collector management endpoints of a tuba-api base URL.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c *Client) url(path string) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return "", errors.New("agent API base URL is required")
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return "", fmt.Errorf("agent API base URL must start with http:// or https://")
	}
	return base + path, nil
}

// Enroll exchanges a one-time enrollment token for a collector identity.
func (c *Client) Enroll(ctx context.Context, token string, reg Registration) (Registered, error) {
	var out Registered
	endpoint, err := c.url("/api/v1/collector/enroll")
	if err != nil {
		return out, err
	}
	body, err := json.Marshal(struct {
		EnrollmentToken string `json:"enrollment_token"`
		Registration
	}{EnrollmentToken: token, Registration: reg})
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return out, fmt.Errorf("enroll: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if err != nil {
		return out, fmt.Errorf("read enroll response: %w", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return out, ErrUnauthorized
	}
	if resp.StatusCode != http.StatusCreated {
		return out, fmt.Errorf("enroll: unexpected status %d", resp.StatusCode)
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, fmt.Errorf("decode enroll response: %w", err)
	}
	if !strings.HasPrefix(out.CollectorID, "col_") || !strings.HasPrefix(out.Credential, "tuba_col_") {
		return out, errors.New("enroll: server returned a malformed collector identity")
	}
	return out, nil
}

// SendHeartbeat reports one heartbeat. A 401 response means the credential is
// unknown or the collector was disabled; both fail closed as ErrDisabled so
// the caller stops applying management configuration.
func (c *Client) SendHeartbeat(ctx context.Context, credential string, beat Heartbeat) error {
	endpoint, err := c.url("/api/v1/collector/heartbeat")
	if err != nil {
		return err
	}
	if beat.Sources == nil {
		beat.Sources = []SourceStatus{}
	}
	body, err := json.Marshal(beat)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+credential)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxEnrollmentBody))
	switch resp.StatusCode {
	case http.StatusNoContent:
		return nil
	case http.StatusUnauthorized:
		return ErrDisabled
	default:
		return fmt.Errorf("heartbeat: unexpected status %d", resp.StatusCode)
	}
}

// FetchConfig polls the desired configuration. knownVersion is sent as an
// ETag; when the server answers 304 the returned changed flag is false. A 204
// means no configuration has been published yet. A 401 means the collector is
// disabled and fails closed as ErrDisabled.
func (c *Client) FetchConfig(ctx context.Context, credential string, knownVersion int64) (cfg Config, changed bool, err error) {
	endpoint, err := c.url("/api/v1/collector/config")
	if err != nil {
		return cfg, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return cfg, false, err
	}
	req.Header.Set("Authorization", "Bearer "+credential)
	if knownVersion > 0 {
		req.Header.Set("If-None-Match", "\""+strconv.FormatInt(knownVersion, 10)+"\"")
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return cfg, false, fmt.Errorf("fetch config: %w", err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if err != nil {
		return cfg, false, fmt.Errorf("read config response: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusNotModified:
		return cfg, false, nil
	case http.StatusNoContent:
		return cfg, false, nil
	case http.StatusUnauthorized:
		return cfg, false, ErrDisabled
	case http.StatusOK:
	default:
		return cfg, false, fmt.Errorf("fetch config: unexpected status %d", resp.StatusCode)
	}
	if int64(len(payload)) > maxResponseBodyBytes {
		return cfg, false, errors.New("fetch config: response exceeds size limit")
	}
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return cfg, false, fmt.Errorf("decode config response: %w", err)
	}
	if cfg.Version <= 0 {
		return cfg, false, fmt.Errorf("fetch config: %w: server returned a non-positive config version", ErrInvalidRemoteConfig)
	}
	if err := ValidateRemoteConfiguration(cfg.Configuration); err != nil {
		return cfg, false, fmt.Errorf("fetch config: %w: %v", ErrInvalidRemoteConfig, err)
	}
	return cfg, true, nil
}
