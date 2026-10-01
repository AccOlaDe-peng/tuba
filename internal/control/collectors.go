package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

var ErrCollectorUnauthorized = errors.New("collector credential is unknown or disabled")
var ErrCollectorConfigNotFound = errors.New("collector has no published config")
var ErrInvalidCollectorRegistration = errors.New("invalid collector registration")
var ErrInvalidCollectorHeartbeat = errors.New("invalid collector heartbeat")
var ErrInvalidCollectorConfig = errors.New("invalid collector configuration")

type CollectorEnrollment struct {
	Token     string    `json:"enrollment_token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type CollectorRegistration struct {
	InstallID    string `json:"install_id"`
	Hostname     string `json:"hostname"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Version      string `json:"version"`
}

type RegisteredCollector struct {
	ID                       string `json:"collector_id"`
	Credential               string `json:"collector_credential"`
	HeartbeatIntervalSeconds int    `json:"heartbeat_interval_seconds"`
}

type CollectorHeartbeat struct {
	Version        string                  `json:"version"`
	ConfigVersion  int64                   `json:"config_version"`
	State          string                  `json:"state"`
	Sources        []CollectorSourceStatus `json:"sources"`
	QueueDepth     int64                   `json:"queue_depth"`
	OldestQueuedAt *time.Time              `json:"oldest_queued_at,omitempty"`
	Diagnostic     string                  `json:"diagnostic,omitempty"`
}

// CollectorSourceStatus is deliberately limited to operational metadata; event
// payloads and arbitrary source documents must never be sent in heartbeats.
type CollectorSourceStatus struct {
	SourceID   string `json:"source_id"`
	State      string `json:"state"`
	EventsRead int64  `json:"events_read"`
	EventsSent int64  `json:"events_sent"`
	EventsDrop int64  `json:"events_drop"`
	LastError  string `json:"last_error,omitempty"`
}

type CollectorSummary struct {
	ID                   string     `json:"id"`
	Organization         string     `json:"organization_id"`
	Namespace            string     `json:"namespace"`
	Hostname             string     `json:"hostname"`
	OS                   string     `json:"os"`
	Architecture         string     `json:"architecture"`
	InstalledVersion     string     `json:"installed_version"`
	DesiredVersion       string     `json:"desired_version,omitempty"`
	State                string     `json:"state"`
	ConfigVersion        int64      `json:"config_version"`
	DesiredConfigVersion int64      `json:"desired_config_version"`
	LastHeartbeatAt      *time.Time `json:"last_heartbeat_at,omitempty"`
	Online               bool       `json:"online"`
}

type CollectorConfig struct {
	CollectorID   string          `json:"collector_id"`
	Version       int64           `json:"version"`
	Configuration json.RawMessage `json:"configuration"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (s *Store) CreateCollectorEnrollment(ctx context.Context, p auth.Principal, minutes int, requestID string) (CollectorEnrollment, error) {
	var out CollectorEnrollment
	if minutes < 5 || minutes > 1440 {
		return out, errors.New("enrollment expiration must be 5..1440 minutes")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return out, err
	}
	plain := "tuba_enroll_" + base64.RawURLEncoding.EncodeToString(secret)
	digest := digestCredential(plain)
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var orgID, identityID, namespace string
	err = tx.QueryRow(ctx, `SELECT o.id::text,i.id::text,o.namespace FROM organizations o JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL WHERE o.slug=$3`, s.Issuer, p.Subject, p.Organization).Scan(&orgID, &identityID, &namespace)
	if err != nil {
		return out, errors.New("active tenant membership not found")
	}
	err = tx.QueryRow(ctx, `INSERT INTO collector_enrollment_tokens(token_hash,organization_id,namespace,created_by,expires_at) VALUES($1,$2,$3,$4,now()+make_interval(mins=>$5)) RETURNING expires_at`, digest, orgID, namespace, identityID, minutes).Scan(&out.ExpiresAt)
	if err != nil {
		return out, err
	}
	state, _ := json.Marshal(map[string]any{"expires_at": out.ExpiresAt, "expires_in_minutes": minutes})
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state) VALUES($1,$2,'collector.enrollment.create','collector_enrollment',$3,$4,$5)`, orgID, identityID, digest, requestID, state); err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	out.Token = plain
	return out, nil
}

func (s *Store) EnrollCollector(ctx context.Context, token string, in CollectorRegistration) (RegisteredCollector, error) {
	var out RegisteredCollector
	if !strings.HasPrefix(token, "tuba_enroll_") || len(token) > 128 {
		return out, ErrCollectorUnauthorized
	}
	if !bounded(in.InstallID, 1, 128) || !bounded(in.Hostname, 1, 255) || !bounded(in.Version, 1, 64) || !map[string]bool{"linux": true, "windows": true}[in.OS] || !map[string]bool{"amd64": true, "arm64": true}[in.Architecture] {
		return out, ErrInvalidCollectorRegistration
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return out, err
	}
	id := "col_" + hex.EncodeToString(idBytes)
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return out, err
	}
	credential := "tuba_col_" + base64.RawURLEncoding.EncodeToString(secret)
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var orgID, namespace, creatorID string
	err = tx.QueryRow(ctx, `SELECT organization_id::text,namespace,created_by::text FROM collector_enrollment_tokens WHERE token_hash=$1 AND consumed_at IS NULL AND expires_at>now() FOR UPDATE`, digestCredential(token)).Scan(&orgID, &namespace, &creatorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrCollectorUnauthorized
	}
	if err != nil {
		return out, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO collector_agents(id,organization_id,namespace,install_id,hostname,os,architecture,credential_ref,installed_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, id, orgID, namespace, in.InstallID, in.Hostname, in.OS, in.Architecture, digestCredential(credential), in.Version)
	if err != nil {
		return out, err
	}
	if _, err = tx.Exec(ctx, `UPDATE collector_enrollment_tokens SET consumed_at=now() WHERE token_hash=$1`, digestCredential(token)); err != nil {
		return out, err
	}
	state, _ := json.Marshal(map[string]any{"collector_id": id, "hostname": in.Hostname, "os": in.OS, "architecture": in.Architecture, "version": in.Version})
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state) VALUES($1,$2,'collector.enroll','collector',$3,$4,$5)`, orgID, creatorID, id, "enroll:"+id, state); err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	out = RegisteredCollector{ID: id, Credential: credential, HeartbeatIntervalSeconds: 30}
	return out, nil
}

func (s *Store) AuthenticateCollector(ctx context.Context, credential string) (string, error) {
	if !strings.HasPrefix(credential, "tuba_col_") || len(credential) > 128 {
		return "", ErrCollectorUnauthorized
	}
	var id string
	err := s.Pool.QueryRow(ctx, `SELECT id FROM collector_agents WHERE credential_ref=$1 AND state<>'disabled'`, digestCredential(credential)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrCollectorUnauthorized
	}
	return id, err
}

func (s *Store) ReportCollectorHeartbeat(ctx context.Context, credential string, in CollectorHeartbeat) error {
	if len(in.Version) > 64 || in.ConfigVersion < 0 || in.QueueDepth < 0 || !map[string]bool{"running": true, "buffering": true, "backpressured": true, "paused": true, "error": true}[in.State] || len(in.Diagnostic) > 2048 || in.Sources == nil || len(in.Sources) > 256 {
		return ErrInvalidCollectorHeartbeat
	}
	seenSources := make(map[string]bool, len(in.Sources))
	for _, source := range in.Sources {
		if !bounded(source.SourceID, 1, 128) || seenSources[source.SourceID] || !map[string]bool{"running": true, "paused": true, "error": true}[source.State] || source.EventsRead < 0 || source.EventsSent < 0 || source.EventsDrop < 0 || len(source.LastError) > 512 {
			return ErrInvalidCollectorHeartbeat
		}
		seenSources[source.SourceID] = true
	}
	result, err := s.Pool.Exec(ctx, `UPDATE collector_agents SET state=$2,installed_version=$3,config_version=$4,last_heartbeat_at=now(),heartbeat=$5::jsonb,updated_at=now() WHERE credential_ref=$1 AND state<>'disabled'`, digestCredential(credential), in.State, in.Version, in.ConfigVersion, heartbeatJSON(in))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrCollectorUnauthorized
	}
	return nil
}

func (s *Store) ListCollectors(ctx context.Context, p auth.Principal) ([]CollectorSummary, error) {
	rows, err := s.Pool.Query(ctx, `SELECT ca.id,o.id::text,ca.namespace,ca.hostname,ca.os,ca.architecture,ca.installed_version,COALESCE(ca.desired_version,''),ca.state,ca.config_version,COALESCE((SELECT max(c.version) FROM collector_agent_configs c WHERE c.collector_id=ca.id),0),ca.last_heartbeat_at,COALESCE(ca.last_heartbeat_at>now()-interval '90 seconds',false) FROM collector_agents ca JOIN organizations o ON o.id=ca.organization_id WHERE o.slug=$1 ORDER BY ca.hostname,ca.id`, p.Organization)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]CollectorSummary, 0)
	for rows.Next() {
		var x CollectorSummary
		if err := rows.Scan(&x.ID, &x.Organization, &x.Namespace, &x.Hostname, &x.OS, &x.Architecture, &x.InstalledVersion, &x.DesiredVersion, &x.State, &x.ConfigVersion, &x.DesiredConfigVersion, &x.LastHeartbeatAt, &x.Online); err != nil {
			return nil, err
		}
		items = append(items, x)
	}
	return items, rows.Err()
}

func (s *Store) SaveCollectorConfig(ctx context.Context, p auth.Principal, id string, raw json.RawMessage, requestID string) (CollectorConfig, error) {
	var out CollectorConfig
	if !strings.HasPrefix(id, "col_") || len(id) != 36 || len(raw) == 0 || len(raw) > 64<<10 || !json.Valid(raw) {
		return out, ErrInvalidCollectorConfig
	}
	var obj map[string]any
	if json.Unmarshal(raw, &obj) != nil || obj == nil {
		return out, ErrInvalidCollectorConfig
	}
	if containsForbiddenRemoteConfig(obj) {
		return out, ErrInvalidCollectorConfig
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var orgID, identityID string
	err = tx.QueryRow(ctx, `SELECT o.id::text,i.id::text FROM collector_agents ca JOIN organizations o ON o.id=ca.organization_id JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL WHERE ca.id=$3 AND o.slug=$4 FOR UPDATE OF ca`, s.Issuer, p.Subject, id, p.Organization).Scan(&orgID, &identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, errors.New("collector not found")
	}
	if err != nil {
		return out, err
	}
	err = tx.QueryRow(ctx, `INSERT INTO collector_agent_configs(collector_id,version,configuration,created_by) SELECT $1,COALESCE(max(version),0)+1,$2::jsonb,$3 FROM collector_agent_configs WHERE collector_id=$1 RETURNING version,created_at`, id, raw, identityID).Scan(&out.Version, &out.CreatedAt)
	if err != nil {
		return out, err
	}
	state, _ := json.Marshal(map[string]any{"collector_id": id, "config_version": out.Version, "config_sha256": digestCredential(string(raw))})
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state) VALUES($1,$2,'collector.config.publish','collector',$3,$4,$5)`, orgID, identityID, id, requestID, state); err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	out.CollectorID = id
	out.Configuration = append(json.RawMessage(nil), raw...)
	return out, nil
}

func (s *Store) GetCollectorConfig(ctx context.Context, credential string) (CollectorConfig, error) {
	var out CollectorConfig
	err := s.Pool.QueryRow(ctx, `SELECT c.collector_id,c.version,c.configuration,c.created_at FROM collector_agents a JOIN LATERAL (SELECT collector_id,version,configuration,created_at FROM collector_agent_configs WHERE collector_id=a.id ORDER BY version DESC LIMIT 1) c ON true WHERE a.credential_ref=$1 AND a.state<>'disabled'`, digestCredential(credential)).Scan(&out.CollectorID, &out.Version, &out.Configuration, &out.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrCollectorConfigNotFound
	}
	return out, err
}

func (s *Store) DisableCollector(ctx context.Context, p auth.Principal, id, requestID string) error {
	if !strings.HasPrefix(id, "col_") || len(id) != 36 {
		return errors.New("invalid collector ID")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var orgID, identityID string
	err = tx.QueryRow(ctx, `SELECT o.id::text,i.id::text FROM collector_agents ca JOIN organizations o ON o.id=ca.organization_id JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL WHERE ca.id=$3 AND o.slug=$4`, s.Issuer, p.Subject, id, p.Organization).Scan(&orgID, &identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("collector not found")
	}
	if err != nil {
		return err
	}
	var previousState, hostname string
	err = tx.QueryRow(ctx, `SELECT state,hostname FROM collector_agents WHERE id=$1 FOR UPDATE`, id).Scan(&previousState, &hostname)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("collector not found")
	}
	if err != nil {
		return err
	}
	// Disabling is idempotent: a repeated call skips the state update but still
	// reconciles any source write ACLs whose earlier revocation failed.
	if previousState != "disabled" {
		before, _ := json.Marshal(map[string]string{"state": previousState, "hostname": hostname})
		if _, err = tx.Exec(ctx, `UPDATE collector_agents SET state='disabled',updated_at=now() WHERE id=$1`, id); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,before_state,after_state) VALUES($1,$2,'collector.disable','collector',$3,$4,$5,'{"state":"disabled"}'::jsonb)`, orgID, identityID, id, requestID, before); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return s.reconcileSourceWrites(ctx, orgID, identityID, id, requestID, true)
}

// EnableCollector reverses DisableCollector: it first restores every bound
// source's Kafka write ACL and only then re-admits the collector credential.
// If a restore fails the collector stays disabled and the failure is reported
// and audited, so a collector can never come back without its write path.
func (s *Store) EnableCollector(ctx context.Context, p auth.Principal, id, requestID string) error {
	if !validCollectorID(id) {
		return errors.New("invalid collector ID")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	var orgID, identityID string
	err = tx.QueryRow(ctx, `SELECT o.id::text,i.id::text FROM collector_agents ca JOIN organizations o ON o.id=ca.organization_id JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL WHERE ca.id=$3 AND o.slug=$4`, s.Issuer, p.Subject, id, p.Organization).Scan(&orgID, &identityID)
	if errors.Is(err, pgx.ErrNoRows) {
		tx.Rollback(ctx)
		return errors.New("collector not found")
	}
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM collector_agents WHERE id=$1 FOR UPDATE`, id).Scan(&state)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	if state != "disabled" {
		tx.Rollback(ctx)
		return errors.New("collector is not disabled")
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if err = s.reconcileSourceWrites(ctx, orgID, identityID, id, requestID, false); err != nil {
		return err
	}
	after, _ := json.Marshal(map[string]string{"state": "enrolled"})
	if _, err = s.Pool.Exec(ctx, `UPDATE collector_agents SET state='enrolled',updated_at=now() WHERE id=$1 AND state='disabled'`, id); err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,before_state,after_state) VALUES($1,$2,'collector.enable','collector',$3,$4,'{"state":"disabled"}'::jsonb,$5)`, orgID, identityID, id, requestID, after)
	return err
}

func digestCredential(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
func heartbeatJSON(in CollectorHeartbeat) []byte {
	raw, _ := json.Marshal(in)
	return raw
}

func containsForbiddenRemoteConfig(value any) bool {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			name := strings.ToLower(strings.TrimSpace(key))
			normalized := strings.NewReplacer("_", "", "-", "").Replace(name)
			if normalized == "command" || normalized == "commands" || normalized == "shell" || normalized == "exec" || normalized == "executable" || normalized == "script" {
				return true
			}
			if strings.HasSuffix(name, "_env") {
				ref, ok := child.(string)
				if !ok || !bounded(ref, 1, 128) || !validEnvironmentName(ref) {
					return true
				}
				continue
			}
			if strings.Contains(normalized, "password") || strings.Contains(normalized, "secret") || strings.Contains(normalized, "token") || normalized == "apikey" || normalized == "accesskey" || normalized == "credential" || normalized == "credentials" || normalized == "privatekey" || normalized == "authorization" || normalized == "bearer" {
				return true
			}
			if containsForbiddenRemoteConfig(child) {
				return true
			}
		}
	case []any:
		for _, child := range node {
			if containsForbiddenRemoteConfig(child) {
				return true
			}
		}
	}
	return false
}

func validEnvironmentName(value string) bool {
	for i, r := range value {
		if i == 0 {
			if !(r == '_' || r >= 'A' && r <= 'Z') {
				return false
			}
		} else if !(r == '_' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return value != ""
}
