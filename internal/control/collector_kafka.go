package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

// SourceWriteRevoker applies the Kafka side of collector disable/enable. The
// production implementation talks to the broker Admin API; tests use fakes.
// Both directions must be idempotent: revoking an already-absent WRITE ACL and
// restoring an already-present one are successes.
type SourceWriteRevoker interface {
	RevokeSourceWrite(ctx context.Context, principal, topic string) error
	RestoreSourceWrite(ctx context.Context, principal, topic string) error
}

// SourceWriteRevocationError reports that the collector state change committed
// but at least one bound source's Kafka write ACL could not be applied. It is
// never silent: the API surfaces it as a 502 and each failure is audited.
type SourceWriteRevocationError struct {
	CollectorID string
	Action      string
	Failures    []string
}

func (e *SourceWriteRevocationError) Error() string {
	return fmt.Sprintf("collector %s %s: source write %s failed: %s", e.CollectorID, e.Action, e.Action, strings.Join(e.Failures, "; "))
}

var ErrRevokerNotConfigured = errors.New("no Kafka write revoker configured; bound sources would keep their write access")

type SourceKafkaBinding struct {
	CollectorID      string     `json:"collector_id"`
	SourceID         string     `json:"source_id"`
	SourceContextID  string     `json:"source_context_id"`
	KafkaPrincipal   string     `json:"kafka_principal"`
	KafkaTopic       string     `json:"kafka_topic"`
	WriteRevokedAt   *time.Time `json:"write_revoked_at,omitempty"`
	WriteRevokeError string     `json:"write_revoke_error,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
}

var sourceTopicPattern = regexp.MustCompile(`^tuba\.source\.(ctx_[a-f0-9]{32})\.v[0-9]+$`)

func validCollectorID(id string) bool {
	return strings.HasPrefix(id, "col_") && len(id) == 36
}

// RegisterSourceKafkaBinding records which Kafka SCRAM principal writes to a
// source's topic on behalf of a collector. The topic must be the source topic
// of a context that belongs to the source, so a binding can never point the
// disable path at an unrelated topic. The principal is stored, not verified
// against the broker; the revoke path verifies ACL state at execution time.
func (s *Store) RegisterSourceKafkaBinding(ctx context.Context, p auth.Principal, collectorID, sourceID, principal, topic, requestID string) (SourceKafkaBinding, error) {
	var out SourceKafkaBinding
	if !validCollectorID(collectorID) || !strings.HasPrefix(sourceID, "src_") || !bounded(principal, 1, 255) || !bounded(topic, 1, 255) {
		return out, errors.New("invalid binding fields")
	}
	match := sourceTopicPattern.FindStringSubmatch(topic)
	if match == nil {
		return out, errors.New("kafka_topic must be a source context topic (tuba.source.<context>.vN)")
	}
	contextID := match[1]
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var orgID, identityID string
	err = tx.QueryRow(ctx, `SELECT o.id::text,i.id::text FROM organizations o JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL WHERE o.slug=$3`, s.Issuer, p.Subject, p.Organization).Scan(&orgID, &identityID)
	if err != nil {
		return out, errors.New("active tenant membership not found")
	}
	var collectorOK, sourceOK bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM collector_agents WHERE id=$1 AND organization_id=$2),EXISTS(SELECT 1 FROM source_instances si JOIN source_contexts sc ON sc.source_instance_id=si.id AND sc.id=$3 WHERE si.id=$4 AND si.organization_id=$2)`, collectorID, orgID, contextID, sourceID).Scan(&collectorOK, &sourceOK); err != nil {
		return out, err
	}
	if !collectorOK {
		return out, errors.New("collector not found")
	}
	if !sourceOK {
		return out, errors.New("source not found or topic does not match its context")
	}
	err = tx.QueryRow(ctx, `INSERT INTO collector_source_kafka_bindings(collector_id,source_instance_id,source_context_id,kafka_principal,kafka_topic,created_by) VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT (collector_id,source_instance_id) DO UPDATE SET source_context_id=$3,kafka_principal=$4,kafka_topic=$5
		RETURNING created_at`, collectorID, sourceID, contextID, principal, topic, identityID).Scan(&out.CreatedAt)
	if err != nil {
		return out, err
	}
	state, _ := json.Marshal(map[string]any{"collector_id": collectorID, "source_id": sourceID, "source_context_id": contextID, "kafka_principal": principal, "kafka_topic": topic})
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state) VALUES($1,$2,'collector.source_binding.register','collector',$3,$4,$5)`, orgID, identityID, collectorID, requestID, state); err != nil {
		return out, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, err
	}
	return SourceKafkaBinding{CollectorID: collectorID, SourceID: sourceID, SourceContextID: contextID, KafkaPrincipal: principal, KafkaTopic: topic, CreatedAt: out.CreatedAt}, nil
}

// pendingWriteBindings loads the bindings whose broker-side state still needs
// to change: revoke lists bindings not yet revoked, restore lists revoked ones.
func (s *Store) pendingWriteBindings(ctx context.Context, collectorID string, revoked bool) ([]SourceKafkaBinding, error) {
	condition := "write_revoked_at IS NULL"
	if revoked {
		condition = "write_revoked_at IS NOT NULL"
	}
	rows, err := s.Pool.Query(ctx, `SELECT collector_id,source_instance_id,source_context_id,kafka_principal,kafka_topic,write_revoked_at,COALESCE(write_revoke_error,''),created_at FROM collector_source_kafka_bindings WHERE collector_id=$1 AND `+condition+` ORDER BY source_instance_id`, collectorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]SourceKafkaBinding, 0)
	for rows.Next() {
		var b SourceKafkaBinding
		if err := rows.Scan(&b.CollectorID, &b.SourceID, &b.SourceContextID, &b.KafkaPrincipal, &b.KafkaTopic, &b.WriteRevokedAt, &b.WriteRevokeError, &b.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return items, rows.Err()
}

// reconcileSourceWrites applies revoke/restore to every pending binding,
// persisting and auditing each outcome individually. A nil revoker with pending
// bindings is an error, never a silent skip.
func (s *Store) reconcileSourceWrites(ctx context.Context, orgID, identityID, collectorID, requestID string, revoke bool) error {
	pending, err := s.pendingWriteBindings(ctx, collectorID, revoke == false)
	if err != nil {
		return err
	}
	action, failedAction := "collector.source_write.restore", "collector.source_write.restore_failed"
	verb := "restore"
	if revoke {
		action, failedAction = "collector.source_write.revoke", "collector.source_write.revoke_failed"
		verb = "revoke"
	}
	if len(pending) == 0 {
		return nil
	}
	if s.WriteRevoker == nil {
		s.auditSourceWrite(ctx, orgID, identityID, collectorID, requestID, failedAction, SourceKafkaBinding{}, ErrRevokerNotConfigured)
		return &SourceWriteRevocationError{CollectorID: collectorID, Action: verb, Failures: []string{ErrRevokerNotConfigured.Error()}}
	}
	failures := make([]string, 0)
	for _, binding := range pending {
		var err error
		if revoke {
			err = s.WriteRevoker.RevokeSourceWrite(ctx, binding.KafkaPrincipal, binding.KafkaTopic)
		} else {
			err = s.WriteRevoker.RestoreSourceWrite(ctx, binding.KafkaPrincipal, binding.KafkaTopic)
		}
		s.recordSourceWriteOutcome(ctx, orgID, identityID, requestID, binding, err, action, failedAction)
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s on %s: %v", binding.KafkaPrincipal, binding.KafkaTopic, err))
		}
	}
	if len(failures) > 0 {
		return &SourceWriteRevocationError{CollectorID: collectorID, Action: verb, Failures: failures}
	}
	return nil
}

func (s *Store) recordSourceWriteOutcome(ctx context.Context, orgID, identityID, requestID string, b SourceKafkaBinding, outcome error, action, failedAction string) {
	if outcome == nil {
		if b.WriteRevokedAt == nil {
			_, _ = s.Pool.Exec(ctx, `UPDATE collector_source_kafka_bindings SET write_revoked_at=now(),write_revoke_error=NULL WHERE collector_id=$1 AND source_instance_id=$2`, b.CollectorID, b.SourceID)
		} else {
			_, _ = s.Pool.Exec(ctx, `UPDATE collector_source_kafka_bindings SET write_revoked_at=NULL,write_revoke_error=NULL WHERE collector_id=$1 AND source_instance_id=$2`, b.CollectorID, b.SourceID)
		}
		s.auditSourceWrite(ctx, orgID, identityID, b.CollectorID, requestID, action, b, nil)
		return
	}
	_, _ = s.Pool.Exec(ctx, `UPDATE collector_source_kafka_bindings SET write_revoke_error=$3 WHERE collector_id=$1 AND source_instance_id=$2`, b.CollectorID, b.SourceID, outcome.Error())
	s.auditSourceWrite(ctx, orgID, identityID, b.CollectorID, requestID, failedAction, b, outcome)
}

func (s *Store) auditSourceWrite(ctx context.Context, orgID, identityID, collectorID, requestID, action string, b SourceKafkaBinding, outcome error) {
	state, _ := json.Marshal(map[string]any{"collector_id": collectorID, "source_id": b.SourceID, "source_context_id": b.SourceContextID, "kafka_principal": b.KafkaPrincipal, "kafka_topic": b.KafkaTopic})
	if outcome != nil {
		state, _ = json.Marshal(map[string]any{"collector_id": collectorID, "source_id": b.SourceID, "kafka_principal": b.KafkaPrincipal, "kafka_topic": b.KafkaTopic, "error": outcome.Error()})
	}
	_, _ = s.Pool.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state) VALUES($1,$2,$3,'collector',$4,$5,$6)`, orgID, identityID, action, collectorID, requestID, state)
}
