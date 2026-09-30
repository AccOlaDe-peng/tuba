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

type SourceInput struct {
	VendorName    string `json:"vendor_name"`
	VendorProduct string `json:"vendor_product"`
	VendorDataset string `json:"vendor_dataset"`
	ReleaseID     string `json:"release_id"`
	RateLimit     int    `json:"rate_limit"`
}

type SourceInstance struct {
	ID            string `json:"id"`
	Organization  string `json:"organization_id"`
	Namespace     string `json:"namespace"`
	VendorName    string `json:"vendor_name"`
	VendorProduct string `json:"vendor_product"`
	VendorDataset string `json:"vendor_dataset"`
	SourceEpoch   string `json:"source_epoch"`
	ReleaseID     string `json:"release_id"`
	// State is the lifecycle: active, paused or revoked. It is reported next to
	// the older boolean because the two are not the same fact — a paused source
	// is expected to resume and keeps its consumer offset, while a revoked one is
	// refused for good and its records are quarantined. An operator who cannot
	// see which of the two happened has no way to tell a hold from a retirement.
	State           string    `json:"state"`
	Enabled         bool      `json:"enabled"`
	RateLimit       int       `json:"rate_limit"`
	SourceContextID string    `json:"source_context_id"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type RegisteredSource struct {
	SourceInstance
	APIKey string `json:"api_key"`
}

func (s *Store) RegisterSource(ctx context.Context, principal auth.Principal, in SourceInput, requestID string) (RegisteredSource, error) {
	var out RegisteredSource
	in.VendorName = strings.TrimSpace(in.VendorName)
	in.VendorProduct = strings.TrimSpace(in.VendorProduct)
	in.VendorDataset = strings.TrimSpace(in.VendorDataset)
	in.ReleaseID = strings.TrimSpace(in.ReleaseID)
	if !bounded(in.VendorName, 1, 128) || !bounded(in.VendorProduct, 1, 128) || !bounded(in.VendorDataset, 1, 128) || !bounded(in.ReleaseID, 1, 128) || in.RateLimit < 1 || in.RateLimit > 100000 {
		return out, errors.New("invalid source fields or rate_limit (allowed range: 1..100000 events/second)")
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return out, err
	}
	apiKey, credentialRef, err := newSourceCredential()
	if err != nil {
		return out, err
	}
	id := "src_" + hex.EncodeToString(idBytes)
	contextID := "ctx_" + hex.EncodeToString(idBytes)

	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var organizationID, identityID string
	err = tx.QueryRow(ctx, `
		SELECT o.id::text,i.id::text
		FROM organizations o
		JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL
		JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL
		WHERE o.slug=$3`, s.Issuer, principal.Subject, principal.Organization).Scan(&organizationID, &identityID)
	if err != nil {
		return out, errors.New("active tenant membership not found")
	}
	var namespace string
	err = tx.QueryRow(ctx, `SELECT namespace FROM organizations WHERE id=$1`, organizationID).Scan(&namespace)
	if err != nil {
		return out, err
	}
	var releaseState string
	err = tx.QueryRow(ctx, `SELECT state FROM release_bundles WHERE id=$1`, in.ReleaseID).Scan(&releaseState)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, errors.New("release must exist and be active or staged")
	}
	if err != nil {
		return out, err
	}
	if releaseState != "active" && releaseState != "staged" {
		return out, errors.New("release must exist and be active or staged")
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO source_instances(id,organization_id,namespace,vendor_name,vendor_product,vendor_dataset,source_epoch,credential_ref,release_id,rate_limit)
		VALUES($1,$2,$3,$4,$5,$6,'1',$7,$8,$9)
		RETURNING id,namespace,vendor_name,vendor_product,vendor_dataset,source_epoch,release_id,state,enabled,rate_limit,created_at,updated_at`,
		id, organizationID, namespace, in.VendorName, in.VendorProduct, in.VendorDataset, credentialRef, in.ReleaseID, in.RateLimit).Scan(
		&out.ID, &out.Namespace, &out.VendorName, &out.VendorProduct, &out.VendorDataset, &out.SourceEpoch, &out.ReleaseID, &out.State, &out.Enabled, &out.RateLimit, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return out, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO source_contexts(id,source_instance_id,source_epoch,organization_slug,namespace,vendor_name,vendor_product,vendor_dataset,release_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, contextID, out.ID, out.SourceEpoch, principal.Organization, out.Namespace, out.VendorName, out.VendorProduct, out.VendorDataset, out.ReleaseID); err != nil {
		return RegisteredSource{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO source_credentials(credential_ref,source_instance_id) VALUES($1,$2)`, credentialRef, out.ID); err != nil {
		return RegisteredSource{}, err
	}
	out.SourceContextID = contextID
	out.Organization = principal.Organization
	out.APIKey = apiKey
	state, _ := json.Marshal(map[string]any{"source_id": out.ID, "dataset": out.VendorDataset, "release_id": out.ReleaseID, "rate_limit": out.RateLimit})
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state)
		VALUES($1,$2,'source.register','source_instance',$3,$4,$5)`, organizationID, identityID, out.ID, requestID, state); err != nil {
		return RegisteredSource{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RegisteredSource{}, err
	}
	return out, nil
}

func (s *Store) RotateSourceCredential(ctx context.Context, principal auth.Principal, id, requestID string) (RegisteredSource, error) {
	var out RegisteredSource
	if !strings.HasPrefix(id, "src_") || len(id) != 36 {
		return out, errors.New("invalid source ID")
	}
	apiKey, credentialRef, err := newSourceCredential()
	if err != nil {
		return out, err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	var organizationID, identityID string
	err = tx.QueryRow(ctx, `
		SELECT o.id::text,i.id::text
		FROM organizations o
		JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL
		JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL
		WHERE o.slug=$3`, s.Issuer, principal.Subject, principal.Organization).Scan(&organizationID, &identityID)
	if err != nil {
		return out, errors.New("active tenant membership not found")
	}
	var previousEpoch string
	err = tx.QueryRow(ctx, `SELECT source_epoch FROM source_instances WHERE id=$1 AND organization_id=$2 AND state='active' FOR UPDATE`, id, organizationID).Scan(&previousEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return RegisteredSource{}, errors.New("source not found or disabled")
	}
	if err != nil {
		return RegisteredSource{}, err
	}
	err = tx.QueryRow(ctx, `
		UPDATE source_instances
		SET credential_ref=$3,updated_at=now()
		WHERE id=$1 AND organization_id=$2 AND state='active'
		RETURNING id,namespace,vendor_name,vendor_product,vendor_dataset,source_epoch,COALESCE(release_id,''),state,enabled,rate_limit,created_at,updated_at`,
		id, organizationID, credentialRef).Scan(
		&out.ID, &out.Namespace, &out.VendorName, &out.VendorProduct, &out.VendorDataset, &out.SourceEpoch, &out.ReleaseID, &out.State, &out.Enabled, &out.RateLimit, &out.CreatedAt, &out.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RegisteredSource{}, errors.New("source not found or disabled")
	}
	if err != nil {
		return RegisteredSource{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE source_credentials SET valid_until=now()+interval '24 hours' WHERE source_instance_id=$1 AND valid_until IS NULL`, id); err != nil {
		return RegisteredSource{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO source_credentials(credential_ref,source_instance_id) VALUES($1,$2)`, credentialRef, id); err != nil {
		return RegisteredSource{}, err
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM source_contexts WHERE source_instance_id=$1 AND source_epoch=$2 ORDER BY created_at DESC LIMIT 1`, id, out.SourceEpoch).Scan(&out.SourceContextID); err != nil {
		return RegisteredSource{}, err
	}
	out.Organization = principal.Organization
	out.APIKey = apiKey
	before, _ := json.Marshal(map[string]string{"source_id": id, "source_epoch": previousEpoch})
	after, _ := json.Marshal(map[string]string{"source_id": id, "source_epoch": out.SourceEpoch, "credential_overlap_hours": "24"})
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,before_state,after_state)
		VALUES($1,$2,'source.rotate_credential','source_instance',$3,$4,$5,$6)`, organizationID, identityID, id, requestID, before, after); err != nil {
		return RegisteredSource{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RegisteredSource{}, err
	}
	return out, nil
}

func (s *Store) ListSources(ctx context.Context, principal auth.Principal) ([]SourceInstance, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT si.id,o.slug,si.namespace,si.vendor_name,si.vendor_product,si.vendor_dataset,
		       si.source_epoch,COALESCE(si.release_id,''),si.state,si.enabled,si.rate_limit,
		       COALESCE((SELECT sc.id FROM source_contexts sc WHERE sc.source_instance_id=si.id ORDER BY sc.created_at DESC LIMIT 1),''),si.created_at,si.updated_at
		FROM source_instances si JOIN organizations o ON o.id=si.organization_id
		WHERE o.slug=$1 ORDER BY si.created_at DESC,si.id`, principal.Organization)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]SourceInstance, 0)
	for rows.Next() {
		var item SourceInstance
		if err := rows.Scan(&item.ID, &item.Organization, &item.Namespace, &item.VendorName, &item.VendorProduct, &item.VendorDataset, &item.SourceEpoch, &item.ReleaseID, &item.Enabled, &item.RateLimit, &item.SourceContextID, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) RevokeSource(ctx context.Context, principal auth.Principal, id, requestID string) error {
	if !strings.HasPrefix(id, "src_") || len(id) > 64 {
		return errors.New("invalid source ID")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var organizationID, identityID string
	err = tx.QueryRow(ctx, `
		SELECT o.id::text,i.id::text
		FROM organizations o
		JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL
		JOIN memberships m ON m.organization_id=o.id AND m.identity_id=i.id AND m.revoked_at IS NULL
		WHERE o.slug=$3`, s.Issuer, principal.Subject, principal.Organization).Scan(&organizationID, &identityID)
	if err != nil {
		return errors.New("active tenant membership not found")
	}
	// 'revoked', not merely disabled: the difference is what the ingest answers
	// with. A revoked source is a definitive refusal, so the adapter quarantines
	// its records and advances; a paused one stays retryable. `enabled` is set
	// alongside it because the table constrains the two to agree while binaries
	// built before the state column are still allowed to run.
	var before []byte
	err = tx.QueryRow(ctx, `
		UPDATE source_instances SET state='revoked',enabled=false,updated_at=now()
		WHERE id=$1 AND organization_id=$2 AND state='active'
		RETURNING jsonb_build_object('source_id',id,'dataset',vendor_dataset,'state','active')::text`, id, organizationID).Scan(&before)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("source not found or already revoked")
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,before_state,after_state)
		VALUES($1,$2,'source.revoke','source_instance',$3,$4,$5,'{"state":"revoked","enabled":false}'::jsonb)`, organizationID, identityID, id, requestID, before); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func bounded(value string, min, max int) bool {
	return len(value) >= min && len(value) <= max && strings.TrimSpace(value) == value
}

func newSourceCredential() (string, string, error) {
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return "", "", err
	}
	key := "tuba_src_" + base64.RawURLEncoding.EncodeToString(keyBytes)
	digest := sha256.Sum256([]byte(key))
	return key, "sha256:" + hex.EncodeToString(digest[:]), nil
}
