package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

type ReleaseAsset struct {
	ID           string   `json:"asset_id"`
	Kind         string   `json:"kind"`
	Version      string   `json:"version"`
	Path         string   `json:"path"`
	SHA256       string   `json:"sha256"`
	Dependencies []string `json:"dependencies"`
}
type ReleaseManifest struct {
	SchemaVersion string            `json:"schema_version"`
	ReleaseID     string            `json:"release_id"`
	Version       string            `json:"version"`
	Assets        []ReleaseAsset    `json:"assets"`
	Compatibility map[string]string `json:"compatibility"`
}
type Release struct {
	ID          string          `json:"id"`
	Version     string          `json:"version"`
	Manifest    json.RawMessage `json:"manifest"`
	SHA256      string          `json:"sha256"`
	State       string          `json:"state"`
	CreatedAt   time.Time       `json:"created_at"`
	ActivatedAt *time.Time      `json:"activated_at,omitempty"`
}

var ErrReleaseConflict = errors.New("release ID already exists with different immutable content")

func canonicalManifest(m ReleaseManifest) ([]byte, string, error) {
	raw, e := json.Marshal(m)
	if e != nil {
		return nil, "", e
	}
	var value any
	if e = json.Unmarshal(raw, &value); e != nil {
		return nil, "", e
	}
	b, e := json.Marshal(value)
	if e != nil {
		return nil, "", e
	}
	h := sha256.Sum256(b)
	return b, hex.EncodeToString(h[:]), nil
}

func (s *Store) CreateRelease(ctx context.Context, p auth.Principal, m ReleaseManifest, key, reqID string) (Release, error) {
	var out Release
	if m.SchemaVersion != "1.0.0" || !releaseIDValid(m.ReleaseID) || !semver(m.Version) || key == "" || len(key) < 16 || len(key) > 128 {
		return out, errors.New("invalid release manifest or idempotency key")
	}
	b, hash, e := canonicalManifest(m)
	if e != nil {
		return out, e
	}
	var actor string
	if e = s.Pool.QueryRow(ctx, `SELECT id::text FROM identities WHERE issuer=$1 AND subject=$2 AND disabled_at IS NULL`, s.Issuer, p.Subject).Scan(&actor); e != nil {
		return out, e
	}
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "release-publish:"+actor+":"+key); e != nil {
		return out, e
	}
	var oldHash, oldRelease string
	e = tx.QueryRow(ctx, `SELECT request_sha256,release_id FROM platform_release_idempotency WHERE publisher_identity_id=$1 AND idempotency_key=$2`, actor, key).Scan(&oldHash, &oldRelease)
	if e == nil {
		if oldHash != hash {
			return Release{}, ErrReleaseConflict
		}
		return s.GetRelease(ctx, oldRelease)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return out, e
	}
	e = tx.QueryRow(ctx, `INSERT INTO release_bundles(id,version,manifest,sha256,state,created_by) VALUES($1,$2,$3,'`+hash+`','draft',$4) ON CONFLICT(id) DO NOTHING RETURNING id,version,manifest,sha256,state,created_at,activated_at`, m.ReleaseID, m.Version, b, actor).Scan(&out.ID, &out.Version, &out.Manifest, &out.SHA256, &out.State, &out.CreatedAt, &out.ActivatedAt)
	if errors.Is(e, pgx.ErrNoRows) {
		e = tx.QueryRow(ctx, `SELECT id,version,manifest,sha256,state,created_at,activated_at FROM release_bundles WHERE id=$1`, m.ReleaseID).Scan(&out.ID, &out.Version, &out.Manifest, &out.SHA256, &out.State, &out.CreatedAt, &out.ActivatedAt)
		if e == nil && out.SHA256 != hash {
			return Release{}, ErrReleaseConflict
		}
		if e == nil {
			if _, e = tx.Exec(ctx, `INSERT INTO platform_release_idempotency(publisher_identity_id,idempotency_key,request_sha256,release_id) VALUES($1,$2,$3,$4)`, actor, key, hash, m.ReleaseID); e != nil {
				return out, e
			}
			if _, e = tx.Exec(ctx, `INSERT INTO audit_events(actor_identity_id,action,resource_type,resource_id,request_id,after_state,metadata) VALUES($1,'release.create_replay','release',$2,$3,jsonb_build_object('version',$4::text,'sha256',$5::text,'state',$6::text),jsonb_build_object('idempotency_key',$7::text))`, actor, m.ReleaseID, reqID, m.Version, hash, out.State, key); e != nil {
				return out, e
			}
			return out, tx.Commit(ctx)
		}
	}
	if e != nil {
		return out, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO platform_release_idempotency(publisher_identity_id,idempotency_key,request_sha256,release_id) VALUES($1,$2,$3,$4)`, actor, key, hash, m.ReleaseID); e != nil {
		return out, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO audit_events(actor_identity_id,action,resource_type,resource_id,request_id,after_state,metadata) VALUES($1,'release.create','release',$2,$3,jsonb_build_object('version',$4::text,'sha256',$5::text,'state','draft'),jsonb_build_object('idempotency_key',$6::text))`, actor, m.ReleaseID, reqID, m.Version, hash, key); e != nil {
		return out, e
	}
	return out, tx.Commit(ctx)
}

func releaseIDValid(s string) bool {
	if !bounded(s, 1, 128) || s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:-", r)) {
			return false
		}
	}
	return true
}
func semver(s string) bool {
	p := strings.Split(s, ".")
	if len(p) != 3 {
		return false
	}
	for _, x := range p {
		if x == "" {
			return false
		}
		for _, r := range x {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func (s *Store) ListReleases(ctx context.Context) ([]Release, error) {
	rows, e := s.Pool.Query(ctx, `SELECT id,version,manifest,sha256,state,created_at,activated_at FROM release_bundles ORDER BY created_at DESC,id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Release{}
	for rows.Next() {
		var x Release
		if e = rows.Scan(&x.ID, &x.Version, &x.Manifest, &x.SHA256, &x.State, &x.CreatedAt, &x.ActivatedAt); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) GetRelease(ctx context.Context, id string) (Release, error) {
	var x Release
	e := s.Pool.QueryRow(ctx, `SELECT id,version,manifest,sha256,state,created_at,activated_at FROM release_bundles WHERE id=$1`, id).Scan(&x.ID, &x.Version, &x.Manifest, &x.SHA256, &x.State, &x.CreatedAt, &x.ActivatedAt)
	return x, e
}

func (s *Store) SetReleasePublisher(ctx context.Context, p auth.Principal, subject string, revoke bool, requestID string) error {
	if !bounded(subject, 1, 256) || subject == p.Subject && revoke {
		return errors.New("invalid publisher identity change")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var actor, target string
	if err = tx.QueryRow(ctx, `SELECT id::text FROM identities WHERE issuer=$1 AND subject=$2 AND disabled_at IS NULL`, s.Issuer, p.Subject).Scan(&actor); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO identities(issuer,subject) VALUES($1,$2) ON CONFLICT(issuer,subject) DO UPDATE SET subject=excluded.subject RETURNING id::text`, s.Issuer, subject).Scan(&target); err != nil {
		return err
	}
	action := "release_publisher.grant"
	if revoke {
		action = "release_publisher.revoke"
		if _, err = tx.Exec(ctx, `UPDATE platform_release_publishers SET revoked_at=now(),revoked_by=$2 WHERE identity_id=$1 AND revoked_at IS NULL`, target, actor); err != nil {
			return err
		}
	} else {
		if _, err = tx.Exec(ctx, `INSERT INTO platform_release_publishers(identity_id,granted_by,revoked_at,revoked_by) VALUES($1,$2,NULL,NULL) ON CONFLICT(identity_id) DO UPDATE SET granted_by=$2,granted_at=now(),revoked_at=NULL,revoked_by=NULL`, target, actor); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,'platform_release_publisher',$3,$4,jsonb_build_object('issuer',$5::text))`, actor, action, subject, requestID, s.Issuer)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListReleaseAudit(ctx context.Context, id string) ([]map[string]any, error) {
	rows, err := s.Pool.Query(ctx, `SELECT a.id,a.action,a.resource_id,a.request_id,a.occurred_at,a.before_state,a.after_state,a.metadata,i.subject FROM audit_events a LEFT JOIN identities i ON i.id=a.actor_identity_id WHERE a.organization_id IS NULL AND a.resource_type='release' AND a.resource_id=$1 ORDER BY a.id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var seq int64
		var action, res, req string
		var at time.Time
		var before, after, meta []byte
		var actor *string
		if err = rows.Scan(&seq, &action, &res, &req, &at, &before, &after, &meta, &actor); err != nil {
			return nil, err
		}
		item := map[string]any{"id": seq, "action": action, "resource_id": res, "request_id": req, "occurred_at": at, "metadata": json.RawMessage(meta)}
		if actor != nil {
			item["actor_subject"] = *actor
		}
		if len(before) > 0 {
			item["before_state"] = json.RawMessage(before)
		}
		if len(after) > 0 {
			item["after_state"] = json.RawMessage(after)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func verifyReleaseBundle(root string, m ReleaseManifest) error {
	if len(m.Assets) != 7 {
		return errors.New("release must contain exactly seven required asset kinds")
	}
	if !releaseIDValid(m.ReleaseID) {
		return errors.New("invalid release ID")
	}
	if root == "" {
		return errors.New("TUBA_RELEASE_ROOT is not configured")
	}
	if len(m.Compatibility) != 4 || m.Compatibility["raw_contract"] != "1" || m.Compatibility["uim_contract"] != "1" || (m.Compatibility["analysis_result_contract"] != "1" && m.Compatibility["analysis_result_contract"] != "2") || !releaseIDValid(m.Compatibility["generation"]) {
		return errors.New("invalid release compatibility declaration")
	}
	root, e := filepath.EvalSymlinks(root)
	if e != nil {
		return fmt.Errorf("release root unavailable: %w", e)
	}
	base, e := filepath.EvalSymlinks(filepath.Join(root, m.ReleaseID))
	if e != nil {
		return fmt.Errorf("release bundle directory unavailable: %w", e)
	}
	rootRel, e := filepath.Rel(root, base)
	if e != nil || rootRel == ".." || strings.HasPrefix(rootRel, ".."+string(filepath.Separator)) || rootRel == "." {
		return errors.New("release bundle directory resolves outside release root")
	}
	kinds := map[string]bool{}
	ids := map[string]ReleaseAsset{}
	for _, a := range m.Assets {
		if !releaseIDValid(a.ID) || !semver(a.Version) || len(a.SHA256) != 64 || ids[a.ID].ID != "" {
			return errors.New("invalid or duplicate release asset")
		}
		for _, c := range a.SHA256 {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return errors.New("invalid release asset hash")
			}
		}
		switch a.Kind {
		case "dip", "uim", "routing", "es_mapping", "entity_rules", "data_model", "analysis_rules":
		default:
			return errors.New("unknown release asset kind")
		}
		if kinds[a.Kind] {
			return errors.New("duplicate release asset kind")
		}
		kinds[a.Kind] = true
		if a.Path == "" || strings.Contains(a.Path, "\\") || filepath.IsAbs(a.Path) {
			return errors.New("unsafe release asset path")
		}
		for _, c := range a.Path {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._/-", c)) {
				return errors.New("release asset path must use ASCII portable characters")
			}
		}
		for _, part := range strings.Split(a.Path, "/") {
			if part == ".." || part == "." || part == "" {
				return errors.New("unsafe release asset path")
			}
		}
		ids[a.ID] = a
	}
	for id, a := range ids {
		for _, d := range a.Dependencies {
			if _, ok := ids[d]; !ok {
				return fmt.Errorf("unknown dependency %s for %s", d, id)
			}
		}
	}
	visiting, done := map[string]bool{}, map[string]bool{}
	var visit func(string) bool
	visit = func(id string) bool {
		if visiting[id] {
			return false
		}
		if done[id] {
			return true
		}
		visiting[id] = true
		for _, d := range ids[id].Dependencies {
			if !visit(d) {
				return false
			}
		}
		delete(visiting, id)
		done[id] = true
		return true
	}
	for id := range ids {
		if !visit(id) {
			return errors.New("release dependency graph contains a cycle")
		}
	}
	for _, a := range m.Assets {
		target := filepath.Join(base, filepath.FromSlash(a.Path))
		resolved, e := filepath.EvalSymlinks(target)
		if e != nil {
			return fmt.Errorf("asset %s unavailable: %w", a.ID, e)
		}
		rel, e := filepath.Rel(base, resolved)
		if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return errors.New("asset resolves outside release bundle")
		}
		st, e := os.Stat(resolved)
		if e != nil || !st.Mode().IsRegular() {
			return fmt.Errorf("asset %s is not a regular file", a.ID)
		}
		f, e := os.Open(resolved)
		if e != nil {
			return e
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		f.Close()
		if e != nil {
			return e
		}
		if hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
			return fmt.Errorf("asset SHA-256 mismatch: %s", a.ID)
		}
	}
	return nil
}
func (s *Store) TransitionRelease(ctx context.Context, p auth.Principal, id, action, requestID, releaseRoot string) (Release, error) {
	x, e := s.GetRelease(ctx, id)
	if e != nil {
		return x, e
	}
	var m ReleaseManifest
	if e = json.Unmarshal(x.Manifest, &m); e != nil {
		return x, e
	}
	var next string
	switch action {
	case "validate":
		if x.State != "draft" {
			return x, errors.New("release is not draft")
		}
		if e = verifyReleaseBundle(releaseRoot, m); e != nil {
			return x, e
		}
		next = "validated"
	case "stage":
		if x.State != "validated" {
			return x, errors.New("release is not validated")
		}
		if e = verifyReleaseBundle(releaseRoot, m); e != nil {
			return x, fmt.Errorf("bundle changed after validation: %w", e)
		}
		next = "staged"
	case "activate":
		if x.State != "staged" {
			return x, errors.New("release is not staged")
		}
		if e = verifyReleaseBundle(releaseRoot, m); e != nil {
			return x, fmt.Errorf("bundle changed after staging: %w", e)
		}
		next = "active"
	default:
		return x, errors.New("invalid release action")
	}
	var actor string
	if e = s.Pool.QueryRow(ctx, `SELECT id::text FROM identities WHERE issuer=$1 AND subject=$2 AND disabled_at IS NULL`, s.Issuer, p.Subject).Scan(&actor); e != nil {
		return x, e
	}
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return x, e
	}
	defer tx.Rollback(ctx)
	var before []byte
	e = tx.QueryRow(ctx, `UPDATE release_bundles SET state=$2,activated_at=CASE WHEN $2='active' THEN now() ELSE activated_at END WHERE id=$1 AND state=$3 RETURNING jsonb_build_object('state',$3::text),id,version,manifest,sha256,state,created_at,activated_at`, id, next, x.State).Scan(&before, &x.ID, &x.Version, &x.Manifest, &x.SHA256, &x.State, &x.CreatedAt, &x.ActivatedAt)
	if e != nil {
		return x, errors.New("release state changed concurrently")
	}
	var after []byte
	after, _ = json.Marshal(map[string]any{"state": next, "sha256": x.SHA256})
	if _, e = tx.Exec(ctx, `INSERT INTO audit_events(actor_identity_id,action,resource_type,resource_id,request_id,before_state,after_state) VALUES($1,$2,'release',$3,$4,$5,$6)`, actor, "release."+action, id, requestID, before, after); e != nil {
		return x, e
	}
	return x, tx.Commit(ctx)
}
