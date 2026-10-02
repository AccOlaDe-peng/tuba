package control

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

// Q03 asynchronous query exports. An export freezes its request snapshot
// (SPL, fixed time range, dataset, format, limits) and the creator's
// permission snapshot at creation; execution pins the dataset retention
// boundary and re-verifies the creator; downloads re-authorize the live
// principal against the frozen fingerprint. Links expire
// ExportDownloadTTL after completion and are single-use. Every lifecycle
// transition is audited.

const (
	ExportFormatNDJSON = "ndjson"
	ExportFormatCSV    = "csv"

	ExportMaxRows  = 100000
	ExportMaxBytes = int64(1) << 30 // 1 GiB (design baseline §7)
	// ExportDownloadTTL is the download-link lifetime after completion
	// (design baseline §7: 链接 15 分钟过期且单次使用).
	ExportDownloadTTL = 15 * time.Minute
	// ExportDatasetRetention is the approved queryable window for every
	// dataset (A03: ES keeps 7 UTC day partitions). Export execution pins
	// now-ExportDatasetRetention as the retention boundary; ranges starting
	// before it fail with retention_exceeded, never silent truncation.
	ExportDatasetRetention = 7 * 24 * time.Hour

	ExportStateQueued    = "queued"
	ExportStateRunning   = "running"
	ExportStateSucceeded = "succeeded"
	ExportStateFailed    = "failed"
	ExportStateExpired   = "expired"
)

type Export struct {
	ID               string     `json:"id"`
	JobID            string     `json:"job_id"`
	Dataset          string     `json:"dataset"`
	Format           string     `json:"format"`
	Query            string     `json:"query"`
	From             time.Time  `json:"from"`
	To               time.Time  `json:"to"`
	RowLimit         int        `json:"row_limit"`
	ByteLimit        int64      `json:"byte_limit"`
	IncludeSensitive bool       `json:"include_sensitive"`
	IncludeRaw       bool       `json:"include_raw"`
	State            string     `json:"state"`
	RetentionFrom    *time.Time `json:"retention_from,omitempty"`
	LimitReached     string     `json:"limit_reached,omitempty"`
	FileSHA256       string     `json:"file_sha256,omitempty"`
	RowCount         *int64     `json:"row_count,omitempty"`
	ByteCount        *int64     `json:"byte_count,omitempty"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
	DownloadedAt     *time.Time `json:"downloaded_at,omitempty"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
	Error            string     `json:"error,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`

	// Internal fields, never serialized to API responses.
	OrganizationID string `json:"-"`
	Namespace      string `json:"-"`
	CreatedBy      string `json:"-"`
	Fingerprint    string `json:"-"`
}

type CreateExportInput struct {
	Dataset          string
	Format           string
	Query            string
	Generation       string
	From             time.Time
	To               time.Time
	RowLimit         int
	ByteLimit        int64
	IncludeSensitive bool
	IncludeRaw       bool
}

// ExportPermissionFingerprint freezes the download-relevant permissions of a
// principal: subject plus the three data-access scopes that decide what an
// export contains. Download re-authorization recomputes it from the live
// (membership-resolved) principal; any role revocation or subject change
// mismatches and is denied.
func ExportPermissionFingerprint(p auth.Principal) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|event:read=%t|sensitive:read=%t|raw:read=%t",
		p.Subject, p.Can("event:read"), p.Can("sensitive:read"), p.Can("raw:read"))))
	return hex.EncodeToString(sum[:])
}

func validateExportInput(in CreateExportInput) error {
	if in.Format != ExportFormatNDJSON && in.Format != ExportFormatCSV {
		return errors.New("format must be ndjson or csv")
	}
	if len(in.Dataset) < 1 || len(in.Dataset) > 64 || len(in.Query) < 1 || len(in.Query) > 4096 {
		return errors.New("invalid dataset or query")
	}
	if !in.From.Before(in.To) {
		return errors.New("invalid time range")
	}
	if in.RowLimit < 1 || in.RowLimit > ExportMaxRows {
		return fmt.Errorf("row limit must be 1..%d", ExportMaxRows)
	}
	if in.ByteLimit < 1 || in.ByteLimit > ExportMaxBytes {
		return fmt.Errorf("byte limit must be 1..%d", ExportMaxBytes)
	}
	if in.Generation == "" || len(in.Generation) > 32 {
		return errors.New("invalid generation")
	}
	return nil
}

func (s *Store) CreateExport(ctx context.Context, p auth.Principal, in CreateExportInput, requestID string) (Export, int, error) {
	var out Export
	if err := validateExportInput(in); err != nil {
		return out, 400, err
	}
	org, user, err := s.ids(ctx, p)
	if err != nil {
		return out, 403, errors.New("active membership not found")
	}
	fingerprint := ExportPermissionFingerprint(p)
	request, _ := json.Marshal(map[string]any{"export": true})
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return out, 500, err
	}
	defer tx.Rollback(ctx)
	var jobID string
	if err = tx.QueryRow(ctx, `
		INSERT INTO processing_jobs(organization_id,namespace,job_type,generation,request,max_attempts,created_by)
		VALUES($1,$2,'export',$3,$4,1,$5) RETURNING id::text`,
		org, p.Namespace, in.Generation, request, user).Scan(&jobID); err != nil {
		return out, 500, err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO export_jobs(job_id,organization_id,namespace,created_by,dataset,format,query,from_ts,to_ts,row_limit,byte_limit,include_sensitive,include_raw,permission_fingerprint)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		RETURNING id::text, created_at`,
		jobID, org, p.Namespace, user, in.Dataset, in.Format, in.Query, in.From.UTC(), in.To.UTC(),
		in.RowLimit, in.ByteLimit, in.IncludeSensitive, in.IncludeRaw, fingerprint).Scan(&out.ID, &out.CreatedAt)
	if err != nil {
		return out, 500, err
	}
	meta, _ := json.Marshal(map[string]any{
		"dataset": in.Dataset, "format": in.Format, "from": in.From.UTC(), "to": in.To.UTC(),
		"row_limit": in.RowLimit, "byte_limit": in.ByteLimit,
		"include_sensitive": in.IncludeSensitive, "include_raw": in.IncludeRaw,
	})
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,'export.create','export',$3,$4,$5)`, org, user, out.ID, requestID, meta); err != nil {
		return out, 500, err
	}
	if err = tx.Commit(ctx); err != nil {
		return out, 500, err
	}
	out.JobID = jobID
	out.Dataset, out.Format, out.Query = in.Dataset, in.Format, in.Query
	out.From, out.To = in.From.UTC(), in.To.UTC()
	out.RowLimit, out.ByteLimit = in.RowLimit, in.ByteLimit
	out.IncludeSensitive, out.IncludeRaw = in.IncludeSensitive, in.IncludeRaw
	out.State = ExportStateQueued
	return out, 201, nil
}

const exportColumns = `id::text,job_id::text,organization_id::text,namespace,created_by::text,dataset,format,query,from_ts,to_ts,row_limit,byte_limit,include_sensitive,include_raw,permission_fingerprint,state,retention_from,COALESCE(limit_reached,''),COALESCE(file_sha256,''),row_count,byte_count,expires_at,downloaded_at,completed_at,COALESCE(error,''),created_at`

func scanExport(row pgx.Row) (Export, error) {
	var e Export
	err := row.Scan(&e.ID, &e.JobID, &e.OrganizationID, &e.Namespace, &e.CreatedBy, &e.Dataset, &e.Format, &e.Query,
		&e.From, &e.To, &e.RowLimit, &e.ByteLimit, &e.IncludeSensitive, &e.IncludeRaw, &e.Fingerprint, &e.State,
		&e.RetentionFrom, &e.LimitReached, &e.FileSHA256, &e.RowCount, &e.ByteCount, &e.ExpiresAt, &e.DownloadedAt,
		&e.CompletedAt, &e.Error, &e.CreatedAt)
	return e, err
}

func (s *Store) GetExport(ctx context.Context, p auth.Principal, id string) (Export, error) {
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return Export{}, errors.New("active membership not found")
	}
	e, err := scanExport(s.Pool.QueryRow(ctx, `SELECT `+exportColumns+` FROM export_jobs WHERE id=$1 AND organization_id=$2`, id, org))
	if err != nil {
		return Export{}, errors.New("export not found")
	}
	return e, nil
}

func (s *Store) ListExports(ctx context.Context, p auth.Principal, limit int, cursor string) ([]Export, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", errors.New("invalid limit")
	}
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return nil, "", errors.New("active membership not found")
	}
	q := `SELECT ` + exportColumns + ` FROM export_jobs WHERE organization_id=$1`
	args := []any{org, limit + 1}
	if cursor != "" {
		t, cid, err := parseExportCursor(cursor)
		if err != nil {
			return nil, "", errors.New("invalid cursor")
		}
		q += ` AND (created_at,id)<($3,$4::uuid)`
		args = append(args, t, cid)
	}
	q += ` ORDER BY created_at DESC,id DESC LIMIT $2`
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := []Export{}
	for rows.Next() {
		e, err := scanExport(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, e)
	}
	next := ""
	if len(out) > limit {
		x := out[limit-1]
		next = exportCursor(x.CreatedAt, x.ID)
		out = out[:limit]
	}
	return out, next, rows.Err()
}

func exportCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func parseExportCursor(v string) (time.Time, string, error) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.Split(string(b), "|")
	if len(parts) != 2 {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", err
	}
	return t, parts[1], nil
}

// AuthorizeExportDownload re-authorizes a download against the live
// principal (design baseline §7: 下载时重新授权，不是"有链接就能下"). It is
// fail-closed and single-use: on success it marks the export downloaded in
// the same transaction as the audit record. Denials (permission drift,
// replay, expiry) are audited as export.download_denied / export.expire.
func (s *Store) AuthorizeExportDownload(ctx context.Context, p auth.Principal, id, requestID string) (Export, int, error) {
	org, user, err := s.ids(ctx, p)
	if err != nil {
		return Export{}, 403, errors.New("active membership not found")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Export{}, 500, err
	}
	defer tx.Rollback(ctx)
	e, err := scanExport(tx.QueryRow(ctx, `SELECT `+exportColumns+` FROM export_jobs WHERE id=$1 AND organization_id=$2 FOR UPDATE`, id, org))
	if errors.Is(err, pgx.ErrNoRows) {
		return Export{}, 404, errors.New("export not found")
	}
	if err != nil {
		return Export{}, 500, err
	}
	deny := func(status int, action, reason string) (Export, int, error) {
		meta, _ := json.Marshal(map[string]any{"reason": reason})
		if _, err := tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,$3,'export',$4,$5,$6)`, org, user, action, e.ID, requestID, meta); err != nil {
			return Export{}, 500, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Export{}, 500, err
		}
		return Export{}, status, errors.New(reason)
	}
	switch e.State {
	case ExportStateQueued, ExportStateRunning:
		return Export{}, 409, errors.New("export is not ready")
	case ExportStateFailed:
		return Export{}, 410, errors.New("export failed")
	case ExportStateExpired:
		return Export{}, 410, errors.New("export link expired")
	}
	now := time.Now().UTC()
	if e.ExpiresAt != nil && !now.Before(*e.ExpiresAt) {
		if _, err := tx.Exec(ctx, `UPDATE export_jobs SET state='expired' WHERE id=$1`, e.ID); err != nil {
			return Export{}, 500, err
		}
		return deny(410, "export.expire", "export link expired")
	}
	if e.DownloadedAt != nil {
		return deny(410, "export.download_denied", "export link already used")
	}
	if ExportPermissionFingerprint(p) != e.Fingerprint {
		return deny(403, "export.download_denied", "export download re-authorization failed: permissions changed since export creation")
	}
	if _, err := tx.Exec(ctx, `UPDATE export_jobs SET downloaded_at=now(), downloaded_by=$2 WHERE id=$1`, e.ID, user); err != nil {
		return Export{}, 500, err
	}
	meta, _ := json.Marshal(map[string]any{"dataset": e.Dataset, "format": e.Format})
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,'export.download','export',$3,$4,$5)`, org, user, e.ID, requestID, meta); err != nil {
		return Export{}, 500, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Export{}, 500, err
	}
	return e, 200, nil
}

// --- worker-side lifecycle (control worker export executor) ---

// LoadExportForJob returns the export row bound to a claimed processing job.
func (s *Store) LoadExportForJob(ctx context.Context, jobID string) (Export, error) {
	return scanExport(s.Pool.QueryRow(ctx, `SELECT `+exportColumns+` FROM export_jobs WHERE job_id=$1`, jobID))
}

// FingerprintForIdentity recomputes the export permission fingerprint of an
// identity from its current active memberships (execution-time re-authorization).
func (s *Store) FingerprintForIdentity(ctx context.Context, identityID string) (string, error) {
	var subject string
	if err := s.Pool.QueryRow(ctx, `SELECT subject FROM identities WHERE id=$1 AND disabled_at IS NULL`, identityID).Scan(&subject); err != nil {
		return "", errors.New("export creator identity not active")
	}
	rows, err := s.Pool.Query(ctx, `SELECT r.name FROM memberships m JOIN roles r ON r.id=m.role_id WHERE m.identity_id=$1 AND m.revoked_at IS NULL`, identityID)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	roles := []string{}
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return "", err
		}
		roles = append(roles, role)
	}
	if len(roles) == 0 {
		return "", errors.New("export creator membership not active")
	}
	return ExportPermissionFingerprint(auth.Principal{Subject: subject, Roles: roles}), rows.Err()
}

// MarkExportRunning pins the execution-time retention boundary on the export.
func (s *Store) MarkExportRunning(ctx context.Context, id string, retentionFrom time.Time) error {
	_, err := s.Pool.Exec(ctx, `UPDATE export_jobs SET state='running', retention_from=$2 WHERE id=$1 AND state='queued'`, id, retentionFrom.UTC())
	return err
}

type ExportResult struct {
	FileSHA256  string
	RowCount    int64
	ByteCount   int64
	LimitReached string
}

// CompleteExport marks the export downloadable (15-minute single-use link)
// and audits export.complete in one transaction.
func (s *Store) CompleteExport(ctx context.Context, id string, result ExportResult, workerID string) error {
	meta, _ := json.Marshal(map[string]any{
		"worker_id": workerID, "row_count": result.RowCount, "byte_count": result.ByteCount,
		"limit_reached": result.LimitReached, "file_sha256": result.FileSHA256,
	})
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE export_jobs
		SET state='succeeded', file_sha256=$2, row_count=$3, byte_count=$4, limit_reached=NULLIF($5,''),
		    completed_at=now(), expires_at=now() + interval '15 minutes'
		WHERE id=$1 AND state='running'`, id, result.FileSHA256, result.RowCount, result.ByteCount, result.LimitReached)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("export is not in running state")
	}
	var org string
	if err := tx.QueryRow(ctx, `SELECT organization_id::text FROM export_jobs WHERE id=$1`, id).Scan(&org); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,NULL,'export.complete','export',$2,$3,$4)`, org, id, "worker:"+workerID, meta); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FailExport marks the export failed and audits export.failed.
func (s *Store) FailExport(ctx context.Context, id, errorText, workerID string) error {
	if len(errorText) > 2048 {
		errorText = errorText[:2048]
	}
	meta, _ := json.Marshal(map[string]any{"worker_id": workerID, "error": errorText})
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE export_jobs SET state='failed', error=$2 WHERE id=$1 AND state IN ('queued','running')`, id, errorText)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("export is not in an active state")
	}
	var org string
	if err := tx.QueryRow(ctx, `SELECT organization_id::text FROM export_jobs WHERE id=$1`, id).Scan(&org); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,NULL,'export.failed','export',$2,$3,$4)`, org, id, "worker:"+workerID, meta); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ExpiredExport identifies an export whose download link lapsed.
type ExpiredExport struct {
	ID             string
	OrganizationID string
}

// ExpireExports transitions succeeded, undownloaded exports past expires_at
// to expired and audits export.expire for each (design baseline §7: 过期审计).
func (s *Store) ExpireExports(ctx context.Context, limit int) ([]ExpiredExport, error) {
	rows, err := s.Pool.Query(ctx, `
		WITH doomed AS (
			SELECT id FROM export_jobs
			WHERE state='succeeded' AND downloaded_at IS NULL AND expires_at IS NOT NULL AND expires_at <= now()
			ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE export_jobs e SET state='expired'
		FROM doomed WHERE e.id=doomed.id
		RETURNING e.id::text, e.organization_id::text`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExpiredExport
	for rows.Next() {
		var x ExpiredExport
		if err := rows.Scan(&x.ID, &x.OrganizationID); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, x := range out {
		if _, err := s.Pool.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,NULL,'export.expire','export',$2,$3,'{}')`, x.OrganizationID, x.ID, "sweeper"); err != nil {
			return out, err
		}
	}
	return out, nil
}
