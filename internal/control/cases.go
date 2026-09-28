package control

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
	"time"
	"tuba/product/internal/auth"
)

type Case struct {
	ID            string     `json:"id"`
	Title         string     `json:"title"`
	Description   string     `json:"description"`
	Status        string     `json:"status"`
	Severity      string     `json:"severity"`
	Assignee      string     `json:"assignee,omitempty"`
	Verdict       string     `json:"verdict,omitempty"`
	VerdictReason string     `json:"verdict_reason,omitempty"`
	Version       int64      `json:"version"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	ClosedAt      *time.Time `json:"closed_at,omitempty"`
	AnomalyIDs    []string   `json:"anomaly_ids"`
}
type CreateCaseInput struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Severity    string   `json:"severity"`
	AnomalyIDs  []string `json:"anomaly_ids"`
}
type CaseActionInput struct {
	Status          string `json:"status"`
	Assignee        string `json:"assignee"`
	Verdict         string `json:"verdict"`
	ExpectedVersion int64  `json:"expected_version"`
	Reason          string `json:"reason"`
}

func (s *Store) ids(ctx context.Context, p auth.Principal) (org, identity string, err error) {
	err = s.Pool.QueryRow(ctx, `SELECT o.id,i.id FROM organizations o JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL WHERE o.slug=$3`, s.Issuer, p.Subject, p.Organization).Scan(&org, &identity)
	return
}
func (s *Store) CreateCase(ctx context.Context, p auth.Principal, key string, in CreateCaseInput) (Case, int, error) {
	var out Case
	if len(key) < 8 || len(key) > 128 {
		return out, 400, errors.New("invalid Idempotency-Key")
	}
	if len(in.Title) < 1 || len(in.Title) > 300 || len(in.Description) > 5000 || !map[string]bool{"low": true, "medium": true, "high": true, "critical": true}[in.Severity] {
		return out, 400, errors.New("invalid case")
	}
	raw, _ := json.Marshal(in)
	sum := sha256.Sum256(raw)
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return out, 503, e
	}
	defer tx.Rollback(ctx)
	var org, user string
	e = tx.QueryRow(ctx, `SELECT o.id,i.id FROM organizations o JOIN identities i ON i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL WHERE o.slug=$3`, s.Issuer, p.Subject, p.Organization).Scan(&org, &user)
	if e != nil {
		return out, 403, e
	}
	var oldHash []byte
	var body []byte
	var status int
	e = tx.QueryRow(ctx, `SELECT request_hash,response_body,response_status FROM idempotency_records WHERE organization_id=$1 AND identity_id=$2 AND operation='create_case' AND idempotency_key=$3 AND expires_at>now()`, org, user, key).Scan(&oldHash, &body, &status)
	if e == nil {
		if string(oldHash) != string(sum[:]) {
			return out, 409, errors.New("idempotency key reused")
		}
		_ = json.Unmarshal(body, &out)
		return out, status, nil
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return out, 503, e
	}
	e = tx.QueryRow(ctx, `INSERT INTO cases(organization_id,title,description,severity,created_by) VALUES($1,$2,$3,$4,$5) RETURNING id,title,description,status,severity,version,created_at,updated_at`, org, in.Title, in.Description, in.Severity, user).Scan(&out.ID, &out.Title, &out.Description, &out.Status, &out.Severity, &out.Version, &out.CreatedAt, &out.UpdatedAt)
	if e != nil {
		return out, 503, e
	}
	out.AnomalyIDs = make([]string, 0, len(in.AnomalyIDs))
	for _, a := range in.AnomalyIDs {
		if len(a) > 256 {
			return out, 400, errors.New("invalid anomaly id")
		}
		if _, e = tx.Exec(ctx, `INSERT INTO case_anomalies(case_id,anomaly_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, out.ID, a); e != nil {
			return out, 503, e
		}
		out.AnomalyIDs = append(out.AnomalyIDs, a)
	}
	b, _ := json.Marshal(out)
	_, e = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,after_state) VALUES($1,$2,'case.create','case',$3,$4,$5)`, org, user, out.ID, key, b)
	if e != nil {
		return out, 503, e
	}
	_, e = tx.Exec(ctx, `INSERT INTO idempotency_records(organization_id,identity_id,operation,idempotency_key,request_hash,response_status,response_body,expires_at) VALUES($1,$2,'create_case',$3,$4,201,$5,now()+interval '24 hours')`, org, user, key, sum[:], b)
	if e != nil {
		return out, 503, e
	}
	if e = tx.Commit(ctx); e != nil {
		return out, 503, e
	}
	return out, 201, nil
}
func encodeCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}
func decodeCursor(v string) (time.Time, string, error) {
	b, e := base64.RawURLEncoding.DecodeString(v)
	if e != nil {
		return time.Time{}, "", e
	}
	var t, id string
	if _, e = fmt.Sscanf(string(b), "%s", &t); e != nil {
		return time.Time{}, "", e
	}
	for i := len(t) - 1; i >= 0; i-- {
		if t[i] == '|' {
			id = t[i+1:]
			t = t[:i]
			break
		}
	}
	x, e := time.Parse(time.RFC3339Nano, t)
	if e != nil || id == "" {
		return time.Time{}, "", errors.New("invalid cursor")
	}
	return x, id, nil
}
func (s *Store) ListCases(ctx context.Context, p auth.Principal, limit int, cursor string) ([]Case, string, error) {
	return s.ListCasesFiltered(ctx, p, limit, cursor, "", "", "")
}

func (s *Store) ListCasesFiltered(ctx context.Context, p auth.Principal, limit int, cursor, status, severity, query string) ([]Case, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", errors.New("invalid limit")
	}
	if status != "" && !map[string]bool{"open": true, "in_progress": true, "closed": true}[status] {
		return nil, "", errors.New("invalid status")
	}
	if severity != "" && !map[string]bool{"low": true, "medium": true, "high": true, "critical": true}[severity] {
		return nil, "", errors.New("invalid severity")
	}
	org, _, e := s.ids(ctx, p)
	if e != nil {
		return nil, "", e
	}
	args := []any{org, limit + 1}
	filters := []string{"c.organization_id=$1"}
	addFilter := func(expression string, value any) {
		args = append(args, value)
		filters = append(filters, fmt.Sprintf(expression, len(args)))
	}
	if status != "" {
		addFilter("c.status=$%d", status)
	}
	if severity != "" {
		addFilter("c.severity=$%d", severity)
	}
	if strings.TrimSpace(query) != "" {
		args = append(args, strings.TrimSpace(query))
		last := len(args)
		filters = append(filters, fmt.Sprintf("(c.title ILIKE '%%' || $%d || '%%' OR c.description ILIKE '%%' || $%d || '%%')", last, last))
	}
	if cursor != "" {
		t, id, e := decodeCursor(cursor)
		if e != nil {
			return nil, "", e
		}
		args = append(args, t, id)
		filters = append(filters, fmt.Sprintf("(c.updated_at,c.id)<($%d,$%d)", len(args)-1, len(args)))
	}
	q := `SELECT c.id,c.title,c.description,c.status,c.severity,coalesce(i.subject,''),coalesce(c.verdict,''),coalesce(c.verdict_reason,''),c.version,c.created_at,c.updated_at,c.closed_at,ARRAY(SELECT ca.anomaly_id FROM case_anomalies ca WHERE ca.case_id=c.id ORDER BY ca.anomaly_id) FROM cases c LEFT JOIN identities i ON i.id=c.assignee_identity_id WHERE ` + strings.Join(filters, " AND ") + ` ORDER BY c.updated_at DESC,c.id DESC LIMIT $2`
	rows, e := s.Pool.Query(ctx, q, args...)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []Case{}
	for rows.Next() {
		var c Case
		if e = rows.Scan(&c.ID, &c.Title, &c.Description, &c.Status, &c.Severity, &c.Assignee, &c.Verdict, &c.VerdictReason, &c.Version, &c.CreatedAt, &c.UpdatedAt, &c.ClosedAt, &c.AnomalyIDs); e != nil {
			return nil, "", e
		}
		out = append(out, c)
	}
	next := ""
	if len(out) > limit {
		last := out[limit-1]
		next = encodeCursor(last.UpdatedAt, last.ID)
		out = out[:limit]
	}
	return out, next, rows.Err()
}
func (s *Store) GetCase(ctx context.Context, p auth.Principal, id string) (Case, error) {
	var c Case
	org, _, e := s.ids(ctx, p)
	if e != nil {
		return c, e
	}
	e = s.Pool.QueryRow(ctx, `SELECT c.id,c.title,c.description,c.status,c.severity,coalesce(i.subject,''),coalesce(c.verdict,''),coalesce(c.verdict_reason,''),c.version,c.created_at,c.updated_at,c.closed_at,ARRAY(SELECT ca.anomaly_id FROM case_anomalies ca WHERE ca.case_id=c.id ORDER BY ca.anomaly_id) FROM cases c LEFT JOIN identities i ON i.id=c.assignee_identity_id WHERE c.organization_id=$1 AND c.id=$2`, org, id).Scan(&c.ID, &c.Title, &c.Description, &c.Status, &c.Severity, &c.Assignee, &c.Verdict, &c.VerdictReason, &c.Version, &c.CreatedAt, &c.UpdatedAt, &c.ClosedAt, &c.AnomalyIDs)
	return c, e
}
func (s *Store) UpdateCase(ctx context.Context, p auth.Principal, id string, in CaseActionInput, requestID string) (Case, error) {
	var out Case
	validVerdict := map[string]bool{"true_positive": true, "benign_positive": true, "false_positive": true, "inconclusive": true}
	if in.ExpectedVersion < 1 || len(in.Reason) > 2000 || (in.Status == "" && in.Assignee == "" && in.Verdict == "") || (in.Verdict != "" && !validVerdict[in.Verdict]) {
		return out, errors.New("invalid case action")
	}
	org, user, e := s.ids(ctx, p)
	if e != nil {
		return out, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	var before Case
	e = tx.QueryRow(ctx, `SELECT id,title,description,status,severity,version,created_at,updated_at FROM cases WHERE organization_id=$1 AND id=$2 FOR UPDATE`, org, id).Scan(&before.ID, &before.Title, &before.Description, &before.Status, &before.Severity, &before.Version, &before.CreatedAt, &before.UpdatedAt)
	if e != nil {
		return out, e
	}
	if before.Version != in.ExpectedVersion {
		return out, fmt.Errorf("version conflict")
	}
	newStatus := before.Status
	if in.Status != "" {
		allowed := map[string]map[string]bool{"open": {"in_progress": true, "closed": true}, "in_progress": {"open": true, "closed": true}, "closed": {"open": true}}
		if !allowed[before.Status][in.Status] {
			return out, errors.New("invalid case transition")
		}
		newStatus = in.Status
	}
	var assignee any
	if in.Assignee != "" {
		e = tx.QueryRow(ctx, `SELECT id FROM identities WHERE issuer=$1 AND subject=$2 AND disabled_at IS NULL`, s.Issuer, in.Assignee).Scan(&assignee)
		if e != nil {
			return out, errors.New("assignee not found")
		}
	}
	e = tx.QueryRow(ctx, `UPDATE cases SET status=$1,assignee_identity_id=coalesce($2,assignee_identity_id),verdict=coalesce(nullif($3,''),verdict),verdict_reason=CASE WHEN $3='' THEN verdict_reason ELSE $4 END,verdict_by=CASE WHEN $3='' THEN verdict_by ELSE $5 END,verdict_at=CASE WHEN $3='' THEN verdict_at ELSE now() END,version=version+1,updated_at=now(),closed_at=CASE WHEN $1='closed' THEN coalesce(closed_at,now()) ELSE NULL END WHERE organization_id=$6 AND id=$7 AND version=$8 RETURNING id,title,description,status,severity,version,created_at,updated_at,closed_at`, newStatus, assignee, in.Verdict, in.Reason, user, org, id, in.ExpectedVersion).Scan(&out.ID, &out.Title, &out.Description, &out.Status, &out.Severity, &out.Version, &out.CreatedAt, &out.UpdatedAt, &out.ClosedAt)
	if e != nil {
		return out, e
	}
	if e = tx.QueryRow(ctx, `SELECT coalesce(i.subject,''),coalesce(c.verdict,''),coalesce(c.verdict_reason,''),ARRAY(SELECT ca.anomaly_id FROM case_anomalies ca WHERE ca.case_id=c.id ORDER BY ca.anomaly_id) FROM cases c LEFT JOIN identities i ON i.id=c.assignee_identity_id WHERE c.id=$1`, id).Scan(&out.Assignee, &out.Verdict, &out.VerdictReason, &out.AnomalyIDs); e != nil {
		return out, e
	}
	b1, _ := json.Marshal(before)
	b2, _ := json.Marshal(out)
	_, e = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,before_state,after_state,metadata) VALUES($1,$2,'case.update','case',$3,$4,$5,$6,jsonb_build_object('reason',$7::text,'assignee',$8::text,'verdict',$9::text))`, org, user, id, requestID, b1, b2, in.Reason, in.Assignee, in.Verdict)
	if e != nil {
		return out, e
	}
	if in.Verdict != "" {
		_, e = tx.Exec(ctx, `
			INSERT INTO analysis_feedback(organization_id,anomaly_id,feedback_type,reason,actor_identity_id,source_case_id)
			SELECT $1,ca.anomaly_id,$2,$3,$4,$5
			FROM case_anomalies ca
			WHERE ca.case_id=$5
			ON CONFLICT(source_case_id,anomaly_id,feedback_type) WHERE source_case_id IS NOT NULL
			DO UPDATE SET reason=excluded.reason,actor_identity_id=excluded.actor_identity_id,created_at=now()`,
			org, in.Verdict, in.Reason, user, id,
		)
		if e != nil {
			return out, e
		}
	}
	return out, tx.Commit(ctx)
}
