package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"tuba/product/internal/auth"
)

type AuditEvent struct {
	ID           int64           `json:"id"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	RequestID    string          `json:"request_id"`
	OccurredAt   time.Time       `json:"occurred_at"`
	Metadata     json.RawMessage `json:"metadata"`
}

func auditCursor(t time.Time, id int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + strconv.FormatInt(id, 10)))
}
func parseAuditCursor(v string) (time.Time, int64, error) {
	b, e := base64.RawURLEncoding.DecodeString(v)
	if e != nil {
		return time.Time{}, 0, e
	}
	parts := strings.Split(string(b), "|")
	if len(parts) != 2 {
		return time.Time{}, 0, errors.New("invalid cursor")
	}
	t, e := time.Parse(time.RFC3339Nano, parts[0])
	if e != nil {
		return t, 0, e
	}
	id, e := strconv.ParseInt(parts[1], 10, 64)
	return t, id, e
}
func (s *Store) ListAudit(ctx context.Context, p auth.Principal, limit int, cursor string) ([]AuditEvent, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", errors.New("invalid limit")
	}
	org, _, e := s.ids(ctx, p)
	if e != nil {
		return nil, "", e
	}
	q := `SELECT id,action,resource_type,resource_id,request_id,occurred_at,metadata FROM audit_events WHERE organization_id=$1`
	args := []any{org, limit + 1}
	if cursor != "" {
		t, id, e := parseAuditCursor(cursor)
		if e != nil {
			return nil, "", e
		}
		q += ` AND (occurred_at,id)<($3,$4)`
		args = append(args, t, id)
	}
	q += ` ORDER BY occurred_at DESC,id DESC LIMIT $2`
	rows, e := s.Pool.Query(ctx, q, args...)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []AuditEvent{}
	for rows.Next() {
		var x AuditEvent
		if e = rows.Scan(&x.ID, &x.Action, &x.ResourceType, &x.ResourceID, &x.RequestID, &x.OccurredAt, &x.Metadata); e != nil {
			return nil, "", e
		}
		out = append(out, x)
	}
	next := ""
	if len(out) > limit {
		x := out[limit-1]
		next = auditCursor(x.OccurredAt, x.ID)
		out = out[:limit]
	}
	return out, next, rows.Err()
}
