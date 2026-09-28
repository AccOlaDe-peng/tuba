package control

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"tuba/product/internal/auth"
)

type CaseActivity struct {
	ID          int64           `json:"id"`
	Action      string          `json:"action"`
	Actor       string          `json:"actor,omitempty"`
	RequestID   string          `json:"request_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	BeforeState json.RawMessage `json:"before_state,omitempty"`
	AfterState  json.RawMessage `json:"after_state,omitempty"`
	Metadata    json.RawMessage `json:"metadata"`
}

func (s *Store) ListCaseActivity(ctx context.Context, p auth.Principal, caseID string, limit int, cursor string) ([]CaseActivity, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", errors.New("invalid limit")
	}
	if _, err := s.GetCase(ctx, p, caseID); err != nil {
		return nil, "", err
	}
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return nil, "", err
	}
	args := []any{org, caseID, limit + 1}
	query := `SELECT a.id,a.action,coalesce(i.subject,''),a.request_id,a.occurred_at,a.before_state,a.after_state,a.metadata
		FROM audit_events a
		LEFT JOIN identities i ON i.id=a.actor_identity_id
		WHERE a.organization_id=$1 AND a.resource_type='case' AND a.resource_id=$2`
	if cursor != "" {
		occurredAt, id, err := parseAuditCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, occurredAt, id)
		query += ` AND (a.occurred_at,a.id)<($4,$5)`
	}
	query += ` ORDER BY a.occurred_at DESC,a.id DESC LIMIT $3`
	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := []CaseActivity{}
	for rows.Next() {
		var item CaseActivity
		if err := rows.Scan(&item.ID, &item.Action, &item.Actor, &item.RequestID, &item.OccurredAt, &item.BeforeState, &item.AfterState, &item.Metadata); err != nil {
			return nil, "", err
		}
		items = append(items, item)
	}
	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = auditCursor(last.OccurredAt, last.ID)
		items = items[:limit]
	}
	return items, next, rows.Err()
}
