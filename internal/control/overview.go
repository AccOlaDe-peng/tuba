package control

import (
	"context"

	"tuba/product/internal/auth"
)

type Overview struct {
	ActiveCases   int64 `json:"active_cases"`
	Unassigned    int64 `json:"unassigned_cases"`
	CriticalCases int64 `json:"critical_cases"`
	ClosedToday   int64 `json:"closed_today"`
}

func (s *Store) Overview(ctx context.Context, p auth.Principal) (Overview, error) {
	var result Overview
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return result, err
	}
	err = s.Pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status='in_progress'),
			count(*) FILTER (WHERE status<>'closed' AND assignee_identity_id IS NULL),
			count(*) FILTER (WHERE status<>'closed' AND severity='critical'),
			count(*) FILTER (WHERE status='closed' AND closed_at>=date_trunc('day',now()))
		FROM cases
		WHERE organization_id=$1`, org).Scan(
		&result.ActiveCases,
		&result.Unassigned,
		&result.CriticalCases,
		&result.ClosedToday,
	)
	return result, err
}
