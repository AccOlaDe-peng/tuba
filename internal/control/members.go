package control

import (
	"context"
	"errors"
	"time"
	"tuba/product/internal/auth"
)

type Member struct {
	Subject     string     `json:"subject"`
	Email       string     `json:"email,omitempty"`
	DisplayName string     `json:"display_name,omitempty"`
	Role        string     `json:"role"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
}

func (s *Store) ListMembers(ctx context.Context, p auth.Principal) ([]Member, error) {
	org, _, e := s.ids(ctx, p)
	if e != nil {
		return nil, e
	}
	rows, e := s.Pool.Query(ctx, `SELECT i.subject,coalesce(i.email,''),coalesce(i.display_name,''),r.name,m.revoked_at FROM memberships m JOIN identities i ON i.id=m.identity_id JOIN roles r ON r.id=m.role_id WHERE m.organization_id=$1 AND i.issuer='tuba:local' ORDER BY i.subject,r.name`, org)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Member{}
	for rows.Next() {
		var x Member
		if e = rows.Scan(&x.Subject, &x.Email, &x.DisplayName, &x.Role, &x.RevokedAt); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) SetMember(ctx context.Context, p auth.Principal, subject, role string, revoke bool, requestID string) error {
	if subject == "" || len(subject) > 256 || !map[string]bool{"viewer": true, "analyst": true, "tenant_admin": true}[role] {
		return errors.New("invalid member")
	}
	org, actor, e := s.ids(ctx, p)
	if e != nil {
		return e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var identity, roleID string
	e = tx.QueryRow(ctx, `SELECT i.id FROM identities i JOIN local_accounts a ON a.identity_id=i.id WHERE i.issuer=$1 AND i.subject=$2 AND a.organization_id=$3 AND i.disabled_at IS NULL`, s.Issuer, subject, org).Scan(&identity)
	if e != nil {
		return e
	}
	e = tx.QueryRow(ctx, `SELECT id FROM roles WHERE organization_id=$1 AND name=$2`, org, role).Scan(&roleID)
	if e != nil {
		return e
	}
	if revoke {
		_, e = tx.Exec(ctx, `UPDATE memberships SET revoked_at=now() WHERE organization_id=$1 AND identity_id=$2 AND role_id=$3 AND revoked_at IS NULL`, org, identity, roleID)
	} else {
		_, e = tx.Exec(ctx, `INSERT INTO memberships(organization_id,identity_id,role_id,revoked_at) VALUES($1,$2,$3,NULL) ON CONFLICT(organization_id,identity_id,role_id) DO UPDATE SET revoked_at=NULL`, org, identity, roleID)
	}
	if e != nil {
		return e
	}
	action := "membership.grant"
	if revoke {
		action = "membership.revoke"
	}
	_, e = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,$3,'membership',$4,$5,jsonb_build_object('role',$6::text))`, org, actor, action, subject, requestID, role)
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
