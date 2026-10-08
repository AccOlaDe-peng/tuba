package control

import (
	"context"
	"tuba/product/internal/auth"
)

func (s *Store) AuditAuthDenial(ctx context.Context, p auth.Principal, reason, requestID string) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata)
 SELECT (SELECT id FROM organizations WHERE slug=$1),(SELECT id FROM identities WHERE issuer=$2 AND subject=$3),'auth.authorization_denied','system_session',$3,$4,jsonb_build_object('reason',$5::text)`, p.Organization, auth.LocalIssuer, p.Subject, requestID, reason)
	return err
}
