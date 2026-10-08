package control

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

// BootstrapLocalAdmin is the audited, one-time transition into native login.
// Adopting an existing identity preserves all membership, case and publisher
// references; it never changes the external identity service or its users.
func (s *Store) BootstrapLocalAdmin(ctx context.Context, username, password, organization, existingSubject, approvedBy string) (string, error) {
	name, err := auth.NormalizeUsername(username)
	if err != nil {
		return "", err
	}
	if approvedBy == "" || len(approvedBy) > 256 {
		return "", errors.New("approval reference required")
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return "", err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var org, role, id, subject, oldIssuer string
	if err = tx.QueryRow(ctx, `SELECT id::text FROM organizations WHERE slug=$1 FOR UPDATE`, organization).Scan(&org); err != nil {
		return "", err
	}
	if err = tx.QueryRow(ctx, `SELECT id::text FROM roles WHERE organization_id=$1 AND name='tenant_admin'`, org).Scan(&role); err != nil {
		return "", err
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM local_accounts a JOIN identities i ON i.id=a.identity_id JOIN memberships m ON m.identity_id=i.id WHERE m.organization_id=$1 AND m.role_id=$2 AND m.revoked_at IS NULL AND i.issuer=$3 AND i.disabled_at IS NULL)`, org, role, auth.LocalIssuer).Scan(&exists); err != nil {
		return "", err
	}
	if exists {
		return "", errors.New("local admin already exists; use authenticated account management")
	}
	if existingSubject != "" {
		// The specified subject must already be an enabled administrator of this
		// organization. Never adopt an arbitrary low-privilege or foreign account.
		err = tx.QueryRow(ctx, `SELECT i.id::text,i.subject,i.issuer FROM identities i JOIN memberships m ON m.identity_id=i.id WHERE i.subject=$1 AND i.disabled_at IS NULL AND m.organization_id=$2 AND m.role_id=$3 AND m.revoked_at IS NULL FOR UPDATE OF i`, existingSubject, org, role).Scan(&id, &subject, &oldIssuer)
		if err != nil {
			return "", fmt.Errorf("existing administrator lookup: %w", err)
		}
		if _, err = tx.Exec(ctx, `UPDATE identities SET issuer=$2,display_name=$3 WHERE id=$1`, id, auth.LocalIssuer, name); err != nil {
			return "", err
		}
	} else {
		if err = tx.QueryRow(ctx, `INSERT INTO identities(issuer,subject,display_name) VALUES($1,gen_random_uuid()::text,$2) RETURNING id::text,subject`, auth.LocalIssuer, name).Scan(&id, &subject); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO memberships(organization_id,identity_id,role_id) VALUES($1,$2,$3)`, org, id, role); err != nil {
			return "", err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO local_accounts(identity_id,username,organization_id,password_hash) VALUES($1,$2,$3,$4)`, id, name, org, hash); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,'auth.bootstrap_local_admin','local_account',$2,$3,jsonb_build_object('approved_by',$3::text,'previous_issuer',$4::text,'username',$5::text,'method','tuba-bootstrap-operator'))`, org, id, approvedBy, oldIssuer, name); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return subject, nil
}

func (s *Store) CreateLocalAccount(ctx context.Context, p auth.Principal, username, password, role, requestID string) (string, error) {
	name, err := auth.NormalizeUsername(username)
	if err != nil {
		return "", err
	}
	if !p.Can("user:manage") || !map[string]bool{"viewer": true, "analyst": true, "tenant_admin": true}[role] {
		return "", errors.New("invalid role")
	}
	hash, err := auth.HashPassword(password)
	if err != nil {
		return "", err
	}
	org, actor, err := s.ids(ctx, p)
	if err != nil {
		return "", err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var roleID, id, subject string
	if err = tx.QueryRow(ctx, `SELECT id::text FROM roles WHERE organization_id=$1 AND name=$2`, org, role).Scan(&roleID); err != nil {
		return "", err
	}
	if err = tx.QueryRow(ctx, `INSERT INTO identities(issuer,subject,display_name) VALUES($1,gen_random_uuid()::text,$2) RETURNING id::text,subject`, auth.LocalIssuer, name).Scan(&id, &subject); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO local_accounts(identity_id,username,organization_id,password_hash) VALUES($1,$2,$3,$4)`, id, name, org, hash); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO memberships(organization_id,identity_id,role_id) VALUES($1,$2,$3)`, org, id, roleID); err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,'auth.account_created','local_account',$3,$4,jsonb_build_object('username',$5::text,'role',$6::text))`, org, actor, id, requestID, name, role); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return subject, nil
}
