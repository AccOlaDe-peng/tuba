package control

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

// LocalLogin owns system credentials and revocable, opaque sessions. Tenant and
// publisher permissions remain resolved by Store.Authorize for every request.
type LocalLogin struct {
	Store    *Store
	Lifetime time.Duration
}

func (l *LocalLogin) Verify(ctx context.Context, token string) (auth.Principal, error) {
	var p auth.Principal
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		return p, auth.ErrCredentials
	}
	digest := sha256.Sum256([]byte(token))
	err = l.Store.Pool.QueryRow(ctx, `SELECT i.subject,o.slug,o.namespace FROM auth_sessions s JOIN local_accounts a ON a.identity_id=s.identity_id JOIN identities i ON i.id=a.identity_id JOIN organizations o ON o.id=a.organization_id WHERE s.token_hash=$1 AND s.expires_at>now() AND i.issuer=$2 AND i.disabled_at IS NULL`, digest[:], auth.LocalIssuer).Scan(&p.Subject, &p.Organization, &p.Namespace)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, auth.ErrCredentials
	}
	return p, err
}

func (l *LocalLogin) Login(ctx context.Context, username, password, requestID string) (string, auth.Principal, time.Time, error) {
	var p auth.Principal
	name, err := auth.NormalizeUsername(username)
	if err != nil || len(password) > 256 {
		return "", p, time.Time{}, auth.ErrCredentials
	}
	tx, err := l.Store.Pool.Begin(ctx)
	if err != nil {
		return "", p, time.Time{}, err
	}
	defer tx.Rollback(ctx)
	var id, org, hash string
	var attempts int
	var locked *time.Time
	err = tx.QueryRow(ctx, `SELECT a.identity_id::text,a.organization_id::text,a.password_hash,a.failed_attempts,a.locked_until,i.subject,o.slug,o.namespace FROM local_accounts a JOIN identities i ON i.id=a.identity_id JOIN organizations o ON o.id=a.organization_id WHERE a.username=$1 AND i.issuer=$2 AND i.disabled_at IS NULL FOR UPDATE OF a`, name, auth.LocalIssuer).Scan(&id, &org, &hash, &attempts, &locked, &p.Subject, &p.Organization, &p.Namespace)
	if errors.Is(err, pgx.ErrNoRows) {
		auth.DummyPasswordCheck(password)
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events(action,resource_type,resource_id,request_id) VALUES('auth.login_failed','local_account',$1,$2)`, name, requestID); err != nil {
			return "", p, time.Time{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return "", p, time.Time{}, err
		}
		return "", p, time.Time{}, auth.ErrCredentials
	}
	if err != nil {
		return "", p, time.Time{}, err
	}
	if locked != nil && locked.After(time.Now()) {
		auth.DummyPasswordCheck(password)
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id) VALUES($1::uuid,$2::uuid,'auth.login_locked','local_account',$2::text,$3)`, org, id, requestID); err != nil {
			return "", p, time.Time{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return "", p, time.Time{}, err
		}
		return "", p, time.Time{}, auth.ErrLocked
	}
	if locked != nil {
		attempts = 0
	}
	if !auth.CheckPassword(hash, password) {
		attempts++
		if _, err = tx.Exec(ctx, `UPDATE local_accounts SET failed_attempts=$2,locked_until=CASE WHEN $2>=5 THEN now()+interval '15 minutes' ELSE NULL END WHERE identity_id=$1`, id, attempts); err != nil {
			return "", p, time.Time{}, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,action,resource_type,resource_id,request_id) VALUES($1,'auth.login_failed','local_account',$2::text,$3)`, org, id, requestID); err != nil {
			return "", p, time.Time{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return "", p, time.Time{}, err
		}
		return "", p, time.Time{}, auth.ErrCredentials
	}
	p, err = l.Store.Authorize(ctx, p)
	if err != nil {
		if !errors.Is(err, ErrMembership) {
			return "", p, time.Time{}, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id) VALUES($1::uuid,$2::uuid,'auth.login_denied','local_account',$2::text,$3)`, org, id, requestID); err != nil {
			return "", p, time.Time{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return "", p, time.Time{}, err
		}
		return "", p, time.Time{}, auth.ErrCredentials
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", p, time.Time{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(token))
	lifetime := l.Lifetime
	if lifetime <= 0 {
		lifetime = 8 * time.Hour
	}
	expiry := time.Now().UTC().Add(lifetime)
	if _, err = tx.Exec(ctx, `DELETE FROM auth_sessions WHERE expires_at<=now()`); err != nil {
		return "", p, time.Time{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO auth_sessions(token_hash,identity_id,expires_at) VALUES($1,$2,$3)`, digest[:], id, expiry); err != nil {
		return "", p, time.Time{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE local_accounts SET failed_attempts=0,locked_until=NULL WHERE identity_id=$1`, id); err != nil {
		return "", p, time.Time{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id) VALUES($1::uuid,$2::uuid,'auth.login','local_account',$2::text,$3)`, org, id, requestID); err != nil {
		return "", p, time.Time{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", p, time.Time{}, err
	}
	return token, p, expiry, nil
}

func (l *LocalLogin) Logout(ctx context.Context, token, requestID string) error {
	digest := sha256.Sum256([]byte(token))
	tx, err := l.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id, org string
	err = tx.QueryRow(ctx, `DELETE FROM auth_sessions s USING local_accounts a WHERE s.token_hash=$1 AND a.identity_id=s.identity_id RETURNING s.identity_id::text,a.organization_id::text`, digest[:]).Scan(&id, &org)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id) VALUES($1::uuid,$2::uuid,'auth.logout','local_account',$2::text,$3)`, org, id, requestID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (l *LocalLogin) ChangePassword(ctx context.Context, p auth.Principal, current, password, requestID string) error {
	hash, err := auth.HashPassword(password)
	if err != nil {
		return err
	}
	tx, err := l.Store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id, old string
	err = tx.QueryRow(ctx, `SELECT a.identity_id::text,a.password_hash FROM local_accounts a JOIN identities i ON i.id=a.identity_id WHERE i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL FOR UPDATE OF a`, auth.LocalIssuer, p.Subject).Scan(&id, &old)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.ErrCredentials
	}
	if err != nil {
		return err
	}
	if !auth.CheckPassword(old, current) {
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id) VALUES((SELECT organization_id FROM local_accounts WHERE identity_id=$1::uuid),$1::uuid,'auth.password_change_denied','local_account',$1::text,$2)`, id, requestID); err != nil {
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		return auth.ErrCredentials
	}
	if _, err = tx.Exec(ctx, `UPDATE local_accounts SET password_hash=$2,password_changed_at=now(),failed_attempts=0,locked_until=NULL WHERE identity_id=$1`, id, hash); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM auth_sessions WHERE identity_id=$1`, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id) SELECT o.id,$1::uuid,'auth.password_changed','local_account',$1::text,$2 FROM organizations o WHERE o.slug=$3`, id, requestID, p.Organization); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
