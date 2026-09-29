package control

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/auth"
	"tuba/product/internal/pgutil"
)

type Store struct {
	Pool   *pgxpool.Pool
	Issuer string
}

func Open(ctx context.Context, url, issuer string) (*Store, error) {
	if url == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	p, e := pgutil.NewPool(ctx, url)
	if e != nil {
		return nil, e
	}
	pingCtx, cancel := context.WithTimeout(ctx, pgutil.PoolPingTimeout)
	defer cancel()
	if e = p.Ping(pingCtx); e != nil {
		p.Close()
		return nil, e
	}
	return &Store{p, issuer}, nil
}
func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Ping(ctx context.Context) (time.Duration, error) {
	started := time.Now()
	err := s.Pool.Ping(ctx)
	return time.Since(started), err
}

func (s *Store) Authorize(ctx context.Context, p auth.Principal) (auth.Principal, error) {
	rows, err := s.Pool.Query(ctx, `SELECT o.slug,o.namespace,r.name FROM identities i JOIN memberships m ON m.identity_id=i.id AND m.revoked_at IS NULL JOIN organizations o ON o.id=m.organization_id JOIN roles r ON r.id=m.role_id WHERE i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL AND o.slug=$3`, s.Issuer, p.Subject, p.Organization)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	roles := []string{}
	for rows.Next() {
		var org, ns, role string
		if err := rows.Scan(&org, &ns, &role); err != nil {
			return p, err
		}
		p.Organization, p.Namespace = org, ns
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return p, err
	}
	if len(roles) == 0 {
		return p, errors.New("active membership not found")
	}
	// Platform release permissions come from a separate, audited global grant,
	// never from tenant membership or token-supplied role claims.
	var platformPublisher bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM platform_release_publishers pp JOIN identities i ON i.id=pp.identity_id WHERE i.issuer=$1 AND i.subject=$2 AND i.disabled_at IS NULL AND pp.revoked_at IS NULL)`, s.Issuer, p.Subject).Scan(&platformPublisher); err != nil {
		return p, err
	}
	if platformPublisher {
		roles = append(roles, "platform_publisher")
	}
	p.Roles = roles
	return p, nil
}
