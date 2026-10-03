package api

// Organization slug -> UUID resolver. The bearer principal carries the
// organization slug (organizations.slug), but analysis v2 projections written
// by the F07 sink (ueba-analysis-* state indices) store the control-plane
// organization UUID as organization.id. Read paths that touch those indices
// resolve the slug once and cache it; any resolution failure fails closed so a
// tenant filter is never silently widened or skipped.

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// OrgRowSource is the subset of *pgxpool.Pool the resolver needs; tests
// substitute a fake.
type OrgRowSource interface {
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

type orgCacheEntry struct {
	id      string
	expires time.Time
}

type OrgResolver struct {
	source OrgRowSource
	ttl    time.Duration
	now    func() time.Time
	mu     sync.Mutex
	cache  map[string]orgCacheEntry
}

func NewOrgResolver(source OrgRowSource) *OrgResolver {
	return &OrgResolver{source: source, ttl: 5 * time.Minute, now: time.Now, cache: map[string]orgCacheEntry{}}
}

var ErrOrgNotFound = errors.New("organization not found")

// Resolve returns the control-plane UUID for the organization slug. Positive
// results are cached for the TTL; failures are never cached.
func (r *OrgResolver) Resolve(ctx context.Context, slug string) (string, error) {
	if slug == "" {
		return "", ErrOrgNotFound
	}
	r.mu.Lock()
	if entry, ok := r.cache[slug]; ok && r.now().Before(entry.expires) {
		r.mu.Unlock()
		return entry.id, nil
	}
	r.mu.Unlock()
	var id string
	err := r.source.QueryRow(ctx, `SELECT id FROM organizations WHERE slug=$1`, slug).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrOrgNotFound
		}
		return "", err
	}
	if id == "" {
		return "", ErrOrgNotFound
	}
	r.mu.Lock()
	r.cache[slug] = orgCacheEntry{id: id, expires: r.now().Add(r.ttl)}
	r.mu.Unlock()
	return id, nil
}
