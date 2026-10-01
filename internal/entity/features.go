package entity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SingleEntityFeatures is the per-entity feature view over a [From, To)
// window. Relation enrichment is optional context: when the relation lookup
// fails or relations are absent the single-entity counts are still produced —
// RelationsDegraded records that the relation context could not be loaded,
// instead of blocking the feature path.
type SingleEntityFeatures struct {
	OrganizationID string `json:"organization_id"`
	EntityID       string `json:"entity_id"`
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	// ResolvedByRole counts resolved attributions per role in the window.
	ResolvedByRole map[string]int `json:"resolved_by_role"`
	ResolvedTotal  int            `json:"resolved_total"`
	// Relations is the temporal relation view at the window end.
	Relations []Relation `json:"relations,omitempty"`
	// RelationsDegraded is true when the relation lookup failed; the feature
	// counts above are unaffected and remain authoritative.
	RelationsDegraded bool   `json:"relations_degraded,omitempty"`
	RelationError     string `json:"relation_error,omitempty"`
}

// FeatureAssembler builds single-entity feature views from attribution
// projections, optionally enriched with temporal relations.
type FeatureAssembler struct {
	Pool *pgxpool.Pool
	// RelationsLookup is injectable for tests; nil uses RelationStore over Pool.
	RelationsLookup func(ctx context.Context, organizationID, entityID string, at time.Time) ([]Relation, error)
}

func NewFeatureAssembler(pool *pgxpool.Pool) *FeatureAssembler {
	return &FeatureAssembler{Pool: pool}
}

// assembleSingleEntity is the pure assembly core: attribution counts are
// always produced; a relation lookup error degrades to RelationsDegraded
// instead of failing.
func assembleSingleEntity(orgID, entityID string, from, to time.Time, roleCounts map[string]int, rels []Relation, relErr error) SingleEntityFeatures {
	out := SingleEntityFeatures{
		OrganizationID: orgID,
		EntityID:       entityID,
		From:           from,
		To:             to,
		ResolvedByRole: roleCounts,
		Relations:      rels,
	}
	for _, n := range roleCounts {
		out.ResolvedTotal += n
	}
	if relErr != nil {
		out.Relations = nil
		out.RelationsDegraded = true
		out.RelationError = relErr.Error()
	}
	return out
}

// SingleEntity computes the single-entity feature view for the [from, to)
// window. The attribution counts are the feature path proper and any failure
// there is a hard error. The relation enrichment at the window end is
// best-effort: a failing relation lookup degrades the result (RelationsDegraded)
// rather than blocking the single-entity features.
func (fa *FeatureAssembler) SingleEntity(ctx context.Context, organizationID, entityID string, from, to time.Time) (SingleEntityFeatures, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return SingleEntityFeatures{}, fmt.Errorf("invalid organization id: %w", err)
	}
	if strings.TrimSpace(entityID) == "" {
		return SingleEntityFeatures{}, errors.New("entity id is required")
	}
	if from.IsZero() || to.IsZero() || !to.After(from) {
		return SingleEntityFeatures{}, fmt.Errorf("invalid window [%s, %s)", from, to)
	}
	from, to = from.UTC(), to.UTC()

	roleCounts := map[string]int{}
	rows, err := fa.Pool.Query(ctx, `
		SELECT role, count(*) FROM entity_attributions
		WHERE organization_id = $1 AND entity_id = $2
		  AND status = 'resolved'
		  AND event_time >= $3 AND event_time < $4
		GROUP BY role ORDER BY role`,
		orgID, entityID, from, to)
	if err != nil {
		return SingleEntityFeatures{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var role string
		var n int
		if err := rows.Scan(&role, &n); err != nil {
			return SingleEntityFeatures{}, err
		}
		roleCounts[role] = n
	}
	if err := rows.Err(); err != nil {
		return SingleEntityFeatures{}, err
	}

	lookup := fa.RelationsLookup
	if lookup == nil {
		store := NewRelationStore(fa.Pool)
		lookup = func(ctx context.Context, org, entity string, at time.Time) ([]Relation, error) {
			return store.RelationsAt(ctx, org, entity, at, DirectionBoth, "")
		}
	}
	rels, relErr := lookup(ctx, orgID.String(), entityID, to)
	return assembleSingleEntity(orgID.String(), entityID, from, to, roleCounts, rels, relErr), nil
}
