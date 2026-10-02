package entity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Read-side queries behind the entity profile API. Everything is scoped by
// the tenant slug (resolved to the organization id inside SQL), never by a
// caller-supplied organization id, so tenant isolation cannot be bypassed by
// crafting identifiers. All methods are read-only.
type Queries struct {
	Pool *pgxpool.Pool
}

func NewQueries(pool *pgxpool.Pool) *Queries {
	return &Queries{Pool: pool}
}

// ErrInvalidCursor rejects a malformed pagination cursor fail-closed.
var ErrInvalidCursor = errors.New("invalid cursor")

// orgFilter resolves the tenant slug to the organization id in SQL.
const orgFilter = `organization_id = (SELECT id FROM organizations WHERE slug = $1)`

// EntitySummary is one row of the entity directory listing.
type EntitySummary struct {
	EntityID     string     `json:"entity_id"`
	EntityType   string     `json:"entity_type"`
	Authority    string     `json:"authority"`
	CanonicalKey string     `json:"canonical_key"`
	Strength     Strength   `json:"identity_strength"`
	Revision     int64      `json:"revision"`
	ValidFrom    time.Time  `json:"valid_from"`
	ValidTo      *time.Time `json:"valid_to,omitempty"`
}

func encodeCursor(values ...string) string {
	b, _ := json.Marshal(values)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(raw string, fields int) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	var values []string
	if err := json.Unmarshal(b, &values); err != nil || len(values) != fields {
		return nil, ErrInvalidCursor
	}
	return values, nil
}

// SearchEntities lists registered entities ordered by (canonical_key,
// entity_id) with keyset pagination. query is a case-sensitive substring
// match on the canonical key (keys are normalized at registration); an empty
// query lists everything. entityType optionally narrows to account/device.
func (q *Queries) SearchEntities(ctx context.Context, slug, query, entityType, cursor string, limit int) ([]EntitySummary, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", fmt.Errorf("limit must be 1..100")
	}
	where := orgFilter
	args := []any{slug}
	if query != "" {
		args = append(args, query)
		where += fmt.Sprintf(" AND strpos(canonical_key, $%d) > 0", len(args))
	}
	if entityType != "" {
		if entityType != TypeAccount && entityType != TypeDevice {
			return nil, "", ErrInvalidEntityType
		}
		args = append(args, entityType)
		where += fmt.Sprintf(" AND entity_type = $%d", len(args))
	}
	if values, err := decodeCursor(cursor, 2); err != nil {
		return nil, "", err
	} else if values != nil {
		args = append(args, values[0], values[1])
		where += fmt.Sprintf(" AND (canonical_key, entity_id) > ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	rows, err := q.Pool.Query(ctx, `
		SELECT entity_id, entity_type, authority, canonical_key, identity_strength, revision, valid_from, valid_to
		FROM entities
		WHERE `+where+`
		ORDER BY canonical_key, entity_id
		LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := []EntitySummary{}
	for rows.Next() {
		var e EntitySummary
		var strength string
		if err := rows.Scan(&e.EntityID, &e.EntityType, &e.Authority, &e.CanonicalKey, &strength, &e.Revision, &e.ValidFrom, &e.ValidTo); err != nil {
			return nil, "", err
		}
		e.Strength = Strength(strength)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = encodeCursor(last.CanonicalKey, last.EntityID)
	}
	return out, next, nil
}

// AttributionSummary is one attribution-history row for an entity: the event
// role, the three-state outcome, the rule version, the evidence reason code
// and the event time.
type AttributionSummary struct {
	AttributionID string          `json:"attribution_id"`
	EventID       string          `json:"event_id"`
	Role          string          `json:"role"`
	State         string          `json:"state"`
	RuleVersion   string          `json:"rule_version"`
	Reason        string          `json:"reason,omitempty"`
	EventTime     time.Time       `json:"event_time"`
	Evidence      json.RawMessage `json:"evidence,omitempty"`
}

// EntityDetail is the entity master record plus its most recent resolved
// attributions (newest first, at most five).
type EntityDetail struct {
	EntitySummary
	Document           json.RawMessage      `json:"document"`
	RecentAttributions []AttributionSummary `json:"recent_attributions"`
}

// GetEntity loads one entity master record by entity.id. A missing entity is
// reported as found=false, never as an error.
func (q *Queries) GetEntity(ctx context.Context, slug, entityID string) (EntityDetail, bool, error) {
	var d EntityDetail
	var strength string
	err := q.Pool.QueryRow(ctx, `
		SELECT entity_id, entity_type, authority, canonical_key, identity_strength, revision, document, valid_from, valid_to
		FROM entities
		WHERE `+orgFilter+` AND entity_id = $2`, slug, entityID).
		Scan(&d.EntityID, &d.EntityType, &d.Authority, &d.CanonicalKey, &strength, &d.Revision, &d.Document, &d.ValidFrom, &d.ValidTo)
	if errors.Is(err, pgx.ErrNoRows) {
		return EntityDetail{}, false, nil
	}
	if err != nil {
		return EntityDetail{}, false, err
	}
	d.Strength = Strength(strength)
	recent, _, err := q.ListAttributions(ctx, slug, entityID, "", 5)
	if err != nil {
		return EntityDetail{}, false, err
	}
	d.RecentAttributions = recent
	return d, true, nil
}

// ListAttributions returns the entity's attribution history newest-first with
// keyset pagination on (event_time, attribution_id).
func (q *Queries) ListAttributions(ctx context.Context, slug, entityID, cursor string, limit int) ([]AttributionSummary, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", fmt.Errorf("limit must be 1..100")
	}
	where := orgFilter + ` AND entity_id = $2`
	args := []any{slug, entityID}
	if values, err := decodeCursor(cursor, 2); err != nil {
		return nil, "", err
	} else if values != nil {
		at, err := time.Parse(time.RFC3339Nano, values[0])
		if err != nil {
			return nil, "", ErrInvalidCursor
		}
		args = append(args, at, values[1])
		where += fmt.Sprintf(" AND (event_time < $%d OR (event_time = $%d AND attribution_id < $%d))", len(args)-1, len(args)-1, len(args))
	}
	args = append(args, limit+1)
	rows, err := q.Pool.Query(ctx, `
		SELECT attribution_id, event_id, role, status, resolution_snapshot, evidence, event_time
		FROM entity_attributions
		WHERE `+where+`
		ORDER BY event_time DESC, attribution_id DESC
		LIMIT $`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := []AttributionSummary{}
	for rows.Next() {
		var a AttributionSummary
		var snapshot string
		if err := rows.Scan(&a.AttributionID, &a.EventID, &a.Role, &a.State, &snapshot, &a.Evidence, &a.EventTime); err != nil {
			return nil, "", err
		}
		var snap resolutionSnapshot
		if err := json.Unmarshal([]byte(snapshot), &snap); err != nil {
			return nil, "", fmt.Errorf("attribution %s has undecodable resolution snapshot: %w", a.AttributionID, err)
		}
		a.RuleVersion = snap.RuleVersion
		var ev struct {
			Reason string `json:"reason,omitempty"`
		}
		_ = json.Unmarshal(a.Evidence, &ev)
		a.Reason = ev.Reason
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = encodeCursor(last.EventTime.UTC().Format(time.RFC3339Nano), last.AttributionID)
	}
	return out, next, nil
}

// ListRelations returns the entity's temporal relations in both directions,
// active intervals first, then newest history. With history=false only
// currently active relations are returned.
func (q *Queries) ListRelations(ctx context.Context, slug, entityID string, history bool) ([]Relation, error) {
	where := orgFilter + ` AND (from_entity_id = $2 OR to_entity_id = $2)`
	if !history {
		where += ` AND valid_to IS NULL`
	}
	rows, err := q.Pool.Query(ctx, `
		SELECT relation_id, from_entity_id, relation_type, to_entity_id, event_id,
		       resolution_snapshot, valid_from, valid_to, confidence, evidence::text
		FROM entity_relations
		WHERE `+where+`
		ORDER BY (valid_to IS NULL) DESC, valid_from DESC, relation_id
		LIMIT 200`, slug, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRelations(rows)
}

// FeatureSample is one closed feature window for the entity.
type FeatureSample struct {
	FeatureID      string          `json:"feature_id"`
	FeatureVersion string          `json:"feature_version"`
	Generation     string          `json:"generation"`
	WindowStart    time.Time       `json:"window_start"`
	WindowEnd      time.Time       `json:"window_end"`
	Revision       int             `json:"revision"`
	Quality        string          `json:"quality"`
	Values         json.RawMessage `json:"values"`
}

// ListFeatureSamples returns the entity's most recent closed feature windows,
// newest first.
func (q *Queries) ListFeatureSamples(ctx context.Context, slug, entityID string, limit int) ([]FeatureSample, error) {
	if limit < 1 || limit > 50 {
		return nil, fmt.Errorf("limit must be 1..50")
	}
	rows, err := q.Pool.Query(ctx, `
		SELECT feature_id, feature_version, generation, window_start, window_end, revision, quality, values
		FROM feature_samples
		WHERE `+orgFilter+` AND entity_id = $2
		ORDER BY window_start DESC
		LIMIT $3`, slug, entityID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FeatureSample{}
	for rows.Next() {
		var s FeatureSample
		if err := rows.Scan(&s.FeatureID, &s.FeatureVersion, &s.Generation, &s.WindowStart, &s.WindowEnd, &s.Revision, &s.Quality, &s.Values); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// BaselineModel is the lifecycle state of one baseline model version.
// CoversEntity reports whether the model's published (immutable) training
// sample range included the requested entity; cold_start rows carry an empty
// sample range, so coverage is false there by definition.
type BaselineModel struct {
	ModelID        string          `json:"model_id"`
	ModelVersion   string          `json:"model_version"`
	FeatureID      string          `json:"feature_id"`
	FeatureVersion string          `json:"feature_version"`
	Generation     string          `json:"generation"`
	Status         string          `json:"status"`
	SampleCount    int             `json:"sample_count"`
	CompleteDays   int             `json:"complete_days"`
	TrainedAt      *time.Time      `json:"trained_at,omitempty"`
	TrainingCutoff *time.Time      `json:"training_cutoff,omitempty"`
	SampleRange    json.RawMessage `json:"sample_range"`
	Metrics        json.RawMessage `json:"metrics"`
	CreatedAt      time.Time       `json:"created_at"`
	CoversEntity   bool            `json:"covers_entity"`
}

// ListBaselines returns the tenant's baseline model versions, newest first.
// Models are population-wide (keyed by model id, not entity); whether one
// covers the requested entity is decided by the immutable sample_range entity
// list published at training time.
func (q *Queries) ListBaselines(ctx context.Context, slug, entityID string) ([]BaselineModel, error) {
	rows, err := q.Pool.Query(ctx, `
		SELECT model_id, model_version, feature_id, feature_version, generation, status,
		       sample_count, complete_days, trained_at, training_cutoff, sample_range, metrics, created_at,
		       COALESCE(sample_range->'entities' ? $2, false)
		FROM baseline_models
		WHERE `+orgFilter+`
		ORDER BY created_at DESC
		LIMIT 50`, slug, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []BaselineModel{}
	for rows.Next() {
		var m BaselineModel
		if err := rows.Scan(&m.ModelID, &m.ModelVersion, &m.FeatureID, &m.FeatureVersion, &m.Generation, &m.Status,
			&m.SampleCount, &m.CompleteDays, &m.TrainedAt, &m.TrainingCutoff, &m.SampleRange, &m.Metrics, &m.CreatedAt, &m.CoversEntity); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
