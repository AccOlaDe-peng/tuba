package entity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Relation types. Cardinality decides the conflict semantics of overlapping
// intervals for the same (from_entity, relation_type):
//   - single: at most one active `to` at any time (a device is assigned to
//     exactly one owner); a concurrent different `to` is a conflict.
//   - multi: several `to` entities may be active simultaneously (an account
//     is a member of many groups).
type RelationCardinality string

const (
	CardinalitySingle RelationCardinality = "single"
	CardinalityMulti  RelationCardinality = "multi"
)

const (
	RelMemberOf   = "member_of"   // account → group/container entity
	RelBelongsTo  = "belongs_to"  // entity → organizational unit entity
	RelAssignedTo = "assigned_to" // device → account (owner)
	RelManagedBy  = "managed_by"  // device → account (administrator)
)

var relationTypes = map[string]RelationCardinality{
	RelMemberOf:   CardinalityMulti,
	RelBelongsTo:  CardinalityMulti,
	RelAssignedTo: CardinalitySingle,
	RelManagedBy:  CardinalitySingle,
}

var (
	ErrUnknownRelationType   = errors.New("unknown relation type")
	ErrRelationConflict      = errors.New("conflicting relation with an overlapping validity interval")
	ErrRelationNotFound      = errors.New("relation not found")
	ErrRelationAlreadyClosed = errors.New("relation already closed with a different valid_to")
	ErrNoActiveRelation      = errors.New("no active relation to transfer")
	ErrRelationEntityMissing = errors.New("relation endpoint entity is not registered")
	ErrInvalidConfidence     = errors.New("confidence must be within [0, 1]")
)

// Relation is one temporal edge between two entities, valid over the
// half-open interval [ValidFrom, ValidTo) — the same interval semantics as
// entity occurrences (E01) and attribution adjudication (E02).
type Relation struct {
	OrganizationID string          `json:"organization_id"`
	RelationID     string          `json:"relation_id"`
	FromEntityID   string          `json:"from_entity_id"`
	RelationType   string          `json:"relation_type"`
	ToEntityID     string          `json:"to_entity_id"`
	EventID        string          `json:"event_id"`
	RuleVersion    string          `json:"rule_version"`
	ValidFrom      time.Time       `json:"valid_from"`
	ValidTo        *time.Time      `json:"valid_to,omitempty"`
	Confidence     float64         `json:"confidence"`
	Evidence       json.RawMessage `json:"evidence,omitempty"`
}

// RelationRequest asks the store to assert one temporal relation.
type RelationRequest struct {
	OrganizationID string
	FromEntityID   string
	RelationType   string
	ToEntityID     string
	// EventID is the event that evidenced this relation fact; it is part of
	// relation.id, so replaying the same event re-derives the same relation.
	EventID string
	// RuleVersion must reference a registered relation_mapping rule snapshot.
	RuleVersion string
	Confidence  float64
	Evidence    json.RawMessage
	// At is the fact's valid_from.
	At time.Time
}

// relationResolutionSnapshot is persisted in entity_relations.resolution_snapshot.
type relationResolutionSnapshot struct {
	RelationType string `json:"relation_type"`
	RuleKind     string `json:"rule_kind"`
	RuleVersion  string `json:"rule_version"`
	AssertedBy   string `json:"asserted_by_event"`
}

// RelationID derives the stable relation.id per contracts/ids.md: tenant,
// from entity, relation type, to entity, event ID and resolution snapshot.
func RelationID(organizationID, fromEntityID, relationType, toEntityID, eventID, resolutionSnapshot string) string {
	return "rel:" + stableHash("relation-v1", organizationID, fromEntityID, relationType, toEntityID, eventID, resolutionSnapshot)
}

func relationSnapshot(relationType, ruleVersion, eventID string) string {
	return strings.Join([]string{relationType, RuleKindRelationMapping, ruleVersion, eventID}, "|")
}

// intervalsOverlap reports whether two half-open [from, to) intervals share
// any instant. Touching intervals (a.to == b.from) do not overlap — the same
// boundary semantics used for occurrence validity in adjudicate.
func intervalsOverlap(aFrom time.Time, aTo *time.Time, bFrom time.Time, bTo *time.Time) bool {
	if aTo != nil && !bFrom.Before(*aTo) {
		return false
	}
	if bTo != nil && !aFrom.Before(*bTo) {
		return false
	}
	return true
}

// RelationStore persists temporal entity relations in PostgreSQL. Writers of
// the same (organization, from_entity, relation_type) key are serialized with
// a transaction-scoped advisory lock and overlapping intervals are rejected
// fail-closed inside the same transaction.
type RelationStore struct {
	Pool *pgxpool.Pool
}

func NewRelationStore(pool *pgxpool.Pool) *RelationStore {
	return &RelationStore{Pool: pool}
}

func (s *RelationStore) validateRequest(req RelationRequest) (uuid.UUID, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(req.OrganizationID))
	if err != nil {
		return orgID, fmt.Errorf("invalid organization id: %w", err)
	}
	if _, ok := relationTypes[req.RelationType]; !ok {
		return orgID, fmt.Errorf("%w: %q", ErrUnknownRelationType, req.RelationType)
	}
	if strings.TrimSpace(req.FromEntityID) == "" || strings.TrimSpace(req.ToEntityID) == "" {
		return orgID, fmt.Errorf("relation endpoints are required")
	}
	if strings.TrimSpace(req.EventID) == "" {
		return orgID, ErrEmptyEventID
	}
	if err := ValidateRuleRef(RuleKindRelationMapping, req.RuleVersion); err != nil {
		return orgID, err
	}
	if req.Confidence < 0 || req.Confidence > 1 {
		return orgID, fmt.Errorf("%w: %v", ErrInvalidConfidence, req.Confidence)
	}
	if req.At.IsZero() {
		return orgID, ErrInvalidEventTime
	}
	return orgID, nil
}

// AssertRelation records one temporal relation fact. It is idempotent per
// derived relation.id (replaying the same event returns the stored row) and
// fail-closed on conflicts: an overlapping interval for the same
// (from, type) pair with a different `to` on a single-cardinality type, or an
// overlapping interval for the same (from, type, to) asserted by a different
// event, is rejected and nothing is written.
func (s *RelationStore) AssertRelation(ctx context.Context, req RelationRequest) (Relation, error) {
	orgID, err := s.validateRequest(req)
	if err != nil {
		return Relation{}, err
	}
	at := req.At.UTC()
	evidence := req.Evidence
	if len(evidence) == 0 {
		evidence = json.RawMessage(`{}`)
	}
	snap := relationSnapshot(req.RelationType, req.RuleVersion, req.EventID)
	rel := Relation{
		OrganizationID: orgID.String(),
		RelationID:     RelationID(orgID.String(), req.FromEntityID, req.RelationType, req.ToEntityID, req.EventID, snap),
		FromEntityID:   req.FromEntityID,
		RelationType:   req.RelationType,
		ToEntityID:     req.ToEntityID,
		EventID:        req.EventID,
		RuleVersion:    req.RuleVersion,
		ValidFrom:      at,
		Confidence:     req.Confidence,
		Evidence:       evidence,
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Relation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRelationKey(ctx, tx, orgID, req.FromEntityID, req.RelationType); err != nil {
		return Relation{}, err
	}
	if err := s.requireRuleSnapshot(ctx, tx, orgID, req.RuleVersion); err != nil {
		return Relation{}, err
	}
	if err := s.requireEntities(ctx, tx, orgID, req.FromEntityID, req.ToEntityID); err != nil {
		return Relation{}, err
	}

	// Replay: the derived relation.id already exists.
	var existingValidFrom time.Time
	var existingValidTo *time.Time
	err = tx.QueryRow(ctx, `
		SELECT valid_from, valid_to FROM entity_relations
		WHERE organization_id = $1 AND relation_id = $2`,
		orgID, rel.RelationID).Scan(&existingValidFrom, &existingValidTo)
	if err == nil {
		if !existingValidFrom.Equal(at) {
			return Relation{}, fmt.Errorf("%w: relation %s stored with valid_from %s, requested %s",
				ErrRelationConflict, rel.RelationID, existingValidFrom, at)
		}
		rel.ValidTo = existingValidTo
		return rel, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Relation{}, err
	}

	// Conflict check: any overlapping interval on the same (from, type).
	overlapping, err := s.overlappingRelations(ctx, tx, orgID, req.FromEntityID, req.RelationType, at, nil)
	if err != nil {
		return Relation{}, err
	}
	for _, other := range overlapping {
		if other.ToEntityID == req.ToEntityID {
			return Relation{}, fmt.Errorf("%w: %s %s→%s already asserted by event %s over [%s, %v)",
				ErrRelationConflict, req.RelationType, req.FromEntityID, req.ToEntityID,
				other.EventID, other.ValidFrom, other.ValidTo)
		}
		if relationTypes[req.RelationType] == CardinalitySingle {
			return Relation{}, fmt.Errorf("%w: single-cardinality %s of %s already held by %s over [%s, %v)",
				ErrRelationConflict, req.RelationType, req.FromEntityID, other.ToEntityID,
				other.ValidFrom, other.ValidTo)
		}
	}

	snapJSON, err := json.Marshal(relationResolutionSnapshot{
		RelationType: req.RelationType,
		RuleKind:     RuleKindRelationMapping,
		RuleVersion:  req.RuleVersion,
		AssertedBy:   req.EventID,
	})
	if err != nil {
		return Relation{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO entity_relations(
			organization_id, relation_id, from_entity_id, relation_type, to_entity_id,
			event_id, resolution_snapshot, valid_from, confidence, evidence)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		orgID, rel.RelationID, rel.FromEntityID, rel.RelationType, rel.ToEntityID,
		rel.EventID, string(snapJSON), rel.ValidFrom, rel.Confidence, string(evidence))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return Relation{}, fmt.Errorf("%w: %s or %s", ErrRelationEntityMissing, req.FromEntityID, req.ToEntityID)
		}
		return Relation{}, err
	}
	return rel, tx.Commit(ctx)
}

// CloseRelation ends a relation's validity interval at the given time.
// Closing with the same valid_to twice is idempotent; closing with a
// different time, a time not after valid_from, or an unknown relation is
// rejected fail-closed.
func (s *RelationStore) CloseRelation(ctx context.Context, organizationID, relationID string, at time.Time) (Relation, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return Relation{}, fmt.Errorf("invalid organization id: %w", err)
	}
	if at.IsZero() {
		return Relation{}, ErrInvalidEventTime
	}
	at = at.UTC()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Relation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rel, err := s.lockRelation(ctx, tx, orgID, relationID)
	if err != nil {
		return Relation{}, err
	}
	if rel.ValidTo != nil {
		if rel.ValidTo.Equal(at) {
			return rel, tx.Commit(ctx)
		}
		return Relation{}, fmt.Errorf("%w: %s closed at %s, requested %s",
			ErrRelationAlreadyClosed, relationID, *rel.ValidTo, at)
	}
	if !at.After(rel.ValidFrom) {
		return Relation{}, fmt.Errorf("%w: %s not after %s", ErrInvalidValidTo, at, rel.ValidFrom)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE entity_relations SET valid_to = $3
		WHERE organization_id = $1 AND relation_id = $2 AND valid_to IS NULL`,
		orgID, relationID, at); err != nil {
		return Relation{}, err
	}
	rel.ValidTo = &at
	return rel, tx.Commit(ctx)
}

// TransferRelation moves a single-cardinality relation to a new `to` entity
// atomically: the active interval is closed at req.At and the new interval
// opens at the same instant, so every point in time resolves to exactly one
// holder. Without an active relation the transfer is rejected fail-closed.
func (s *RelationStore) TransferRelation(ctx context.Context, req RelationRequest) (closed Relation, opened Relation, err error) {
	orgID, err := s.validateRequest(req)
	if err != nil {
		return Relation{}, Relation{}, err
	}
	if relationTypes[req.RelationType] != CardinalitySingle {
		return Relation{}, Relation{}, fmt.Errorf("%w: transfer applies to single-cardinality types, %q is multi",
			ErrUnknownRelationType, req.RelationType)
	}
	at := req.At.UTC()
	evidence := req.Evidence
	if len(evidence) == 0 {
		evidence = json.RawMessage(`{}`)
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Relation{}, Relation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockRelationKey(ctx, tx, orgID, req.FromEntityID, req.RelationType); err != nil {
		return Relation{}, Relation{}, err
	}
	if err := s.requireRuleSnapshot(ctx, tx, orgID, req.RuleVersion); err != nil {
		return Relation{}, Relation{}, err
	}
	if err := s.requireEntities(ctx, tx, orgID, req.FromEntityID, req.ToEntityID); err != nil {
		return Relation{}, Relation{}, err
	}

	active, err := s.overlappingRelations(ctx, tx, orgID, req.FromEntityID, req.RelationType, at, nil)
	if err != nil {
		return Relation{}, Relation{}, err
	}
	var current *Relation
	for i := range active {
		if active[i].ValidTo == nil {
			current = &active[i]
			break
		}
	}
	if current == nil {
		return Relation{}, Relation{}, fmt.Errorf("%w: %s of %s", ErrNoActiveRelation, req.RelationType, req.FromEntityID)
	}
	if current.ToEntityID == req.ToEntityID {
		return Relation{}, Relation{}, fmt.Errorf("%w: %s of %s already held by %s",
			ErrRelationConflict, req.RelationType, req.FromEntityID, req.ToEntityID)
	}
	if !at.After(current.ValidFrom) {
		return Relation{}, Relation{}, fmt.Errorf("%w: %s not after %s", ErrInvalidValidTo, at, current.ValidFrom)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE entity_relations SET valid_to = $3
		WHERE organization_id = $1 AND relation_id = $2 AND valid_to IS NULL`,
		orgID, current.RelationID, at); err != nil {
		return Relation{}, Relation{}, err
	}
	closed = *current
	closed.ValidTo = &at

	snap := relationSnapshot(req.RelationType, req.RuleVersion, req.EventID)
	opened = Relation{
		OrganizationID: orgID.String(),
		RelationID:     RelationID(orgID.String(), req.FromEntityID, req.RelationType, req.ToEntityID, req.EventID, snap),
		FromEntityID:   req.FromEntityID,
		RelationType:   req.RelationType,
		ToEntityID:     req.ToEntityID,
		EventID:        req.EventID,
		RuleVersion:    req.RuleVersion,
		ValidFrom:      at,
		Confidence:     req.Confidence,
		Evidence:       evidence,
	}
	snapJSON, err := json.Marshal(relationResolutionSnapshot{
		RelationType: req.RelationType,
		RuleKind:     RuleKindRelationMapping,
		RuleVersion:  req.RuleVersion,
		AssertedBy:   req.EventID,
	})
	if err != nil {
		return Relation{}, Relation{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO entity_relations(
			organization_id, relation_id, from_entity_id, relation_type, to_entity_id,
			event_id, resolution_snapshot, valid_from, confidence, evidence)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		orgID, opened.RelationID, opened.FromEntityID, opened.RelationType, opened.ToEntityID,
		opened.EventID, string(snapJSON), opened.ValidFrom, opened.Confidence, string(evidence)); err != nil {
		return Relation{}, Relation{}, err
	}
	return closed, opened, tx.Commit(ctx)
}

// RelationDirection selects which side of the edge to match.
type RelationDirection string

const (
	DirectionOutgoing RelationDirection = "outgoing" // from_entity = entity
	DirectionIncoming RelationDirection = "incoming" // to_entity = entity
	DirectionBoth     RelationDirection = "both"
)

// RelationsAt returns the temporal view of an entity's relations at one
// instant: every edge whose [valid_from, valid_to) contains `at`, in both
// directions unless narrowed. A missing relation is an empty slice, never an
// error — single-entity consumers must not be blocked by absent relations.
func (s *RelationStore) RelationsAt(ctx context.Context, organizationID, entityID string, at time.Time, direction RelationDirection, relationType string) ([]Relation, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return nil, fmt.Errorf("invalid organization id: %w", err)
	}
	if at.IsZero() {
		return nil, ErrInvalidEventTime
	}
	at = at.UTC()
	where := "(from_entity_id = $2 OR to_entity_id = $2)"
	switch direction {
	case DirectionOutgoing:
		where = "from_entity_id = $2"
	case DirectionIncoming:
		where = "to_entity_id = $2"
	case DirectionBoth, "":
	default:
		return nil, fmt.Errorf("unknown relation direction %q", direction)
	}
	args := []any{orgID, entityID, at}
	if relationType != "" {
		if _, ok := relationTypes[relationType]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrUnknownRelationType, relationType)
		}
		where += " AND relation_type = $4"
		args = append(args, relationType)
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT relation_id, from_entity_id, relation_type, to_entity_id, event_id,
		       resolution_snapshot, valid_from, valid_to, confidence, evidence::text
		FROM entity_relations
		WHERE organization_id = $1 AND `+where+`
		  AND valid_from <= $3 AND (valid_to IS NULL OR valid_to > $3)
		ORDER BY relation_type, relation_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRelations(rows)
}

func scanRelations(rows pgx.Rows) ([]Relation, error) {
	out := []Relation{}
	for rows.Next() {
		var r Relation
		var snap string
		var evidence string
		if err := rows.Scan(&r.RelationID, &r.FromEntityID, &r.RelationType, &r.ToEntityID,
			&r.EventID, &snap, &r.ValidFrom, &r.ValidTo, &r.Confidence, &evidence); err != nil {
			return nil, err
		}
		var rs relationResolutionSnapshot
		if err := json.Unmarshal([]byte(snap), &rs); err != nil {
			return nil, fmt.Errorf("relation %s has undecodable resolution snapshot: %w", r.RelationID, err)
		}
		r.RuleVersion = rs.RuleVersion
		r.Evidence = json.RawMessage(evidence)
		out = append(out, r)
	}
	return out, rows.Err()
}

func lockRelationKey(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, fromEntityID, relationType string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 23))`,
		orgID.String()+"|"+relationType+"|"+fromEntityID)
	return err
}

func (s *RelationStore) requireRuleSnapshot(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, version string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM rule_snapshots
			WHERE organization_id = $1 AND rule_kind = $2 AND rule_version = $3)`,
		orgID, RuleKindRelationMapping, version).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s %s", ErrRuleSnapshotNotStored, RuleKindRelationMapping, version)
	}
	return nil
}

func (s *RelationStore) requireEntities(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, ids ...string) error {
	for _, id := range ids {
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM entities WHERE organization_id = $1 AND entity_id = $2)`,
			orgID, id).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("%w: %s", ErrRelationEntityMissing, id)
		}
	}
	return nil
}

func (s *RelationStore) lockRelation(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, relationID string) (Relation, error) {
	rows, err := tx.Query(ctx, `
		SELECT relation_id, from_entity_id, relation_type, to_entity_id, event_id,
		       resolution_snapshot, valid_from, valid_to, confidence, evidence::text
		FROM entity_relations
		WHERE organization_id = $1 AND relation_id = $2
		FOR UPDATE`, orgID, relationID)
	if err != nil {
		return Relation{}, err
	}
	defer rows.Close()
	rels, err := scanRelations(rows)
	if err != nil {
		return Relation{}, err
	}
	if len(rels) == 0 {
		return Relation{}, fmt.Errorf("%w: %s", ErrRelationNotFound, relationID)
	}
	rels[0].OrganizationID = orgID.String()
	return rels[0], nil
}

// overlappingRelations returns relations of (from, type) whose
// [valid_from, valid_to) overlaps [from, to).
func (s *RelationStore) overlappingRelations(ctx context.Context, tx pgx.Tx, orgID uuid.UUID, fromEntityID, relationType string, from time.Time, to *time.Time) ([]Relation, error) {
	rows, err := tx.Query(ctx, `
		SELECT relation_id, from_entity_id, relation_type, to_entity_id, event_id,
		       resolution_snapshot, valid_from, valid_to, confidence, evidence::text
		FROM entity_relations
		WHERE organization_id = $1 AND from_entity_id = $2 AND relation_type = $3
		ORDER BY valid_from, relation_id`, orgID, fromEntityID, relationType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rels, err := scanRelations(rows)
	if err != nil {
		return nil, err
	}
	out := rels[:0]
	for _, r := range rels {
		if intervalsOverlap(r.ValidFrom, r.ValidTo, from, to) {
			out = append(out, r)
		}
	}
	return out, nil
}
