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

// SpaceKind classifies an identity space.
type SpaceKind string

const (
	SpaceActiveDirectory SpaceKind = "active_directory"
	SpaceLocalAccounts   SpaceKind = "local_accounts"
	SpaceCloudDirectory  SpaceKind = "cloud_directory"
	SpaceCustom          SpaceKind = "custom"
)

// Entity types delivered by E01.
const (
	TypeAccount = "account"
	TypeDevice  = "device"
)

var (
	ErrInvalidEntityType  = errors.New("entity type must be account or device")
	ErrInvalidSpaceKind   = errors.New("unsupported identity space kind")
	ErrSpaceNotRegistered = errors.New("identity space is not registered")
	ErrSpaceKindConflict  = errors.New("identity space name already registered with a different kind")
	ErrNotWeakIdentity    = errors.New("only weak identities can be transferred")
	ErrNoActiveOccurrence = errors.New("no active occurrence of the weak identity")
	ErrInvalidValidTo     = errors.New("transfer time must be after the active occurrence valid_from")
	ErrEntityIDConflict   = errors.New("entity id already exists with a closed or conflicting record")
)

// Space is a registered identity space.
type Space struct {
	OrganizationID string    `json:"organization_id"`
	SpaceID        string    `json:"space_id"`
	Name           string    `json:"name"`
	Kind           SpaceKind `json:"kind"`
	CreatedAt      time.Time `json:"created_at"`
}

// Entity is a registered Account/Device record.
type Entity struct {
	OrganizationID string          `json:"organization_id"`
	EntityID       string          `json:"entity_id"`
	EntityType     string          `json:"entity_type"`
	Authority      string          `json:"authority"`
	CanonicalKey   string          `json:"canonical_key"`
	Strength       Strength        `json:"identity_strength"`
	Revision       int64           `json:"revision"`
	Document       json.RawMessage `json:"document"`
	ValidFrom      time.Time       `json:"valid_from"`
	ValidTo        *time.Time      `json:"valid_to,omitempty"`
}

// RegisterRequest asks the registry to resolve and persist an entity.
type RegisterRequest struct {
	OrganizationID string
	Space          string
	EntityType     string
	Identifiers    []Identifier
	// At is the observation time used as valid_from; zero means now.
	At time.Time
}

// Registry persists identity spaces and entities in PostgreSQL.
type Registry struct {
	Pool *pgxpool.Pool
	// now is injectable for tests.
	now func() time.Time
}

func NewRegistry(pool *pgxpool.Pool) *Registry {
	return &Registry{Pool: pool, now: func() time.Time { return time.Now().UTC() }}
}

func (r *Registry) clock() time.Time {
	if r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
}

// RegisterSpace registers an identity space idempotently: the same
// (organization, normalized name) always returns the same space ID. A name
// already registered with a different kind is rejected fail-closed.
func (r *Registry) RegisterSpace(ctx context.Context, organizationID, name string, kind SpaceKind) (Space, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return Space{}, fmt.Errorf("invalid organization id: %w", err)
	}
	switch kind {
	case SpaceActiveDirectory, SpaceLocalAccounts, SpaceCloudDirectory, SpaceCustom:
	default:
		return Space{}, fmt.Errorf("%w: %q", ErrInvalidSpaceKind, kind)
	}
	normalized, err := NormalizeSpaceName(name)
	if err != nil {
		return Space{}, err
	}
	space := Space{OrganizationID: orgID.String(), SpaceID: SpaceID(orgID.String(), normalized), Name: normalized, Kind: kind}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return Space{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize concurrent registration of the same space name.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 42))`, orgID.String()+"|"+normalized); err != nil {
		return Space{}, err
	}
	var existingKind SpaceKind
	err = tx.QueryRow(ctx, `
		INSERT INTO identity_spaces(organization_id, space_id, name, kind) VALUES($1, $2, $3, $4)
		ON CONFLICT (organization_id, name) DO NOTHING
		RETURNING kind, created_at`, orgID, space.SpaceID, normalized, kind).Scan(&existingKind, &space.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `
			SELECT kind, created_at FROM identity_spaces WHERE organization_id = $1 AND name = $2`,
			orgID, normalized).Scan(&existingKind, &space.CreatedAt)
	}
	if err != nil {
		return Space{}, err
	}
	if existingKind != kind {
		return Space{}, fmt.Errorf("%w: %q is %s, not %s", ErrSpaceKindConflict, normalized, existingKind, kind)
	}
	if err := tx.Commit(ctx); err != nil {
		return Space{}, err
	}
	space.Kind = existingKind
	return space, nil
}

// Register resolves the request's identifiers to one canonical entity and
// persists it idempotently. Registration of the same real identity — same
// tenant, type, space and canonical identifier — always returns the same
// entity.id. A weak identifier whose previous occurrence was transferred
// away registers as a new occurrence with a new entity.id, so a reused
// username or hostname never silently merges with its previous owner.
func (r *Registry) Register(ctx context.Context, req RegisterRequest) (Entity, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(req.OrganizationID))
	if err != nil {
		return Entity{}, fmt.Errorf("invalid organization id: %w", err)
	}
	if req.EntityType != TypeAccount && req.EntityType != TypeDevice {
		return Entity{}, fmt.Errorf("%w: %q", ErrInvalidEntityType, req.EntityType)
	}
	space, err := NormalizeSpaceName(req.Space)
	if err != nil {
		return Entity{}, err
	}
	canonical, all, err := SelectCanonical(req.Identifiers)
	if err != nil {
		return Entity{}, err
	}
	at := req.At.UTC()
	if req.At.IsZero() {
		at = r.clock()
	}
	base := canonical.CanonicalKey()

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return Entity{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize registration of the same identity key per tenant.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 7))`,
		orgID.String()+"|"+req.EntityType+"|"+space+"|"+base); err != nil {
		return Entity{}, err
	}

	rows, err := tx.Query(ctx, `
		SELECT entity_id, canonical_key, identity_strength, revision, document, valid_from, valid_to
		FROM entities
		WHERE organization_id = $1 AND entity_type = $2 AND authority = $3
		  AND (canonical_key = $4 OR strpos(canonical_key, $5) = 1)
		ORDER BY canonical_key DESC`,
		orgID, req.EntityType, space, base, base+"#")
	if err != nil {
		return Entity{}, err
	}
	var existing []Entity
	for rows.Next() {
		var e Entity
		var strength string
		if err := rows.Scan(&e.EntityID, &e.CanonicalKey, &strength, &e.Revision, &e.Document, &e.ValidFrom, &e.ValidTo); err != nil {
			rows.Close()
			return Entity{}, err
		}
		e.Strength = Strength(strength)
		existing = append(existing, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Entity{}, err
	}

	// Idempotent path: the newest occurrence is still active, return it.
	for _, e := range existing {
		if e.ValidTo == nil {
			expected := EntityID(orgID.String(), req.EntityType, space, e.CanonicalKey)
			if e.EntityID != expected {
				return Entity{}, fmt.Errorf("stored entity id %q does not match canonical derivation %q; refusing to proceed", e.EntityID, expected)
			}
			e.OrganizationID = orgID.String()
			e.EntityType = req.EntityType
			e.Authority = space
			return e, tx.Commit(ctx)
		}
	}

	key := base
	occurrence := 1
	if canonical.Strength == StrengthWeak && len(existing) > 0 {
		// All previous occurrences are closed: this is a weak identity that
		// changed hands, so a fresh occurrence gets a fresh entity.id.
		occurrence = maxOccurrence(existing) + 1
		key = fmt.Sprintf("%s#%d", base, occurrence)
	}

	doc, err := json.Marshal(map[string]any{"identifiers": all, "occurrence": occurrence})
	if err != nil {
		return Entity{}, err
	}
	ent := Entity{
		OrganizationID: orgID.String(),
		EntityID:       EntityID(orgID.String(), req.EntityType, space, key),
		EntityType:     req.EntityType,
		Authority:      space,
		CanonicalKey:   key,
		Strength:       canonical.Strength,
		Revision:       1,
		Document:       doc,
		ValidFrom:      at,
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO entities(organization_id, entity_id, entity_type, authority, canonical_key, identity_strength, revision, document, valid_from)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		orgID, ent.EntityID, ent.EntityType, ent.Authority, ent.CanonicalKey, string(ent.Strength), ent.Revision, ent.Document, ent.ValidFrom)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" && strings.Contains(pgErr.ConstraintName, "identity_space") {
			return Entity{}, fmt.Errorf("%w: %q", ErrSpaceNotRegistered, space)
		}
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return Entity{}, fmt.Errorf("%w: %s in %s", ErrEntityIDConflict, key, space)
		}
		return Entity{}, err
	}
	return ent, tx.Commit(ctx)
}

func maxOccurrence(existing []Entity) int {
	max := 1
	for _, e := range existing {
		if i := strings.LastIndex(e.CanonicalKey, "#"); i >= 0 {
			var n int
			if _, err := fmt.Sscanf(e.CanonicalKey[i+1:], "%d", &n); err == nil && n > max {
				max = n
			}
		}
	}
	return max
}

// TransferWeak closes the active occurrence of a weak identity, recording
// that the identifier changed hands at the given time. The next Register of
// the same weak identifier creates a new occurrence with a new entity.id.
// Strong identities are never transferable.
func (r *Registry) TransferWeak(ctx context.Context, organizationID, space, entityType string, id Identifier, at time.Time) (Entity, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return Entity{}, fmt.Errorf("invalid organization id: %w", err)
	}
	if entityType != TypeAccount && entityType != TypeDevice {
		return Entity{}, fmt.Errorf("%w: %q", ErrInvalidEntityType, entityType)
	}
	normalized, err := Normalize(id)
	if err != nil {
		return Entity{}, err
	}
	if normalized.Strength != StrengthWeak {
		return Entity{}, fmt.Errorf("%w: %s is strong", ErrNotWeakIdentity, id.Kind)
	}
	spaceName, err := NormalizeSpaceName(space)
	if err != nil {
		return Entity{}, err
	}
	base := normalized.CanonicalKey()
	at = at.UTC()

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return Entity{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 7))`,
		orgID.String()+"|"+entityType+"|"+spaceName+"|"+base); err != nil {
		return Entity{}, err
	}
	var e Entity
	var strength string
	err = tx.QueryRow(ctx, `
		SELECT entity_id, canonical_key, identity_strength, revision, document, valid_from
		FROM entities
		WHERE organization_id = $1 AND entity_type = $2 AND authority = $3
		  AND (canonical_key = $4 OR strpos(canonical_key, $5) = 1)
		  AND valid_to IS NULL
		ORDER BY canonical_key DESC LIMIT 1
		FOR UPDATE`,
		orgID, entityType, spaceName, base, base+"#").
		Scan(&e.EntityID, &e.CanonicalKey, &strength, &e.Revision, &e.Document, &e.ValidFrom)
	if errors.Is(err, pgx.ErrNoRows) {
		return Entity{}, fmt.Errorf("%w: %s", ErrNoActiveOccurrence, base)
	}
	if err != nil {
		return Entity{}, err
	}
	if !at.After(e.ValidFrom) {
		return Entity{}, fmt.Errorf("%w: %s not after %s", ErrInvalidValidTo, at, e.ValidFrom)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE entities SET valid_to = $5, updated_at = now()
		WHERE organization_id = $1 AND entity_type = $2 AND authority = $3 AND canonical_key = $4 AND valid_to IS NULL`,
		orgID, entityType, spaceName, e.CanonicalKey, at); err != nil {
		return Entity{}, err
	}
	e.OrganizationID = orgID.String()
	e.EntityType = entityType
	e.Authority = spaceName
	e.Strength = Strength(strength)
	e.ValidTo = &at
	return e, tx.Commit(ctx)
}
