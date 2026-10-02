package entity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Attribution states per docs/DESIGN-BASELINE.md §5: every event role is
// attributed independently and lands in exactly one of these states.
const (
	StateResolved   = "resolved"
	StateUnresolved = "unresolved"
	StateAmbiguous  = "ambiguous"
)

// Unresolved/ambiguous reason codes; stable for audit and triage queries.
const (
	ReasonNoIdentifiers             = "no_identifiers"
	ReasonIdentifierInvalid         = "identifier_invalid"
	ReasonSpaceUnregistered         = "identity_space_unregistered"
	ReasonNoMatchingEntity          = "no_matching_entity"
	ReasonNoActiveOccurrence        = "no_active_occurrence_at_event_time"
	ReasonStrongWeakConflict        = "strong_weak_conflict"
	ReasonMultipleActiveOccurrences = "multiple_active_candidates"
	// ReasonRegistrationFailed marks a role whose register-on-sight call
	// failed (invalid identifier, unregistered space, conflict). The role is
	// forced unresolved — a failed registration never silently resolves.
	ReasonRegistrationFailed = "registration_failed"
)

// RoleMappingVersionV1 is the role→entity mapping version folded into
// attribution.id per contracts/ids.md. Bump only with an explicit identity
// version change.
const RoleMappingVersionV1 = "1.0.0"

var (
	ErrEmptyEventID     = errors.New("event id is required")
	ErrNoRoles          = errors.New("at least one role observation is required")
	ErrEmptyRole        = errors.New("role name is required")
	ErrInvalidEventTime = errors.New("event time is required")
)

// RoleObservation is one event role's attribution input: the role name, the
// entity type the role maps to, the identity space the role resolves in, and
// the identifiers the event reported for that role.
type RoleObservation struct {
	Role        string
	EntityType  string
	Space       string
	Identifiers []Identifier
}

// Candidate is a registered entity occurrence that matched one of the role's
// identifiers.
type Candidate struct {
	EntityID     string     `json:"entity_id"`
	CanonicalKey string     `json:"canonical_key"`
	Strength     Strength   `json:"strength"`
	Revision     int64      `json:"revision"`
	ValidFrom    time.Time  `json:"valid_from"`
	ValidTo      *time.Time `json:"valid_to,omitempty"`
}

// IdentifierEvidence records what one input identifier normalized to and
// which entity occurrences it matched. LookupError carries the failure when
// the identifier could not be normalized or its space was not registered.
type IdentifierEvidence struct {
	Kind        Kind        `json:"kind"`
	Value       string      `json:"value"`
	Canonical   string      `json:"canonical,omitempty"`
	Strength    Strength    `json:"strength,omitempty"`
	Matched     []Candidate `json:"matched,omitempty"`
	LookupError string      `json:"lookup_error,omitempty"`
}

// Evidence is the auditable record of an attribution decision: every input
// identifier with its normalization input/output, the candidates it hit, the
// adjudication path taken, and the reason code for non-resolved states. Event
// optionally carries a minimal summary of the source event's detection
// semantics (outcome/quality/host/source ip); the entity-worker embeds it on
// outbound attributed-event messages so the F08 analysis pipeline can
// reconstruct detection input without re-reading the UIM stream (the
// attributed-event v1 schema is closed at the top level; evidence is the
// contract-legal carrier). It is nil on stored attribution rows.
type Evidence struct {
	Identifiers  []IdentifierEvidence `json:"identifiers"`
	Adjudication []string             `json:"adjudication"`
	Reason       string               `json:"reason,omitempty"`
	Event        map[string]any       `json:"event,omitempty"`
}

// RoleAttribution is the projection of one event role onto the entity
// registry. It never modifies the event: EventID is the input event.id
// carried verbatim.
type RoleAttribution struct {
	AttributionID string     `json:"attribution_id"`
	EventID       string     `json:"event_id"`
	Role          string     `json:"role"`
	State         string     `json:"state"`
	EntityID      string     `json:"entity_id,omitempty"`
	Confidence    float64    `json:"confidence"`
	RuleVersion   string     `json:"rule_version"`
	ValidFrom     time.Time  `json:"valid_from,omitempty"`
	ValidTo       *time.Time `json:"valid_to,omitempty"`
	Evidence      Evidence   `json:"evidence"`
}

// resolutionSnapshot is stored in entity_attributions.resolution_snapshot.
type resolutionSnapshot struct {
	State        string `json:"state"`
	EntityID     string `json:"entity_id,omitempty"`
	CanonicalKey string `json:"canonical_key,omitempty"`
	RuleVersion  string `json:"rule_version"`
}

// AttributionID derives the stable attribution.id per contracts/ids.md:
// canonical input is the event ID, the entity snapshot and the role mapping
// version. Recomputing the same event role at the same event time produces
// the same ID; a different role, state, entity or candidate set produces a
// different one.
func AttributionID(eventID, snapshot, roleMappingVersion string) string {
	return "att:" + stableHash("attribution-v1", eventID, snapshot, roleMappingVersion)
}

func attributionSnapshot(role RoleObservation, state, entityID, canonicalKey string, candidates []Candidate) string {
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, c.EntityID)
	}
	sort.Strings(ids)
	return strings.Join([]string{role.Role, state, entityID, canonicalKey, strings.Join(ids, ",")}, "|")
}

// adjudicate is the pure decision core: given the per-identifier lookup
// evidence, decide resolved/unresolved/ambiguous. Only occurrences whose
// [valid_from, valid_to) interval contains the event time are eligible; an
// occurrence whose interval does not contain the event time belongs to a
// previous or later owner of a weak identity.
func adjudicate(eventID string, at time.Time, role RoleObservation, evidence Evidence, ruleVersion string) RoleAttribution {
	out := RoleAttribution{
		EventID: eventID, Role: role.Role, RuleVersion: ruleVersion,
		State: StateUnresolved, Evidence: evidence,
	}
	finish := func() RoleAttribution {
		out.AttributionID = AttributionID(eventID,
			attributionSnapshot(role, out.State, out.EntityID, canonicalKeyOf(out.Evidence), candidatesOf(out.Evidence)),
			ruleVersion)
		return out
	}
	if len(role.Identifiers) == 0 {
		out.Evidence.Reason = ReasonNoIdentifiers
		out.Evidence.Adjudication = append(out.Evidence.Adjudication, "no_identifiers_reported")
		return finish()
	}
	normalizedOK := false
	for _, ie := range evidence.Identifiers {
		if ie.LookupError == "" {
			normalizedOK = true
		}
	}
	if !normalizedOK {
		out.Evidence.Reason = ReasonIdentifierInvalid
		out.Evidence.Adjudication = append(out.Evidence.Adjudication, "all_identifiers_failed_normalization")
		return finish()
	}

	// Collect distinct entities matched at the event time, and note whether
	// any occurrence exists at all (for the unresolved reason).
	matchedAt := map[string]Candidate{}
	matchedAnyTime := 0
	strongHit, weakHit := false, false
	for _, ie := range evidence.Identifiers {
		for _, c := range ie.Matched {
			matchedAnyTime++
			if at.Before(c.ValidFrom) || (c.ValidTo != nil && !at.Before(*c.ValidTo)) {
				out.Evidence.Adjudication = append(out.Evidence.Adjudication,
					fmt.Sprintf("occurrence_outside_event_time:%s", c.CanonicalKey))
				continue
			}
			if c.Strength == StrengthStrong {
				strongHit = true
			} else {
				weakHit = true
			}
			matchedAt[c.EntityID] = c
		}
	}
	candidates := make([]Candidate, 0, len(matchedAt))
	for _, c := range matchedAt {
		candidates = append(candidates, c)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].EntityID < candidates[j].EntityID })

	switch len(candidates) {
	case 0:
		if matchedAnyTime > 0 {
			out.Evidence.Reason = ReasonNoActiveOccurrence
			out.Evidence.Adjudication = append(out.Evidence.Adjudication, "occurrences_exist_but_none_covers_event_time")
		} else {
			out.Evidence.Reason = ReasonNoMatchingEntity
			out.Evidence.Adjudication = append(out.Evidence.Adjudication, "no_registered_entity_matches")
		}
		return finish()
	case 1:
		c := candidates[0]
		out.State = StateResolved
		out.EntityID = c.EntityID
		out.ValidFrom = c.ValidFrom
		out.ValidTo = c.ValidTo
		if c.Strength == StrengthStrong {
			out.Confidence = 1.0
			out.Evidence.Adjudication = append(out.Evidence.Adjudication, "resolved_by_strong_identifier")
			if len(evidence.Identifiers) > 1 {
				out.Evidence.Adjudication = append(out.Evidence.Adjudication, "strong_identifier_priority_over_weak")
			}
		} else {
			out.Confidence = 0.8
			out.Evidence.Adjudication = append(out.Evidence.Adjudication, "resolved_by_weak_identifier_occurrence_at_event_time")
		}
		if agreedBy(evidence.Identifiers, c.EntityID) > 1 {
			out.Evidence.Adjudication = append(out.Evidence.Adjudication, "multiple_identifiers_agree")
		}
		return finish()
	default:
		out.State = StateAmbiguous
		// Never let the strongest or latest candidate silently win.
		if strongHit && weakHit {
			out.Evidence.Reason = ReasonStrongWeakConflict
			out.Evidence.Adjudication = append(out.Evidence.Adjudication,
				"strong_and_weak_identifiers_resolve_to_different_entities")
		} else {
			out.Evidence.Reason = ReasonMultipleActiveOccurrences
			out.Evidence.Adjudication = append(out.Evidence.Adjudication,
				"multiple_active_candidates_at_event_time")
		}
		out.Evidence.Adjudication = append(out.Evidence.Adjudication, "candidates_preserved_not_overwritten")
		return finish()
	}
}

func agreedBy(evs []IdentifierEvidence, entityID string) int {
	n := 0
	for _, ie := range evs {
		for _, c := range ie.Matched {
			if c.EntityID == entityID {
				n++
				break
			}
		}
	}
	return n
}

func canonicalKeyOf(ev Evidence) string {
	for _, ie := range ev.Identifiers {
		for _, c := range ie.Matched {
			return c.CanonicalKey
		}
	}
	return ""
}

func candidatesOf(ev Evidence) []Candidate {
	seen := map[string]bool{}
	var out []Candidate
	for _, ie := range ev.Identifiers {
		for _, c := range ie.Matched {
			if !seen[c.EntityID] {
				seen[c.EntityID] = true
				out = append(out, c)
			}
		}
	}
	return out
}

// Attributor resolves event roles against the entity registry in PostgreSQL.
// It is read-only towards entities and writes only entity_attributions
// projection rows via Store; events themselves are never touched.
type Attributor struct {
	Pool *pgxpool.Pool
	// RoleMappingVersion defaults to RoleMappingVersionV1.
	RoleMappingVersion string
}

func NewAttributor(pool *pgxpool.Pool) *Attributor {
	return &Attributor{Pool: pool, RoleMappingVersion: RoleMappingVersionV1}
}

func (a *Attributor) ruleVersion() string {
	if a.RoleMappingVersion == "" {
		return RoleMappingVersionV1
	}
	return a.RoleMappingVersion
}

// RuleVersion exposes the effective role mapping version so callers that
// build attributions alongside the Attributor (e.g. register-on-sight
// failure handling) fold the same version into attribution.id.
func (a *Attributor) RuleVersion() string { return a.ruleVersion() }

// RegistrationFailureAttribution builds the deterministic unresolved
// attribution for a role whose register-on-sight registration failed. The
// registration error is recorded as per-identifier evidence; the state is
// forced unresolved with ReasonRegistrationFailed regardless of what a
// registry lookup would have found, so a bad or conflicting identifier never
// blocks the event and never silently resolves.
func RegistrationFailureAttribution(eventID string, at time.Time, role RoleObservation, registerErr error, ruleVersion string) RoleAttribution {
	evidence := Evidence{Identifiers: make([]IdentifierEvidence, 0, len(role.Identifiers))}
	errText := ReasonRegistrationFailed
	if registerErr != nil {
		errText = registerErr.Error()
	}
	for _, id := range role.Identifiers {
		evidence.Identifiers = append(evidence.Identifiers, IdentifierEvidence{
			Kind: id.Kind, Value: id.Value, LookupError: errText,
		})
	}
	ra := adjudicate(eventID, at, role, evidence, ruleVersion)
	// adjudicate would report identifier_invalid; keep the precise reason.
	ra.State = StateUnresolved
	ra.EntityID = ""
	ra.Confidence = 0
	ra.ValidFrom = time.Time{}
	ra.ValidTo = nil
	ra.Evidence.Reason = ReasonRegistrationFailed
	ra.Evidence.Adjudication = append(ra.Evidence.Adjudication, "registration_failed_role_forced_unresolved")
	ra.AttributionID = AttributionID(eventID,
		attributionSnapshot(role, ra.State, "", "", nil), ruleVersion)
	return ra
}

// Attribute resolves every role observation of one event at the event time.
// Event-level input errors (missing event.id, no roles, malformed tenant)
// are hard errors; a role that cannot resolve lands in unresolved/ambiguous
// with evidence instead of failing the whole event.
func (a *Attributor) Attribute(ctx context.Context, organizationID, eventID string, at time.Time, roles []RoleObservation) ([]RoleAttribution, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return nil, fmt.Errorf("invalid organization id: %w", err)
	}
	if strings.TrimSpace(eventID) == "" {
		return nil, ErrEmptyEventID
	}
	if at.IsZero() {
		return nil, ErrInvalidEventTime
	}
	if len(roles) == 0 {
		return nil, ErrNoRoles
	}
	at = at.UTC()
	out := make([]RoleAttribution, 0, len(roles))
	for _, role := range roles {
		ra, err := a.attributeRole(ctx, orgID, eventID, at, role)
		if err != nil {
			return nil, err
		}
		out = append(out, ra)
	}
	return out, nil
}

func (a *Attributor) attributeRole(ctx context.Context, orgID uuid.UUID, eventID string, at time.Time, role RoleObservation) (RoleAttribution, error) {
	if strings.TrimSpace(role.Role) == "" {
		return RoleAttribution{}, fmt.Errorf("%w: event %s", ErrEmptyRole, eventID)
	}
	if role.EntityType != TypeAccount && role.EntityType != TypeDevice {
		return RoleAttribution{}, fmt.Errorf("%w: %q", ErrInvalidEntityType, role.EntityType)
	}
	space, err := NormalizeSpaceName(role.Space)
	if err != nil {
		return RoleAttribution{}, err
	}
	evidence := Evidence{Identifiers: make([]IdentifierEvidence, 0, len(role.Identifiers))}

	registered, err := a.spaceRegistered(ctx, orgID, space)
	if err != nil {
		return RoleAttribution{}, err
	}
	if !registered {
		for _, id := range role.Identifiers {
			evidence.Identifiers = append(evidence.Identifiers, IdentifierEvidence{
				Kind: id.Kind, Value: id.Value, LookupError: ReasonSpaceUnregistered,
			})
		}
		evidence.Reason = ReasonSpaceUnregistered
		evidence.Adjudication = append(evidence.Adjudication, "lookup_skipped:identity_space_unregistered")
		ra := adjudicate(eventID, at, role, evidence, a.ruleVersion())
		// adjudicate would report identifier_invalid; keep the precise reason.
		ra.State = StateUnresolved
		ra.Evidence.Reason = ReasonSpaceUnregistered
		ra.AttributionID = AttributionID(eventID,
			attributionSnapshot(role, ra.State, "", "", nil), a.ruleVersion())
		return ra, nil
	}

	for _, id := range role.Identifiers {
		ie := IdentifierEvidence{Kind: id.Kind, Value: id.Value}
		n, err := Normalize(id)
		if err != nil {
			ie.LookupError = err.Error()
			evidence.Identifiers = append(evidence.Identifiers, ie)
			continue
		}
		ie.Canonical = n.Canonical
		ie.Strength = n.Strength
		candidates, err := a.lookupCandidates(ctx, orgID, role.EntityType, space, n.CanonicalKey())
		if err != nil {
			return RoleAttribution{}, err
		}
		ie.Matched = candidates
		evidence.Identifiers = append(evidence.Identifiers, ie)
	}
	return adjudicate(eventID, at, role, evidence, a.ruleVersion()), nil
}

func (a *Attributor) spaceRegistered(ctx context.Context, orgID uuid.UUID, space string) (bool, error) {
	var exists bool
	if err := a.Pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM identity_spaces WHERE organization_id = $1 AND name = $2)`,
		orgID, space).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// lookupCandidates returns every occurrence registered under the canonical
// key, including transferred-out occurrences (canonical_key base#N), ordered
// deterministically. Time filtering happens in adjudicate.
func (a *Attributor) lookupCandidates(ctx context.Context, orgID uuid.UUID, entityType, space, canonicalKey string) ([]Candidate, error) {
	rows, err := a.Pool.Query(ctx, `
		SELECT entity_id, canonical_key, identity_strength, revision, valid_from, valid_to
		FROM entities
		WHERE organization_id = $1 AND entity_type = $2 AND authority = $3
		  AND (canonical_key = $4 OR strpos(canonical_key, $5) = 1)
		ORDER BY canonical_key`,
		orgID, entityType, space, canonicalKey, canonicalKey+"#")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Candidate
	for rows.Next() {
		var c Candidate
		var strength string
		if err := rows.Scan(&c.EntityID, &c.CanonicalKey, &strength, &c.Revision, &c.ValidFrom, &c.ValidTo); err != nil {
			return nil, err
		}
		c.Strength = Strength(strength)
		out = append(out, c)
	}
	return out, rows.Err()
}

// Store persists attribution rows into entity_attributions. Inserts are
// idempotent per (organization, attribution_id): recomputing and storing the
// same event again never duplicates or rewrites a row. Events themselves are
// never written — attribution is a side projection of the event.
func (a *Attributor) Store(ctx context.Context, organizationID string, at time.Time, attrs []RoleAttribution) error {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return fmt.Errorf("invalid organization id: %w", err)
	}
	tx, err := a.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, ra := range attrs {
		if ra.State != StateResolved && ra.State != StateUnresolved && ra.State != StateAmbiguous {
			return fmt.Errorf("invalid attribution state %q", ra.State)
		}
		if ra.State == StateResolved && ra.EntityID == "" {
			return fmt.Errorf("resolved attribution %s has no entity id", ra.AttributionID)
		}
		snapshot, err := json.Marshal(resolutionSnapshot{
			State: ra.State, EntityID: ra.EntityID,
			CanonicalKey: canonicalKeyOf(ra.Evidence), RuleVersion: ra.RuleVersion,
		})
		if err != nil {
			return err
		}
		evidence, err := json.Marshal(ra.Evidence)
		if err != nil {
			return err
		}
		var entityID any
		if ra.EntityID != "" {
			entityID = ra.EntityID
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO entity_attributions(
				organization_id, attribution_id, event_id, role, status,
				entity_id, resolution_snapshot, evidence, event_time)
			VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (organization_id, attribution_id) DO NOTHING`,
			orgID, ra.AttributionID, ra.EventID, ra.Role, ra.State,
			entityID, string(snapshot), evidence, at.UTC()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
