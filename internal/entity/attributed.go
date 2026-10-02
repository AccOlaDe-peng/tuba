package entity

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// AttributedSchemaVersion is the contracts/events/attributed-event/1 schema
// version carried in every contribution message.
const AttributedSchemaVersion = "1.0.0"

var (
	ErrEmptyOrganization    = errors.New("organization id is required")
	ErrResolvedWithoutEnt   = errors.New("resolved attribution has no ent: entity id")
	ErrUnresolvedWithEntity = errors.New("unresolved/ambiguous attribution must not carry an entity id")
)

// Contribution is one attributed-event message per
// contracts/events/attributed-event/1: the projection of a single event role
// onto the entity registry, addressed to exactly one partition key. The
// Kafka message key equals PartitionKey.
type Contribution struct {
	SchemaVersion  string     `json:"schema_version"`
	OrganizationID string     `json:"organization_id"`
	EventID        string     `json:"event_id"`
	EventTime      time.Time  `json:"event_time"`
	Domain         string     `json:"domain,omitempty"`
	AttributionID  string     `json:"attribution_id"`
	Role           string     `json:"role"`
	State          string     `json:"state"`
	EntityID       string     `json:"entity_id,omitempty"`
	Confidence     float64    `json:"confidence"`
	RuleVersion    string     `json:"rule_version"`
	ValidFrom      time.Time  `json:"valid_from,omitempty"`
	ValidTo        *time.Time `json:"valid_to,omitempty"`
	Reason         string     `json:"reason,omitempty"`
	PartitionKey   string     `json:"partition_key"`
	Evidence       Evidence   `json:"evidence"`
}

// ContributionKey derives the Kafka partition key for one role attribution.
// Resolved attributions key on the entity itself (<org>:<ent:...>) so every
// contribution naming one entity lands in one partition. Unresolved and
// ambiguous attributions are event-level unattributed detection input: they
// key on <org>:unresolved:<state>:<reason> so they stay consumable downstream
// without ever colliding with an entity key (entity ids carry the ent:
// prefix, the unresolved namespace cannot).
func ContributionKey(organizationID string, ra RoleAttribution) (string, error) {
	if strings.TrimSpace(organizationID) == "" {
		return "", ErrEmptyOrganization
	}
	switch ra.State {
	case StateResolved:
		if !strings.HasPrefix(ra.EntityID, "ent:") {
			return "", fmt.Errorf("%w: attribution %s entity %q", ErrResolvedWithoutEnt, ra.AttributionID, ra.EntityID)
		}
		return organizationID + ":" + ra.EntityID, nil
	case StateUnresolved, StateAmbiguous:
		if ra.EntityID != "" {
			return "", fmt.Errorf("%w: attribution %s", ErrUnresolvedWithEntity, ra.AttributionID)
		}
		reason := ra.Evidence.Reason
		if reason == "" {
			reason = "unknown_reason"
		}
		return organizationID + ":unresolved:" + ra.State + ":" + reason, nil
	default:
		return "", fmt.Errorf("invalid attribution state %q", ra.State)
	}
}

// Contributions fans one event's role attributions out into one contribution
// message per role, each with its independent partition key. Nothing is
// filtered: resolved, unresolved and ambiguous attributions all produce a
// message. eventID/eventTime are carried verbatim from the input event.
func Contributions(organizationID, domain, eventID string, eventTime time.Time, attrs []RoleAttribution) ([]Contribution, error) {
	if strings.TrimSpace(organizationID) == "" {
		return nil, ErrEmptyOrganization
	}
	if strings.TrimSpace(eventID) == "" {
		return nil, ErrEmptyEventID
	}
	if eventTime.IsZero() {
		return nil, ErrInvalidEventTime
	}
	out := make([]Contribution, 0, len(attrs))
	for _, ra := range attrs {
		key, err := ContributionKey(organizationID, ra)
		if err != nil {
			return nil, err
		}
		out = append(out, Contribution{
			SchemaVersion:  AttributedSchemaVersion,
			OrganizationID: organizationID,
			EventID:        eventID,
			EventTime:      eventTime.UTC(),
			Domain:         domain,
			AttributionID:  ra.AttributionID,
			Role:           ra.Role,
			State:          ra.State,
			EntityID:       ra.EntityID,
			Confidence:     ra.Confidence,
			RuleVersion:    ra.RuleVersion,
			ValidFrom:      ra.ValidFrom,
			ValidTo:        ra.ValidTo,
			Reason:         ra.Evidence.Reason,
			PartitionKey:   key,
			Evidence:       ra.Evidence,
		})
	}
	return out, nil
}
