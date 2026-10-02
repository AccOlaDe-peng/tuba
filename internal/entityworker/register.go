package entityworker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"tuba/product/internal/entity"
)

// Registerer is the register-on-sight dependency; *entity.Registry is the
// production implementation.
type Registerer interface {
	Register(ctx context.Context, req entity.RegisterRequest) (entity.Entity, error)
}

// RoleAttributor is the attribution dependency; *entity.Attributor is the
// production implementation.
type RoleAttributor interface {
	Attribute(ctx context.Context, organizationID, eventID string, at time.Time, roles []entity.RoleObservation) ([]entity.RoleAttribution, error)
	Store(ctx context.Context, organizationID string, at time.Time, attrs []entity.RoleAttribution) error
	RuleVersion() string
}

// Counter is the minimal metrics dependency (satisfied by
// *telemetry.Registry); nil-safe.
type Counter interface {
	Inc(name string)
}

// Registration metrics, queryable on the worker /metrics endpoint.
const (
	MetricRegistrations     = "entity_registrations_total"
	MetricRegistrationFails = "entity_registration_failures_total"
	MetricStrongCacheHits   = "entity_registration_strong_cache_hits_total"
)

// RegisteringProcessor is the register-on-sight Processor: every role
// observed in a UIM event is registered in its identity space before
// attribution. Strong identifiers register idempotently and are cached
// in-process after the first success (a strong identity never changes
// hands, so the cache cannot break transfer semantics); weak identifiers
// register on every event because occurrence/transfer adjudication lives
// in the database. Identity spaces always come from the deployment
// mapping (AccountSpace/DeviceSpace), never from event content.
//
// A failed registration never blocks the event: the role is attributed as
// unresolved with reason registration_failed, counted in
// entity_registration_failures_total, and processing continues.
type RegisteringProcessor struct {
	Registry   Registerer
	Attributor RoleAttributor
	// AccountSpace, DeviceSpace are the trusted deployment-mapped identity
	// spaces for account and device roles.
	AccountSpace, DeviceSpace string
	Metrics                   Counter

	mu sync.Mutex
	// strongSeen caches canonical keys of successfully registered strong
	// identifiers (org|type|space|kind:value) so repeat events skip the
	// idempotent registration round-trip. Weak identifiers are never
	// cached: occurrence semantics require a database decision per event.
	strongSeen map[string]string
}

func NewRegisteringProcessor(registry Registerer, attributor RoleAttributor, accountSpace, deviceSpace string, metrics Counter) *RegisteringProcessor {
	return &RegisteringProcessor{
		Registry: registry, Attributor: attributor,
		AccountSpace: accountSpace, DeviceSpace: deviceSpace,
		Metrics: metrics, strongSeen: map[string]string{},
	}
}

func (p *RegisteringProcessor) inc(name string) {
	if p.Metrics != nil {
		p.Metrics.Inc(name)
	}
}

// Attribute implements Processor: register every observed role, attribute
// it, then store the projection rows idempotently.
func (p *RegisteringProcessor) Attribute(ctx context.Context, organizationID string, event map[string]any) ([]entity.RoleAttribution, error) {
	eventID, at, roles, err := entity.RolesFromUIM(event, p.AccountSpace, p.DeviceSpace)
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return nil, fmt.Errorf("%w: event %s carries no attributable role", entity.ErrNoRoles, eventID)
	}
	attrs := make([]entity.RoleAttribution, 0, len(roles))
	for _, role := range roles {
		if regErr := p.registerRole(ctx, organizationID, at, role); regErr != nil {
			p.inc(MetricRegistrationFails)
			log.Printf("entity-worker: event %s role %s registration failed (role unresolved): %v", eventID, role.Role, regErr)
			attrs = append(attrs, entity.RegistrationFailureAttribution(eventID, at, role, regErr, p.Attributor.RuleVersion()))
			continue
		}
		ra, err := p.Attributor.Attribute(ctx, organizationID, eventID, at, []entity.RoleObservation{role})
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, ra...)
	}
	if err := p.Attributor.Store(ctx, organizationID, at, attrs); err != nil {
		return nil, fmt.Errorf("store attributions: %w", err)
	}
	return attrs, nil
}

// registerRole registers the role's observed identifiers at the event time.
// Strong identifiers hit the in-process cache after their first successful
// registration; weak identifiers always go through Registry.Register so
// occurrence/transfer adjudication stays authoritative.
func (p *RegisteringProcessor) registerRole(ctx context.Context, organizationID string, at time.Time, role entity.RoleObservation) error {
	canonical, _, err := entity.SelectCanonical(role.Identifiers)
	if err != nil {
		return err
	}
	cacheKey := ""
	if canonical.Strength == entity.StrengthStrong {
		cacheKey = organizationID + "|" + role.EntityType + "|" + role.Space + "|" + canonical.CanonicalKey()
		p.mu.Lock()
		_, seen := p.strongSeen[cacheKey]
		p.mu.Unlock()
		if seen {
			p.inc(MetricStrongCacheHits)
			return nil
		}
	}
	ent, err := p.Registry.Register(ctx, entity.RegisterRequest{
		OrganizationID: organizationID,
		Space:          role.Space,
		EntityType:     role.EntityType,
		Identifiers:    role.Identifiers,
		At:             at,
	})
	if err != nil {
		return err
	}
	p.inc(MetricRegistrations)
	if cacheKey != "" {
		p.mu.Lock()
		p.strongSeen[cacheKey] = ent.EntityID
		p.mu.Unlock()
	}
	return nil
}
