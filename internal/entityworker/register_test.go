package entityworker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"tuba/product/internal/entity"
)

type fakeRegisterer struct {
	mu       sync.Mutex
	calls    []entity.RegisterRequest
	err      error
	errOnKey string // fail requests whose canonical identifiers contain this value
}

func (f *fakeRegisterer) Register(_ context.Context, req entity.RegisterRequest) (entity.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	if f.err != nil {
		return entity.Entity{}, f.err
	}
	for _, id := range req.Identifiers {
		if f.errOnKey != "" && id.Value == f.errOnKey {
			return entity.Entity{}, errors.New("identifier failed normalization")
		}
	}
	canonical, _, err := entity.SelectCanonical(req.Identifiers)
	if err != nil {
		return entity.Entity{}, err
	}
	return entity.Entity{
		OrganizationID: req.OrganizationID,
		EntityID:       entity.EntityID(req.OrganizationID, req.EntityType, req.Space, canonical.CanonicalKey()),
		CanonicalKey:   canonical.CanonicalKey(),
		Strength:       canonical.Strength,
	}, nil
}

func (f *fakeRegisterer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type fakeRoleAttributor struct {
	mu     sync.Mutex
	roles  [][]entity.RoleObservation
	stored []entity.RoleAttribution
}

func (f *fakeRoleAttributor) Attribute(_ context.Context, _, eventID string, _ time.Time, roles []entity.RoleObservation) ([]entity.RoleAttribution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.roles = append(f.roles, roles)
	out := make([]entity.RoleAttribution, 0, len(roles))
	for i, r := range roles {
		out = append(out, entity.RoleAttribution{
			AttributionID: eventID + "-" + r.Role, EventID: eventID, Role: r.Role,
			State: entity.StateResolved, EntityID: "ent:fake" + string(rune('a'+i)),
			Confidence: 1, RuleVersion: entity.RoleMappingVersionV1,
		})
	}
	return out, nil
}

func (f *fakeRoleAttributor) Store(_ context.Context, _ string, _ time.Time, attrs []entity.RoleAttribution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stored = append(f.stored, attrs...)
	return nil
}

func (f *fakeRoleAttributor) RuleVersion() string { return entity.RoleMappingVersionV1 }

type fakeCounter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func (c *fakeCounter) Inc(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[string]uint64{}
	}
	c.counts[name]++
}

func (c *fakeCounter) value(name string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[name]
}

func sightEvent(id, stamp string, extra map[string]any) map[string]any {
	event := map[string]any{"@timestamp": stamp, "event": map[string]any{"id": id}}
	for k, v := range extra {
		event[k] = v
	}
	return event
}

func TestRegisterOnSightFirstObservationRegistersAndAttributes(t *testing.T) {
	reg := &fakeRegisterer{}
	attr := &fakeRoleAttributor{}
	p := NewRegisteringProcessor(reg, attr, "corp.example", "host-local", nil)
	event := sightEvent("evt:1", "2026-10-12T01:00:00Z", map[string]any{
		"user": map[string]any{"id": "S-1-5-21-100-200-300-1001"},
		"host": map[string]any{"name": "WEB01"},
	})
	attrs, err := p.Attribute(context.Background(), "org1", event)
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) != 2 {
		t.Fatalf("attributions: %d", len(attrs))
	}
	if reg.callCount() != 2 {
		t.Fatalf("register calls: %d", reg.callCount())
	}
	for _, call := range reg.calls {
		if call.At != time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC) {
			t.Fatalf("registration must use event time, got %s", call.At)
		}
		if call.Space != "corp.example" && call.Space != "host-local" {
			t.Fatalf("space must come from deployment mapping, got %q", call.Space)
		}
	}
	if len(attr.stored) != 2 {
		t.Fatalf("stored: %d", len(attr.stored))
	}
}

func TestRegisterOnSightStrongIdentityCachedAfterFirstSuccess(t *testing.T) {
	reg := &fakeRegisterer{}
	attr := &fakeRoleAttributor{}
	counter := &fakeCounter{}
	p := NewRegisteringProcessor(reg, attr, "corp.example", "host-local", counter)
	const n = 50
	for i := 0; i < n; i++ {
		event := sightEvent("evt:perf", "2026-10-12T01:00:00Z", map[string]any{
			"user": map[string]any{"id": "S-1-5-21-100-200-300-1001"},
		})
		if _, err := p.Attribute(context.Background(), "org1", event); err != nil {
			t.Fatal(err)
		}
	}
	if reg.callCount() != 1 {
		t.Fatalf("%d events of one strong identity must register once, got %d", n, reg.callCount())
	}
	if got := counter.value(MetricStrongCacheHits); got != n-1 {
		t.Fatalf("cache hits: %d, want %d", got, n-1)
	}
	if got := counter.value(MetricRegistrations); got != 1 {
		t.Fatalf("registration metric: %d", got)
	}
}

func TestRegisterOnSightWeakIdentityRegistersEveryEvent(t *testing.T) {
	reg := &fakeRegisterer{}
	attr := &fakeRoleAttributor{}
	p := NewRegisteringProcessor(reg, attr, "corp.example", "host-local", nil)
	const n = 20
	for i := 0; i < n; i++ {
		event := sightEvent("evt:weak", "2026-10-12T01:00:00Z", map[string]any{
			"host": map[string]any{"name": "WEB01"},
		})
		if _, err := p.Attribute(context.Background(), "org1", event); err != nil {
			t.Fatal(err)
		}
	}
	if reg.callCount() != n {
		t.Fatalf("weak identity must register per event (occurrence semantics): %d of %d", reg.callCount(), n)
	}
}

func TestRegisterOnSightFailureDoesNotBlockEvent(t *testing.T) {
	reg := &fakeRegisterer{errOnKey: "BAD NAME!"}
	attr := &fakeRoleAttributor{}
	counter := &fakeCounter{}
	p := NewRegisteringProcessor(reg, attr, "corp.example", "host-local", counter)
	event := sightEvent("evt:bad", "2026-10-12T01:00:00Z", map[string]any{
		"user": map[string]any{"name": "BAD NAME!"},
		"host": map[string]any{"name": "WEB01"},
	})
	attrs, err := p.Attribute(context.Background(), "org1", event)
	if err != nil {
		t.Fatalf("registration failure must not fail the event: %v", err)
	}
	if len(attrs) != 2 {
		t.Fatalf("attributions: %d", len(attrs))
	}
	byRole := map[string]entity.RoleAttribution{}
	for _, ra := range attrs {
		byRole[ra.Role] = ra
	}
	failed := byRole[entity.RoleActor]
	if failed.State != entity.StateUnresolved || failed.Evidence.Reason != entity.ReasonRegistrationFailed {
		t.Fatalf("failed role: %+v", failed)
	}
	if failed.EntityID != "" {
		t.Fatalf("failed role must not resolve: %+v", failed)
	}
	if byRole[entity.RoleHost].State != entity.StateResolved {
		t.Fatalf("unaffected role must still resolve: %+v", byRole[entity.RoleHost])
	}
	if got := counter.value(MetricRegistrationFails); got != 1 {
		t.Fatalf("failure metric: %d", got)
	}
	if len(attr.stored) != 2 {
		t.Fatalf("both attributions must be stored: %d", len(attr.stored))
	}
}

func TestRegisterOnSightFailureDeterministicAttributionID(t *testing.T) {
	reg := &fakeRegisterer{err: errors.New("space not registered")}
	attr := &fakeRoleAttributor{}
	p := NewRegisteringProcessor(reg, attr, "corp.example", "host-local", nil)
	event := sightEvent("evt:det", "2026-10-12T01:00:00Z", map[string]any{
		"host": map[string]any{"name": "WEB01"},
	})
	first, err := p.Attribute(context.Background(), "org1", event)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.Attribute(context.Background(), "org1", event)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].AttributionID != second[0].AttributionID {
		t.Fatalf("failure attribution id must be deterministic: %q vs %q", first[0].AttributionID, second[0].AttributionID)
	}
}
