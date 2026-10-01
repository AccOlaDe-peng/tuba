package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// attributionOrg creates a dedicated organization and removes it plus all
// attribution/entity fixtures afterwards; tenant data is never touched.
func attributionOrg(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("attr_it_%d", time.Now().UnixNano())
	var orgID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, slug).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, table := range []string{"entity_attributions", "entities", "identity_spaces", "organizations"} {
			var q string
			if table == "organizations" {
				q = `DELETE FROM organizations WHERE id=$1`
			} else {
				q = fmt.Sprintf(`DELETE FROM %s WHERE organization_id=$1`, table)
			}
			if _, err := pool.Exec(ctx, q, orgID); err != nil {
				t.Logf("cleanup %s: %v", table, err)
			}
		}
	})
	return orgID
}

func countAttributions(t *testing.T, pool *pgxpool.Pool, orgID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM entity_attributions WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func attributionByRole(t *testing.T, attrs []RoleAttribution, role string) RoleAttribution {
	t.Helper()
	for _, ra := range attrs {
		if ra.Role == role {
			return ra
		}
	}
	t.Fatalf("role %s missing in %+v", role, attrs)
	return RoleAttribution{}
}

func TestIntegrationAttributeEventResolvedAndStored(t *testing.T) {
	pool := integrationPool(t)
	orgID := attributionOrg(t, pool)
	r := NewRegistry(pool)
	a := NewAttributor(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RegisterSpace(ctx, orgID, "host-local", SpaceLocalAccounts); err != nil {
		t.Fatal(err)
	}
	subject, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-1001"}, {KindUsername, "jsmith"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	target, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-2002"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	host, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "host-local", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindHostname, "WEB01"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}

	event := sampleUIMEvent()
	before, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	attrs, err := a.AttributeEvent(ctx, orgID, event, "corp.example", "host-local")
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) != 5 {
		t.Fatalf("expected 5 role attributions, got %d", len(attrs))
	}
	actor := attributionByRole(t, attrs, RoleActor)
	if actor.State != StateResolved || actor.EntityID != subject.EntityID {
		t.Fatalf("actor: %+v", actor)
	}
	// Actor matched by both SID and NTNAME? NTNAME corp\jsmith was never
	// registered — only the SID matched; evidence must show both inputs.
	var sawSID, sawNTName bool
	for _, ie := range actor.Evidence.Identifiers {
		switch ie.Kind {
		case KindSID:
			sawSID = true
			if ie.Canonical != "s-1-5-21-100-200-300-1001" || len(ie.Matched) != 1 {
				t.Fatalf("sid evidence incomplete: %+v", ie)
			}
		case KindNTName:
			sawNTName = true
			if ie.Canonical != `corp\jsmith` {
				t.Fatalf("ntname normalization evidence missing: %+v", ie)
			}
		}
	}
	if !sawSID || !sawNTName {
		t.Fatalf("evidence missing inputs: %+v", actor.Evidence.Identifiers)
	}
	if got := attributionByRole(t, attrs, RoleTarget); got.State != StateResolved || got.EntityID != target.EntityID {
		t.Fatalf("target: %+v", got)
	}
	if got := attributionByRole(t, attrs, RoleHost); got.State != StateResolved || got.EntityID != host.EntityID {
		t.Fatalf("host: %+v", got)
	}
	for _, role := range []string{RoleSourceDevice, RoleDestinationDevice} {
		got := attributionByRole(t, attrs, role)
		if got.State != StateUnresolved || got.Evidence.Reason != ReasonNoMatchingEntity || got.EntityID != "" {
			t.Fatalf("%s: %+v", role, got)
		}
	}

	// event.id carried verbatim into every attribution; event map untouched.
	for _, ra := range attrs {
		if ra.EventID != "evt:abc" {
			t.Fatalf("event id rewritten in attribution: %q", ra.EventID)
		}
	}
	after, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("event mutated by attribution")
	}

	// Store persists exactly one row per role; recomputation is idempotent.
	eventTime := time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)
	if err := a.Store(ctx, orgID, eventTime, attrs); err != nil {
		t.Fatal(err)
	}
	if n := countAttributions(t, pool, orgID); n != 5 {
		t.Fatalf("stored %d attributions, want 5", n)
	}
	attrs2, err := a.AttributeEvent(ctx, orgID, event, "corp.example", "host-local")
	if err != nil {
		t.Fatal(err)
	}
	for i := range attrs {
		if attrs[i].AttributionID != attrs2[i].AttributionID {
			t.Fatalf("attribution id not stable for role %s", attrs[i].Role)
		}
	}
	if err := a.Store(ctx, orgID, eventTime, attrs2); err != nil {
		t.Fatal(err)
	}
	if n := countAttributions(t, pool, orgID); n != 5 {
		t.Fatalf("re-store duplicated rows: %d", n)
	}

	// The stored rows reference the verbatim event id and the resolved entity.
	var storedEventID, storedEntity string
	if err := pool.QueryRow(ctx, `
		SELECT event_id, entity_id FROM entity_attributions
		WHERE organization_id=$1 AND role='actor' AND status='resolved'`, orgID).
		Scan(&storedEventID, &storedEntity); err != nil {
		t.Fatal(err)
	}
	if storedEventID != "evt:abc" || storedEntity != subject.EntityID {
		t.Fatalf("stored actor row: event=%q entity=%q", storedEventID, storedEntity)
	}
}

func TestIntegrationWeakTransferChangesAttribution(t *testing.T) {
	pool := integrationPool(t)
	orgID := attributionOrg(t, pool)
	r := NewRegistry(pool)
	a := NewAttributor(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "host-local", SpaceLocalAccounts); err != nil {
		t.Fatal(err)
	}
	first, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "host-local", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindHostname, "WEB01"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	role := RoleObservation{Role: RoleHost, EntityType: TypeDevice, Space: "host-local",
		Identifiers: []Identifier{{KindHostname, "web01"}}}
	t1, t2, t3, t4 := t0.Add(time.Hour), t0.Add(2*time.Hour), t0.Add(3*time.Hour), t0.Add(4*time.Hour)

	// Before the transfer the hostname attributes to the first occurrence.
	attrs, err := a.Attribute(ctx, orgID, "evt:before", t1, []RoleObservation{role})
	if err != nil {
		t.Fatal(err)
	}
	if attrs[0].State != StateResolved || attrs[0].EntityID != first.EntityID {
		t.Fatalf("before transfer: %+v", attrs[0])
	}

	// The weak identity changes hands; a new occurrence gets a new entity id.
	if _, err := r.TransferWeak(ctx, orgID, "host-local", TypeDevice, Identifier{KindHostname, "web01"}, t2); err != nil {
		t.Fatal(err)
	}
	second, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "host-local", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindHostname, "web01"}}, At: t3})
	if err != nil {
		t.Fatal(err)
	}
	if second.EntityID == first.EntityID {
		t.Fatal("weak transfer did not produce a new occurrence")
	}

	// After the transfer the same role attributes to the new occurrence...
	attrs, err = a.Attribute(ctx, orgID, "evt:after", t4, []RoleObservation{role})
	if err != nil {
		t.Fatal(err)
	}
	if attrs[0].State != StateResolved || attrs[0].EntityID != second.EntityID {
		t.Fatalf("after transfer: %+v", attrs[0])
	}
	// ...while an old event at t1 still attributes to the previous owner:
	// history is never rewritten.
	attrs, err = a.Attribute(ctx, orgID, "evt:before", t1, []RoleObservation{role})
	if err != nil {
		t.Fatal(err)
	}
	if attrs[0].State != StateResolved || attrs[0].EntityID != first.EntityID {
		t.Fatalf("old event re-attributed: %+v", attrs[0])
	}
}

func TestIntegrationAmbiguousConflictStoredWithoutEntity(t *testing.T) {
	pool := integrationPool(t)
	orgID := attributionOrg(t, pool)
	r := NewRegistry(pool)
	a := NewAttributor(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	bySID, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-1001"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	byName, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindUsername, "jsmith"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	role := RoleObservation{Role: RoleActor, EntityType: TypeAccount, Space: "corp.example",
		Identifiers: []Identifier{{KindSID, "s-1-5-21-100-200-300-1001"}, {KindUsername, "jsmith"}}}
	attrs, err := a.Attribute(ctx, orgID, "evt:conflict", t0.Add(time.Hour), []RoleObservation{role})
	if err != nil {
		t.Fatal(err)
	}
	ra := attrs[0]
	if ra.State != StateAmbiguous || ra.EntityID != "" || ra.Evidence.Reason != ReasonStrongWeakConflict {
		t.Fatalf("not ambiguous: %+v", ra)
	}
	cands := candidatesOf(ra.Evidence)
	if len(cands) != 2 {
		t.Fatalf("candidates: %+v", cands)
	}
	seen := map[string]bool{cands[0].EntityID: true, cands[1].EntityID: true}
	if !seen[bySID.EntityID] || !seen[byName.EntityID] {
		t.Fatalf("candidate list missing entities: %+v", cands)
	}
	// Persisted as ambiguous with NULL entity (CHECK: resolved ⇔ entity set).
	if err := a.Store(ctx, orgID, t0.Add(time.Hour), attrs); err != nil {
		t.Fatal(err)
	}
	var status string
	var entityID *string
	var evidence json.RawMessage
	if err := pool.QueryRow(ctx, `
		SELECT status, entity_id, evidence FROM entity_attributions
		WHERE organization_id=$1 AND event_id='evt:conflict'`, orgID).
		Scan(&status, &entityID, &evidence); err != nil {
		t.Fatal(err)
	}
	if status != StateAmbiguous || entityID != nil {
		t.Fatalf("stored row: status=%s entity=%v", status, entityID)
	}
	if !json.Valid(evidence) || len(evidence) < 10 {
		t.Fatalf("evidence not persisted: %s", evidence)
	}
}

func TestIntegrationAttributionFailClosed(t *testing.T) {
	pool := integrationPool(t)
	orgID := attributionOrg(t, pool)
	r := NewRegistry(pool)
	a := NewAttributor(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}

	// Unregistered identity space: unresolved with a queryable reason, no write.
	attrs, err := a.Attribute(ctx, orgID, "evt:x1", t0.Add(time.Hour), []RoleObservation{
		{Role: RoleActor, EntityType: TypeAccount, Space: "never-registered",
			Identifiers: []Identifier{{KindSID, "S-1-5-21-1-2-3-4"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attrs[0].State != StateUnresolved || attrs[0].Evidence.Reason != ReasonSpaceUnregistered {
		t.Fatalf("unregistered space: %+v", attrs[0])
	}

	// Malformed identifier: unresolved with evidence, no write.
	attrs, err = a.Attribute(ctx, orgID, "evt:x2", t0.Add(time.Hour), []RoleObservation{
		{Role: RoleActor, EntityType: TypeAccount, Space: "corp.example",
			Identifiers: []Identifier{{KindSID, "not-a-sid"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attrs[0].State != StateUnresolved || attrs[0].Evidence.Reason != ReasonIdentifierInvalid {
		t.Fatalf("invalid identifier: %+v", attrs[0])
	}
	if attrs[0].Evidence.Identifiers[0].LookupError == "" {
		t.Fatal("lookup error missing from evidence")
	}

	// Event-level hard errors: missing event id, no roles, bad tenant, bad
	// entity type — all fail-closed and write nothing.
	if _, err := a.Attribute(ctx, orgID, "", t0, []RoleObservation{{Role: RoleActor, EntityType: TypeAccount, Space: "corp.example"}}); err == nil {
		t.Fatal("empty event id accepted")
	}
	if _, err := a.Attribute(ctx, orgID, "evt:x3", t0, nil); err == nil {
		t.Fatal("empty roles accepted")
	}
	if _, err := a.Attribute(ctx, "not-a-uuid", "evt:x3", t0, []RoleObservation{{Role: RoleActor, EntityType: TypeAccount, Space: "corp.example"}}); err == nil {
		t.Fatal("invalid tenant accepted")
	}
	if _, err := a.Attribute(ctx, orgID, "evt:x4", t0, []RoleObservation{{Role: RoleActor, EntityType: "user", Space: "corp.example"}}); err == nil {
		t.Fatal("invalid entity type accepted")
	}
	if n := countAttributions(t, pool, orgID); n != 0 {
		t.Fatalf("fail-closed paths wrote %d rows", n)
	}
}
