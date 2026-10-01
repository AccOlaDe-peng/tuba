package entity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// relationOrg creates a dedicated organization and removes it plus all
// relation/rule/entity fixtures afterwards; tenant data is never touched.
func relationOrg(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("rel_it_%d", time.Now().UnixNano())
	var orgID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, slug).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, table := range []string{"entity_relations", "entity_attributions", "rule_snapshots", "entities", "identity_spaces", "organizations"} {
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

// relationFixture registers one identity space plus two accounts and a
// device, and the v1 relation mapping rule snapshot.
func relationFixture(t *testing.T, pool *pgxpool.Pool, orgID string) (device, ownerA, ownerB Entity) {
	t.Helper()
	ctx := context.Background()
	r := NewRegistry(pool)
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	ownerA, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-1001"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err = r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-2002"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	device, err = r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindDeviceUUID, "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuleSnapshots(pool).Register(ctx, orgID, RuleKindRelationMapping, RelationMappingVersionV1,
		json.RawMessage(`{"relation_types":{"member_of":"multi","belongs_to":"multi","assigned_to":"single","managed_by":"single"}}`)); err != nil {
		t.Fatal(err)
	}
	return device, ownerA, ownerB
}

func countRelations(t *testing.T, pool *pgxpool.Pool, orgID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM entity_relations WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestIntegrationRelationAssertTemporalView(t *testing.T) {
	pool := integrationPool(t)
	orgID := relationOrg(t, pool)
	device, ownerA, _ := relationFixture(t, pool, orgID)
	store := NewRelationStore(pool)
	ctx := context.Background()

	rel, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerA.EntityID, EventID: "evt:assert-1",
		RuleVersion: RelationMappingVersionV1, Confidence: 0.9, At: t0.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantID := RelationID(orgID, device.EntityID, RelAssignedTo, ownerA.EntityID, "evt:assert-1",
		relationSnapshot(RelAssignedTo, RelationMappingVersionV1, "evt:assert-1"))
	if rel.RelationID != wantID {
		t.Fatalf("relation id = %q, want %q", rel.RelationID, wantID)
	}

	// Temporal view: before valid_from the relation does not exist...
	rels, err := store.RelationsAt(ctx, orgID, device.EntityID, t0.Add(30*time.Minute), DirectionBoth, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 0 {
		t.Fatalf("relation visible before valid_from: %+v", rels)
	}
	// ...at valid_from it is active in both directions...
	for _, dir := range []RelationDirection{DirectionOutgoing, DirectionBoth} {
		rels, err = store.RelationsAt(ctx, orgID, device.EntityID, t0.Add(time.Hour), dir, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(rels) != 1 || rels[0].RelationID != rel.RelationID {
			t.Fatalf("outgoing view at valid_from: %+v", rels)
		}
	}
	rels, err = store.RelationsAt(ctx, orgID, ownerA.EntityID, t0.Add(2*time.Hour), DirectionIncoming, RelAssignedTo)
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 1 || rels[0].FromEntityID != device.EntityID {
		t.Fatalf("incoming view: %+v", rels)
	}
	// ...and a type filter excludes other types.
	rels, err = store.RelationsAt(ctx, orgID, device.EntityID, t0.Add(2*time.Hour), DirectionBoth, RelMemberOf)
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 0 {
		t.Fatalf("type filter leaked other relations: %+v", rels)
	}

	// Replay of the same asserting event is idempotent and returns the stored row.
	again, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerA.EntityID, EventID: "evt:assert-1",
		RuleVersion: RelationMappingVersionV1, Confidence: 0.9, At: t0.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.RelationID != rel.RelationID || countRelations(t, pool, orgID) != 1 {
		t.Fatalf("replay not idempotent: %q rows=%d", again.RelationID, countRelations(t, pool, orgID))
	}

	// Closing ends the interval; at valid_to the relation is no longer active.
	closed, err := store.CloseRelation(ctx, orgID, rel.RelationID, t0.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if closed.ValidTo == nil || !closed.ValidTo.Equal(t0.Add(3*time.Hour)) {
		t.Fatalf("close: %+v", closed)
	}
	rels, err = store.RelationsAt(ctx, orgID, device.EntityID, t0.Add(3*time.Hour), DirectionBoth, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 0 {
		t.Fatalf("relation active at valid_to (half-open violated): %+v", rels)
	}
	rels, err = store.RelationsAt(ctx, orgID, device.EntityID, t0.Add(3*time.Hour).Add(-time.Nanosecond), DirectionBoth, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 1 {
		t.Fatalf("relation must be active just before valid_to: %+v", rels)
	}
	// Closing again with the same time is idempotent; a different time conflicts.
	if _, err := store.CloseRelation(ctx, orgID, rel.RelationID, t0.Add(3*time.Hour)); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if _, err := store.CloseRelation(ctx, orgID, rel.RelationID, t0.Add(4*time.Hour)); !errors.Is(err, ErrRelationAlreadyClosed) {
		t.Fatalf("reclose with different time: %v", err)
	}
}

func TestIntegrationRelationConflictFailClosed(t *testing.T) {
	pool := integrationPool(t)
	orgID := relationOrg(t, pool)
	device, ownerA, ownerB := relationFixture(t, pool, orgID)
	store := NewRelationStore(pool)
	ctx := context.Background()

	if _, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerA.EntityID, EventID: "evt:a1",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: t0.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	// Overlapping single-cardinality assignment to a different owner: rejected.
	_, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerB.EntityID, EventID: "evt:a2",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: t0.Add(2 * time.Hour),
	})
	if !errors.Is(err, ErrRelationConflict) {
		t.Fatalf("overlapping single-cardinality assert: %v", err)
	}
	// Overlapping duplicate of the same triple from a different event: rejected.
	_, err = store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerA.EntityID, EventID: "evt:a3",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: t0.Add(2 * time.Hour),
	})
	if !errors.Is(err, ErrRelationConflict) {
		t.Fatalf("overlapping duplicate assert: %v", err)
	}
	if n := countRelations(t, pool, orgID); n != 1 {
		t.Fatalf("conflicts wrote rows: %d", n)
	}

	// Multi-cardinality member_of allows simultaneous different targets.
	for i, target := range []Entity{ownerA, ownerB} {
		if _, err := store.AssertRelation(ctx, RelationRequest{
			OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelMemberOf,
			ToEntityID: target.EntityID, EventID: fmt.Sprintf("evt:m%d", i),
			RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: t0.Add(time.Hour),
		}); err != nil {
			t.Fatalf("multi-cardinality assert %d: %v", i, err)
		}
	}
	if n := countRelations(t, pool, orgID); n != 3 {
		t.Fatalf("rows after multi asserts: %d", n)
	}

	// Unknown type / unregistered rule version / missing entity / bad inputs:
	// all fail-closed, zero writes.
	before := countRelations(t, pool, orgID)
	cases := []RelationRequest{
		{OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: "knows",
			ToEntityID: ownerA.EntityID, EventID: "evt:b1", RuleVersion: RelationMappingVersionV1, Confidence: 1, At: t0},
		{OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelBelongsTo,
			ToEntityID: ownerA.EntityID, EventID: "evt:b2", RuleVersion: "9.9.9", Confidence: 1, At: t0},
		{OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelBelongsTo,
			ToEntityID: "ent:missing", EventID: "evt:b3", RuleVersion: RelationMappingVersionV1, Confidence: 1, At: t0},
		{OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelBelongsTo,
			ToEntityID: ownerA.EntityID, EventID: "evt:b4", RuleVersion: RelationMappingVersionV1, Confidence: 1.5, At: t0},
		{OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelBelongsTo,
			ToEntityID: ownerA.EntityID, EventID: "evt:b5", RuleVersion: RelationMappingVersionV1, Confidence: 1},
	}
	for i, req := range cases {
		if _, err := store.AssertRelation(ctx, req); err == nil {
			t.Fatalf("fail-closed case %d accepted", i)
		}
	}
	if n := countRelations(t, pool, orgID); n != before {
		t.Fatalf("fail-closed rejections wrote rows: %d → %d", before, n)
	}
}

func TestIntegrationRelationTransferOwnership(t *testing.T) {
	pool := integrationPool(t)
	orgID := relationOrg(t, pool)
	device, ownerA, ownerB := relationFixture(t, pool, orgID)
	store := NewRelationStore(pool)
	ctx := context.Background()

	first, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerA.EntityID, EventID: "evt:t1",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: t0.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Transfer at t+2h: old interval closes and the new one opens at the same
	// instant — every point in time has exactly one owner.
	transferAt := t0.Add(2 * time.Hour)
	closed, opened, err := store.TransferRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerB.EntityID, EventID: "evt:t2",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: transferAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if closed.RelationID != first.RelationID || closed.ValidTo == nil || !closed.ValidTo.Equal(transferAt) {
		t.Fatalf("closed half: %+v", closed)
	}
	if opened.ToEntityID != ownerB.EntityID || !opened.ValidFrom.Equal(transferAt) || opened.ValidTo != nil {
		t.Fatalf("opened half: %+v", opened)
	}
	// Boundary instants: just before the transfer owner A, at it owner B.
	rels, err := store.RelationsAt(ctx, orgID, device.EntityID, transferAt.Add(-time.Nanosecond), DirectionOutgoing, RelAssignedTo)
	if err != nil || len(rels) != 1 || rels[0].ToEntityID != ownerA.EntityID {
		t.Fatalf("pre-transfer view: %+v err=%v", rels, err)
	}
	rels, err = store.RelationsAt(ctx, orgID, device.EntityID, transferAt, DirectionOutgoing, RelAssignedTo)
	if err != nil || len(rels) != 1 || rels[0].ToEntityID != ownerB.EntityID {
		t.Fatalf("post-transfer view: %+v err=%v", rels, err)
	}

	// Transfer without an active relation and transfer to the current holder
	// are both rejected fail-closed.
	if _, _, err := store.TransferRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerB.EntityID, EventID: "evt:t3",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: transferAt.Add(time.Hour),
	}); !errors.Is(err, ErrRelationConflict) {
		t.Fatalf("transfer to current holder: %v", err)
	}
	if _, _, err := store.TransferRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: ownerA.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerB.EntityID, EventID: "evt:t4",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: transferAt.Add(time.Hour),
	}); !errors.Is(err, ErrNoActiveRelation) {
		t.Fatalf("transfer without active relation: %v", err)
	}
	// Transfer applies only to single-cardinality types.
	if _, _, err := store.TransferRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelMemberOf,
		ToEntityID: ownerB.EntityID, EventID: "evt:t5",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: transferAt.Add(time.Hour),
	}); !errors.Is(err, ErrUnknownRelationType) {
		t.Fatalf("multi-cardinality transfer: %v", err)
	}
	if n := countRelations(t, pool, orgID); n != 2 {
		t.Fatalf("transfer wrote extra rows: %d", n)
	}
}

func TestIntegrationRuleSnapshotImmutability(t *testing.T) {
	pool := integrationPool(t)
	orgID := relationOrg(t, pool)
	snaps := NewRuleSnapshots(pool)
	ctx := context.Background()

	content := json.RawMessage(`{"relation_types":{"assigned_to":"single"}}`)
	first, err := snaps.Register(ctx, orgID, RuleKindRelationMapping, "2.0.0", content)
	if err != nil {
		t.Fatal(err)
	}
	// Same version, same content (different formatting): idempotent replay.
	second, err := snaps.Register(ctx, orgID, RuleKindRelationMapping, "2.0.0",
		json.RawMessage(`{ "relation_types": { "assigned_to": "single" } }`))
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentSHA256 != second.ContentSHA256 || !first.CreatedAt.Equal(second.CreatedAt) {
		t.Fatalf("snapshot replay not idempotent: %+v vs %+v", first, second)
	}
	// Same version, different content: fail-closed, the stored rule survives.
	if _, err := snaps.Register(ctx, orgID, RuleKindRelationMapping, "2.0.0",
		json.RawMessage(`{"relation_types":{"assigned_to":"multi"}}`)); !errors.Is(err, ErrRuleSnapshotConflict) {
		t.Fatalf("version rewrite: %v", err)
	}
	loaded, err := snaps.Load(ctx, orgID, RuleKindRelationMapping, "2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ContentSHA256 != first.ContentSHA256 {
		t.Fatalf("stored content mutated: %q vs %q", loaded.ContentSHA256, first.ContentSHA256)
	}
	// Unregistered version load is a hard error.
	if _, err := snaps.Load(ctx, orgID, RuleKindRelationMapping, "3.0.0"); !errors.Is(err, ErrRuleSnapshotNotStored) {
		t.Fatalf("unknown version load: %v", err)
	}
	// Invalid refs are rejected before touching the database.
	if _, err := snaps.Register(ctx, orgID, "BadKind", "2.0.0", content); !errors.Is(err, ErrInvalidRuleKind) {
		t.Fatalf("kind: %v", err)
	}
	if _, err := snaps.Register(ctx, orgID, RuleKindRelationMapping, "2.0", content); !errors.Is(err, ErrInvalidRuleVersion) {
		t.Fatalf("version: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM rule_snapshots WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("snapshot rows: %d", count)
	}
}

func TestIntegrationSingleEntityFeaturesDegradeWithoutRelations(t *testing.T) {
	pool := integrationPool(t)
	orgID := relationOrg(t, pool)
	device, ownerA, _ := relationFixture(t, pool, orgID)
	store := NewRelationStore(pool)
	attr := NewAttributor(pool)
	ctx := context.Background()

	// Two resolved attributions for the device in the window.
	role := RoleObservation{Role: RoleHost, EntityType: TypeDevice, Space: "corp.example",
		Identifiers: []Identifier{{KindDeviceUUID, "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}}}
	for i, eventID := range []string{"evt:f1", "evt:f2"} {
		attrs, err := attr.Attribute(ctx, orgID, eventID, t0.Add(time.Duration(i+1)*time.Hour), []RoleObservation{role})
		if err != nil {
			t.Fatal(err)
		}
		if err := attr.Store(ctx, orgID, t0.Add(time.Duration(i+1)*time.Hour), attrs); err != nil {
			t.Fatal(err)
		}
	}

	windowFrom, windowTo := t0, t0.Add(4*time.Hour)
	fa := NewFeatureAssembler(pool)

	// Baseline: no relations exist — counts are produced, not degraded.
	feat, err := fa.SingleEntity(ctx, orgID, device.EntityID, windowFrom, windowTo)
	if err != nil {
		t.Fatal(err)
	}
	if feat.ResolvedTotal != 2 || feat.ResolvedByRole[RoleHost] != 2 {
		t.Fatalf("counts: %+v", feat)
	}
	if feat.RelationsDegraded || len(feat.Relations) != 0 {
		t.Fatalf("absent relations must not degrade: %+v", feat)
	}

	// With an active relation the view is enriched.
	if _, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: ownerA.EntityID, EventID: "evt:f3",
		RuleVersion: RelationMappingVersionV1, Confidence: 1.0, At: t0.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	feat, err = fa.SingleEntity(ctx, orgID, device.EntityID, windowFrom, windowTo)
	if err != nil {
		t.Fatal(err)
	}
	if len(feat.Relations) != 1 || feat.RelationsDegraded || feat.ResolvedTotal != 2 {
		t.Fatalf("enriched view: %+v", feat)
	}

	// Relation lookup failure: single-entity features are still produced.
	failing := NewFeatureAssembler(pool)
	failing.RelationsLookup = func(context.Context, string, string, time.Time) ([]Relation, error) {
		return nil, errors.New("relation store unavailable")
	}
	feat, err = failing.SingleEntity(ctx, orgID, device.EntityID, windowFrom, windowTo)
	if err != nil {
		t.Fatalf("relation failure must not block single-entity features: %v", err)
	}
	if !feat.RelationsDegraded || feat.RelationError == "" {
		t.Fatalf("failure not marked degraded: %+v", feat)
	}
	if feat.ResolvedTotal != 2 || feat.ResolvedByRole[RoleHost] != 2 {
		t.Fatalf("relation failure altered counts: %+v", feat)
	}
}
