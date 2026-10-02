package entity

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// queriesFixture creates a dedicated organization (never tenant data) with
// one account and one device, attributions for the account, one closed and
// one active relation on the device, feature samples and a baseline model
// whose sample range covers the account. Returns (slug, orgID, account,
// device); all fixtures are removed on cleanup.
func queriesFixture(t *testing.T, pool *pgxpool.Pool) (string, string, Entity, Entity) {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("query_it_%d", time.Now().UnixNano())
	var orgID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, slug).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, table := range []string{"feature_samples", "baseline_models", "entity_relations", "entity_attributions", "rule_snapshots", "entities", "identity_spaces"} {
			if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE organization_id=$1`, table), orgID); err != nil {
				t.Logf("cleanup %s: %v", table, err)
			}
		}
		if _, err := pool.Exec(ctx, `DELETE FROM organizations WHERE id=$1`, orgID); err != nil {
			t.Logf("cleanup org: %v", err)
		}
	})

	r := NewRegistry(pool)
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	account, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-1001"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	device, err := r.Register(ctx, RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindDeviceUUID, "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}}, At: t0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRuleSnapshots(pool).Register(ctx, orgID, RuleKindRelationMapping, RelationMappingVersionV1,
		json.RawMessage(`{"relation_types":{"assigned_to":"single"}}`)); err != nil {
		t.Fatal(err)
	}

	a := NewAttributor(pool)
	for i, eventID := range []string{"evt:q-1", "evt:q-2", "evt:q-3"} {
		at := t0.Add(time.Duration(i+1) * time.Hour)
		attrs, err := a.Attribute(ctx, orgID, eventID, at, []RoleObservation{{
			Role: "actor", EntityType: TypeAccount, Space: "corp.example",
			Identifiers: []Identifier{{KindSID, "S-1-5-21-100-200-300-1001"}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Store(ctx, orgID, at, attrs); err != nil {
			t.Fatal(err)
		}
	}

	store := NewRelationStore(pool)
	rel, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: account.EntityID, EventID: "evt:q-rel-1",
		RuleVersion: RelationMappingVersionV1, Confidence: 0.9, At: t0.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CloseRelation(ctx, orgID, rel.RelationID, t0.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AssertRelation(ctx, RelationRequest{
		OrganizationID: orgID, FromEntityID: device.EntityID, RelationType: RelAssignedTo,
		ToEntityID: account.EntityID, EventID: "evt:q-rel-2",
		RuleVersion: RelationMappingVersionV1, Confidence: 0.95, At: t0.Add(2 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		start := t0.Add(time.Duration(i) * 24 * time.Hour)
		if _, err := pool.Exec(ctx, `
			INSERT INTO feature_samples(organization_id, feature_id, feature_version, generation, entity_id,
				window_start, window_end, revision, quality, values, inputs, content_sha256, closed_at)
			VALUES($1, 'logon.count', '1.0.0', 'g1', $2, $3::timestamptz, $3::timestamptz + interval '1 hour', 1, 'qualified',
				'{"count": 3}'::jsonb, '{}'::jsonb,
				repeat($4, 64), $3::timestamptz + interval '1 hour')`,
			orgID, account.EntityID, start, string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO baseline_models(organization_id, model_id, model_version, feature_id, feature_version, generation,
			status, sample_count, complete_days, trained_at, training_cutoff, sample_range, metrics, statistics, content_sha256)
		VALUES($1, 'logon.count.model', '1.0.0', 'logon.count', '1.0.0', 'g1',
			'ready', 2, 2, now(), now(),
			jsonb_build_object('entities', jsonb_build_array($2::text)), '{"algorithm":"moments.v1"}'::jsonb, '{}'::jsonb,
			repeat('c', 64))`, orgID, account.EntityID); err != nil {
		t.Fatal(err)
	}
	return slug, orgID, account, device
}

func TestIntegrationSearchEntities(t *testing.T) {
	pool := integrationPool(t)
	slug, _, account, device := queriesFixture(t, pool)
	q := NewQueries(pool)
	ctx := context.Background()

	all, next, err := q.SearchEntities(ctx, slug, "", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || next != "" {
		t.Fatalf("expected 2 entities without next cursor, got %d next=%q", len(all), next)
	}

	accounts, _, err := q.SearchEntities(ctx, slug, "", TypeAccount, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || accounts[0].EntityID != account.EntityID {
		t.Fatalf("type filter returned %+v", accounts)
	}
	if _, _, err := q.SearchEntities(ctx, slug, "", "printer", "", 10); err == nil {
		t.Fatal("unknown entity type accepted")
	}

	matched, _, err := q.SearchEntities(ctx, slug, account.CanonicalKey, "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(matched) != 1 || matched[0].EntityID != account.EntityID {
		t.Fatalf("substring query returned %+v", matched)
	}

	first, cursor, err := q.SearchEntities(ctx, slug, "", "", "", 1)
	if err != nil || len(first) != 1 || cursor == "" {
		t.Fatalf("first page len=%d cursor=%q err=%v", len(first), cursor, err)
	}
	second, cursor2, err := q.SearchEntities(ctx, slug, "", "", cursor, 1)
	if err != nil || len(second) != 1 || cursor2 != "" {
		t.Fatalf("second page len=%d cursor=%q err=%v", len(second), cursor2, err)
	}
	if first[0].EntityID == second[0].EntityID {
		t.Fatal("pagination returned the same entity twice")
	}
	if first[0].EntityID != account.EntityID && first[0].EntityID != device.EntityID {
		t.Fatalf("unexpected entity %s", first[0].EntityID)
	}
	if _, _, err := q.SearchEntities(ctx, slug, "", "", "not-a-cursor", 1); err == nil {
		t.Fatal("malformed cursor accepted")
	}

	other, _, err := q.SearchEntities(ctx, "tenant_a", "", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range other {
		if e.EntityID == account.EntityID || e.EntityID == device.EntityID {
			t.Fatal("cross-tenant entity leaked")
		}
	}
}

func TestIntegrationGetEntityAndAttributions(t *testing.T) {
	pool := integrationPool(t)
	slug, _, account, _ := queriesFixture(t, pool)
	q := NewQueries(pool)
	ctx := context.Background()

	detail, found, err := q.GetEntity(ctx, slug, account.EntityID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if detail.EntityType != TypeAccount || detail.Strength != StrengthStrong {
		t.Fatalf("unexpected detail %+v", detail.EntitySummary)
	}
	if len(detail.RecentAttributions) != 3 {
		t.Fatalf("expected 3 recent attributions, got %d", len(detail.RecentAttributions))
	}
	for _, a := range detail.RecentAttributions {
		if a.State != StateResolved || a.RuleVersion == "" || a.Role != "actor" {
			t.Fatalf("unexpected attribution %+v", a)
		}
	}

	if _, found, err := q.GetEntity(ctx, slug, "ent:"+fmt.Sprintf("%064d", 0)); err != nil || found {
		t.Fatalf("missing entity found=%v err=%v", found, err)
	}
	if _, found, err := q.GetEntity(ctx, "tenant_a", account.EntityID); err != nil || found {
		t.Fatal("cross-tenant entity detail leaked")
	}

	page, cursor, err := q.ListAttributions(ctx, slug, account.EntityID, "", 2)
	if err != nil || len(page) != 2 || cursor == "" {
		t.Fatalf("first page len=%d cursor=%q err=%v", len(page), cursor, err)
	}
	rest, cursor2, err := q.ListAttributions(ctx, slug, account.EntityID, cursor, 2)
	if err != nil || len(rest) != 1 || cursor2 != "" {
		t.Fatalf("second page len=%d cursor=%q err=%v", len(rest), cursor2, err)
	}
	if !page[0].EventTime.After(page[1].EventTime) {
		t.Fatal("attributions are not newest-first")
	}
	if _, _, err := q.ListAttributions(ctx, slug, account.EntityID, "%%%", 2); err == nil {
		t.Fatal("malformed cursor accepted")
	}
}

func TestIntegrationListRelationsHistory(t *testing.T) {
	pool := integrationPool(t)
	slug, _, account, device := queriesFixture(t, pool)
	q := NewQueries(pool)
	ctx := context.Background()

	withHistory, err := q.ListRelations(ctx, slug, device.EntityID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(withHistory) != 2 {
		t.Fatalf("expected active + history, got %d", len(withHistory))
	}
	if withHistory[0].ValidTo != nil {
		t.Fatal("active relation is not first")
	}
	activeOnly, err := q.ListRelations(ctx, slug, device.EntityID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(activeOnly) != 1 || activeOnly[0].ValidTo != nil {
		t.Fatalf("active-only returned %+v", activeOnly)
	}
	// Incoming direction: the account sees the same edges.
	incoming, err := q.ListRelations(ctx, slug, account.EntityID, false)
	if err != nil || len(incoming) != 1 {
		t.Fatalf("incoming len=%d err=%v", len(incoming), err)
	}
	if other, err := q.ListRelations(ctx, "tenant_a", device.EntityID, true); err != nil || len(other) != 0 {
		t.Fatal("cross-tenant relations leaked")
	}
}

func TestIntegrationFeaturesAndBaseline(t *testing.T) {
	pool := integrationPool(t)
	slug, _, account, device := queriesFixture(t, pool)
	q := NewQueries(pool)
	ctx := context.Background()

	samples, err := q.ListFeatureSamples(ctx, slug, account.EntityID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 || !samples[0].WindowStart.After(samples[1].WindowStart) {
		t.Fatalf("unexpected samples %+v", samples)
	}
	if samples[0].FeatureID != "logon.count" || samples[0].Quality != "qualified" {
		t.Fatalf("unexpected sample %+v", samples[0])
	}
	if none, err := q.ListFeatureSamples(ctx, slug, device.EntityID, 10); err != nil || len(none) != 0 {
		t.Fatalf("device samples=%d err=%v", len(none), err)
	}

	models, err := q.ListBaselines(ctx, slug, account.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].Status != "ready" || models[0].SampleCount != 2 {
		t.Fatalf("unexpected models %+v", models)
	}
	if models[0].TrainedAt == nil || models[0].TrainingCutoff == nil {
		t.Fatal("ready model misses training timestamps")
	}
	if !models[0].CoversEntity {
		t.Fatal("model sample range covers the account but covers_entity is false")
	}
	// Models are population-wide: the device sees the same row, flagged as
	// not covering it.
	deviceModels, err := q.ListBaselines(ctx, slug, device.EntityID)
	if err != nil || len(deviceModels) != 1 {
		t.Fatalf("device models=%d err=%v", len(deviceModels), err)
	}
	if deviceModels[0].CoversEntity {
		t.Fatal("device is not in the sample range but covers_entity is true")
	}
	// Cross-tenant: another tenant's listing never contains the fixture
	// model (tenant_a has its own legitimate baseline rows on 248).
	other, err := q.ListBaselines(ctx, "tenant_a", account.EntityID)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range other {
		if m.ModelID == "logon.count.model" {
			t.Fatal("cross-tenant baseline leaked")
		}
	}
}
