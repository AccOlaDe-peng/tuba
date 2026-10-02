package entityworker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/entity"
)

// sightOrg creates a dedicated organization and both identity spaces, and
// removes all fixtures afterwards; tenant data is never touched.
func sightOrg(t *testing.T, pool *pgxpool.Pool) (orgID string) {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("regsight_it_%d", time.Now().UnixNano())
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, slug).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	r := entity.NewRegistry(pool)
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", entity.SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RegisterSpace(ctx, orgID, "host-local", entity.SpaceLocalAccounts); err != nil {
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

func countRows(t *testing.T, pool *pgxpool.Pool, table, orgID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE organization_id=$1`, table), orgID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// countingRegisterer wraps the real registry to observe registration calls.
type countingRegisterer struct {
	inner *entity.Registry
	mu    sync.Mutex
	calls []entity.RegisterRequest
}

func (c *countingRegisterer) Register(ctx context.Context, req entity.RegisterRequest) (entity.Entity, error) {
	c.mu.Lock()
	c.calls = append(c.calls, req)
	c.mu.Unlock()
	return c.inner.Register(ctx, req)
}

func (c *countingRegisterer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func sightProcessor(pool *pgxpool.Pool, reg Registerer) *RegisteringProcessor {
	return NewRegisteringProcessor(reg, entity.NewAttributor(pool), "corp.example", "host-local", nil)
}

func TestIntegrationRegisterOnSightFirstObservationResolved(t *testing.T) {
	pool := integrationPool(t)
	orgID := sightOrg(t, pool)
	ctx := context.Background()
	p := sightProcessor(pool, entity.NewRegistry(pool))

	event := map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:sight-1"},
		"user":       map[string]any{"id": "S-1-5-21-100-200-300-1001"},
		"host":       map[string]any{"name": "WEB01"},
		"source":     map[string]any{"ip": "10.0.0.5"},
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{{Value: body}}, events: &events}
	writer := &fakeWriter{events: &events}
	runOnce(t, orgID, consumer, writer, p)

	if len(writer.messages) != 3 {
		t.Fatalf("contributions: %d", len(writer.messages))
	}
	for _, m := range writer.messages {
		var c entity.Contribution
		if err := json.Unmarshal(m.Value, &c); err != nil {
			t.Fatal(err)
		}
		if c.State != entity.StateResolved || c.EntityID == "" {
			t.Fatalf("%s must resolve on first sight: %+v", c.Role, c)
		}
	}
	// 3 entities registered: SID account, hostname device, ip device.
	if n := countRows(t, pool, "entities", orgID); n != 3 {
		t.Fatalf("entities: %d, want 3", n)
	}
	if n := countRows(t, pool, "entity_attributions", orgID); n != 3 {
		t.Fatalf("attributions: %d, want 3", n)
	}
	// Stored rows are all resolved.
	var resolved int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM entity_attributions WHERE organization_id=$1 AND status='resolved'`, orgID).Scan(&resolved); err != nil {
		t.Fatal(err)
	}
	if resolved != 3 {
		t.Fatalf("stored resolved rows: %d, want 3", resolved)
	}
	if len(consumer.committed) != 1 {
		t.Fatalf("input committed %d times", len(consumer.committed))
	}
}

func TestIntegrationRegisterOnSightIdempotent(t *testing.T) {
	pool := integrationPool(t)
	orgID := sightOrg(t, pool)
	p := sightProcessor(pool, entity.NewRegistry(pool))
	event := map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:sight-idem"},
		"user":       map[string]any{"id": "S-1-5-21-100-200-300-1001"},
		"host":       map[string]any{"name": "WEB01"},
	}
	for i := 0; i < 3; i++ {
		if _, err := p.Attribute(context.Background(), orgID, event); err != nil {
			t.Fatal(err)
		}
	}
	if n := countRows(t, pool, "entities", orgID); n != 2 {
		t.Fatalf("repeated observation must not re-register: entities=%d, want 2", n)
	}
	if n := countRows(t, pool, "entity_attributions", orgID); n != 2 {
		t.Fatalf("store must stay idempotent: attributions=%d, want 2", n)
	}
}

func TestIntegrationRegisterOnSightWeakTransferNewOccurrence(t *testing.T) {
	pool := integrationPool(t)
	orgID := sightOrg(t, pool)
	ctx := context.Background()
	r := entity.NewRegistry(pool)
	p := sightProcessor(pool, r)

	t1 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	eventAt := func(id string, at time.Time) map[string]any {
		return map[string]any{
			"@timestamp": at.Format(time.RFC3339Nano),
			"event":      map[string]any{"id": id},
			"host":       map[string]any{"name": "WEB01"},
		}
	}
	first, err := p.Attribute(ctx, orgID, eventAt("evt:weak-t1", t1))
	if err != nil {
		t.Fatal(err)
	}
	if first[0].State != entity.StateResolved {
		t.Fatalf("first observation must resolve: %+v", first[0])
	}
	entityA := first[0].EntityID

	// Transfer the hostname away, then observe it again later.
	if _, err := r.TransferWeak(ctx, orgID, "host-local", entity.TypeDevice,
		entity.Identifier{Kind: entity.KindHostname, Value: "WEB01"}, t1.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	t3 := t1.Add(2 * time.Hour)
	second, err := p.Attribute(ctx, orgID, eventAt("evt:weak-t3", t3))
	if err != nil {
		t.Fatal(err)
	}
	if second[0].State != entity.StateResolved || second[0].EntityID == entityA {
		t.Fatalf("post-transfer observation must resolve to a new entity: %+v (old %s)", second[0], entityA)
	}

	// Re-attributing the old event time still resolves to the old entity.
	attrs, err := entity.NewAttributor(pool).Attribute(ctx, orgID, "evt:weak-recheck", t1, []entity.RoleObservation{
		{Role: entity.RoleHost, EntityType: entity.TypeDevice, Space: "host-local",
			Identifiers: []entity.Identifier{{Kind: entity.KindHostname, Value: "WEB01"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attrs[0].State != entity.StateResolved || attrs[0].EntityID != entityA {
		t.Fatalf("history must not be rewritten: %+v (want entity %s)", attrs[0], entityA)
	}
	if n := countRows(t, pool, "entities", orgID); n != 2 {
		t.Fatalf("two occurrences expected: %d", n)
	}
}

func TestIntegrationRegisterOnSightBadIdentifierDoesNotBlock(t *testing.T) {
	pool := integrationPool(t)
	orgID := sightOrg(t, pool)
	p := sightProcessor(pool, entity.NewRegistry(pool))
	event := map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:sight-bad"},
		"user":       map[string]any{"name": "BAD NAME!"},
		"host":       map[string]any{"name": "WEB01"},
	}
	attrs, err := p.Attribute(context.Background(), orgID, event)
	if err != nil {
		t.Fatalf("bad identifier must not block the event: %v", err)
	}
	if len(attrs) != 2 {
		t.Fatalf("attributions: %d", len(attrs))
	}
	byRole := map[string]entity.RoleAttribution{}
	for _, ra := range attrs {
		byRole[ra.Role] = ra
	}
	bad := byRole[entity.RoleActor]
	if bad.State != entity.StateUnresolved || bad.Evidence.Reason != entity.ReasonRegistrationFailed || bad.EntityID != "" {
		t.Fatalf("bad role must be forced unresolved: %+v", bad)
	}
	if byRole[entity.RoleHost].State != entity.StateResolved {
		t.Fatalf("unaffected role must resolve: %+v", byRole[entity.RoleHost])
	}
	if n := countRows(t, pool, "entities", orgID); n != 1 {
		t.Fatalf("only the valid role registers: entities=%d, want 1", n)
	}
	if n := countRows(t, pool, "entity_attributions", orgID); n != 2 {
		t.Fatalf("both attributions stored: %d", n)
	}
}

func TestIntegrationRegisterOnSightStrongRegisteredOncePerProcess(t *testing.T) {
	pool := integrationPool(t)
	orgID := sightOrg(t, pool)
	counting := &countingRegisterer{inner: entity.NewRegistry(pool)}
	p := sightProcessor(pool, counting)
	event := map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:sight-perf"},
		"user":       map[string]any{"id": "S-1-5-21-100-200-300-1001"},
	}
	const n = 25
	for i := 0; i < n; i++ {
		if _, err := p.Attribute(context.Background(), orgID, event); err != nil {
			t.Fatal(err)
		}
	}
	if got := counting.count(); got != 1 {
		t.Fatalf("%d events of one strong identity must issue 1 registration, got %d", n, got)
	}
	if rows := countRows(t, pool, "entities", orgID); rows != 1 {
		t.Fatalf("entities: %d, want 1", rows)
	}
}
