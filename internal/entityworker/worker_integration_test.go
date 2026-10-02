package entityworker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/entity"
)

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TUBA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// integrationOrg creates a dedicated organization and removes it plus all
// entity fixtures afterwards; tenant data is never touched.
func integrationOrg(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("attrmsg_it_%d", time.Now().UnixNano())
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

func TestIntegrationWorkerPublishesEntityPartitionedContributions(t *testing.T) {
	pool := integrationPool(t)
	orgID := integrationOrg(t, pool)
	ctx := context.Background()
	r := entity.NewRegistry(pool)
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", entity.SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RegisterSpace(ctx, orgID, "host-local", entity.SpaceLocalAccounts); err != nil {
		t.Fatal(err)
	}
	subject, err := r.Register(ctx, entity.RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: entity.TypeAccount,
		Identifiers: []entity.Identifier{{Kind: entity.KindSID, Value: "S-1-5-21-100-200-300-1001"}}, At: at})
	if err != nil {
		t.Fatal(err)
	}
	target, err := r.Register(ctx, entity.RegisterRequest{OrganizationID: orgID, Space: "corp.example", EntityType: entity.TypeAccount,
		Identifiers: []entity.Identifier{{Kind: entity.KindSID, Value: "S-1-5-21-100-200-300-2002"}}, At: at})
	if err != nil {
		t.Fatal(err)
	}
	host, err := r.Register(ctx, entity.RegisterRequest{OrganizationID: orgID, Space: "host-local", EntityType: entity.TypeDevice,
		Identifiers: []entity.Identifier{{Kind: entity.KindHostname, Value: "WEB01"}}, At: at})
	if err != nil {
		t.Fatal(err)
	}

	event := map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:int-1", "dataset": "authentication"},
		"user": map[string]any{
			"id":     "S-1-5-21-100-200-300-1001",
			"target": map[string]any{"id": "S-1-5-21-100-200-300-2002"},
		},
		"host":        map[string]any{"name": "WEB01"},
		"source":      map[string]any{"ip": "10.0.0.5"},
		"destination": map[string]any{"ip": "10.0.0.9"},
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{{Value: body}}, events: &events}
	writer := &fakeWriter{events: &events}
	processor := AttributionProcessor{Attributor: entity.NewAttributor(pool), AccountSpace: "corp.example", DeviceSpace: "host-local"}
	runOnce(t, orgID, consumer, writer, processor)

	if len(writer.messages) != 5 {
		t.Fatalf("expected 5 role contributions, got %d", len(writer.messages))
	}
	entityIDs := map[string]string{
		entity.RoleActor:  subject.EntityID,
		entity.RoleTarget: target.EntityID,
		entity.RoleHost:   host.EntityID,
	}
	unresolved := 0
	for _, m := range writer.messages {
		var c entity.Contribution
		if err := json.Unmarshal(m.Value, &c); err != nil {
			t.Fatal(err)
		}
		if string(m.Key) != c.PartitionKey {
			t.Fatalf("kafka key %q != body partition_key %q", m.Key, c.PartitionKey)
		}
		if c.EventID != "evt:int-1" {
			t.Fatalf("event id rewritten: %q", c.EventID)
		}
		if want, ok := entityIDs[c.Role]; ok {
			if c.State != entity.StateResolved || c.EntityID != want {
				t.Fatalf("%s: %+v", c.Role, c)
			}
			if c.PartitionKey != orgID+":"+want {
				t.Fatalf("%s partition key %q, want entity key", c.Role, c.PartitionKey)
			}
		} else {
			unresolved++
			if c.State != entity.StateUnresolved || c.EntityID != "" {
				t.Fatalf("%s: %+v", c.Role, c)
			}
			want := orgID + ":unresolved:unresolved:" + entity.ReasonNoMatchingEntity
			if c.PartitionKey != want {
				t.Fatalf("%s partition key %q, want %q", c.Role, c.PartitionKey, want)
			}
			for _, id := range entityIDs {
				if c.PartitionKey == orgID+":"+id {
					t.Fatalf("unresolved contribution %s pollutes entity partition", c.Role)
				}
			}
		}
	}
	if unresolved != 2 {
		t.Fatalf("unresolved contributions must not be dropped: got %d of 2", unresolved)
	}
	var stored int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM entity_attributions WHERE organization_id=$1`, orgID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 5 {
		t.Fatalf("attribution projection rows: %d", stored)
	}
	if len(consumer.committed) != 1 {
		t.Fatalf("input committed %d times", len(consumer.committed))
	}
	if !strings.HasPrefix(writer.messages[0].Headers[0].Key, "schema-version") {
		t.Fatalf("headers: %+v", writer.messages[0].Headers)
	}
}
