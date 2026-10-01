package control

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/auth"
)

// TestListSourcesIntegration pins the column/scan alignment of ListSources: the
// query selects state next to release_id, and dropping it from the Scan call
// makes every list fail with a scan error (observed as a 503 from
// GET /api/v1/sources). The paused row also proves state is read, not derived.
func TestListSourcesIntegration(t *testing.T) {
	url := os.Getenv("TUBA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Exec(ctx, `INSERT INTO source_instances(id,organization_id,namespace,vendor_name,vendor_product,vendor_dataset,credential_ref,state,enabled,rate_limit)
		SELECT 'src_listsources_integration_active',o.id,'listsources','vendor','product','dataset','cred_listsources_active','active',true,100 FROM organizations o WHERE o.slug='tenant_a'
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Exec(ctx, `INSERT INTO source_instances(id,organization_id,namespace,vendor_name,vendor_product,vendor_dataset,credential_ref,state,enabled,rate_limit)
		SELECT 'src_listsources_integration_paused',o.id,'listsources','vendor','product','dataset','cred_listsources_paused','paused',false,100 FROM organizations o WHERE o.slug='tenant_a'
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	s := &Store{Pool: p, Issuer: "issuer"}
	items, err := s.ListSources(ctx, auth.Principal{Subject: "subject", Organization: "tenant_a"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	byID := map[string]SourceInstance{}
	for _, item := range items {
		byID[item.ID] = item
	}
	active, ok := byID["src_listsources_integration_active"]
	if !ok {
		t.Fatalf("active source missing from list: %+v", items)
	}
	if active.State != "active" || !active.Enabled || active.Namespace != "listsources" {
		t.Fatalf("active source fields wrong: %+v", active)
	}
	paused, ok := byID["src_listsources_integration_paused"]
	if !ok {
		t.Fatal("paused source missing from list")
	}
	if paused.State != "paused" || paused.Enabled {
		t.Fatalf("paused source must list as state=paused enabled=false: %+v", paused)
	}
	if _, err = p.Exec(ctx, `DELETE FROM source_instances WHERE id IN ('src_listsources_integration_active','src_listsources_integration_paused')`); err != nil {
		t.Fatal(err)
	}
}
