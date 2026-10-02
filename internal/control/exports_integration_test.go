package control

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/auth"
)

// Q03 export lifecycle integration against a real PostgreSQL
// (gated: TUBA_TEST_DATABASE_URL). Fixtures follow the tenant_a convention
// (audit rows are append-only and remain; mutable fixture rows are deleted).

func exportTestStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TUBA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `
		INSERT INTO identities(issuer,subject) VALUES('issuer','export-analyst'),('issuer','export-viewer') ON CONFLICT DO NOTHING;
		INSERT INTO memberships(organization_id,identity_id,role_id)
		  SELECT o.id,i.id,r.id FROM organizations o,identities i,roles r
		  WHERE o.slug='tenant_a' AND i.issuer='issuer' AND i.subject='export-analyst' AND r.organization_id=o.id AND r.name='analyst' ON CONFLICT DO NOTHING;
		INSERT INTO memberships(organization_id,identity_id,role_id)
		  SELECT o.id,i.id,r.id FROM organizations o,identities i,roles r
		  WHERE o.slug='tenant_a' AND i.issuer='issuer' AND i.subject='export-viewer' AND r.organization_id=o.id AND r.name='viewer' ON CONFLICT DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	return &Store{Pool: pool, Issuer: "issuer"}, pool
}

var exportAnalyst = auth.Principal{Subject: "export-analyst", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"analyst"}}
var exportViewer = auth.Principal{Subject: "export-viewer", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"viewer"}}

func exportInput() CreateExportInput {
	now := time.Now().UTC()
	return CreateExportInput{
		Dataset: "authentication", Format: ExportFormatNDJSON, Query: "search authentication",
		Generation: "g1", From: now.Add(-time.Hour), To: now, RowLimit: 100, ByteLimit: 1 << 20,
		IncludeSensitive: true,
	}
}

func cleanupExports(t *testing.T, pool *pgxpool.Pool, ids ...string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		if _, err := pool.Exec(ctx, `DELETE FROM processing_jobs WHERE id=(SELECT job_id FROM export_jobs WHERE id=$1)`, id); err != nil {
			t.Fatal(err)
		}
	}
}

func auditCount(t *testing.T, pool *pgxpool.Pool, exportID, action string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM audit_events WHERE resource_type='export' AND resource_id=$1 AND action=$2`, exportID, action).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func completeExport(t *testing.T, s *Store, pool *pgxpool.Pool, id string) {
	t.Helper()
	ctx := context.Background()
	if err := s.MarkExportRunning(ctx, id, time.Now().UTC().Add(-ExportDatasetRetention)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteExport(ctx, id, ExportResult{FileSHA256: strings.Repeat("a", 64), RowCount: 2, ByteCount: 100}, "test-worker"); err != nil {
		t.Fatal(err)
	}
}

func TestExportLifecycleAndDownloadIntegration(t *testing.T) {
	s, pool := exportTestStore(t)
	ctx := context.Background()

	export, status, err := s.CreateExport(ctx, exportAnalyst, exportInput(), "req-create")
	if err != nil || status != 201 {
		t.Fatalf("create: %d %v", status, err)
	}
	defer cleanupExports(t, pool, export.ID)
	if export.State != ExportStateQueued {
		t.Fatalf("state=%s", export.State)
	}
	if auditCount(t, pool, export.ID, "export.create") != 1 {
		t.Fatal("export.create audit missing")
	}

	loaded, err := s.GetExport(ctx, exportAnalyst, export.ID)
	if err != nil || loaded.ID != export.ID || loaded.Fingerprint == "" {
		t.Fatalf("get: %v %+v", err, loaded)
	}
	// Tenant isolation: another org principal must not see it.
	if _, err := s.GetExport(ctx, auth.Principal{Subject: "export-analyst", Organization: "tenant_b", Namespace: "x"}, export.ID); err == nil {
		t.Fatal("cross-tenant export visible")
	}
	items, _, err := s.ListExports(ctx, exportAnalyst, 10, "")
	if err != nil || len(items) == 0 {
		t.Fatalf("list: %v %d", err, len(items))
	}

	// Not ready yet.
	if _, status, err := s.AuthorizeExportDownload(ctx, exportAnalyst, export.ID, "req-early"); err == nil || status != 409 {
		t.Fatalf("queued download: %d %v", status, err)
	}

	completeExport(t, s, pool, export.ID)
	if auditCount(t, pool, export.ID, "export.complete") != 1 {
		t.Fatal("export.complete audit missing")
	}

	// Re-authorization: a different subject is denied even with the link
	// (换人拒，红线), and the denial is audited.
	if _, status, err := s.AuthorizeExportDownload(ctx, exportViewer, export.ID, "req-other"); err == nil || status != 403 {
		t.Fatalf("other subject download: %d %v", status, err)
	}
	if auditCount(t, pool, export.ID, "export.download_denied") != 1 {
		t.Fatal("export.download_denied audit missing")
	}

	// 降权拒: same subject with reduced permissions mismatches the fingerprint.
	downgraded := auth.Principal{Subject: "export-analyst", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"viewer"}}
	if _, status, err := s.AuthorizeExportDownload(ctx, downgraded, export.ID, "req-downgraded"); err == nil || status != 403 {
		t.Fatalf("downgraded download: %d %v", status, err)
	}

	// Authorized download succeeds exactly once; the replay is refused (410)
	// and audited (single-use link).
	allowed, status, err := s.AuthorizeExportDownload(ctx, exportAnalyst, export.ID, "req-download")
	if err != nil || status != 200 || allowed.ID != export.ID {
		t.Fatalf("download: %d %v", status, err)
	}
	if auditCount(t, pool, export.ID, "export.download") != 1 {
		t.Fatal("export.download audit missing")
	}
	if _, status, err := s.AuthorizeExportDownload(ctx, exportAnalyst, export.ID, "req-replay"); err == nil || status != 410 {
		t.Fatalf("replay download: %d %v", status, err)
	}
	if auditCount(t, pool, export.ID, "export.download_denied") != 3 {
		t.Fatalf("download_denied audits = %d", auditCount(t, pool, export.ID, "export.download_denied"))
	}
}

func TestExportExpiryIntegration(t *testing.T) {
	s, pool := exportTestStore(t)
	ctx := context.Background()

	// Link already past expires_at: download marks it expired and audits export.expire.
	export, _, err := s.CreateExport(ctx, exportAnalyst, exportInput(), "req-exp1")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupExports(t, pool, export.ID)
	completeExport(t, s, pool, export.ID)
	if _, err := pool.Exec(ctx, `UPDATE export_jobs SET expires_at=now()-interval '1 minute' WHERE id=$1`, export.ID); err != nil {
		t.Fatal(err)
	}
	if _, status, err := s.AuthorizeExportDownload(ctx, exportAnalyst, export.ID, "req-exp-dl"); err == nil || status != 410 {
		t.Fatalf("expired download: %d %v", status, err)
	}
	if auditCount(t, pool, export.ID, "export.expire") != 1 {
		t.Fatal("export.expire audit missing")
	}
	loaded, _ := s.GetExport(ctx, exportAnalyst, export.ID)
	if loaded.State != ExportStateExpired {
		t.Fatalf("state=%s", loaded.State)
	}

	// Sweeper path: ExpireExports transitions lapsed links and audits each.
	second, _, err := s.CreateExport(ctx, exportAnalyst, exportInput(), "req-exp2")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupExports(t, pool, second.ID)
	completeExport(t, s, pool, second.ID)
	if _, err := pool.Exec(ctx, `UPDATE export_jobs SET expires_at=now()-interval '1 minute' WHERE id=$1`, second.ID); err != nil {
		t.Fatal(err)
	}
	expired, err := s.ExpireExports(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, x := range expired {
		if x.ID == second.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("sweeper did not expire the lapsed export")
	}
	if auditCount(t, pool, second.ID, "export.expire") != 1 {
		t.Fatal("sweeper export.expire audit missing")
	}

	// Downloaded exports are never re-expired by the sweeper.
	third, _, err := s.CreateExport(ctx, exportAnalyst, exportInput(), "req-exp3")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupExports(t, pool, third.ID)
	completeExport(t, s, pool, third.ID)
	if _, status, err := s.AuthorizeExportDownload(ctx, exportAnalyst, third.ID, "req-exp3-dl"); err != nil || status != 200 {
		t.Fatalf("download: %d %v", status, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE export_jobs SET expires_at=now()-interval '1 minute' WHERE id=$1`, third.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExpireExports(ctx, 100); err != nil {
		t.Fatal(err)
	}
	loaded, _ = s.GetExport(ctx, exportAnalyst, third.ID)
	if loaded.State != ExportStateSucceeded {
		t.Fatalf("downloaded export re-expired: %s", loaded.State)
	}
}
