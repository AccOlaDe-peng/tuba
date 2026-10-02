package controlworker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

// Q03 export executor integration against real PostgreSQL + a fake event
// store (gated: TUBA_TEST_DATABASE_URL).

func exportExecutorFixture(t *testing.T, esResponse string) (ExportExecutor, *control.Store) {
	t.Helper()
	pool := integrationPool(t)
	_, err := pool.Exec(context.Background(), `
		INSERT INTO identities(issuer,subject) VALUES('issuer','export-exec'),('issuer','export-revoker') ON CONFLICT DO NOTHING;
		INSERT INTO memberships(organization_id,identity_id,role_id)
		  SELECT o.id,i.id,r.id FROM organizations o,identities i,roles r
		  WHERE o.slug='tenant_a' AND i.issuer='issuer' AND i.subject IN ('export-exec','export-revoker') AND r.organization_id=o.id AND r.name='analyst' ON CONFLICT DO NOTHING`)
	if err != nil {
		t.Fatal(err)
	}
	store := &control.Store{Pool: pool, Issuer: "issuer"}
	return ExportExecutor{Config: ExportExecutorConfig{
		Store: store, ES: fakeES(t, esResponse, nil), ExportRoot: t.TempDir(), WorkerID: "test-worker",
	}}, store
}

var exportExecPrincipal = auth.Principal{Subject: "export-exec", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"analyst"}}

func createExecutorExport(t *testing.T, store *control.Store, p auth.Principal) control.Export {
	t.Helper()
	now := time.Now().UTC()
	export, status, err := store.CreateExport(context.Background(), p, control.CreateExportInput{
		Dataset: "authentication", Format: control.ExportFormatNDJSON, Query: "search authentication",
		Generation: "g1", From: now.Add(-time.Hour), To: now, RowLimit: 100, ByteLimit: 1 << 20,
		IncludeSensitive: p.Can("sensitive:read"), IncludeRaw: p.Can("raw:read"),
	}, "req-exec")
	if err != nil || status != 201 {
		t.Fatalf("create: %d %v", status, err)
	}
	t.Cleanup(func() {
		if _, err := store.Pool.Exec(context.Background(), `DELETE FROM processing_jobs WHERE id=$1`, export.JobID); err != nil {
			t.Fatal(err)
		}
	})
	return export
}

func TestExportExecutorEndToEndIntegration(t *testing.T) {
	executor, store := exportExecutorFixture(t, exportHitsPage)
	export := createExecutorExport(t, store, exportExecPrincipal)

	err := executor.execute(context.Background(), Job{ID: export.JobID})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadExportForJob(context.Background(), export.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != control.ExportStateSucceeded || loaded.ExpiresAt == nil || loaded.CompletedAt == nil {
		t.Fatalf("export not completed: %+v", loaded)
	}
	if loaded.RetentionFrom == nil {
		t.Fatal("retention boundary not pinned")
	}
	if loaded.RowCount == nil || *loaded.RowCount != 2 || loaded.FileSHA256 == "" {
		t.Fatalf("result: %+v", loaded)
	}

	// Analyst policy: sensitive visible, original payload absent (raw:read not held).
	path, err := ExportFilePath(executor.Config.ExportRoot, export.ID, control.ExportFormatNDJSON)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "alice") {
		t.Fatalf("sensitive field missing for sensitive:read export: %s", content)
	}
	if strings.Contains(string(content), "raw-secret") {
		t.Fatalf("original payload leaked without raw:read: %s", content)
	}
	var n int
	if err := store.Pool.QueryRow(context.Background(), `SELECT count(*) FROM audit_events WHERE resource_type='export' AND resource_id=$1 AND action='export.complete'`, export.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("export.complete audit: %d %v", n, err)
	}
}

func TestExportExecutorRetentionExceededIntegration(t *testing.T) {
	executor, store := exportExecutorFixture(t, exportHitsPage)
	export := createExecutorExport(t, store, exportExecPrincipal)
	// Move the frozen range outside retention; execution must fail with
	// retention_exceeded, never silently truncate.
	if _, err := store.Pool.Exec(context.Background(), `UPDATE export_jobs SET from_ts=now()-interval '10 days', to_ts=now()-interval '9 days' WHERE id=$1`, export.ID); err != nil {
		t.Fatal(err)
	}
	err := executor.execute(context.Background(), Job{ID: export.JobID})
	if err == nil || !strings.Contains(err.Error(), "retention_exceeded") {
		t.Fatalf("expected retention_exceeded, got %v", err)
	}
	loaded, _ := store.LoadExportForJob(context.Background(), export.JobID)
	if loaded.State != control.ExportStateFailed || !strings.Contains(loaded.Error, "retention_exceeded") {
		t.Fatalf("export not failed with retention_exceeded: %+v", loaded)
	}
	var n int
	if err := store.Pool.QueryRow(context.Background(), `SELECT count(*) FROM audit_events WHERE resource_type='export' AND resource_id=$1 AND action='export.failed'`, export.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("export.failed audit: %d %v", n, err)
	}
}

func TestExportExecutorCreatorRevokedIntegration(t *testing.T) {
	executor, store := exportExecutorFixture(t, exportHitsPage)
	revoker := auth.Principal{Subject: "export-revoker", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"analyst"}}
	export := createExecutorExport(t, store, revoker)
	// Revoke the creator's only membership between creation and execution:
	// completion re-authorization must fail closed.
	if _, err := store.Pool.Exec(context.Background(), `
		UPDATE memberships SET revoked_at=now()
		WHERE identity_id=(SELECT id FROM identities WHERE issuer='issuer' AND subject='export-revoker') AND revoked_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Restore an active membership so later runs stay idempotent.
		if _, err := store.Pool.Exec(context.Background(), `
			INSERT INTO memberships(organization_id,identity_id,role_id)
			SELECT o.id,i.id,r.id FROM organizations o,identities i,roles r
			WHERE o.slug='tenant_a' AND i.issuer='issuer' AND i.subject='export-revoker' AND r.organization_id=o.id AND r.name='analyst' ON CONFLICT DO NOTHING`); err != nil {
			t.Fatal(err)
		}
	}()
	err := executor.execute(context.Background(), Job{ID: export.JobID})
	if err == nil || !strings.Contains(err.Error(), "re-authorization") {
		t.Fatalf("expected re-authorization failure, got %v", err)
	}
	loaded, _ := store.LoadExportForJob(context.Background(), export.JobID)
	if loaded.State != control.ExportStateFailed {
		t.Fatalf("state=%s", loaded.State)
	}
}

func TestExportExpirerRemovesFileIntegration(t *testing.T) {
	executor, store := exportExecutorFixture(t, exportHitsPage)
	export := createExecutorExport(t, store, exportExecPrincipal)
	if err := executor.execute(context.Background(), Job{ID: export.JobID}); err != nil {
		t.Fatal(err)
	}
	path, err := ExportFilePath(executor.Config.ExportRoot, export.ID, control.ExportFormatNDJSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool.Exec(context.Background(), `UPDATE export_jobs SET expires_at=now()-interval '1 minute' WHERE id=$1`, export.ID); err != nil {
		t.Fatal(err)
	}
	expirer := ExportExpirer{Config: ExportExpirerConfig{Store: store, ExportRoot: executor.Config.ExportRoot}}
	if err := expirer.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expired export file still present: %v", err)
	}
	loaded, _ := store.LoadExportForJob(context.Background(), export.JobID)
	if loaded.State != control.ExportStateExpired {
		t.Fatalf("state=%s", loaded.State)
	}
}
