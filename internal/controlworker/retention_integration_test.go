package controlworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/telemetry"
)

// The fixture uses a dedicated organization, consumer group, outbox producer,
// and temp root so it never touches tenant data; everything is deleted at the
// end.

func retentionFixture(t *testing.T) (context.Context, *pgxpool.Pool, string, string) {
	t.Helper()
	ctx := context.Background()
	pool := integrationPool(t)
	suffix := fmt.Sprintf("retention_it_%d", time.Now().UnixNano())
	var orgID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, suffix).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean := context.Background()
		if _, err := pool.Exec(clean, `DELETE FROM processing_jobs WHERE organization_id=$1`, orgID); err != nil {
			t.Logf("cleanup jobs: %v", err)
		}
		if _, err := pool.Exec(clean, `DELETE FROM processor_inbox WHERE consumer_group=$1`, suffix); err != nil {
			t.Logf("cleanup inbox: %v", err)
		}
		if _, err := pool.Exec(clean, `DELETE FROM processor_outbox WHERE producer=$1`, suffix); err != nil {
			t.Logf("cleanup outbox: %v", err)
		}
		if _, err := pool.Exec(clean, `DELETE FROM organizations WHERE id=$1`, orgID); err != nil {
			t.Logf("cleanup org: %v", err)
		}
	})
	return ctx, pool, orgID, suffix
}

func insertRetentionJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID, state string, finishedAt *time.Time) string {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
		INSERT INTO processing_jobs(organization_id, namespace, job_type, generation, request,
			state, finished_at, lease_owner, lease_until)
		VALUES($1, 'retention-it', 'replay', 'g1', '{}'::jsonb, $2::text, $3,
			CASE WHEN $2::text = 'running' THEN 'retention-it-worker' END,
			CASE WHEN $2::text = 'running' THEN now() - interval '1 hour' END)
		RETURNING id::text`, orgID, state, finishedAt).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insertInboxRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, group, messageID string, offset int64, processedAt time.Time) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO processor_inbox(consumer_group, message_id, topic, partition, message_offset, payload_hash, processed_at)
		VALUES($1, $2, 'retention-it', 0, $3, $4, $5)`,
		group, messageID, offset,
		"0000000000000000000000000000000000000000000000000000000000000001", processedAt)
	if err != nil {
		t.Fatal(err)
	}
}



func insertOutboxRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, producer string, sequence int64, sentAt *time.Time, leased bool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO processor_outbox(producer, aggregate_key, sequence, topic, message_key, payload, payload_hash,
			sent_at, lease_owner, lease_until)
		VALUES($1, 'retention-it', $2, 'retention-it', '\\x00', '{}'::jsonb, $3, $4,
			CASE WHEN $5 THEN 'retention-it-publisher' END,
			CASE WHEN $5 THEN now() + interval '1 hour' END)`,
		producer, sequence,
		"0000000000000000000000000000000000000000000000000000000000000002", sentAt, leased)
	if err != nil {
		t.Fatal(err)
	}
}

func countWhere(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRetentionSweepIntegration(t *testing.T) {
	ctx, pool, orgID, group := retentionFixture(t)
	now := time.Now()
	old := now.Add(-3 * 24 * time.Hour)

	// Inbox: one record past the window, one inside it.
	insertInboxRow(t, ctx, pool, group, group+"-old", 1, old)
	insertInboxRow(t, ctx, pool, group, group+"-new", 2, now)

	// Outbox: delivered past the window (eligible), delivered inside the window
	// (kept), undelivered (kept, undrained), delivered old but still leased
	// (kept, fail-closed).
	sentOld, sentNew := old, now
	insertOutboxRow(t, ctx, pool, group, 1, &sentOld, false)
	insertOutboxRow(t, ctx, pool, group, 2, &sentNew, false)
	insertOutboxRow(t, ctx, pool, group, 3, nil, false)
	insertOutboxRow(t, ctx, pool, group, 4, &sentOld, true)

	// Jobs: eligible terminal rows, evidence-window rows, and a running row
	// with an expired lease that must still be protected.
	succeededOld := insertRetentionJob(t, ctx, pool, orgID, "succeeded", ptrTime(now.Add(-10*24*time.Hour)))
	insertRetentionJob(t, ctx, pool, orgID, "succeeded", ptrTime(now.Add(-time.Hour)))
	insertRetentionJob(t, ctx, pool, orgID, "failed", ptrTime(now.Add(-40*24*time.Hour)))
	insertRetentionJob(t, ctx, pool, orgID, "failed", ptrTime(now.Add(-10*24*time.Hour)))
	insertRetentionJob(t, ctx, pool, orgID, "cancelled", ptrTime(now.Add(-40*24*time.Hour)))
	insertRetentionJob(t, ctx, pool, orgID, "cancelled", ptrTime(now.Add(-time.Hour)))
	runningID := insertRetentionJob(t, ctx, pool, orgID, "running", nil)
	insertRetentionJob(t, ctx, pool, orgID, "queued", nil)

	// Temp directories: one for the eligible succeeded job, one for the
	// running job, one orphan whose job row never existed, one entry that is
	// not a canonical UUID.
	tempRoot := t.TempDir()
	for _, id := range []string{succeededOld, runningID, "123e4567-e89b-42d3-a456-426614174000"} {
		dir, err := RegisterJobTempDir(tempRoot, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "scratch.bin"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(tempRoot, "not-a-job"), 0o700); err != nil {
		t.Fatal(err)
	}

	metrics := telemetry.New()
	cleaner := RetentionCleaner{Config: RetentionConfig{
		Pool: pool, Metrics: metrics, TempRoot: tempRoot, BatchSize: 100,
	}}
	if err := cleaner.Sweep(ctx); err != nil {
		t.Fatal(err)
	}

	if got := countWhere(t, ctx, pool, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1`, group); got != 1 {
		t.Fatalf("inbox survivors: %d, want 1 (only the in-window record)", got)
	}
	if got := countWhere(t, ctx, pool, `SELECT count(*) FROM processor_outbox WHERE producer=$1`, group); got != 3 {
		t.Fatalf("outbox survivors: %d, want 3 (recent sent, unsent, leased)", got)
	}
	if got := countWhere(t, ctx, pool,
		`SELECT count(*) FROM processor_outbox WHERE producer=$1 AND sequence=1`, group); got != 0 {
		t.Fatal("delivered outbox row past the window must be deleted")
	}
	if got := countWhere(t, ctx, pool,
		`SELECT count(*) FROM processing_jobs WHERE organization_id=$1`, orgID); got != 5 {
		t.Fatalf("job survivors: %d, want 5 (recent succeeded, evidence-window failed/cancelled, running, queued)", got)
	}
	if got := countWhere(t, ctx, pool,
		`SELECT count(*) FROM processing_jobs WHERE organization_id=$1 AND state='running'`, orgID); got != 1 {
		t.Fatal("active-lease job must be protected")
	}
	for _, kept := range []string{runningID, "not-a-job"} {
		if _, err := os.Stat(filepath.Join(tempRoot, kept)); err != nil {
			t.Fatalf("temp entry %s must be kept: %v", kept, err)
		}
	}
	for _, removed := range []string{succeededOld, "123e4567-e89b-42d3-a456-426614174000"} {
		if _, err := os.Stat(filepath.Join(tempRoot, removed)); !os.IsNotExist(err) {
			t.Fatalf("temp dir %s must be removed", removed)
		}
	}
	if metrics.Value("retention_inbox_deleted_total") != 1 ||
		metrics.Value("retention_outbox_deleted_total") != 1 ||
		metrics.Value("retention_jobs_deleted_total") != 3 {
		t.Fatalf("metrics: inbox=%d outbox=%d jobs=%d",
			metrics.Value("retention_inbox_deleted_total"),
			metrics.Value("retention_outbox_deleted_total"),
			metrics.Value("retention_jobs_deleted_total"))
	}

	// Idempotency: a second sweep deletes nothing.
	if err := cleaner.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if metrics.Value("retention_inbox_deleted_total") != 1 ||
		metrics.Value("retention_outbox_deleted_total") != 1 ||
		metrics.Value("retention_jobs_deleted_total") != 3 {
		t.Fatal("second sweep must be a no-op")
	}
}
