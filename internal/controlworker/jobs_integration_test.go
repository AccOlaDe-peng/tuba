package controlworker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// insertIntegrationJob creates a queued job under a dedicated organization so
// the test never touches tenant data. Returns the job id.
func insertIntegrationJob(t *testing.T, pool *pgxpool.Pool, maxAttempts int) string {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("controlworker_it_%d", time.Now().UnixNano())
	var orgID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, slug).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM processing_jobs WHERE organization_id=$1`, orgID); err != nil {
			t.Logf("cleanup jobs: %v", err)
		}
		if _, err := pool.Exec(context.Background(), `DELETE FROM organizations WHERE id=$1`, orgID); err != nil {
			t.Logf("cleanup org: %v", err)
		}
	})
	var jobID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO processing_jobs(organization_id, namespace, job_type, generation, request, max_attempts)
		VALUES($1, $2, 'replay', 'g1', '{}'::jsonb, $3) RETURNING id::text`, orgID, slug, maxAttempts).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	return jobID
}

func integrationConfig(pool *pgxpool.Pool, workerID string) JobWorkerConfig {
	return JobWorkerConfig{
		Pool:          pool,
		WorkerID:      workerID,
		Handlers:      map[string]JobHandler{"replay": func(context.Context, Job) error { return nil }},
		PollInterval:  50 * time.Millisecond,
		LeaseDuration: 30 * time.Second,
	}
}

func claimByID(ctx context.Context, c JobWorkerConfig) (Job, error) {
	return claimJob(ctx, c, []string{"replay"})
}

func TestJobClaimIsExclusiveUnderConcurrencyIntegration(t *testing.T) {
	pool := integrationPool(t)
	jobID := insertIntegrationJob(t, pool, 3)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i, worker := range []string{"worker-a", "worker-b"} {
		wg.Add(1)
		go func(i int, worker string) {
			defer wg.Done()
			_, err := claimByID(ctx, integrationConfig(pool, worker))
			results[i] = err
		}(i, worker)
	}
	wg.Wait()
	succeeded := 0
	for _, err := range results {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly one worker to claim the job, got %d", succeeded)
	}
	// Exactly one attempt row exists for this job, owned by the winner.
	var attemptCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processing_job_attempts WHERE job_id=$1`, jobID).Scan(&attemptCount); err != nil {
		t.Fatal(err)
	}
	if attemptCount != 1 {
		t.Fatalf("expected exactly one running attempt for the claimed job, got %d", attemptCount)
	}
}

func TestJobFencingTokenRejectsStaleLeaseIntegration(t *testing.T) {
	pool := integrationPool(t)
	jobID := insertIntegrationJob(t, pool, 3)
	ctx := context.Background()
	stale, err := claimByID(ctx, integrationConfig(pool, "worker-a"))
	if err != nil {
		t.Fatal(err)
	}
	// The lease expires and a second worker reaps and reclaims the job, which
	// increments the fencing token.
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET lease_until = now() - interval '1 second' WHERE id=$1`, stale.ID); err != nil {
		t.Fatal(err)
	}
	if err := reapExpiredJobs(ctx, pool, 10); err != nil {
		t.Fatal(err)
	}
	// 回收重排队带有界退避（生产语义）；测试直接拨正到可立即领取。
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET next_attempt_at = now() WHERE id=$1`, stale.ID); err != nil {
		t.Fatal(err)
	}
	fresh, err := claimByID(ctx, integrationConfig(pool, "worker-b"))
	if err != nil {
		t.Fatal(err)
	}
	if fresh.FencingToken <= stale.FencingToken {
		t.Fatalf("fencing token did not advance: stale=%d fresh=%d", stale.FencingToken, fresh.FencingToken)
	}
	// The stale holder's writes must fail closed.
	if err := finishJob(ctx, integrationConfig(pool, "worker-a"), stale, nil, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale finish accepted: %v", err)
	}
	if _, err := renewJobLease(ctx, pool, "worker-a", stale, 30*time.Second); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("stale heartbeat accepted: %v", err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM processing_jobs WHERE id=$1`, jobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(JobStateRunning) {
		t.Fatalf("stale write mutated job: state=%s", state)
	}
	// The fresh holder completes normally.
	if err := finishJob(ctx, integrationConfig(pool, "worker-b"), fresh, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM processing_jobs WHERE id=$1`, jobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(JobStateSucceeded) {
		t.Fatalf("state=%s, want succeeded", state)
	}
}

func TestJobHeartbeatRenewsLeaseIntegration(t *testing.T) {
	pool := integrationPool(t)
	insertIntegrationJob(t, pool, 3)
	ctx := context.Background()
	cfg := integrationConfig(pool, "worker-a")
	job, err := claimByID(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var before time.Time
	if err := pool.QueryRow(ctx, `SELECT lease_until FROM processing_jobs WHERE id=$1`, job.ID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	cancelRequested, err := renewJobLease(ctx, pool, "worker-a", job, cfg.LeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	if cancelRequested {
		t.Fatal("unexpected cancellation")
	}
	var after time.Time
	if err := pool.QueryRow(ctx, `SELECT lease_until FROM processing_jobs WHERE id=$1`, job.ID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Fatalf("heartbeat did not extend lease: before=%v after=%v", before, after)
	}
	// Another worker must not be able to renew a lease it does not own.
	if _, err := renewJobLease(ctx, pool, "worker-b", job, cfg.LeaseDuration); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("foreign worker renewed lease: %v", err)
	}
}

func TestJobCancellationIntegration(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	// Queued job with a cancel request is moved to cancelled by the sweeper.
	queuedID := insertIntegrationJob(t, pool, 3)
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET cancel_requested_at=now() WHERE id=$1`, queuedID); err != nil {
		t.Fatal(err)
	}
	if err := cancelQueuedJobs(ctx, pool, 10); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM processing_jobs WHERE id=$1`, queuedID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(JobStateCancelled) {
		t.Fatalf("queued cancel: state=%s, want cancelled", state)
	}
	// A cancel request on a running job wins over a successful handler result.
	runningID := insertIntegrationJob(t, pool, 3)
	cfg := integrationConfig(pool, "worker-a")
	job, err := claimByID(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET cancel_requested_at=now() WHERE id=$1`, runningID); err != nil {
		t.Fatal(err)
	}
	cancelRequested, err := renewJobLease(ctx, pool, "worker-a", job, cfg.LeaseDuration)
	if err != nil || !cancelRequested {
		t.Fatalf("heartbeat must observe cancellation: %v %v", cancelRequested, err)
	}
	if err := finishJob(ctx, cfg, job, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM processing_jobs WHERE id=$1`, runningID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(JobStateCancelled) {
		t.Fatalf("running cancel: state=%s, want cancelled", state)
	}
}

func TestJobRetryBackoffAndExhaustionIntegration(t *testing.T) {
	pool := integrationPool(t)
	jobID := insertIntegrationJob(t, pool, 2)
	ctx := context.Background()
	cfg := integrationConfig(pool, "worker-a")
	for attempt := 1; attempt <= 2; attempt++ {
		job, err := claimByID(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if job.Attempt != attempt {
			t.Fatalf("attempt=%d, want %d", job.Attempt, attempt)
		}
		if err := finishJob(ctx, cfg, job, errors.New("handler exploded"), false); err != nil {
			t.Fatal(err)
		}
		var state string
		var next time.Time
		if err := pool.QueryRow(ctx, `SELECT state, next_attempt_at FROM processing_jobs WHERE id=$1`, jobID).Scan(&state, &next); err != nil {
			t.Fatal(err)
		}
		if attempt < 2 {
			if state != string(JobStateQueued) {
				t.Fatalf("attempt %d: state=%s, want queued", attempt, state)
			}
			delay := time.Until(next)
			if delay > MaxRetryBackoff {
				t.Fatalf("attempt %d: requeue delay %v exceeds cap %v", attempt, delay, MaxRetryBackoff)
			}
			if want := RetryBackoff(attempt); delay < want-5*time.Second {
				t.Fatalf("attempt %d: requeue delay %v, want about %v", attempt, delay, want)
			}
			// 退避是生产语义；测试拨正为可立即领取，避免空等。
			if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET next_attempt_at = now() WHERE id=$1`, jobID); err != nil {
				t.Fatal(err)
			}
		} else if state != string(JobStateFailed) {
			t.Fatalf("exhausted: state=%s, want failed", state)
		}
	}
}

func TestJobShutdownRequeuesWithoutFailureIntegration(t *testing.T) {
	pool := integrationPool(t)
	jobID := insertIntegrationJob(t, pool, 1)
	ctx := context.Background()
	cfg := integrationConfig(pool, "worker-a")
	job, err := claimByID(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Even with the attempt budget fully spent, a worker shutdown requeues.
	if err := finishJob(ctx, cfg, job, errors.New("interrupted"), true); err != nil {
		t.Fatal(err)
	}
	var state, attemptState string
	if err := pool.QueryRow(ctx, `SELECT j.state, a.state FROM processing_jobs j JOIN processing_job_attempts a ON a.job_id=j.id AND a.attempt=j.attempt WHERE j.id=$1`, jobID).Scan(&state, &attemptState); err != nil {
		t.Fatal(err)
	}
	if state != string(JobStateQueued) || attemptState != string(AttemptStateLeaseExpired) {
		t.Fatalf("state=%s attempt=%s, want queued/lease_expired", state, attemptState)
	}
}

func TestReapExpiredJobsIntegration(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	// Budget remaining: requeued with bounded backoff.
	retryableID := insertIntegrationJob(t, pool, 5)
	// Budget spent: failed terminally.
	exhaustedID := insertIntegrationJob(t, pool, 1)
	cfg := integrationConfig(pool, "worker-a")
	if _, err := claimByID(ctx, cfg); err != nil { // claims the older of the two jobs
		t.Fatal(err)
	}
	if _, err := claimByID(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET lease_until = now() - interval '1 second' WHERE id = ANY($1::uuid[])`, []string{exhaustedID, retryableID}); err != nil {
		t.Fatal(err)
	}
	if err := reapExpiredJobs(ctx, pool, 10); err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	rows, err := pool.Query(ctx, `SELECT id::text, state FROM processing_jobs WHERE id = ANY($1::uuid[])`, []string{exhaustedID, retryableID})
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, state string
		if err := rows.Scan(&id, &state); err != nil {
			t.Fatal(err)
		}
		states[id] = state
	}
	rows.Close()
	if states[retryableID] != string(JobStateQueued) {
		t.Fatalf("retryable: state=%s, want queued", states[retryableID])
	}
	if states[exhaustedID] != string(JobStateFailed) {
		t.Fatalf("exhausted: state=%s, want failed", states[exhaustedID])
	}
}
