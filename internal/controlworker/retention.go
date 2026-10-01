package controlworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/telemetry"
)

// RetentionConfig configures the periodic cleaner for processor inbox,
// processor outbox, processing-job state, and per-job temporary directories.
// Every protection rule is fail-closed: only rows that are terminal, older
// than their retention, and provably unreferenced are deleted; anything
// uncertain is kept.
type RetentionConfig struct {
	Pool    *pgxpool.Pool
	Metrics *telemetry.Registry
	// InboxRetention bounds processor_inbox dedup records. The bound is the
	// retention of the topics a redelivery could come from, same as the
	// ingest_receipts window (scripts/prune_ingest_receipts.sh): Kafka keeps
	// 24 hours, so twice that is safe. Default 2 days.
	InboxRetention time.Duration
	// OutboxRetention bounds processor_outbox rows after delivery. Only rows
	// with sent_at set and no live lease are ever deleted; undelivered,
	// retrying, or dead-but-blocked rows are kept. Default 2 days.
	OutboxRetention time.Duration
	// SucceededJobRetention bounds terminal processing_jobs rows in state
	// succeeded. Default 7 days.
	SucceededJobRetention time.Duration
	// FailedJobRetention bounds terminal processing_jobs rows in state failed
	// or cancelled. These rows carry failure/cancellation evidence (last_error,
	// attempts), so they are retained longer than succeeded jobs. Default 30 days.
	FailedJobRetention time.Duration
	// BatchSize bounds each DELETE statement. Default 10000.
	BatchSize int
	// MaxBatchesPerSweep caps batches per table per sweep so a future change
	// cannot turn the loop into an endless delete against a live table.
	// Default 1000.
	MaxBatchesPerSweep int
	// PollInterval is the delay between sweeps in Run. Default 1 hour.
	PollInterval time.Duration
	// TempRoot is the root directory holding per-job temporary directories
	// (one per job id, see JobTempDir). When empty the filesystem sweep is
	// disabled; database retention still runs.
	TempRoot string
}

type RetentionCleaner struct{ Config RetentionConfig }

func normalizeRetentionConfig(c RetentionConfig) RetentionConfig {
	if c.InboxRetention <= 0 {
		c.InboxRetention = 48 * time.Hour
	}
	if c.OutboxRetention <= 0 {
		c.OutboxRetention = 48 * time.Hour
	}
	if c.SucceededJobRetention <= 0 {
		c.SucceededJobRetention = 7 * 24 * time.Hour
	}
	if c.FailedJobRetention <= 0 {
		c.FailedJobRetention = 30 * 24 * time.Hour
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10000
	}
	if c.MaxBatchesPerSweep <= 0 {
		c.MaxBatchesPerSweep = 1000
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Hour
	}
	return c
}

func (r RetentionCleaner) add(name string, n int) {
	if r.Config.Metrics != nil && n > 0 {
		r.Config.Metrics.Add(name, uint64(n))
	}
}

func (r RetentionCleaner) inc(name string) {
	if r.Config.Metrics != nil {
		r.Config.Metrics.Inc(name)
	}
}

// Run sweeps immediately and then on every PollInterval until ctx ends.
func (r RetentionCleaner) Run(ctx context.Context) error {
	c := normalizeRetentionConfig(r.Config)
	if c.Pool == nil {
		return errors.New("retention cleaner requires PostgreSQL")
	}
	for ctx.Err() == nil {
		if err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
			slog.Error("retention sweep failed", "error", err)
		}
		if !wait(ctx, c.PollInterval) {
			return nil
		}
	}
	return nil
}

// Sweep runs one retention pass over inbox, outbox, job state, and job
// temporary directories. It is idempotent: a sweep that finds nothing eligible
// deletes nothing.
func (r RetentionCleaner) Sweep(ctx context.Context) error {
	c := normalizeRetentionConfig(r.Config)
	if c.Pool == nil {
		return errors.New("retention cleaner requires PostgreSQL")
	}
	// Fixed cutoffs for the whole sweep, resolved once like
	// prune_ingest_receipts.sh: a moving cutoff keeps admitting rows that age
	// past it while batches run.
	now := time.Now()
	inboxCutoff := now.Add(-c.InboxRetention)
	outboxCutoff := now.Add(-c.OutboxRetention)
	succeededCutoff := now.Add(-c.SucceededJobRetention)
	failedCutoff := now.Add(-c.FailedJobRetention)

	deleted, err := deleteInBatches(ctx, c.Pool, "processor_inbox",
		`SELECT ctid FROM processor_inbox WHERE processed_at < $1 ORDER BY processed_at LIMIT $2`,
		[]any{inboxCutoff, c.BatchSize}, c.MaxBatchesPerSweep)
	if err != nil {
		r.inc("retention_sweep_failures_total")
		return fmt.Errorf("inbox retention: %w", err)
	}
	r.add("retention_inbox_deleted_total", deleted)

	deleted, err = deleteInBatches(ctx, c.Pool, "processor_outbox",
		`SELECT ctid FROM processor_outbox WHERE sent_at IS NOT NULL AND sent_at < $1 AND lease_owner IS NULL ORDER BY sent_at LIMIT $2`,
		[]any{outboxCutoff, c.BatchSize}, c.MaxBatchesPerSweep)
	if err != nil {
		r.inc("retention_sweep_failures_total")
		return fmt.Errorf("outbox retention: %w", err)
	}
	r.add("retention_outbox_deleted_total", deleted)

	deleted, err = r.sweepJobs(ctx, c, succeededCutoff, failedCutoff)
	if err != nil {
		r.inc("retention_sweep_failures_total")
		return fmt.Errorf("job state retention: %w", err)
	}
	r.add("retention_jobs_deleted_total", deleted)

	if c.TempRoot != "" {
		removed, err := r.sweepTempDirs(ctx, c, succeededCutoff, failedCutoff)
		if err != nil {
			r.inc("retention_sweep_failures_total")
			return fmt.Errorf("job temp file retention: %w", err)
		}
		r.add("retention_temp_dirs_deleted_total", removed)
	}
	return nil
}

// deleteInBatches deletes rows in ctid-bounded batches so no single statement
// holds locks across the whole table while workers write to it. The selector
// must be a SELECT of ctid values and must already encode every protection
// condition; it is never constructed from caller input.
func deleteInBatches(ctx context.Context, pool *pgxpool.Pool, table, selector string, args []any, maxBatches int) (int, error) {
	statement := fmt.Sprintf("WITH doomed AS (%s) DELETE FROM %s WHERE ctid IN (SELECT ctid FROM doomed)", selector, table)
	total := 0
	for batch := 0; ; batch++ {
		if batch >= maxBatches {
			return total, fmt.Errorf("stopped after %d batches with rows still eligible; inspect before retrying", maxBatches)
		}
		result, err := pool.Exec(ctx, statement, args...)
		if err != nil {
			return total, err
		}
		n := int(result.RowsAffected())
		total += n
		if n == 0 {
			return total, nil
		}
	}
}

// jobDeletable is the fail-closed decision for one processing_jobs row:
// terminal state, finished_at set and older than the retention for its
// category, and no live lease. Anything else — queued, running, missing
// finished_at, an unexpected state — is kept.
func jobDeletable(state JobState, finishedAt *time.Time, leaseOwner *string, succeededCutoff, failedCutoff time.Time) bool {
	if leaseOwner != nil {
		return false
	}
	if finishedAt == nil {
		return false
	}
	switch state {
	case JobStateSucceeded:
		return finishedAt.Before(succeededCutoff)
	case JobStateFailed, JobStateCancelled:
		return finishedAt.Before(failedCutoff)
	default:
		return false
	}
}

// sweepJobs deletes eligible terminal jobs in batches and removes each deleted
// job's temporary directory. Attempt rows cascade with the job. Jobs with a
// live lease, a non-terminal state, or a missing finished_at are kept even
// when old; failed and cancelled jobs are kept for the longer evidence window.
func (r RetentionCleaner) sweepJobs(ctx context.Context, c RetentionConfig, succeededCutoff, failedCutoff time.Time) (int, error) {
	total := 0
	for batch := 0; ; batch++ {
		if batch >= c.MaxBatchesPerSweep {
			return total, fmt.Errorf("stopped after %d batches with rows still eligible; inspect before retrying", c.MaxBatchesPerSweep)
		}
		rows, err := c.Pool.Query(ctx, `
			WITH doomed AS (
				SELECT ctid FROM processing_jobs
				WHERE finished_at IS NOT NULL AND lease_owner IS NULL AND (
					(state = 'succeeded' AND finished_at < $1) OR
					(state IN ('failed','cancelled') AND finished_at < $2)
				)
				ORDER BY finished_at LIMIT $3
			)
			DELETE FROM processing_jobs WHERE ctid IN (SELECT ctid FROM doomed)
			RETURNING id::text`, succeededCutoff, failedCutoff, c.BatchSize)
		if err != nil {
			return total, err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return total, err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return total, err
		}
		rows.Close()
		if c.TempRoot != "" {
			for _, id := range ids {
				removed, err := removeJobTempDir(c.TempRoot, id)
				if err != nil {
					slog.Error("job temp directory removal failed", "job_id", id, "error", err)
					continue
				}
				if removed {
					r.add("retention_temp_dirs_deleted_total", 1)
				}
			}
		}
		total += len(ids)
		if len(ids) == 0 {
			return total, nil
		}
	}
}

// JobTempDir returns the per-job temporary directory under root. The job id
// must be a canonical UUID so a malformed id can never escape the root; on
// violation it fails closed with an error instead of a path.
func JobTempDir(root, jobID string) (string, error) {
	if root == "" {
		return "", errors.New("job temp root is not configured")
	}
	if !isCanonicalUUID(jobID) {
		return "", fmt.Errorf("job id %q is not a canonical UUID; refusing to derive a temp path", jobID)
	}
	return filepath.Join(root, jobID), nil
}

// RegisterJobTempDir creates the per-job temporary directory and returns its
// path. Handlers register files by writing them inside this directory; the
// retention cleaner removes the directory once the job's own state row is
// deleted or provably terminal and past its retention.
func RegisterJobTempDir(root, jobID string) (string, error) {
	dir, err := JobTempDir(root, jobID)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

func isCanonicalUUID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	for i := 0; i < len(id); i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// removeJobTempDir removes root/jobID when the name is a canonical UUID;
// anything else is refused. It returns whether a directory was removed.
func removeJobTempDir(root, jobID string) (bool, error) {
	dir, err := JobTempDir(root, jobID)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("job temp path %s is not a directory; refusing to remove", dir)
	}
	return true, os.RemoveAll(dir)
}

// sweepTempDirs removes temp directories whose job row is already gone (the
// job retention pass deleted it) or provably terminal and past retention.
// Directories for queued/running/leased jobs, directories whose job lookup
// fails, and entries that are not canonical UUID directories are all kept.
func (r RetentionCleaner) sweepTempDirs(ctx context.Context, c RetentionConfig, succeededCutoff, failedCutoff time.Time) (int, error) {
	entries, err := os.ReadDir(c.TempRoot)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() || !isCanonicalUUID(entry.Name()) {
			continue
		}
		jobID := entry.Name()
		var state JobState
		var finishedAt *time.Time
		var leaseOwner *string
		err := c.Pool.QueryRow(ctx,
			`SELECT state, finished_at, lease_owner FROM processing_jobs WHERE id = $1`, jobID).
			Scan(&state, &finishedAt, &leaseOwner)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			slog.Error("job lookup for temp directory failed; keeping directory", "job_id", jobID, "error", err)
			continue
		}
		orphaned := errors.Is(err, pgx.ErrNoRows)
		if !orphaned && !jobDeletable(state, finishedAt, leaseOwner, succeededCutoff, failedCutoff) {
			continue
		}
		ok, err := removeJobTempDir(c.TempRoot, jobID)
		if err != nil {
			slog.Error("job temp directory removal failed", "job_id", jobID, "error", err)
			continue
		}
		if ok {
			removed++
		}
	}
	return removed, nil
}
