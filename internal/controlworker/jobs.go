package controlworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	ID             string          `json:"id"`
	OrganizationID string          `json:"organization_id"`
	Namespace      string          `json:"namespace"`
	Type           string          `json:"job_type"`
	Generation     string          `json:"generation"`
	ReleaseID      string          `json:"release_id,omitempty"`
	Request        json.RawMessage `json:"request"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	FencingToken   int64           `json:"fencing_token"`
}

type JobHandler func(context.Context, Job) error

type JobWorkerConfig struct {
	Pool          *pgxpool.Pool
	WorkerID      string
	Handlers      map[string]JobHandler
	PollInterval  time.Duration
	LeaseDuration time.Duration
}

type JobWorker struct{ Config JobWorkerConfig }

func (w JobWorker) Run(ctx context.Context) error {
	c := w.Config
	if c.Pool == nil || c.WorkerID == "" {
		return errors.New("job worker requires PostgreSQL and worker ID")
	}
	types := make([]string, 0, len(c.Handlers))
	for kind, handler := range c.Handlers {
		if kind == "" || handler == nil {
			return errors.New("job handler registration contains an empty type or nil handler")
		}
		types = append(types, kind)
	}
	sort.Strings(types)
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.LeaseDuration < 3*time.Second {
		c.LeaseDuration = 30 * time.Second
	}
	for ctx.Err() == nil {
		if err := cancelQueuedJobs(ctx, c.Pool, 100); err != nil {
			slog.Error("cancel queued processing jobs failed", "error", err)
		}
		if err := reapExpiredJobs(ctx, c.Pool, 50); err != nil {
			slog.Error("reap expired processing jobs failed", "error", err)
		}
		if len(types) == 0 {
			if !wait(ctx, c.PollInterval) {
				return nil
			}
			continue
		}
		job, err := claimJob(ctx, c, types)
		if errors.Is(err, pgx.ErrNoRows) {
			if !wait(ctx, c.PollInterval) {
				return nil
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("claim processing job failed", "error", err)
			if !wait(ctx, c.PollInterval) {
				return nil
			}
			continue
		}
		handlerCtx, cancel := context.WithCancel(ctx)
		heartbeatDone := make(chan struct{})
		go func() {
			defer close(heartbeatDone)
			w.heartbeat(handlerCtx, c, cancel, job)
		}()
		handlerErr := c.Handlers[job.Type](handlerCtx, job)
		cancel()
		<-heartbeatDone
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 10*time.Second)
		finishErr := finishJob(finishCtx, c, job, handlerErr, ctx.Err() != nil)
		finishCancel()
		if finishErr != nil {
			slog.Error("finish processing job failed; lease will recover it", "job_id", job.ID, "error", finishErr)
		}
	}
	return nil
}

func (w JobWorker) heartbeat(ctx context.Context, c JobWorkerConfig, cancel context.CancelFunc, job Job) {
	ticker := time.NewTicker(c.LeaseDuration / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, heartbeatCancel := context.WithTimeout(context.Background(), 5*time.Second)
			cancelRequested, err := renewJobLease(heartbeatCtx, c.Pool, c.WorkerID, job, c.LeaseDuration)
			heartbeatCancel()
			if err != nil {
				slog.Error("processing job lease heartbeat failed", "job_id", job.ID, "error", err)
				cancel()
				return
			}
			if cancelRequested {
				cancel()
				return
			}
		}
	}
}

// renewJobLease extends the lease of a running job only while the caller still
// holds it: a stale fencing token or lost lease matches no row and fails
// closed. It also reports whether cancellation was requested.
func renewJobLease(ctx context.Context, pool *pgxpool.Pool, workerID string, job Job, leaseDuration time.Duration) (bool, error) {
	var cancelRequested bool
	err := pool.QueryRow(ctx, `
		UPDATE processing_jobs
		SET lease_until = now() + make_interval(secs => $4::double precision)
		WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND fencing_token = $3
		RETURNING cancel_requested_at IS NOT NULL`, job.ID, workerID, job.FencingToken, leaseDuration.Seconds()).Scan(&cancelRequested)
	if err != nil {
		return false, err
	}
	return cancelRequested, nil
}

func claimJob(ctx context.Context, c JobWorkerConfig, types []string) (Job, error) {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)
	var job Job
	err = tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id FROM processing_jobs
			WHERE state = 'queued' AND next_attempt_at <= now()
			  AND cancel_requested_at IS NULL AND job_type = ANY($1::text[])
			ORDER BY created_at, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE processing_jobs j
		SET state = 'running', attempt = j.attempt + 1,
		    lease_owner = $2,
		    lease_until = now() + make_interval(secs => $3::double precision),
		    fencing_token = j.fencing_token + 1,
		    started_at = COALESCE(j.started_at, now())
		FROM candidate
		WHERE j.id = candidate.id
		RETURNING j.id::text,j.organization_id::text,j.namespace,j.job_type,j.generation,
		          COALESCE(j.release_id,''),j.request::text,j.attempt,j.max_attempts,j.fencing_token`,
		types, c.WorkerID, c.LeaseDuration.Seconds()).Scan(
		&job.ID, &job.OrganizationID, &job.Namespace, &job.Type, &job.Generation,
		&job.ReleaseID, &job.Request, &job.Attempt, &job.MaxAttempts, &job.FencingToken)
	if err != nil {
		return Job{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO processing_job_attempts(job_id,attempt,fencing_token,worker_id,state) VALUES($1,$2,$3,$4,'running')`, job.ID, job.Attempt, job.FencingToken, c.WorkerID); err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func finishJob(ctx context.Context, c JobWorkerConfig, job Job, handlerErr error, stopping bool) error {
	tx, err := c.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	errorText := ""
	if handlerErr != nil {
		errorText = handlerErr.Error()
		if len(errorText) > 2048 {
			errorText = errorText[:2048]
		}
	}
	// Lock the row and re-read the cancellation flag; the fencing token guard
	// makes a stale lease holder match no row and fail closed.
	var cancelRequested bool
	err = tx.QueryRow(ctx, `
		SELECT cancel_requested_at IS NOT NULL
		FROM processing_jobs
		WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND fencing_token = $3
		FOR UPDATE`, job.ID, c.WorkerID, job.FencingToken).Scan(&cancelRequested)
	if err != nil {
		return err
	}
	outcome := FinishTransition(cancelRequested, stopping, handlerErr, job.Attempt, job.MaxAttempts)
	if !CanTransition(JobStateRunning, outcome.JobState) {
		return fmt.Errorf("illegal job state transition running -> %s", outcome.JobState)
	}
	_, err = tx.Exec(ctx, `
		UPDATE processing_jobs
		SET state = $4,
		    lease_owner = NULL, lease_until = NULL,
		    next_attempt_at = CASE WHEN $4::text = 'queued' THEN now() + make_interval(secs => $5::double precision) ELSE next_attempt_at END,
		    finished_at = CASE WHEN $6::boolean THEN now() ELSE NULL END,
		    last_error = CASE WHEN $4::text = 'cancelled' THEN 'cancelled by requester' ELSE NULLIF($7::text,'') END
		WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND fencing_token = $3`,
		job.ID, c.WorkerID, job.FencingToken, string(outcome.JobState), outcome.RequeueDelay.Seconds(), outcome.Finished, errorText)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE processing_job_attempts
		SET state = $3, finished_at = now(), error_code = CASE WHEN $4::text <> '' THEN 'HANDLER_FAILED' END,
		    error_message = NULLIF($4::text,'')
		WHERE job_id = $1 AND attempt = $2 AND fencing_token = $5`, job.ID, job.Attempt, string(outcome.AttemptState), errorText, job.FencingToken)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func reapExpiredJobs(ctx context.Context, pool *pgxpool.Pool, limit int) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT id::text,attempt,max_attempts,fencing_token
		FROM processing_jobs
		WHERE state='running' AND lease_until < now()
		ORDER BY lease_until
		LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return err
	}
	type expired struct {
		id           string
		attempt, max int
		token        int64
	}
	var jobs []expired
	for rows.Next() {
		var job expired
		if err := rows.Scan(&job.id, &job.attempt, &job.max, &job.token); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, job := range jobs {
		state := JobStateQueued
		if job.attempt >= job.max {
			state = JobStateFailed
		}
		if !CanTransition(JobStateRunning, state) {
			return fmt.Errorf("illegal job state transition running -> %s", state)
		}
		_, err := tx.Exec(ctx, `
			UPDATE processing_jobs
			SET state=$2, lease_owner=NULL, lease_until=NULL,
			    next_attempt_at=CASE WHEN $2::text='queued' THEN now()+make_interval(secs => $4::double precision) ELSE next_attempt_at END,
			    finished_at=CASE WHEN $2::text='failed' THEN now() ELSE NULL END,
			    last_error='worker lease expired'
			WHERE id=$1 AND state='running' AND fencing_token=$3`, job.id, string(state), job.token, RetryBackoff(job.attempt).Seconds())
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE processing_job_attempts SET state='lease_expired',finished_at=now(),error_code='LEASE_EXPIRED',error_message='worker lease expired' WHERE job_id=$1 AND attempt=$2 AND fencing_token=$3`, job.id, job.attempt, job.token)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func cancelQueuedJobs(ctx context.Context, pool *pgxpool.Pool, limit int) error {
	_, err := pool.Exec(ctx, `
		WITH cancelled AS (
			SELECT id FROM processing_jobs
			WHERE state='queued' AND cancel_requested_at IS NOT NULL
			ORDER BY cancel_requested_at
			LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE processing_jobs j
		SET state='cancelled',finished_at=now(),last_error='cancelled by requester'
		FROM cancelled WHERE j.id=cancelled.id`, limit)
	return err
}
