package controlworker

import "time"

// JobState is the lifecycle state of a processing_jobs row.
type JobState string

const (
	JobStateQueued    JobState = "queued"
	JobStateRunning   JobState = "running"
	JobStateSucceeded JobState = "succeeded"
	JobStateFailed    JobState = "failed"
	JobStateCancelled JobState = "cancelled"
)

// AttemptState is the outcome recorded for one processing_job_attempts row.
type AttemptState string

const (
	AttemptStateRunning      AttemptState = "running"
	AttemptStateSucceeded    AttemptState = "succeeded"
	AttemptStateFailed       AttemptState = "failed"
	AttemptStateCancelled    AttemptState = "cancelled"
	AttemptStateLeaseExpired AttemptState = "lease_expired"
)

// jobTransitions lists every legal job state migration. Anything not listed
// is a programming or data error and must be rejected, never performed.
var jobTransitions = map[JobState][]JobState{
	JobStateQueued:  {JobStateRunning, JobStateCancelled},
	JobStateRunning: {JobStateQueued, JobStateSucceeded, JobStateFailed, JobStateCancelled},
}

// CanTransition reports whether moving a job from one state to another is a
// legal migration. Terminal states (succeeded/failed/cancelled) have no
// outgoing transitions.
func CanTransition(from, to JobState) bool {
	for _, next := range jobTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

// MaxRetryBackoff caps the delay between attempts so a poisoned job is always
// retried within a bounded window.
const MaxRetryBackoff = 300 * time.Second

// RetryBackoff returns the bounded exponential delay before the next attempt:
// 2^min(attempt,8) seconds, capped at MaxRetryBackoff. It mirrors the SQL
// expression LEAST(300, power(2, LEAST(attempt,8))) used at the persistence
// layer; keep both in sync. In practice the attempt exponent cap (8) binds
// first, so the effective maximum delay is 256s.
func RetryBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 8 {
		attempt = 8
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > MaxRetryBackoff {
		return MaxRetryBackoff
	}
	return delay
}

// FinishOutcome is the decision the job state machine makes when a claimed
// job leaves the running state.
type FinishOutcome struct {
	JobState     JobState
	AttemptState AttemptState
	// RequeueDelay applies only when JobState is JobStateQueued.
	RequeueDelay time.Duration
	// Finished marks outcomes that set finished_at (terminal states).
	Finished bool
}

// FinishTransition is the single authority for how a running job completes.
// A pending cancellation always wins; a worker shutdown requeues without
// consuming the attempt's retry budget in intent (the attempt is recorded as
// lease_expired); a handler error requeues while attempts remain, otherwise
// fails permanently.
func FinishTransition(cancelRequested, stopping bool, handlerErr error, attempt, maxAttempts int) FinishOutcome {
	switch {
	case cancelRequested:
		return FinishOutcome{JobState: JobStateCancelled, AttemptState: AttemptStateCancelled, Finished: true}
	case stopping:
		return FinishOutcome{JobState: JobStateQueued, AttemptState: AttemptStateLeaseExpired}
	case handlerErr == nil:
		return FinishOutcome{JobState: JobStateSucceeded, AttemptState: AttemptStateSucceeded, Finished: true}
	case attempt < maxAttempts:
		return FinishOutcome{JobState: JobStateQueued, AttemptState: AttemptStateFailed, RequeueDelay: RetryBackoff(attempt)}
	default:
		return FinishOutcome{JobState: JobStateFailed, AttemptState: AttemptStateFailed, Finished: true}
	}
}
