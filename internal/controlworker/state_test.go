package controlworker

import (
	"errors"
	"testing"
	"time"
)

func TestJobStateTransitionMatrix(t *testing.T) {
	legal := map[JobState][]JobState{
		JobStateQueued:  {JobStateRunning, JobStateCancelled},
		JobStateRunning: {JobStateQueued, JobStateSucceeded, JobStateFailed, JobStateCancelled},
	}
	states := []JobState{JobStateQueued, JobStateRunning, JobStateSucceeded, JobStateFailed, JobStateCancelled}
	for _, from := range states {
		for _, to := range states {
			want := false
			for _, allowed := range legal[from] {
				if allowed == to {
					want = true
				}
			}
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestTerminalStatesHaveNoOutgoingTransitions(t *testing.T) {
	for _, terminal := range []JobState{JobStateSucceeded, JobStateFailed, JobStateCancelled} {
		for _, to := range []JobState{JobStateQueued, JobStateRunning, JobStateSucceeded, JobStateFailed, JobStateCancelled} {
			if CanTransition(terminal, to) {
				t.Errorf("terminal state %s must not transition to %s", terminal, to)
			}
		}
	}
}

func TestRetryBackoffIsBoundedAndExponential(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{-1, time.Second},
		{0, time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{8, 256 * time.Second},
		{9, 256 * time.Second},
		{100, 256 * time.Second},
	}
	for _, tc := range cases {
		if got := RetryBackoff(tc.attempt); got != tc.want {
			t.Errorf("RetryBackoff(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
		if got := RetryBackoff(tc.attempt); got > MaxRetryBackoff {
			t.Errorf("RetryBackoff(%d) = %v exceeds cap %v", tc.attempt, got, MaxRetryBackoff)
		}
	}
	for attempt := 0; attempt < 8; attempt++ {
		if RetryBackoff(attempt) >= RetryBackoff(attempt+1) {
			t.Errorf("backoff must strictly grow below the cap: attempt %d -> %v, %d -> %v",
				attempt, RetryBackoff(attempt), attempt+1, RetryBackoff(attempt+1))
		}
	}
}

func TestFinishTransitionCancellationWins(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		outcome := FinishTransition(true, stopping, errors.New("boom"), 3, 5)
		if outcome.JobState != JobStateCancelled || outcome.AttemptState != AttemptStateCancelled || !outcome.Finished {
			t.Errorf("cancel outcome = %+v, want cancelled terminal", outcome)
		}
		if !CanTransition(JobStateRunning, outcome.JobState) {
			t.Errorf("cancel outcome %s must be a legal migration", outcome.JobState)
		}
	}
}

func TestFinishTransitionSuccess(t *testing.T) {
	outcome := FinishTransition(false, false, nil, 1, 5)
	if outcome.JobState != JobStateSucceeded || outcome.AttemptState != AttemptStateSucceeded || !outcome.Finished {
		t.Errorf("success outcome = %+v, want succeeded terminal", outcome)
	}
}

func TestFinishTransitionRetryAndExhaustion(t *testing.T) {
	retry := FinishTransition(false, false, errors.New("boom"), 2, 5)
	if retry.JobState != JobStateQueued || retry.AttemptState != AttemptStateFailed || retry.Finished {
		t.Errorf("retry outcome = %+v, want requeued failed attempt", retry)
	}
	if retry.RequeueDelay != RetryBackoff(2) {
		t.Errorf("retry delay = %v, want %v", retry.RequeueDelay, RetryBackoff(2))
	}
	exhausted := FinishTransition(false, false, errors.New("boom"), 5, 5)
	if exhausted.JobState != JobStateFailed || exhausted.AttemptState != AttemptStateFailed || !exhausted.Finished {
		t.Errorf("exhausted outcome = %+v, want failed terminal", exhausted)
	}
}

func TestFinishTransitionShutdownRequeuesImmediately(t *testing.T) {
	outcome := FinishTransition(false, true, errors.New("boom"), 4, 5)
	if outcome.JobState != JobStateQueued || outcome.AttemptState != AttemptStateLeaseExpired || outcome.Finished {
		t.Errorf("shutdown outcome = %+v, want immediate requeue with lease_expired attempt", outcome)
	}
	if outcome.RequeueDelay != 0 {
		t.Errorf("shutdown requeue delay = %v, want 0", outcome.RequeueDelay)
	}
	// A shutdown requeue must succeed even when the attempt budget is spent:
	// the job is not being punished for the worker stopping.
	outcome = FinishTransition(false, true, nil, 5, 5)
	if outcome.JobState != JobStateQueued {
		t.Errorf("shutdown at max attempts = %+v, want queued", outcome)
	}
}
