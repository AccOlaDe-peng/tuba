package controlworker

import (
	"path/filepath"
	"testing"
	"time"
)

func TestNormalizeRetentionConfigDefaults(t *testing.T) {
	c := normalizeRetentionConfig(RetentionConfig{})
	if c.InboxRetention != 48*time.Hour {
		t.Fatalf("inbox retention default: %v", c.InboxRetention)
	}
	if c.OutboxRetention != 48*time.Hour {
		t.Fatalf("outbox retention default: %v", c.OutboxRetention)
	}
	if c.SucceededJobRetention != 7*24*time.Hour {
		t.Fatalf("succeeded retention default: %v", c.SucceededJobRetention)
	}
	if c.FailedJobRetention != 30*24*time.Hour {
		t.Fatalf("failed retention default: %v", c.FailedJobRetention)
	}
	if c.FailedJobRetention <= c.SucceededJobRetention {
		t.Fatal("evidence retention for failed/cancelled jobs must exceed the succeeded window")
	}
	if c.BatchSize != 10000 || c.MaxBatchesPerSweep != 1000 || c.PollInterval != time.Hour {
		t.Fatalf("batch/poll defaults: %+v", c)
	}
	explicit := RetentionConfig{InboxRetention: time.Hour, OutboxRetention: 2 * time.Hour,
		SucceededJobRetention: 3 * time.Hour, FailedJobRetention: 4 * time.Hour,
		BatchSize: 5, MaxBatchesPerSweep: 6, PollInterval: time.Minute}
	got := normalizeRetentionConfig(explicit)
	if got != explicit {
		t.Fatalf("explicit config must be preserved: %+v", got)
	}
}

func TestJobDeletable(t *testing.T) {
	now := time.Now()
	succeededCutoff := now.Add(-7 * 24 * time.Hour)
	failedCutoff := now.Add(-30 * 24 * time.Hour)
	old := now.Add(-40 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	owner := "worker-1"
	cases := []struct {
		name       string
		state      JobState
		finishedAt *time.Time
		leaseOwner *string
		want       bool
	}{
		{"old succeeded", JobStateSucceeded, &old, nil, true},
		{"old failed", JobStateFailed, &old, nil, true},
		{"old cancelled", JobStateCancelled, &old, nil, true},
		{"recent succeeded kept", JobStateSucceeded, &recent, nil, false},
		{"recent failed kept", JobStateFailed, &recent, nil, false},
		{"failed between windows kept", JobStateFailed, ptrTime(now.Add(-10 * 24 * time.Hour)), nil, false},
		{"running never deleted", JobStateRunning, &old, nil, false},
		{"queued never deleted", JobStateQueued, &old, nil, false},
		{"lease owner protects terminal row", JobStateSucceeded, &old, &owner, false},
		{"missing finished_at kept", JobStateSucceeded, nil, nil, false},
		{"unknown state kept", JobState("mystery"), &old, nil, false},
	}
	for _, tc := range cases {
		if got := jobDeletable(tc.state, tc.finishedAt, tc.leaseOwner, succeededCutoff, failedCutoff); got != tc.want {
			t.Errorf("%s: jobDeletable=%v want %v", tc.name, got, tc.want)
		}
	}
}

func ptrTime(v time.Time) *time.Time { return &v }

func TestJobTempDirValidation(t *testing.T) {
	root := t.TempDir()
	id := "123e4567-e89b-42d3-a456-426614174000"
	dir, err := JobTempDir(root, id)
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Join(root, id) {
		t.Fatalf("unexpected dir %s", dir)
	}
	for _, bad := range []string{"", "../escape", "a/b", "123e4567-e89b-42d3-a456-42661417400g",
		"123E4567-E89B-42D3-A456-426614174000", "not-a-uuid", "..", ".", "123e4567e89b42d3a456426614174000"} {
		if _, err := JobTempDir(root, bad); err == nil {
			t.Errorf("JobTempDir(%q) must fail closed", bad)
		}
	}
	if _, err := JobTempDir("", id); err == nil {
		t.Error("empty root must fail closed")
	}
	created, err := RegisterJobTempDir(root, id)
	if err != nil {
		t.Fatal(err)
	}
	if created != dir {
		t.Fatalf("register returned %s, want %s", created, dir)
	}
	// Registration is idempotent.
	if _, err := RegisterJobTempDir(root, id); err != nil {
		t.Fatal(err)
	}
	removed, err := removeJobTempDir(root, id)
	if err != nil || !removed {
		t.Fatalf("remove: removed=%v err=%v", removed, err)
	}
	// Removal is idempotent.
	removed, err = removeJobTempDir(root, id)
	if err != nil || removed {
		t.Fatalf("second remove: removed=%v err=%v", removed, err)
	}
	if _, err := removeJobTempDir(root, "../escape"); err == nil {
		t.Error("non-UUID removal must fail closed")
	}
}
