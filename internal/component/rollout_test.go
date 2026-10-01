package component

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"
)

const (
	rolloutObserve = 200 * time.Millisecond
	rolloutGrace   = 60 * time.Millisecond
	rolloutPoll    = 10 * time.Millisecond
)

// newRolloutTarget installs a confirmed v1 of filebeat under its own root and
// returns a target whose health gate the test controls.
func newRolloutTarget(t *testing.T, name string, ring Keyring, key ed25519.PrivateKey) (RolloutTarget, *fakeProcesses, *scriptedHealth) {
	t.Helper()
	root := t.TempDir()
	upgrader := &Upgrader{Root: root, Keyring: ring}
	v1 := makePackage(t, "filebeat", "8.19.0", defaultFormat(), map[string]string{"bin/filebeat": "v1"}, "test-key", key)
	if _, err := upgrader.Apply(v1, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := upgrader.Confirm("filebeat", nil); err != nil {
		t.Fatal(err)
	}
	pm := &fakeProcesses{alive: true}
	health := &scriptedHealth{gate: func() bool { return true }}
	target := RolloutTarget{
		Name:      name,
		Root:      root,
		Keyring:   ring,
		Component: "filebeat",
		Processes: pm,
		Health:    health,
	}
	return target, pm, health
}

func TestRolloutAllHealthy(t *testing.T) {
	ring, key := testKeyring(t)
	targets := make([]RolloutTarget, 4)
	for i := range targets {
		name := string(rune('a' + i))
		target, _, _ := newRolloutTarget(t, name, ring, key)
		targets[i] = target
	}
	pkg := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	report, err := RunRollout(context.Background(), pkg, targets, 2, rolloutObserve, rolloutGrace, rolloutPoll)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Confirmed) != 4 || len(report.Reverted) != 0 || report.FailedTarget != "" {
		t.Fatalf("unexpected report: %#v", report)
	}
	for _, target := range targets {
		current, _ := CurrentVersion(target.Root, "filebeat")
		if current != "8.19.1" {
			t.Fatalf("target %s current = %q", target.Name, current)
		}
	}
}

func TestRolloutAbortsAndRevertsOnBatchFailure(t *testing.T) {
	ring, key := testKeyring(t)
	targets := make([]RolloutTarget, 4)
	pms := make([]*fakeProcesses, 4)
	for i := range targets {
		name := string(rune('a' + i))
		target, pm, health := newRolloutTarget(t, name, ring, key)
		// Target c (first of batch 2) only becomes healthy on its second
		// start — i.e. its automatic rollback recovers, its upgrade does not.
		if name == "c" {
			health.gate = func() bool { return pm.startCount() >= 2 }
		}
		targets[i] = target
		pms[i] = pm
	}
	pkg := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	report, err := RunRollout(context.Background(), pkg, targets, 2, rolloutObserve, rolloutGrace, rolloutPoll)
	if err == nil {
		t.Fatal("rollout must report the batch failure")
	}
	if report.FailedTarget != "c" {
		t.Fatalf("failed target = %q", report.FailedTarget)
	}
	// Batch 1 (a, b) was confirmed and then reverted to v1.
	if strings.Join(report.Confirmed, ",") != "a,b" {
		t.Fatalf("confirmed = %v", report.Confirmed)
	}
	if strings.Join(report.Reverted, ",") != "a,b" {
		t.Fatalf("reverted = %v", report.Reverted)
	}
	if strings.Join(report.AutoRolledBack, ",") != "c" {
		t.Fatalf("auto rolled back = %v", report.AutoRolledBack)
	}
	// a, b, c all run v1 again; d was never touched.
	for _, target := range targets {
		current, _ := CurrentVersion(target.Root, "filebeat")
		if current != "8.19.0" {
			t.Fatalf("target %s current = %q", target.Name, current)
		}
	}
	if len(pms[3].calls()) != 0 {
		t.Fatalf("target d must not have been touched: %v", pms[3].calls())
	}
	// Reverted targets were observed healthy after the revert.
	for _, name := range []string{"a", "b"} {
		state, err := (&Upgrader{Root: targets[name[0]-'a'].Root}).Orchestration("filebeat")
		if err != nil {
			t.Fatal(err)
		}
		if state.Phase != PhaseIdle || !state.RolledBack {
			t.Fatalf("target %s orchestration: %#v", name, state)
		}
	}
}

func TestRolloutStopsBeforeNextBatch(t *testing.T) {
	ring, key := testKeyring(t)
	targets := make([]RolloutTarget, 3)
	pms := make([]*fakeProcesses, 3)
	for i := range targets {
		name := string(rune('a' + i))
		target, pm, health := newRolloutTarget(t, name, ring, key)
		if name == "a" {
			health.gate = func() bool { return pm.startCount() >= 2 }
		}
		targets[i] = target
		pms[i] = pm
	}
	pkg := makePackage(t, "filebeat", "8.19.1", defaultFormat(), map[string]string{"bin/filebeat": "v2"}, "test-key", key)
	report, err := RunRollout(context.Background(), pkg, targets, 1, rolloutObserve, rolloutGrace, rolloutPoll)
	if err == nil || report.FailedTarget != "a" {
		t.Fatalf("expected failure at a, got %#v, %v", report, err)
	}
	for i, name := range []string{"b", "c"} {
		if len(pms[i+1].calls()) != 0 {
			t.Fatalf("target %s must not have been started: %v", name, pms[i+1].calls())
		}
		current, _ := CurrentVersion(targets[i+1].Root, "filebeat")
		if current != "8.19.0" {
			t.Fatalf("target %s current = %q", name, current)
		}
	}
}
