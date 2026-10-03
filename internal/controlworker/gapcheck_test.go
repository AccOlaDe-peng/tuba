package controlworker

import (
	"context"
	"errors"
	"testing"

	"tuba/product/internal/telemetry"
)

type fakeOffsetSource struct {
	committed   map[string]map[TopicPartition]int64
	earliest    map[TopicPartition]int64
	committedErr error
	earliestErr  error
}

func (f *fakeOffsetSource) Committed(_ context.Context, group string) (map[TopicPartition]int64, error) {
	if f.committedErr != nil {
		return nil, f.committedErr
	}
	offsets, ok := f.committed[group]
	if !ok {
		return map[TopicPartition]int64{}, nil
	}
	return offsets, nil
}

func (f *fakeOffsetSource) Earliest(_ context.Context, partitions []TopicPartition) (map[TopicPartition]int64, error) {
	if f.earliestErr != nil {
		return nil, f.earliestErr
	}
	out := make(map[TopicPartition]int64, len(partitions))
	for _, tp := range partitions {
		offset, ok := f.earliest[tp]
		if !ok {
			return nil, errors.New("partition missing from fake source")
		}
		out[tp] = offset
	}
	return out, nil
}

func TestRetentionGapCheckDetectsLostMessages(t *testing.T) {
	tp := TopicPartition{Topic: "events", Partition: 0}
	metrics := telemetry.New()
	checker := RetentionGapChecker{
		Offsets: &fakeOffsetSource{
			committed: map[string]map[TopicPartition]int64{"g1": {tp: 100}},
			earliest:  map[TopicPartition]int64{tp: 260},
		},
		Groups:  []string{"g1"},
		Metrics: metrics,
	}
	if err := checker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := metrics.Value("tuba_control_worker_retention_gap_partitions"); got != 1 {
		t.Fatalf("gap partitions = %d, want 1", got)
	}
	if got := metrics.Value("tuba_control_worker_retention_gap_messages"); got != 160 {
		t.Fatalf("gap messages = %d, want 160", got)
	}
}

func TestRetentionGapCheckHealthyGroupReportsZero(t *testing.T) {
	tp := TopicPartition{Topic: "events", Partition: 0}
	metrics := telemetry.New()
	checker := RetentionGapChecker{
		Offsets: &fakeOffsetSource{
			committed: map[string]map[TopicPartition]int64{"g1": {tp: 260}},
			earliest:  map[TopicPartition]int64{tp: 260},
		},
		Groups:  []string{"g1"},
		Metrics: metrics,
	}
	if err := checker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := metrics.Value("tuba_control_worker_retention_gap_partitions"); got != 0 {
		t.Fatalf("gap partitions = %d, want 0", got)
	}
}

func TestRetentionGapCheckNoCommitIsNotAGap(t *testing.T) {
	// A group that never committed simply has not started; comparing against
	// earliest would misreport the whole topic as lost.
	metrics := telemetry.New()
	checker := RetentionGapChecker{
		Offsets: &fakeOffsetSource{
			committed: map[string]map[TopicPartition]int64{"g1": {}},
			earliest:  map[TopicPartition]int64{},
		},
		Groups:  []string{"g1"},
		Metrics: metrics,
	}
	if err := checker.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := metrics.Value("tuba_control_worker_retention_gap_partitions"); got != 0 {
		t.Fatalf("gap partitions = %d, want 0", got)
	}
}

func TestRetentionGapCheckGroupFailureDoesNotSkipOthers(t *testing.T) {
	tp := TopicPartition{Topic: "events", Partition: 1}
	metrics := telemetry.New()
	source := &fakeOffsetSource{
		committed: map[string]map[TopicPartition]int64{"ok": {tp: 10}},
		earliest:  map[TopicPartition]int64{tp: 40},
	}
	checker := RetentionGapChecker{
		Groups:  []string{"bad", "ok"},
		Metrics: metrics,
	}
	failSource := &selectiveFailSource{fail: map[string]bool{"bad": true}, inner: source}
	checker.Offsets = failSource
	if err := checker.Sweep(context.Background()); err == nil {
		t.Fatal("expected sweep error from failing group")
	}
	if got := metrics.Value("tuba_control_worker_retention_gap_partitions"); got != 1 {
		t.Fatalf("healthy group still checked: gap partitions = %d, want 1", got)
	}
	if got := metrics.Value("tuba_control_worker_retention_gap_check_failures_total"); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}
}

type selectiveFailSource struct {
	fail  map[string]bool
	inner *fakeOffsetSource
}

func (s *selectiveFailSource) Committed(ctx context.Context, group string) (map[TopicPartition]int64, error) {
	if s.fail[group] {
		return nil, errors.New("group coordinator unreachable")
	}
	return s.inner.Committed(ctx, group)
}

func (s *selectiveFailSource) Earliest(ctx context.Context, partitions []TopicPartition) (map[TopicPartition]int64, error) {
	return s.inner.Earliest(ctx, partitions)
}

func TestParseGapCheckGroups(t *testing.T) {
	if got := ParseGapCheckGroups(""); len(got) != 0 {
		t.Fatalf("empty: %v", got)
	}
	got := ParseGapCheckGroups(" g1 , ,g2,")
	if len(got) != 2 || got[0] != "g1" || got[1] != "g2" {
		t.Fatalf("parsed: %v", got)
	}
}
