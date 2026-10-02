package feature

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// goldenContributions is the shared F03 golden test vector, mirrored
// event-for-event by python/tests/test_features.py
// (WindowFeatureGoldenTests). Both sides must produce identical values for
// the identical logical input: window [2026-10-12T00:00:00Z, 00:10:00Z),
// attempts 7, failures 4, failure rate 4/7, 2 distinct devices, 2 distinct
// IPs, 2 failure-then-success sequences (e3 after e1/e2; e7 after e4/e6).
func goldenContributions() []Contribution {
	base := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	mk := func(id string, minute int, outcome, device, ip string) Contribution {
		return Contribution{
			EntityID:      "ent:a",
			Role:          "actor",
			EventID:       id,
			AttributionID: "att:" + id,
			At:            base.Add(time.Duration(minute) * time.Minute),
			Outcome:       outcome,
			SourceDevice:  device,
			SourceIP:      ip,
		}
	}
	return []Contribution{
		mk("e1", 1, "failure", "d1", "10.0.0.1"),
		mk("e2", 2, "failure", "d1", "10.0.0.1"),
		mk("e3", 3, "success", "d2", "10.0.0.2"),
		mk("e4", 5, "failure", "d1", "10.0.0.1"),
		mk("e5", 6, "", "d2", "10.0.0.2"),
		mk("e6", 7, "failure", "d1", "10.0.0.2"),
		mk("e7", 8, "success", "d2", "10.0.0.2"),
	}
}

func goldenWindow() Window {
	start := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	return Window{Start: start, End: start.Add(10 * time.Minute), AllowedLateness: DefaultAllowedLateness}
}

func TestAuthComputerGoldenVector(t *testing.T) {
	values, err := AuthComputer{}.Compute(goldenWindow(), goldenContributions())
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	want := Values{
		FeatureAuthAttemptCount:            7,
		FeatureAuthFailureCount:            4,
		FeatureAuthFailureRate:             4.0 / 7.0,
		FeatureAuthSourceDeviceCount:       2,
		FeatureAuthSourceIPCount:           2,
		FeatureAuthFailureThenSuccessCount: 2,
	}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
}

func TestAuthComputerOrderIndependent(t *testing.T) {
	contributions := goldenContributions()
	reversed := make([]Contribution, len(contributions))
	for i, c := range contributions {
		reversed[len(contributions)-1-i] = c
	}
	first, err := AuthComputer{}.Compute(goldenWindow(), contributions)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	second, err := AuthComputer{}.Compute(goldenWindow(), reversed)
	if err != nil {
		t.Fatalf("compute reversed: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("order changed values: %v vs %v", first, second)
	}
}

func TestAuthComputerRejectsInvalidWindow(t *testing.T) {
	if _, err := (AuthComputer{}).Compute(Window{}, goldenContributions()); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("err = %v, want ErrInvalidWindow", err)
	}
}

func TestNewRecordValidation(t *testing.T) {
	values, _ := AuthComputer{}.Compute(goldenWindow(), goldenContributions())
	record, err := NewRecord("ent:a", goldenWindow(), AuthFeatureVersionV1, 1, values, goldenContributions())
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	wantInputs := []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7"}
	if !reflect.DeepEqual(record.Inputs, wantInputs) {
		t.Fatalf("inputs = %v, want %v", record.Inputs, wantInputs)
	}
	for _, bad := range []struct {
		name    string
		entity  string
		version string
		rev     uint64
	}{
		{"empty entity", "", AuthFeatureVersionV1, 1},
		{"empty version", "ent:a", "", 1},
		{"zero revision", "ent:a", AuthFeatureVersionV1, 0},
	} {
		if _, err := NewRecord(bad.entity, goldenWindow(), bad.version, bad.rev, values, goldenContributions()); err == nil {
			t.Fatalf("%s: expected rejection", bad.name)
		}
	}
}

type recordingSink struct {
	items []BackfillItem
	err   error
}

func (s *recordingSink) RecordBackfill(item BackfillItem) error {
	if s.err != nil {
		return s.err
	}
	s.items = append(s.items, item)
	return nil
}

func newAuthTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := NewEngine(EngineConfig{WindowSize: 10 * time.Minute})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	return engine
}

// Late correction: a LateAccepted contribution enters a still-open window,
// and recomputing the same window from the full contribution set yields
// updated feature values under the same business key with a higher revision.
func TestLateAcceptedRecomputesWindow(t *testing.T) {
	engine := newAuthTestEngine(t)
	base := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	received := base.Add(12 * time.Minute)

	initial := goldenContributions()[:3] // e1 failure, e2 failure, e3 success
	for _, c := range initial {
		if d, err := engine.Add(c, c.At); err != nil || d != Added {
			t.Fatalf("add %s: %v %v", c.EventID, d, err)
		}
	}
	// Push the entity watermark to 00:05 (max event 00:15 − 10m lateness):
	// window [00:00, 00:10) stays open until the watermark reaches 00:10.
	received = base.Add(16 * time.Minute)
	progress := Contribution{EntityID: "ent:a", Role: "actor", EventID: "e9", AttributionID: "att:e9", At: base.Add(15 * time.Minute), Outcome: "success"}
	if d, err := engine.Add(progress, received); err != nil || d != Added {
		t.Fatalf("add progress: %v %v", d, err)
	}

	window, contributions, ok := engine.WindowContributions("ent:a", base)
	if !ok {
		t.Fatal("window must still be open")
	}
	before, err := AuthComputer{}.Compute(window, contributions)
	if err != nil {
		t.Fatalf("compute before: %v", err)
	}

	// Late arrival for the open window: one more failure at 00:04.
	late := Contribution{EntityID: "ent:a", Role: "actor", EventID: "eL", AttributionID: "att:eL", At: base.Add(4 * time.Minute), Outcome: "failure", SourceDevice: "d3", SourceIP: "10.0.0.3"}
	if d, err := engine.Add(late, received); err != nil || d != LateAccepted {
		t.Fatalf("late add: %v %v", d, err)
	}

	window2, recomputed, ok := engine.WindowContributions("ent:a", base)
	if !ok {
		t.Fatal("window vanished after late acceptance")
	}
	if window2 != window {
		t.Fatalf("same window expected, got %+v vs %+v", window2, window)
	}
	after, err := AuthComputer{}.Compute(window2, recomputed)
	if err != nil {
		t.Fatalf("compute after: %v", err)
	}
	if after[FeatureAuthFailureCount] != before[FeatureAuthFailureCount]+1 {
		t.Fatalf("failure count not corrected: before %v after %v", before[FeatureAuthFailureCount], after[FeatureAuthFailureCount])
	}
	if after[FeatureAuthFailureThenSuccessCount] != 1 || after[FeatureAuthAttemptCount] != 4 {
		t.Fatalf("unexpected corrected values: %v", after)
	}
	// Same business key, higher revision carries the corrected values.
	record, err := NewRecord("ent:a", window2, AuthFeatureVersionV1, 2, after, recomputed)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if record.Revision != 2 || record.FeatureVersion != AuthFeatureVersionV1 {
		t.Fatalf("record = %+v", record)
	}
}

// Window close: Advance drains the final contribution set, and the closed
// feature record carries the feature version stamp and input references.
func TestClosedWindowEmitsVersionedRecord(t *testing.T) {
	engine := newAuthTestEngine(t)
	base := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	for _, c := range goldenContributions() {
		if d, err := engine.Add(c, c.At); err != nil || d != Added {
			t.Fatalf("add %s: %v %v", c.EventID, d, err)
		}
	}
	// Watermark still at 23:58 (max 00:08 − 10m): not closed.
	if closed := engine.Advance(base.Add(9 * time.Minute)); len(closed) != 0 {
		t.Fatalf("closed too early: %v", closed)
	}
	// New event at 00:20 pushes the watermark to 00:10 = window end.
	progress := Contribution{EntityID: "ent:a", Role: "actor", EventID: "e9", AttributionID: "att:e9", At: base.Add(20 * time.Minute), Outcome: "success"}
	if _, err := engine.Add(progress, progress.At); err != nil {
		t.Fatalf("add progress: %v", err)
	}
	closed := engine.Advance(base.Add(21 * time.Minute))
	if len(closed) != 1 {
		t.Fatalf("closed = %d, want 1", len(closed))
	}
	values, err := AuthComputer{}.Compute(closed[0].Window, closed[0].Contributions)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	record, err := NewRecord(closed[0].EntityID, closed[0].Window, AuthFeatureVersionV1, 1, values, closed[0].Contributions)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if record.FeatureVersion != AuthFeatureVersionV1 {
		t.Fatalf("feature version missing: %+v", record)
	}
	if record.Values[FeatureAuthFailureCount] != 4 || record.Values[FeatureAuthFailureThenSuccessCount] != 2 {
		t.Fatalf("final values wrong: %v", record.Values)
	}
	if len(record.Inputs) != 7 {
		t.Fatalf("input references missing: %v", record.Inputs)
	}
	// After closing, a contribution for the closed window is out of bounds.
	late := Contribution{EntityID: "ent:a", Role: "actor", EventID: "eL", AttributionID: "att:eL", At: base.Add(5 * time.Minute), Outcome: "failure"}
	if d, err := engine.Add(late, base.Add(21*time.Minute)); err != nil || d != LateRejected {
		t.Fatalf("post-close add: %v %v", d, err)
	}
}

// Out-of-bounds backfill: LateRejected data is recorded for controlled
// backfill (both closed-window and beyond-retention reasons), evicted open
// windows are recorded too, and a sink failure is fail-closed.
func TestLateRejectedRecordedForBackfill(t *testing.T) {
	engine := newAuthTestEngine(t)
	sink := &recordingSink{}
	engine.SetBackfillSink(sink)
	base := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	mk := func(id string, minute int) Contribution {
		return Contribution{EntityID: "ent:a", Role: "actor", EventID: id, AttributionID: "att:" + id, At: base.Add(time.Duration(minute) * time.Minute), Outcome: "failure"}
	}
	// Close the first window via watermark progress.
	engine.Add(mk("e1", 1), base.Add(1*time.Minute))
	engine.Add(mk("e2", 25), base.Add(25*time.Minute))
	engine.Advance(base.Add(26 * time.Minute))

	// Closed window: late contribution at 00:05 is rejected and recorded.
	d, err := engine.Add(mk("eL", 5), base.Add(26*time.Minute))
	if err != nil || d != LateRejected {
		t.Fatalf("late add: %v %v", d, err)
	}
	if len(sink.items) != 1 || sink.items[0].Reason != BackfillWindowClosed {
		t.Fatalf("sink = %+v", sink.items)
	}
	if sink.items[0].Contribution.EventID != "eL" || sink.items[0].EntityID != "ent:a" {
		t.Fatalf("backfill item lost the contribution: %+v", sink.items[0])
	}
	wantStart := base
	if !sink.items[0].WindowStart.Equal(wantStart) {
		t.Fatalf("window start = %s, want %s", sink.items[0].WindowStart, wantStart)
	}

	// Sink failure is fail-closed.
	sink.err = errors.New("store unavailable")
	if _, err := engine.Add(mk("eL2", 6), base.Add(26*time.Minute)); err == nil {
		t.Fatal("sink failure must fail closed")
	}
	sink.err = nil
}

func TestBeyondRetentionAndEvictionBackfill(t *testing.T) {
	engine, err := NewEngine(EngineConfig{WindowSize: 10 * time.Minute, AllowedLateness: 30 * time.Minute, MaxWindowsPerEntity: 2})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	sink := &recordingSink{}
	engine.SetBackfillSink(sink)
	base := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	mk := func(id string, minute int) Contribution {
		return Contribution{EntityID: "ent:a", Role: "actor", EventID: id, AttributionID: "att:" + id, At: base.Add(time.Duration(minute) * time.Minute), Outcome: "failure"}
	}
	// Fill the window budget (2) and evict the oldest open window. With
	// 30m lateness no window closes, so the budget drives both paths.
	engine.Add(mk("e1", 1), base.Add(1*time.Minute))   // window 00:00
	engine.Add(mk("e2", 11), base.Add(11*time.Minute)) // window 00:10
	engine.Add(mk("e3", 21), base.Add(21*time.Minute)) // evicts window 00:00
	if len(sink.items) != 1 || sink.items[0].Reason != BackfillWindowEvicted || sink.items[0].Contribution.EventID != "e1" {
		t.Fatalf("evicted window not recorded: %+v", sink.items)
	}
	if !sink.items[0].WindowStart.Equal(base) {
		t.Fatalf("evicted window start = %s, want %s", sink.items[0].WindowStart, base)
	}
	// Older than every retained window while the budget is full.
	d, err := engine.Add(mk("e0", 5), base.Add(21*time.Minute))
	if err != nil || d != LateRejected {
		t.Fatalf("beyond retention: %v %v", d, err)
	}
	if len(sink.items) != 2 || sink.items[1].Reason != BackfillBeyondRetention {
		t.Fatalf("beyond-retention not recorded: %+v", sink.items)
	}
}
