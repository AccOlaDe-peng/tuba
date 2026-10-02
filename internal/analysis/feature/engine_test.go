package feature

import (
	"encoding/json"
	"testing"
	"time"
)

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func jsonMarshal(v any) ([]byte, error)      { return json.Marshal(v) }

var base = time.Date(2026, 10, 12, 2, 0, 0, 0, time.UTC)

func testConfig() EngineConfig {
	return EngineConfig{
		WindowSize:                5 * time.Minute,
		AllowedLateness:           10 * time.Minute,
		IdleTimeout:               5 * time.Minute,
		FutureTimeLimit:           5 * time.Minute,
		MaxWindowsPerEntity:       4,
		MaxContributionsPerWindow: 3,
		MaxEntities:               2,
	}
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := NewEngine(testConfig())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return engine
}

func contrib(entity, attID string, at time.Time) Contribution {
	return Contribution{
		EntityID:      entity,
		Role:          "actor",
		EventID:       "evt-" + attID,
		AttributionID: attID,
		At:            at,
	}
}

func mustAdd(t *testing.T, e *Engine, c Contribution, received time.Time, want Disposition) {
	t.Helper()
	got, err := e.Add(c, received)
	if err != nil {
		t.Fatalf("Add(%s): %v", c.AttributionID, err)
	}
	if got != want {
		t.Fatalf("Add(%s) = %s, want %s", c.AttributionID, got, want)
	}
}

func TestWindowOpenCloseBoundaries(t *testing.T) {
	e := newTestEngine(t)
	// Window [02:00, 02:05). Watermark = max event time - 10m.
	mustAdd(t, e, contrib("ent:a", "att:1", base.Add(2*time.Minute)), base.Add(2*time.Minute), Added)
	// Max event time 02:07 -> watermark 01:57: window still open.
	mustAdd(t, e, contrib("ent:a", "att:2", base.Add(7*time.Minute)), base.Add(7*time.Minute), Added)
	if closed := e.Advance(base.Add(7 * time.Minute)); len(closed) != 0 {
		t.Fatalf("window closed before watermark reached end: %+v", closed)
	}
	// Contribution inside the still-open window arrives within lateness.
	mustAdd(t, e, contrib("ent:a", "att:3", base.Add(4*time.Minute)), base.Add(8*time.Minute), Added)
	// Max event time 02:14 -> watermark 02:04: one minute before end.
	mustAdd(t, e, contrib("ent:a", "att:4", base.Add(14*time.Minute)), base.Add(14*time.Minute), Added)
	if closed := e.Advance(base.Add(14 * time.Minute)); len(closed) != 0 {
		t.Fatalf("window closed with watermark before end: %+v", closed)
	}
	// Max event time 02:15 -> watermark 02:05 = end: closed exactly at boundary.
	mustAdd(t, e, contrib("ent:a", "att:5", base.Add(15*time.Minute)), base.Add(15*time.Minute), Added)
	closed := e.Advance(base.Add(15 * time.Minute))
	if len(closed) != 1 {
		t.Fatalf("expected 1 closed window, got %d", len(closed))
	}
	w := closed[0]
	if w.EntityID != "ent:a" || !w.Window.Start.Equal(base) || !w.Window.End.Equal(base.Add(5*time.Minute)) {
		t.Fatalf("closed window frame wrong: %+v", w.Window)
	}
	if len(w.Contributions) != 2 {
		t.Fatalf("closed window contributions = %d, want 2", len(w.Contributions))
	}
	if e.OpenWindows("ent:a") != 3 {
		t.Fatalf("open windows after close = %d, want 3", e.OpenWindows("ent:a"))
	}
	if e.Counters().WindowsClosed != 1 {
		t.Fatalf("WindowsClosed = %d, want 1", e.Counters().WindowsClosed)
	}
}

func TestLatenessAcceptedWithinWatermarkRejectedBeyond(t *testing.T) {
	e := newTestEngine(t)
	// Establish watermark: event at 02:29:30 -> watermark 02:19:30.
	mustAdd(t, e, contrib("ent:a", "att:1", base.Add(29*time.Minute+30*time.Second)), base.Add(30*time.Minute), Added)
	// Within lateness: 02:25 is at/after the watermark -> plain Added.
	mustAdd(t, e, contrib("ent:a", "att:2", base.Add(25*time.Minute)), base.Add(31*time.Minute), Added)
	// Behind the watermark but its window [02:15, 02:20) is still open
	// (end 02:20 > watermark 02:19:30): accepted, flagged late so the
	// window is treated as recomputed.
	mustAdd(t, e, contrib("ent:a", "att:3", base.Add(19*time.Minute)), base.Add(32*time.Minute), LateAccepted)
	// Beyond: window [02:10, 02:15) closed at the watermark -> rejected
	// and counted, never re-created.
	mustAdd(t, e, contrib("ent:a", "att:4", base.Add(14*time.Minute)), base.Add(33*time.Minute), LateRejected)
	c := e.Counters()
	if c.Added != 2 || c.LateAccepted != 1 || c.LateRejected != 1 {
		t.Fatalf("counters = %+v, want added=2 late_accepted=1 late=1", c)
	}
	// Late contributions never mutate state: closing drains only accepted ones.
	closed := e.Advance(base.Add(46 * time.Minute))
	total := 0
	for _, w := range closed {
		total += len(w.Contributions)
	}
	if total != 3 {
		t.Fatalf("drained contributions = %d, want 3", total)
	}
}

func TestDedupByAttributionID(t *testing.T) {
	e := newTestEngine(t)
	at := base.Add(2 * time.Minute)
	mustAdd(t, e, contrib("ent:a", "att:1", at), at, Added)
	// Same attribution id redelivered (same and different receive time /
	// event id): counted exactly once.
	dup := contrib("ent:a", "att:1", at)
	dup.EventID = "evt-redelivery"
	mustAdd(t, e, dup, at.Add(time.Minute), Duplicate)
	mustAdd(t, e, contrib("ent:a", "att:1", at), at.Add(2*time.Minute), Duplicate)
	// Distinct attribution ids for the same event both count.
	two := contrib("ent:a", "att:2", at)
	two.EventID = "evt-att:1"
	mustAdd(t, e, two, at.Add(3*time.Minute), Added)
	// A different entity may hold the same attribution id independently.
	mustAdd(t, e, contrib("ent:b", "att:1", at), at, Added)
	if got := e.Counters().Duplicates; got != 2 {
		t.Fatalf("Duplicates = %d, want 2", got)
	}
	closed := e.Advance(at.Add(20 * time.Minute))
	for _, w := range closed {
		if w.EntityID == "ent:a" && len(w.Contributions) != 2 {
			t.Fatalf("ent:a window contributions = %d, want 2 (dedup)", len(w.Contributions))
		}
	}
}

func TestIdlePartitionWatermarkAdvances(t *testing.T) {
	e := newTestEngine(t)
	at := base.Add(2 * time.Minute)
	mustAdd(t, e, contrib("ent:a", "att:1", at), at, Added)
	// No new input. Before the idle timeout elapses the window stays open.
	if closed := e.Advance(at.Add(4 * time.Minute)); len(closed) != 0 {
		t.Fatalf("window closed before idle timeout: %+v", closed)
	}
	// After idle timeout, watermark advances from processing time:
	// now - lateness = 02:05+ -> window [02:00,02:05) closes.
	closed := e.Advance(at.Add(5*time.Minute + 10*time.Minute + time.Second))
	if len(closed) != 1 {
		t.Fatalf("idle advance closed %d windows, want 1", len(closed))
	}
	// Once idle-advanced, genuinely old data is rejected instead of
	// silently entering a reopened window.
	mustAdd(t, e, contrib("ent:a", "att:late", at.Add(time.Minute)), at.Add(20*time.Minute), LateRejected)
}

func TestFutureTimeProtection(t *testing.T) {
	e := newTestEngine(t)
	now := base
	// 5 minutes ahead is the limit: exactly at the limit is accepted.
	mustAdd(t, e, contrib("ent:a", "att:ok", now.Add(5*time.Minute)), now, Added)
	// Beyond the limit (clock skew / forged): rejected, counted, and no
	// state is polluted.
	mustAdd(t, e, contrib("ent:a", "att:forged", now.Add(6*time.Hour)), now, FutureRejected)
	if got := e.Counters().FutureRejected; got != 1 {
		t.Fatalf("FutureRejected = %d, want 1", got)
	}
	wm, ok := e.Watermark("ent:a")
	if !ok {
		t.Fatal("entity missing")
	}
	// Watermark must reflect only the legitimate 02:05 event, not the
	// forged far-future one.
	if want := now.Add(5*time.Minute - 10*time.Minute); !wm.Equal(want) {
		t.Fatalf("watermark = %s, want %s (future input must not advance it)", wm, want)
	}
	closed := e.Advance(now.Add(20 * time.Minute))
	if len(closed) != 1 || len(closed[0].Contributions) != 1 {
		t.Fatalf("closed = %+v, want exactly the legitimate contribution", closed)
	}
}

func TestBoundedStateEvictionAndRejection(t *testing.T) {
	e := newTestEngine(t) // MaxWindowsPerEntity=4, MaxContributionsPerWindow=3, MaxEntities=2
	// Fill 4 windows for ent:a with events 5 minutes apart.
	for i := 0; i < 4; i++ {
		at := base.Add(time.Duration(i*5+1) * time.Minute)
		mustAdd(t, e, contrib("ent:a", "att:w"+string(rune('0'+i)), at), at, Added)
	}
	if e.OpenWindows("ent:a") != 4 {
		t.Fatalf("open windows = %d, want 4", e.OpenWindows("ent:a"))
	}
	// A newer window beyond the budget evicts the oldest (counted), never
	// drops silently.
	at := base.Add(21 * time.Minute)
	mustAdd(t, e, contrib("ent:a", "att:w4", at), at, Added)
	if e.OpenWindows("ent:a") != 4 {
		t.Fatalf("open windows after eviction = %d, want 4", e.OpenWindows("ent:a"))
	}
	if got := e.Counters().WindowsEvicted; got != 1 {
		t.Fatalf("WindowsEvicted = %d, want 1", got)
	}
	// A contribution for the evicted (oldest) window: watermark 02:11
	// hasn't passed its end 02:05... it has: window [02:00,02:05) end is
	// behind watermark -> late rejected, not silently re-created.
	mustAdd(t, e, contrib("ent:a", "att:old", base.Add(time.Minute)), at, LateRejected)
	// Per-window contribution cap: excess rejected and counted. Window
	// [02:20, 02:25) already holds att:w4, so two more fit, the next is
	// rejected.
	w5 := base.Add(20 * time.Minute)
	for i, id := range []string{"att:c1", "att:c2"} {
		mustAdd(t, e, contrib("ent:a", id, w5.Add(time.Duration(i+1)*time.Second)), at, Added)
	}
	mustAdd(t, e, contrib("ent:a", "att:c3", w5.Add(4*time.Second)), at, CapacityRejected)
	// Entity cap: a third entity is rejected fail-closed and counted.
	mustAdd(t, e, contrib("ent:b", "att:b1", at), at, Added)
	mustAdd(t, e, contrib("ent:c", "att:c1", at), at, CapacityRejected)
	c := e.Counters()
	if c.CapacityRejected != 2 {
		t.Fatalf("CapacityRejected = %d, want 2", c)
	}
}

func TestSnapshotRestartRecovery(t *testing.T) {
	e := newTestEngine(t)
	at := base.Add(2 * time.Minute)
	mustAdd(t, e, contrib("ent:a", "att:1", at), at, Added)
	mustAdd(t, e, contrib("ent:a", "att:2", at.Add(time.Minute)), at.Add(time.Minute), Added)
	mustAdd(t, e, contrib("ent:a", "att:1", at), at.Add(2*time.Minute), Duplicate)
	mustAdd(t, e, contrib("ent:a", "att:bad", at.Add(6*time.Hour)), at, FutureRejected)

	data, err := e.Export().Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	restored, err := Import(data)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	// Counters survive the restart.
	if restored.Counters() != e.Counters() {
		t.Fatalf("counters diverged: %+v vs %+v", restored.Counters(), e.Counters())
	}
	// Watermark survives.
	wm0, _ := e.Watermark("ent:a")
	wm1, ok := restored.Watermark("ent:a")
	if !ok || !wm0.Equal(wm1) {
		t.Fatalf("watermark not restored: %s vs %s", wm0, wm1)
	}
	// Dedup survives: the same redelivery is still a duplicate.
	mustAdd(t, restored, contrib("ent:a", "att:1", at), at.Add(3*time.Minute), Duplicate)
	// Windows drain identically after recovery.
	closed := restored.Advance(at.Add(20 * time.Minute))
	if len(closed) != 1 || len(closed[0].Contributions) != 2 {
		t.Fatalf("restored drain = %+v, want 1 window with 2 contributions", closed)
	}
}

func TestImportFailClosed(t *testing.T) {
	e := newTestEngine(t)
	mustAdd(t, e, contrib("ent:a", "att:1", base), base, Added)
	data, _ := e.Export().Marshal()
	if _, err := Import([]byte("{garbage")); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
	if _, err := Import([]byte(`{"version":99,"config":{},"entities":{}}`)); err == nil {
		t.Fatal("unknown snapshot version accepted")
	}
	// Tampered contribution (empty event id) must be rejected, not
	// half-restored.
	var tampered map[string]any
	if err := jsonUnmarshal(data, &tampered); err != nil {
		t.Fatal(err)
	}
	ent := tampered["entities"].(map[string]any)["ent:a"].(map[string]any)
	for _, w := range ent["windows"].(map[string]any) {
		w.(map[string]any)["contributions"].([]any)[0].(map[string]any)["event_id"] = ""
	}
	bad, _ := jsonMarshal(tampered)
	if _, err := Import(bad); err == nil {
		t.Fatal("corrupt contribution accepted")
	}
}

func TestDeterministicRegardlessOfArrivalOrder(t *testing.T) {
	build := func(order []int) *Engine {
		e := newTestEngine(t)
		times := []time.Time{
			base.Add(3 * time.Minute), base.Add(7 * time.Minute),
			base.Add(5 * time.Minute), base.Add(10 * time.Minute),
		}
		for _, i := range order {
			mustAdd(t, e, contrib("ent:a", "att:"+string(rune('a'+i)), times[i]), times[i], Added)
		}
		return e
	}
	forward := build([]int{0, 1, 2, 3})
	reverse := build([]int{3, 2, 1, 0})
	cf := forward.Advance(base.Add(32 * time.Minute))
	cr := reverse.Advance(base.Add(32 * time.Minute))
	if len(cf) != len(cr) {
		t.Fatalf("closed count differs: %d vs %d", len(cf), len(cr))
	}
	for i := range cf {
		if !cf[i].Window.Start.Equal(cr[i].Window.Start) || len(cf[i].Contributions) != len(cr[i].Contributions) {
			t.Fatalf("window %d differs: %+v vs %+v", i, cf[i], cr[i])
		}
	}
	if forward.Counters() != reverse.Counters() {
		t.Fatalf("counters differ: %+v vs %+v", forward.Counters(), reverse.Counters())
	}
}

func TestInvalidConfigAndContributionRejected(t *testing.T) {
	if _, err := NewEngine(EngineConfig{WindowSize: 0}); err == nil {
		t.Fatal("zero window size accepted")
	}
	e := newTestEngine(t)
	if _, err := e.Add(Contribution{Role: "actor", EventID: "e", AttributionID: "att:1", At: base}, base); err == nil {
		t.Fatal("empty entity accepted")
	}
	if _, err := e.Add(contrib("ent:a", "", base), base); err == nil {
		t.Fatal("empty attribution id accepted")
	}
}

func TestEntityCapEvictsIdleShellNotActiveState(t *testing.T) {
	e := newTestEngine(t) // MaxEntities=2
	at := base.Add(2 * time.Minute)
	mustAdd(t, e, contrib("ent:a", "att:1", at), at, Added)
	// Drain ent:a's only window; the shell keeps its watermark.
	if closed := e.Advance(at.Add(20 * time.Minute)); len(closed) != 1 {
		t.Fatalf("closed = %d, want 1", len(closed))
	}
	bt := base.Add(22 * time.Minute)
	mustAdd(t, e, contrib("ent:b", "att:b1", bt), bt, Added)
	// Cap reached: ent:a is an idle shell, ent:b holds window state. A new
	// entity evicts the shell (counted), never ent:b's active state.
	mustAdd(t, e, contrib("ent:c", "att:c1", bt), bt, Added)
	if got := e.Counters().EntitiesPruned; got != 1 {
		t.Fatalf("EntitiesPruned = %d, want 1", got)
	}
	if e.OpenWindows("ent:b") != 1 {
		t.Fatalf("ent:b window state lost during shell eviction")
	}
	// With every tracked entity holding window state, a further new entity
	// is rejected fail-closed.
	mustAdd(t, e, contrib("ent:d", "att:d1", bt), bt, CapacityRejected)
}
