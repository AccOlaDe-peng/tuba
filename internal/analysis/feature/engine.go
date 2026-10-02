package feature

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// F02 window state engine. This file implements the shared windowing
// semantics contract that python/tuba_analysis/features.py (FeatureWindows)
// mirrors: per-entity event-time watermark = max observed event time minus
// allowed lateness; a contribution is late once its event time falls behind
// the watermark; windows close when the watermark reaches the window end
// (equivalently, when max event time passes window end + allowed lateness);
// an entity partition with no input for the idle timeout still advances its
// watermark from processing time so windows close instead of waiting forever;
// contributions whose event time exceeds receive time by more than the future
// time limit are rejected and counted, never applied to state. The two
// implementations differ only in state layout: the Go engine aggregates
// contributions into tumbling windows here, while the Python side retains the
// bounded event history those windows are computed from.

var (
	ErrInvalidEngineConfig  = errors.New("engine config bounds must be positive and durations non-negative")
	ErrSnapshotVersion      = errors.New("unsupported engine snapshot version")
	ErrSnapshotCorrupt      = errors.New("engine snapshot is corrupt")
	ErrEmptyAttribution     = errors.New("contribution attribution id is required")
	ErrNonPositiveWindowCfg = errors.New("window size and allowed lateness must be positive")
)

// Default bounds from docs/DESIGN-BASELINE.md §6: allowed lateness 10
// minutes, idle partitions participate in the watermark after 5 minutes
// without input, event time more than 5 minutes ahead of receive time is
// quarantined.
const (
	DefaultAllowedLateness           = 10 * time.Minute
	DefaultIdleTimeout               = 5 * time.Minute
	DefaultFutureTimeLimit           = 5 * time.Minute
	DefaultMaxWindowsPerEntity       = 16
	DefaultMaxContributionsPerWindow = 1024
	DefaultMaxEntities               = 100000
)

// Disposition is the outcome of offering one contribution to the engine.
// Every non-Added outcome is counted in Counters: nothing is silently
// dropped.
type Disposition int

const (
	// Added — the contribution entered exactly one open window.
	Added Disposition = iota
	// LateAccepted — the contribution is behind the entity watermark but
	// its window is still open, so it is accepted and downstream consumers
	// must treat the window as recomputed (design baseline: late data
	// within the retention boundary recomputes the same window).
	LateAccepted
	// Duplicate — the attribution id was already recorded in its window;
	// the repeat delivery is counted exactly once.
	Duplicate
	// LateRejected — the event time is behind the entity watermark, or its
	// window is already closed/evicted.
	LateRejected
	// FutureRejected — the event time is further ahead of the receive time
	// than the future time limit (clock skew / forged input).
	FutureRejected
	// CapacityRejected — accepting the contribution would exceed a
	// configured bound (max entities, max contributions per window, or a
	// too-old window while the entity's window budget is full).
	CapacityRejected
)

func (d Disposition) String() string {
	switch d {
	case Added:
		return "added"
	case LateAccepted:
		return "late_accepted"
	case Duplicate:
		return "duplicate"
	case LateRejected:
		return "late_rejected"
	case FutureRejected:
		return "future_rejected"
	case CapacityRejected:
		return "capacity_rejected"
	default:
		return "unknown"
	}
}

// Counters are the fail-closed audit trail of the engine: every rejected,
// deduplicated, closed or evicted unit of state is visible here. They are
// exported with the snapshot so restarts never reset the accounting.
type Counters struct {
	Added            uint64 `json:"added"`
	LateAccepted     uint64 `json:"late_accepted"`
	Duplicates       uint64 `json:"duplicates"`
	LateRejected     uint64 `json:"late_rejected"`
	FutureRejected   uint64 `json:"future_rejected"`
	CapacityRejected uint64 `json:"capacity_rejected"`
	WindowsClosed    uint64 `json:"windows_closed"`
	WindowsEvicted   uint64 `json:"windows_evicted"`
	EntitiesPruned   uint64 `json:"entities_pruned"`
}

// EngineConfig bounds the window state. All durations and bounds must be
// positive; ApplyDefaults fills zero-valued fields with the design-baseline
// defaults.
type EngineConfig struct {
	// WindowSize is the tumbling event-time window length [start, end).
	WindowSize time.Duration
	// AllowedLateness is how far the per-entity watermark trails the
	// maximum observed event time.
	AllowedLateness time.Duration
	// IdleTimeout: an entity partition with no input for this long has its
	// watermark advanced from processing time, so its windows still close.
	IdleTimeout time.Duration
	// FutureTimeLimit: event times further than this ahead of the receive
	// time are rejected (future_time guard).
	FutureTimeLimit time.Duration
	// MaxWindowsPerEntity bounds per-entity window state; the oldest window
	// is evicted (counted) when a newer window needs the slot.
	MaxWindowsPerEntity int
	// MaxContributionsPerWindow bounds contributions inside one window;
	// excess is rejected and counted (fail-closed).
	MaxContributionsPerWindow int
	// MaxEntities bounds how many entities the engine tracks; new entities
	// beyond the bound are rejected and counted (fail-closed).
	MaxEntities int
}

// ApplyDefaults returns the config with zero fields replaced by the
// design-baseline defaults.
func (c EngineConfig) ApplyDefaults() EngineConfig {
	if c.AllowedLateness == 0 {
		c.AllowedLateness = DefaultAllowedLateness
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.FutureTimeLimit == 0 {
		c.FutureTimeLimit = DefaultFutureTimeLimit
	}
	if c.MaxWindowsPerEntity == 0 {
		c.MaxWindowsPerEntity = DefaultMaxWindowsPerEntity
	}
	if c.MaxContributionsPerWindow == 0 {
		c.MaxContributionsPerWindow = DefaultMaxContributionsPerWindow
	}
	if c.MaxEntities == 0 {
		c.MaxEntities = DefaultMaxEntities
	}
	return c
}

// Validate rejects non-positive windows/bounds fail-closed.
func (c EngineConfig) Validate() error {
	if c.WindowSize <= 0 || c.AllowedLateness <= 0 {
		return fmt.Errorf("%w: window=%s lateness=%s", ErrNonPositiveWindowCfg, c.WindowSize, c.AllowedLateness)
	}
	if c.IdleTimeout <= 0 || c.FutureTimeLimit <= 0 ||
		c.MaxWindowsPerEntity <= 0 || c.MaxContributionsPerWindow <= 0 || c.MaxEntities <= 0 {
		return fmt.Errorf("%w: %+v", ErrInvalidEngineConfig, c)
	}
	return nil
}

type windowState struct {
	Start time.Time
	// End is the exclusive window end, stored (not recomputed from the
	// config) so snapshots survive WindowSize changes across restarts.
	End           time.Time
	Contributions []Contribution
	ids           map[string]struct{}
}

type entityState struct {
	MaxEventTime time.Time
	LastReceive  time.Time
	IdleFloor    time.Time // watermark floor applied after idle timeout
	Windows      map[int64]*windowState
}

func (s *entityState) watermark(cfg EngineConfig) time.Time {
	wm := s.MaxEventTime.Add(-cfg.AllowedLateness)
	if s.IdleFloor.After(wm) {
		wm = s.IdleFloor
	}
	return wm
}

// ClosedWindow is a window whose watermark has passed its end: its
// contributions are final and drained for downstream persistence (feature
// samples per design baseline §6). Closed state leaves the engine, keeping
// retained state bounded.
type ClosedWindow struct {
	EntityID      string
	Window        Window
	Contributions []Contribution
}

// BackfillReason identifies why a contribution (or a whole window) left the
// live path and must enter through controlled backfill instead.
type BackfillReason string

const (
	// BackfillWindowClosed — the contribution's event time falls into a
	// window the watermark already closed (LateRejected).
	BackfillWindowClosed BackfillReason = "window_closed"
	// BackfillBeyondRetention — the contribution is older than every
	// retained window while the entity's window budget is full
	// (LateRejected).
	BackfillBeyondRetention BackfillReason = "beyond_retention"
	// BackfillWindowEvicted — an open window was evicted to make room for a
	// newer one; its contributions never produced a closed feature record.
	BackfillWindowEvicted BackfillReason = "window_evicted"
)

// BackfillItem records one unit of out-of-bounds data for the controlled
// backfill path (design baseline §6: out-of-bounds data may only enter a new
// generation through controlled backfill). The sink records items durably;
// scheduling the actual backfill job is F08/replay scope.
type BackfillItem struct {
	EntityID     string
	Contribution Contribution
	WindowStart  time.Time
	Reason       BackfillReason
	RejectedAt   time.Time
}

// BackfillSink receives out-of-bounds data. A nil sink keeps the F02
// behavior (counted only). A sink error is fail-closed: Add returns the
// error and the contribution is not treated as handled.
type BackfillSink interface {
	RecordBackfill(BackfillItem) error
}

// Engine is the bounded per-entity window state machine. It is deterministic
// and order-independent: the same contributions in any arrival order produce
// the same windows. Persistence is snapshot-based (Export/Import); the
// caller stores snapshot bytes as business state inside the T02 inbox +
// state + checkpoint + outbox transaction, so recovery is authoritative from
// the PG checkpoint.
type Engine struct {
	cfg      EngineConfig
	entities map[string]*entityState
	counters Counters
	backfill BackfillSink
}

// NewEngine validates the config fail-closed and returns an empty engine.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	cfg = cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg, entities: map[string]*entityState{}}, nil
}

// Counters returns a copy of the engine's audit counters.
func (e *Engine) Counters() Counters { return e.counters }

// SetBackfillSink installs the controlled-backfill recording path for
// out-of-bounds data (LateRejected contributions and evicted windows).
func (e *Engine) SetBackfillSink(sink BackfillSink) { e.backfill = sink }

// recordBackfill hands one out-of-bounds contribution to the sink; a sink
// failure is fail-closed.
func (e *Engine) recordBackfill(entityID string, c Contribution, windowStart time.Time, reason BackfillReason, at time.Time) error {
	if e.backfill == nil {
		return nil
	}
	return e.backfill.RecordBackfill(BackfillItem{
		EntityID:     entityID,
		Contribution: c,
		WindowStart:  windowStart,
		Reason:       reason,
		RejectedAt:   at,
	})
}

// windowStart aligns an event time to its tumbling window start.
func (e *Engine) windowStart(t time.Time) time.Time {
	return t.UTC().Truncate(e.cfg.WindowSize)
}

// refreshIdle advances an entity's watermark floor from processing time once
// the partition has been idle for the configured timeout.
func (e *Engine) refreshIdle(s *entityState, now time.Time) {
	if s.LastReceive.IsZero() {
		return
	}
	if now.Sub(s.LastReceive) >= e.cfg.IdleTimeout {
		if floor := now.Add(-e.cfg.AllowedLateness); floor.After(s.IdleFloor) {
			s.IdleFloor = floor
		}
	}
}

// Add offers one contribution. receivedAt is the processing (receive) time
// used for the future-time guard and idle tracking. The disposition tells
// the caller exactly what happened; every outcome is counted.
func (e *Engine) Add(c Contribution, receivedAt time.Time) (Disposition, error) {
	if err := c.Validate(); err != nil {
		return Added, err
	}
	if c.AttributionID == "" {
		return Added, ErrEmptyAttribution
	}
	at := c.At.UTC()
	receivedAt = receivedAt.UTC()

	if at.After(receivedAt.Add(e.cfg.FutureTimeLimit)) {
		e.counters.FutureRejected++
		return FutureRejected, nil
	}

	s, ok := e.entities[c.EntityID]
	if !ok {
		if len(e.entities) >= e.cfg.MaxEntities {
			// Bound churn fail-closed but without losing late-rejection
			// knowledge for active entities: only an idle shell with no
			// windows may be evicted, the stalest first. If every tracked
			// entity still holds window state, reject.
			victim := ""
			var oldestReceive time.Time
			for id, other := range e.entities {
				if len(other.Windows) != 0 {
					continue
				}
				if victim == "" || other.LastReceive.Before(oldestReceive) {
					victim, oldestReceive = id, other.LastReceive
				}
			}
			if victim == "" {
				e.counters.CapacityRejected++
				return CapacityRejected, nil
			}
			delete(e.entities, victim)
			e.counters.EntitiesPruned++
		}
		s = &entityState{Windows: map[int64]*windowState{}}
		e.entities[c.EntityID] = s
	}

	e.refreshIdle(s, receivedAt)
	wm := s.watermark(e.cfg)

	start := e.windowStart(at)
	key := start.Unix()
	w, ok := s.Windows[key]
	if !ok {
		if !wm.Before(start.Add(e.cfg.WindowSize)) {
			// The window is already closed (or was evicted) — same
			// fail-closed outcome as a late contribution. Not dropped:
			// recorded for controlled backfill.
			e.counters.LateRejected++
			if err := e.recordBackfill(c.EntityID, c, start, BackfillWindowClosed, receivedAt); err != nil {
				return Added, fmt.Errorf("backfill recording failed: %w", err)
			}
			return LateRejected, nil
		}
		if len(s.Windows) >= e.cfg.MaxWindowsPerEntity {
			oldest := oldestWindowKey(s)
			if !start.After(time.Unix(oldest, 0)) {
				// Beyond the retention boundary while the budget is
				// full: reject (counted, backfill-recorded) rather than
				// silently evicting newer state; controlled backfill is
				// the only way in.
				e.counters.LateRejected++
				if err := e.recordBackfill(c.EntityID, c, start, BackfillBeyondRetention, receivedAt); err != nil {
					return Added, fmt.Errorf("backfill recording failed: %w", err)
				}
				return LateRejected, nil
			}
			// The evicted open window never produced a closed feature
			// record; record every contribution of it for backfill.
			for _, evicted := range s.Windows[oldest].Contributions {
				if err := e.recordBackfill(c.EntityID, evicted, time.Unix(oldest, 0).UTC(), BackfillWindowEvicted, receivedAt); err != nil {
					return Added, fmt.Errorf("backfill recording failed: %w", err)
				}
			}
			delete(s.Windows, oldest)
			e.counters.WindowsEvicted++
		}
		w = &windowState{Start: start, ids: map[string]struct{}{}}
		w.End = start.Add(e.cfg.WindowSize)
		s.Windows[key] = w
	}

	if _, seen := w.ids[c.AttributionID]; seen {
		e.counters.Duplicates++
		return Duplicate, nil
	}
	if len(w.Contributions) >= e.cfg.MaxContributionsPerWindow {
		e.counters.CapacityRejected++
		return CapacityRejected, nil
	}

	w.ids[c.AttributionID] = struct{}{}
	c.At = at
	w.Contributions = append(w.Contributions, c)
	if at.After(s.MaxEventTime) {
		s.MaxEventTime = at
	}
	s.LastReceive = receivedAt
	if at.Before(wm) {
		e.counters.LateAccepted++
		return LateAccepted, nil
	}
	e.counters.Added++
	return Added, nil
}

func oldestWindowKey(s *entityState) int64 {
	first := true
	var oldest int64
	for key := range s.Windows {
		if first || key < oldest {
			oldest = key
			first = false
		}
	}
	return oldest
}

// Advance moves every entity's watermark to now (idle partitions included)
// and drains windows whose end the watermark has reached. Returned windows
// are sorted by (entity, window start) for deterministic downstream
// persistence. Drained entity shells are kept (bounded by MaxEntities) so
// their watermark — and with it late-rejection and the idle floor — survives
// window drainage.
func (e *Engine) Advance(now time.Time) []ClosedWindow {
	now = now.UTC()
	var closed []ClosedWindow
	for id, s := range e.entities {
		e.refreshIdle(s, now)
		wm := s.watermark(e.cfg)
		for key, w := range s.Windows {
			if !wm.Before(w.End) {
				closed = append(closed, ClosedWindow{
					EntityID: id,
					Window: Window{
						Start:           w.Start,
						End:             w.End,
						AllowedLateness: e.cfg.AllowedLateness,
					},
					Contributions: w.Contributions,
				})
				delete(s.Windows, key)
				e.counters.WindowsClosed++
			}
		}
	}
	sort.Slice(closed, func(i, j int) bool {
		if closed[i].EntityID != closed[j].EntityID {
			return closed[i].EntityID < closed[j].EntityID
		}
		return closed[i].Window.Start.Before(closed[j].Window.Start)
	})
	return closed
}

// Watermark returns the entity's current event-time watermark and whether
// the entity is tracked.
func (e *Engine) Watermark(entityID string) (time.Time, bool) {
	s, ok := e.entities[entityID]
	if !ok {
		return time.Time{}, false
	}
	return s.watermark(e.cfg), true
}

// MinWatermark is the global watermark: the minimum over all tracked
// entities, matching the Python FeatureWindows global watermark.
func (e *Engine) MinWatermark() (time.Time, bool) {
	var min time.Time
	ok := false
	for _, s := range e.entities {
		wm := s.watermark(e.cfg)
		if !ok || wm.Before(min) {
			min, ok = wm, true
		}
	}
	return min, ok
}

// OpenWindows reports how many windows the entity currently retains.
func (e *Engine) OpenWindows(entityID string) int {
	if s, ok := e.entities[entityID]; ok {
		return len(s.Windows)
	}
	return 0
}

// Entities reports how many entities the engine currently tracks.
func (e *Engine) Entities() int { return len(e.entities) }

// WindowContributions returns the current contents of one open window. F03
// late correction uses it: after a LateAccepted contribution enters a still
// open window, the caller recomputes the same window from the full
// contribution set and emits the same business key with a higher revision.
func (e *Engine) WindowContributions(entityID string, start time.Time) (Window, []Contribution, bool) {
	s, ok := e.entities[entityID]
	if !ok {
		return Window{}, nil, false
	}
	w, ok := s.Windows[start.UTC().Unix()]
	if !ok {
		return Window{}, nil, false
	}
	cp := make([]Contribution, len(w.Contributions))
	copy(cp, w.Contributions)
	return Window{Start: w.Start, End: w.End, AllowedLateness: e.cfg.AllowedLateness}, cp, true
}

const snapshotVersion = 1

type windowSnapshot struct {
	Start         time.Time      `json:"start"`
	End           time.Time      `json:"end"`
	Contributions []Contribution `json:"contributions"`
}

type entitySnapshot struct {
	MaxEventTime time.Time                 `json:"max_event_time"`
	LastReceive  time.Time                 `json:"last_receive"`
	IdleFloor    time.Time                 `json:"idle_floor"`
	Windows      map[int64]*windowSnapshot `json:"windows"`
}

// Snapshot is the restart-recovery state of the engine. Callers persist it
// as business state within the T02 single-transaction protocol, so recovery
// follows the PG checkpoint.
type Snapshot struct {
	Version  int                        `json:"version"`
	Config   EngineConfig               `json:"config"`
	Entities map[string]*entitySnapshot `json:"entities"`
	Counters Counters                   `json:"counters"`
}

// Export serializes the full engine state deterministically.
func (e *Engine) Export() *Snapshot {
	snap := &Snapshot{
		Version:  snapshotVersion,
		Config:   e.cfg,
		Entities: make(map[string]*entitySnapshot, len(e.entities)),
		Counters: e.counters,
	}
	for id, s := range e.entities {
		es := &entitySnapshot{
			MaxEventTime: s.MaxEventTime,
			LastReceive:  s.LastReceive,
			IdleFloor:    s.IdleFloor,
			Windows:      make(map[int64]*windowSnapshot, len(s.Windows)),
		}
		for key, w := range s.Windows {
			cp := make([]Contribution, len(w.Contributions))
			copy(cp, w.Contributions)
			es.Windows[key] = &windowSnapshot{Start: w.Start, End: w.End, Contributions: cp}
		}
		snap.Entities[id] = es
	}
	return snap
}

// Marshal renders the snapshot as JSON bytes for durable storage.
func (s *Snapshot) Marshal() ([]byte, error) { return json.Marshal(s) }

// Import rebuilds an engine from a snapshot, fail-closed on an unsupported
// version or any corruption: a half-restored engine is never returned.
func Import(data []byte) (*Engine, error) {
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotCorrupt, err)
	}
	if snap.Version != snapshotVersion {
		return nil, fmt.Errorf("%w: %d", ErrSnapshotVersion, snap.Version)
	}
	cfg := snap.Config.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotCorrupt, err)
	}
	engine := &Engine{cfg: cfg, entities: map[string]*entityState{}, counters: snap.Counters}
	for id, es := range snap.Entities {
		if id == "" || es == nil {
			return nil, fmt.Errorf("%w: entity entry", ErrSnapshotCorrupt)
		}
		s := &entityState{
			MaxEventTime: es.MaxEventTime,
			LastReceive:  es.LastReceive,
			IdleFloor:    es.IdleFloor,
			Windows:      map[int64]*windowState{},
		}
		for key, ws := range es.Windows {
			if ws == nil || ws.Start.Unix() != key || !ws.Start.Before(ws.End) {
				return nil, fmt.Errorf("%w: window entry for %s", ErrSnapshotCorrupt, id)
			}
			w := &windowState{Start: ws.Start, End: ws.End, ids: map[string]struct{}{}}
			for _, c := range ws.Contributions {
				if err := c.Validate(); err != nil {
					return nil, fmt.Errorf("%w: contribution for %s: %v", ErrSnapshotCorrupt, id, err)
				}
				if c.AttributionID == "" {
					return nil, fmt.Errorf("%w: contribution for %s missing attribution id", ErrSnapshotCorrupt, id)
				}
				w.ids[c.AttributionID] = struct{}{}
				w.Contributions = append(w.Contributions, c)
			}
			s.Windows[key] = w
		}
		engine.entities[id] = s
	}
	return engine, nil
}
