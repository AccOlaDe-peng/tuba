// Reference/diagnostic mirror of the F06 finding business-key revision /
// retracted / generation semantics. The authoritative production
// implementation is Python tuba_analysis/revisions.py; both sides implement
// the exact same rules and are locked by the same golden vector (see
// revisions_test.go and python/tests/test_revisions.py, which cite each
// other). There is no worker, topic, or database wiring here.
//
// Business key composition (locked by tests, aligned with contracts/ids.md
// anomaly.id: tenant, rule/version, entity key, stable window key,
// generation):
//
//	organization_id | rule_id | entity_id | window_start(UTC RFC3339) | generation
//
// Rules mirrored from the Python module:
//   - Correction re-publishes the same business key at the next monotonic
//     revision; older revisions stay in history, never deleted.
//   - Retracted is a tombstone frame (OperationRetracted) at the next
//     revision; the current view marks the key retracted, history retained.
//   - Apply mirrors the E05 external-version projection semantics: strictly
//     greater revision accepted; equal revision with identical content is an
//     idempotent no-op; equal revision with different content is
//     ErrRevisionConflict (fail-closed); an older revision is StaleRevisionError
//     and the stored newer value is never overwritten.
//   - Generation isolation: the generation is part of the business key, so a
//     rule upgrade on a new generation never overwrites old-generation
//     findings; SwitchGeneration retires the old generation by retracting all
//     of its live keys, so the alert view never holds the same
//     (entity, window, rule) finding from two generations — no double online
//     alerts.
//   - Recompute (late data inside the retention boundary): unchanged verdict
//     is a no-op, changed verdict is the next revision, a flipped verdict
//     publishes a retracted tombstone.
package detection

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// DefaultGeneration is the first-phase finding generation.
const DefaultGeneration = "g1"

// Finding operations.
const (
	OperationUpsert    = "upsert"
	OperationRetracted = "retracted"
)

const ledgerSnapshotVersion = 1

// ErrRevisionConflict reports a same-revision different-content write
// (mirrors the E05 PROJECTION_REVISION_CONFLICT semantics).
var ErrRevisionConflict = errors.New("revision already exists with different content")

// StaleRevisionError reports an out-of-order write whose revision is older
// than the stored one; the stored newer value is never overwritten (mirrors
// the E05 StaleRevisionError semantics).
type StaleRevisionError struct {
	BusinessKey string
	Existing    int64
	Incoming    int64
}

func (e StaleRevisionError) Error() string {
	return fmt.Sprintf("stale revision for %s: stored revision %d, incoming %d", e.BusinessKey, e.Existing, e.Incoming)
}

// IsStaleRevision reports whether err is a StaleRevisionError.
func IsStaleRevision(err error) bool {
	var target StaleRevisionError
	return errors.As(err, &target)
}

// BusinessKey builds the canonical finding business key; every part is
// mandatory (fail-closed).
func BusinessKey(organizationID, ruleID, entityID string, windowStart time.Time, generation string) (string, error) {
	if organizationID == "" || ruleID == "" || entityID == "" {
		return "", fmt.Errorf("%w: business key requires organization id, rule id, and entity id", ErrEmptyFindingRef)
	}
	if generation == "" {
		return "", fmt.Errorf("%w: business key requires a generation", ErrMissingGeneration)
	}
	if windowStart.IsZero() {
		return "", fmt.Errorf("%w: business key requires a window start", ErrEmptyFindingRef)
	}
	for _, part := range []string{organizationID, ruleID, entityID, generation} {
		if strings.Contains(part, "|") {
			return "", fmt.Errorf("%w: business key parts must not contain the separator", ErrEmptyFindingRef)
		}
	}
	return strings.Join([]string{organizationID, ruleID, entityID, windowStart.UTC().Format(time.RFC3339), generation}, "|"), nil
}

// ContentHash is the deterministic sha256 over the canonical JSON document.
func ContentHash(document json.RawMessage) string {
	canonical := "null"
	if len(document) > 0 {
		var value any
		if err := json.Unmarshal(document, &value); err == nil {
			if buf, err := json.Marshal(value); err == nil {
				canonical = string(buf)
			}
		}
	}
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// Frame is one immutable revision frame of a finding business key.
type Frame struct {
	BusinessKey string          `json:"business_key"`
	ObjectID    string          `json:"object_id"`
	Revision    int64           `json:"revision"`
	Operation   string          `json:"operation"`
	Generation  string          `json:"generation"`
	Hash        string          `json:"content_hash"`
	At          time.Time       `json:"at"`
	Reason      string          `json:"reason,omitempty"`
	Document    json.RawMessage `json:"document,omitempty"`
}

func (f Frame) validate() error {
	if f.Revision < 1 {
		return fmt.Errorf("%w: frame revision must be >= 1", ErrInvalidRevision)
	}
	if f.Operation != OperationUpsert && f.Operation != OperationRetracted {
		return fmt.Errorf("%w: unknown frame operation %q", ErrInvalidRevision, f.Operation)
	}
	if f.BusinessKey == "" || f.ObjectID == "" || f.Generation == "" {
		return fmt.Errorf("%w: frame requires business key, object id, and generation", ErrEmptyFindingRef)
	}
	if f.At.IsZero() {
		return fmt.Errorf("%w: frame requires a timestamp", ErrEmptyFindingRef)
	}
	return nil
}

// FindingLedger is the revision ledger over finding business keys. Producer
// side: Publish / Retract / Recompute / SwitchGeneration. Consumer side:
// Apply in external-version order.
type FindingLedger struct {
	history map[string][]Frame
}

// NewFindingLedger returns an empty ledger.
func NewFindingLedger() *FindingLedger {
	return &FindingLedger{history: map[string][]Frame{}}
}

// Apply consumes one frame in external-version order. It reports whether the
// frame became the new current state; an idempotent replay returns false.
// Same revision with different content fails closed with ErrRevisionConflict;
// an older revision is a StaleRevisionError and never overwrites.
func (l *FindingLedger) Apply(frame Frame) (bool, error) {
	if err := frame.validate(); err != nil {
		return false, err
	}
	frames := l.history[frame.BusinessKey]
	var current *Frame
	if len(frames) > 0 {
		current = &frames[len(frames)-1]
	}
	switch {
	case current == nil || frame.Revision > current.Revision:
		l.history[frame.BusinessKey] = append(frames, frame)
		return true, nil
	case frame.Revision == current.Revision:
		if frame.Hash == current.Hash && frame.Operation == current.Operation {
			return false, nil
		}
		return false, fmt.Errorf("%w: %s revision %d", ErrRevisionConflict, frame.BusinessKey, frame.Revision)
	default:
		return false, StaleRevisionError{BusinessKey: frame.BusinessKey, Existing: current.Revision, Incoming: frame.Revision}
	}
}

// Publish publishes (or corrects) one finding at the next revision of its
// business key.
func (l *FindingLedger) Publish(document json.RawMessage, organizationID, ruleID, entityID string, windowStart time.Time, generation, objectID string, at time.Time, reason string) (Frame, error) {
	if objectID == "" {
		return Frame{}, fmt.Errorf("%w: finding object id is required", ErrEmptyFindingRef)
	}
	key, err := BusinessKey(organizationID, ruleID, entityID, windowStart, generation)
	if err != nil {
		return Frame{}, err
	}
	frame := Frame{
		BusinessKey: key,
		ObjectID:    objectID,
		Revision:    l.nextRevision(key),
		Operation:   OperationUpsert,
		Generation:  generation,
		Hash:        ContentHash(document),
		At:          at.UTC(),
		Reason:      reason,
		Document:    document,
	}
	_, err = l.Apply(frame)
	return frame, err
}

// Retract tombstones one business key at the next revision; history is
// retained.
func (l *FindingLedger) Retract(key string, at time.Time, reason string) (Frame, error) {
	if reason == "" {
		return Frame{}, fmt.Errorf("%w: retraction requires a reason", ErrInvalidRevision)
	}
	current := l.Current(key)
	if current == nil {
		return Frame{}, fmt.Errorf("%w: cannot retract an unknown business key: %s", ErrEmptyFindingRef, key)
	}
	if current.Operation == OperationRetracted {
		return Frame{}, fmt.Errorf("%w: business key is already retracted: %s", ErrInvalidRevision, key)
	}
	frame := Frame{
		BusinessKey: key,
		ObjectID:    current.ObjectID,
		Revision:    current.Revision + 1,
		Operation:   OperationRetracted,
		Generation:  current.Generation,
		Hash:        current.Hash,
		At:          at.UTC(),
		Reason:      reason,
		Document:    current.Document,
	}
	_, err := l.Apply(frame)
	return frame, err
}

// Recompute re-evaluates one business key after late data (inside the
// retention boundary). An unchanged verdict is an idempotent no-op (nil
// frame); a changed verdict publishes the next revision; a flipped verdict
// (nil document) publishes a retracted tombstone.
func (l *FindingLedger) Recompute(document json.RawMessage, organizationID, ruleID, entityID string, windowStart time.Time, generation, objectID string, at time.Time) (*Frame, error) {
	key, err := BusinessKey(organizationID, ruleID, entityID, windowStart, generation)
	if err != nil {
		return nil, err
	}
	current := l.Current(key)
	if len(document) == 0 {
		if current == nil || current.Operation == OperationRetracted {
			return nil, nil
		}
		frame, err := l.Retract(key, at, "recompute_flip")
		if err != nil {
			return nil, err
		}
		return &frame, nil
	}
	reason := ""
	if current != nil {
		if current.Operation == OperationUpsert && current.Hash == ContentHash(document) {
			return nil, nil
		}
		reason = "recompute_correction"
	}
	frame, err := l.Publish(document, organizationID, ruleID, entityID, windowStart, generation, objectID, at, reason)
	if err != nil {
		return nil, err
	}
	return &frame, nil
}

// SwitchGeneration retires one rule generation by retracting every live
// old-generation key of the rule. The new generation publishes under its own
// business keys, so old and new never overwrite each other and the alert view
// never holds both — no double online alerts.
func (l *FindingLedger) SwitchGeneration(organizationID, ruleID, oldGeneration, newGeneration string, at time.Time) ([]Frame, error) {
	if organizationID == "" || ruleID == "" || oldGeneration == "" || newGeneration == "" {
		return nil, fmt.Errorf("%w: generation switch requires organization, rule, and both generations", ErrEmptyFindingRef)
	}
	if oldGeneration == newGeneration {
		return nil, fmt.Errorf("%w: generation switch requires distinct generations", ErrInvalidRevision)
	}
	reason := fmt.Sprintf("generation_retired:%s->%s", oldGeneration, newGeneration)
	prefix := organizationID + "|" + ruleID + "|"
	suffix := "|" + oldGeneration
	keys := make([]string, 0, len(l.history))
	for key := range l.history {
		if strings.HasPrefix(key, prefix) && strings.HasSuffix(key, suffix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	retired := make([]Frame, 0, len(keys))
	for _, key := range keys {
		current := l.Current(key)
		if current == nil || current.Operation != OperationUpsert {
			continue
		}
		frame, err := l.Retract(key, at, reason)
		if err != nil {
			return nil, err
		}
		retired = append(retired, frame)
	}
	return retired, nil
}

// Current returns the highest-revision frame of one key, or nil.
func (l *FindingLedger) Current(key string) *Frame {
	frames := l.history[key]
	if len(frames) == 0 {
		return nil
	}
	frame := frames[len(frames)-1]
	return &frame
}

// CurrentView returns the highest-revision frame per key. With alertsOnly it
// is the online alert view: retracted keys are marked out, never deleted.
func (l *FindingLedger) CurrentView(alertsOnly bool) map[string]Frame {
	view := make(map[string]Frame, len(l.history))
	for key, frames := range l.history {
		if len(frames) == 0 {
			continue
		}
		frame := frames[len(frames)-1]
		if alertsOnly && frame.Operation != OperationUpsert {
			continue
		}
		view[key] = frame
	}
	return view
}

// History returns every frame of one key in revision order.
func (l *FindingLedger) History(key string) []Frame {
	return append([]Frame(nil), l.history[key]...)
}

type ledgerSnapshot struct {
	StateVersion int                `json:"state_version"`
	History      map[string][]Frame `json:"history"`
}

// Export serializes the ledger (versioned snapshot).
func (l *FindingLedger) Export() ([]byte, error) {
	return json.Marshal(ledgerSnapshot{StateVersion: ledgerSnapshotVersion, History: l.history})
}

// ImportFindingLedger restores a ledger snapshot fail-closed.
func ImportFindingLedger(raw []byte) (*FindingLedger, error) {
	var snapshot ledgerSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, fmt.Errorf("invalid ledger snapshot: %w", err)
	}
	if snapshot.StateVersion != ledgerSnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported ledger snapshot version", ErrInvalidRevision)
	}
	if snapshot.History == nil {
		return nil, fmt.Errorf("%w: ledger snapshot requires a history object", ErrInvalidRevision)
	}
	ledger := NewFindingLedger()
	keys := make([]string, 0, len(snapshot.History))
	for key := range snapshot.History {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, frame := range snapshot.History[key] {
			if frame.BusinessKey != key {
				return nil, fmt.Errorf("%w: ledger snapshot frame filed under the wrong key", ErrInvalidRevision)
			}
			if _, err := ledger.Apply(frame); err != nil {
				return nil, err
			}
		}
	}
	return ledger, nil
}

func (l *FindingLedger) nextRevision(key string) int64 {
	current := l.Current(key)
	if current == nil {
		return 1
	}
	return current.Revision + 1
}
