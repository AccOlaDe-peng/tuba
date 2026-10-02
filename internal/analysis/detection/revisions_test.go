package detection

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Golden vector, identical to the authoritative Python suite
// python/tests/test_revisions.py (both files cite each other).
const (
	goldenOrg    = "tenant_a"
	goldenRule   = "auth.failure-burst"
	goldenEntity = "ent:u1"
	goldenKey    = "tenant_a|auth.failure-burst|ent:u1|2026-10-12T00:00:00Z|g1"
)

var (
	goldenWindow = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	goldenT0     = time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)
	docV1        = json.RawMessage(`{"anomaly":{"score":0.9},"marker":"v1"}`)
	docV2        = json.RawMessage(`{"anomaly":{"score":0.95},"marker":"v2"}`)
	docV1Alt     = json.RawMessage(`{"anomaly":{"score":0.91},"marker":"v1-alt"}`)
)

func publishGolden(t *testing.T, ledger *FindingLedger, doc json.RawMessage, at time.Time) Frame {
	t.Helper()
	frame, err := ledger.Publish(doc, goldenOrg, goldenRule, goldenEntity, goldenWindow, "g1", "anom:x", at, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return frame
}

func TestBusinessKeyCompositionLocked(t *testing.T) {
	key, err := BusinessKey(goldenOrg, goldenRule, goldenEntity, goldenWindow, "g1")
	if err != nil {
		t.Fatalf("business key: %v", err)
	}
	if key != goldenKey {
		t.Fatalf("business key %q, want %q", key, goldenKey)
	}
	for _, parts := range [][4]string{
		{"", goldenRule, goldenEntity, "g1"},
		{goldenOrg, "", goldenEntity, "g1"},
		{goldenOrg, goldenRule, "", "g1"},
		{goldenOrg, goldenRule, goldenEntity, ""},
		{goldenOrg, "rule|x", goldenEntity, "g1"},
	} {
		if _, err := BusinessKey(parts[0], parts[1], parts[2], goldenWindow, parts[3]); err == nil {
			t.Fatalf("business key parts %v must be rejected", parts)
		}
	}
	if _, err := BusinessKey(goldenOrg, goldenRule, goldenEntity, time.Time{}, "g1"); err == nil {
		t.Fatal("zero window start must be rejected")
	}
	g2, err := BusinessKey(goldenOrg, goldenRule, goldenEntity, goldenWindow, "g2")
	if err != nil || g2 == goldenKey {
		t.Fatalf("generation must isolate business keys: %q %v", g2, err)
	}
}

func TestCorrectionIncrementsRevisionAndKeepsHistory(t *testing.T) {
	ledger := NewFindingLedger()
	first := publishGolden(t, ledger, docV1, goldenT0)
	if first.Revision != 1 {
		t.Fatalf("first revision %d, want 1", first.Revision)
	}
	second := publishGolden(t, ledger, docV2, goldenT0.Add(5*time.Minute))
	if second.Revision != 2 {
		t.Fatalf("second revision %d, want 2", second.Revision)
	}
	history := ledger.History(goldenKey)
	if len(history) != 2 || history[0].Revision != 1 || history[1].Revision != 2 {
		t.Fatalf("history %+v, want revisions 1,2", history)
	}
	if string(history[0].Document) != string(docV1) {
		t.Fatal("history must retain the superseded document")
	}
	if current := ledger.Current(goldenKey); current == nil || string(current.Document) != string(docV2) {
		t.Fatal("current view must show only the highest revision")
	}
}

func TestOutOfOrderNeverOverwrites(t *testing.T) {
	ledger := NewFindingLedger()
	publishGolden(t, ledger, docV1, goldenT0)
	publishGolden(t, ledger, docV2, goldenT0.Add(5*time.Minute))
	stale := Frame{
		BusinessKey: goldenKey,
		ObjectID:    "anom:x",
		Revision:    1,
		Operation:   OperationUpsert,
		Generation:  "g1",
		Hash:        ContentHash(docV1),
		At:          goldenT0,
		Document:    docV1,
	}
	_, err := ledger.Apply(stale)
	var staleErr StaleRevisionError
	if !errors.As(err, &staleErr) || !IsStaleRevision(err) {
		t.Fatalf("stale apply error %v, want StaleRevisionError", err)
	}
	if staleErr.Existing != 2 || staleErr.Incoming != 1 {
		t.Fatalf("stale revisions (%d, %d), want (2, 1)", staleErr.Existing, staleErr.Incoming)
	}
	if current := ledger.Current(goldenKey); string(current.Document) != string(docV2) {
		t.Fatal("stored newer value must never be overwritten")
	}
	if len(ledger.History(goldenKey)) != 2 {
		t.Fatal("stale frame must not be recorded")
	}
}

func TestSameRevisionIdempotentOrConflict(t *testing.T) {
	ledger := NewFindingLedger()
	first := publishGolden(t, ledger, docV1, goldenT0)
	applied, err := ledger.Apply(first)
	if err != nil || applied {
		t.Fatalf("replay of the same frame must be an idempotent no-op: %v %v", applied, err)
	}
	conflicting := first
	conflicting.Hash = ContentHash(docV1Alt)
	conflicting.Document = docV1Alt
	if _, err := ledger.Apply(conflicting); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("same revision different content must fail closed: %v", err)
	}
	if current := ledger.Current(goldenKey); string(current.Document) != string(docV1) {
		t.Fatal("conflicting frame must not overwrite")
	}
}

func TestRetractedMarksCurrentViewAndKeepsHistory(t *testing.T) {
	ledger := NewFindingLedger()
	publishGolden(t, ledger, docV1, goldenT0)
	tombstone, err := ledger.Retract(goldenKey, goldenT0.Add(10*time.Minute), "input_withdrawn")
	if err != nil {
		t.Fatalf("retract: %v", err)
	}
	if tombstone.Revision != 2 || tombstone.Operation != OperationRetracted {
		t.Fatalf("tombstone %+v, want revision 2 retracted", tombstone)
	}
	if len(ledger.History(goldenKey)) != 2 {
		t.Fatal("history must be retained after retraction")
	}
	if ledger.Current(goldenKey).Operation != OperationRetracted {
		t.Fatal("current view must mark the key retracted, not delete it")
	}
	if _, ok := ledger.CurrentView(true)[goldenKey]; ok {
		t.Fatal("retracted key must leave the alert view")
	}
	if _, ok := ledger.CurrentView(false)[goldenKey]; !ok {
		t.Fatal("retracted key must stay in the full current view")
	}
	if _, err := ledger.Retract(goldenKey, goldenT0, "again"); err == nil {
		t.Fatal("re-retract must fail closed")
	}
	if _, err := ledger.Retract("tenant_a|r|e|2026-01-01T00:00:00Z|g1", goldenT0, "unknown key"); err == nil {
		t.Fatal("retracting an unknown key must fail closed")
	}
	ledger2 := NewFindingLedger()
	publishGolden(t, ledger2, docV1, goldenT0)
	if _, err := ledger2.Retract(goldenKey, goldenT0, ""); err == nil {
		t.Fatal("retraction without a reason must fail closed")
	}
}

func TestRecomputeFlipPublishesRetracted(t *testing.T) {
	ledger := NewFindingLedger()
	publishGolden(t, ledger, docV1, goldenT0)
	unchanged, err := ledger.Recompute(docV1, goldenOrg, goldenRule, goldenEntity, goldenWindow, "g1", "anom:x", goldenT0.Add(time.Minute))
	if err != nil || unchanged != nil {
		t.Fatalf("unchanged verdict must be a no-op: %+v %v", unchanged, err)
	}
	if ledger.Current(goldenKey).Revision != 1 {
		t.Fatal("no-op must not bump the revision")
	}
	corrected, err := ledger.Recompute(docV2, goldenOrg, goldenRule, goldenEntity, goldenWindow, "g1", "anom:x", goldenT0.Add(2*time.Minute))
	if err != nil || corrected == nil || corrected.Revision != 2 || corrected.Reason != "recompute_correction" {
		t.Fatalf("corrected verdict %+v, want revision 2 recompute_correction (%v)", corrected, err)
	}
	flipped, err := ledger.Recompute(nil, goldenOrg, goldenRule, goldenEntity, goldenWindow, "g1", "anom:x", goldenT0.Add(3*time.Minute))
	if err != nil || flipped == nil || flipped.Revision != 3 || flipped.Operation != OperationRetracted || flipped.Reason != "recompute_flip" {
		t.Fatalf("flipped verdict %+v, want revision 3 retracted recompute_flip (%v)", flipped, err)
	}
	again, err := ledger.Recompute(nil, goldenOrg, goldenRule, goldenEntity, goldenWindow, "g1", "anom:x", goldenT0.Add(4*time.Minute))
	if err != nil || again != nil {
		t.Fatalf("flipping an already-retracted key must be a no-op: %+v %v", again, err)
	}
}

func TestGenerationSwitchNoDoubleAlerts(t *testing.T) {
	ledger := NewFindingLedger()
	if _, err := ledger.Publish(docV1, goldenOrg, goldenRule, "ent:u1", goldenWindow, "g1", "anom:old1", goldenT0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Publish(docV1, goldenOrg, goldenRule, "ent:u2", goldenWindow, "g1", "anom:old2", goldenT0, ""); err != nil {
		t.Fatal(err)
	}
	retired, err := ledger.SwitchGeneration(goldenOrg, goldenRule, "g1", "g2", goldenT0.Add(time.Hour))
	if err != nil {
		t.Fatalf("switch generation: %v", err)
	}
	if len(retired) != 2 {
		t.Fatalf("retired %d frames, want 2", len(retired))
	}
	for _, frame := range retired {
		if frame.Operation != OperationRetracted || frame.Reason != "generation_retired:g1->g2" {
			t.Fatalf("retirement frame %+v", frame)
		}
	}
	newKey, err := BusinessKey(goldenOrg, goldenRule, "ent:u1", goldenWindow, "g2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Publish(docV2, goldenOrg, goldenRule, "ent:u1", goldenWindow, "g2", "anom:new1", goldenT0.Add(time.Hour), ""); err != nil {
		t.Fatal(err)
	}
	// Red line: the online alert view never holds the same finding from two
	// generations; history keeps both generations fully.
	alerts := ledger.CurrentView(true)
	if len(alerts) != 1 {
		t.Fatalf("alert view holds %d keys, want exactly the new generation key", len(alerts))
	}
	if _, ok := alerts[newKey]; !ok {
		t.Fatal("alert view must hold only the new-generation key")
	}
	if len(ledger.History(goldenKey)) != 2 {
		t.Fatal("old-generation history must be retained")
	}
	if ledger.Current(goldenKey).Operation != OperationRetracted || ledger.Current(goldenKey).Generation != "g1" {
		t.Fatal("old generation must be retired under its own key, not overwritten")
	}
	if ledger.Current(newKey).Generation != "g2" {
		t.Fatal("new generation publishes under its own key")
	}
}

func TestGenerationSwitchValidation(t *testing.T) {
	ledger := NewFindingLedger()
	if _, err := ledger.SwitchGeneration(goldenOrg, goldenRule, "g1", "g1", goldenT0); err == nil {
		t.Fatal("same-generation switch must fail closed")
	}
	if _, err := ledger.SwitchGeneration(goldenOrg, goldenRule, "", "g2", goldenT0); err == nil {
		t.Fatal("empty generation must fail closed")
	}
}

func TestLedgerExportImportRoundTrip(t *testing.T) {
	ledger := NewFindingLedger()
	publishGolden(t, ledger, docV1, goldenT0)
	publishGolden(t, ledger, docV2, goldenT0.Add(5*time.Minute))
	if _, err := ledger.Retract(goldenKey, goldenT0.Add(10*time.Minute), "input_withdrawn"); err != nil {
		t.Fatal(err)
	}
	raw, err := ledger.Export()
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	restored, err := ImportFindingLedger(raw)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	before, after := ledger.History(goldenKey), restored.History(goldenKey)
	if len(before) != len(after) {
		t.Fatalf("history length %d, want %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Revision != after[i].Revision || before[i].Operation != after[i].Operation || before[i].Hash != after[i].Hash {
			t.Fatalf("frame %d mismatch: %+v vs %+v", i, before[i], after[i])
		}
	}
	if applied, err := restored.Apply(*restored.Current(goldenKey)); err != nil || applied {
		t.Fatal("replaying the restored top frame must be an idempotent no-op")
	}
	if _, err := ImportFindingLedger([]byte(`{"state_version":999,"history":{}}`)); err == nil {
		t.Fatal("unknown snapshot version must fail closed")
	}
	if _, err := ImportFindingLedger([]byte("not json")); err == nil {
		t.Fatal("corrupt snapshot must fail closed")
	}
}

func TestFindingValidateRequiresGeneration(t *testing.T) {
	finding := Finding{
		ID:          "anom:x",
		EntityID:    goldenEntity,
		RuleID:      goldenRule,
		RuleVersion: RuleVersionV1,
		Score:       1,
		Explanation: "x",
		ReasonCodes: []string{"AUTH_FAILURE_BURST"},
		Threshold:   "failure_count >= 10",
		Features:    map[string]float64{"auth.failure.count": 10},
		Evidence:    []string{"e1"},
	}
	if err := finding.Validate(); !errors.Is(err, ErrMissingGeneration) {
		t.Fatalf("finding without generation must fail closed: %v", err)
	}
	finding.Generation = DefaultGeneration
	if err := finding.Validate(); err != nil {
		t.Fatalf("valid finding rejected: %v", err)
	}
}

func TestDetectorsDefaultGenerationG1(t *testing.T) {
	record := goldenRecord(t)
	record.Values["auth.failure.count"] = 10
	findings, err := FailureBurstDetector{Threshold: 10}.Detect(record, nil)
	if err != nil || len(findings) != 1 {
		t.Fatalf("burst detect: %v %v", findings, err)
	}
	if findings[0].Generation != DefaultGeneration {
		t.Fatalf("generation %q, want g1", findings[0].Generation)
	}
	other, err := FailureBurstDetector{Threshold: 10, Generation: "g2"}.Detect(record, nil)
	if err != nil || len(other) != 1 {
		t.Fatalf("burst detect g2: %v %v", other, err)
	}
	if other[0].ID == findings[0].ID {
		t.Fatal("generation must be part of the finding id derivation")
	}
}
