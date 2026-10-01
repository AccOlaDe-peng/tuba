package entity

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRelationIDDeterministicAndScoped(t *testing.T) {
	org := "3f6b0f90-1c2b-4d3e-8f4a-5b6c7d8e9f0a"
	snap := relationSnapshot(RelAssignedTo, RelationMappingVersionV1, "evt:1")
	id := RelationID(org, "ent:a", RelAssignedTo, "ent:b", "evt:1", snap)
	if !strings.HasPrefix(id, "rel:") || len(id) != len("rel:")+64 {
		t.Fatalf("relation id format: %q", id)
	}
	if got := RelationID(org, "ent:a", RelAssignedTo, "ent:b", "evt:1", snap); got != id {
		t.Fatalf("relation id not deterministic: %q vs %q", got, id)
	}
	cases := []struct{ name string; mutate func() string }{
		{"tenant", func() string { return RelationID("aaaaaaaa-1111-1111-1111-111111111111", "ent:a", RelAssignedTo, "ent:b", "evt:1", snap) }},
		{"from", func() string { return RelationID(org, "ent:x", RelAssignedTo, "ent:b", "evt:1", snap) }},
		{"type", func() string { return RelationID(org, "ent:a", RelManagedBy, "ent:b", "evt:1", snap) }},
		{"to", func() string { return RelationID(org, "ent:a", RelAssignedTo, "ent:x", "evt:1", snap) }},
		{"event", func() string { return RelationID(org, "ent:a", RelAssignedTo, "ent:b", "evt:2", snap) }},
		{"snapshot", func() string { return RelationID(org, "ent:a", RelAssignedTo, "ent:b", "evt:1", relationSnapshot(RelAssignedTo, "2.0.0", "evt:1")) }},
	}
	for _, tc := range cases {
		if got := tc.mutate(); got == id {
			t.Fatalf("relation id did not change with %s", tc.name)
		}
	}
}

func TestIntervalsOverlapHalfOpen(t *testing.T) {
	t0 := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	t1, t2, t3 := t0.Add(time.Hour), t0.Add(2*time.Hour), t0.Add(3*time.Hour)
	close := func(t time.Time) *time.Time { return &t }
	cases := []struct {
		name           string
		aFrom, aTo     time.Time
		aClosed, bClosed bool
		bFrom, bTo     time.Time
		want           bool
	}{
		{"contained", t1, t2, true, true, t0, t3, true},
		{"touching boundary no overlap", t0, t1, true, true, t1, t2, false},
		{"touching reverse no overlap", t1, t2, true, true, t0, t1, false},
		{"open interval overlaps", t1, time.Time{}, false, true, t2, t3, true},
		{"closed before open", t0, t1, true, false, t1, time.Time{}, false},
		{"open vs open overlap", t1, time.Time{}, false, false, t2, time.Time{}, true},
		{"disjoint", t0, t1, true, true, t2, t3, false},
	}
	for _, tc := range cases {
		var aTo, bTo *time.Time
		if tc.aClosed {
			aTo = close(tc.aTo)
		}
		if tc.bClosed {
			bTo = close(tc.bTo)
		}
		if got := intervalsOverlap(tc.aFrom, aTo, tc.bFrom, bTo); got != tc.want {
			t.Errorf("%s: intervalsOverlap = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestRelationTypeCardinality(t *testing.T) {
	if relationTypes[RelAssignedTo] != CardinalitySingle || relationTypes[RelManagedBy] != CardinalitySingle {
		t.Fatal("ownership relations must be single-cardinality")
	}
	if relationTypes[RelMemberOf] != CardinalityMulti || relationTypes[RelBelongsTo] != CardinalityMulti {
		t.Fatal("membership relations must be multi-cardinality")
	}
}

func TestValidateRuleRef(t *testing.T) {
	if err := ValidateRuleRef(RuleKindRelationMapping, "1.0.0"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRuleRef("Bad-Kind", "1.0.0"); !errors.Is(err, ErrInvalidRuleKind) {
		t.Fatalf("kind: %v", err)
	}
	if err := ValidateRuleRef(RuleKindRelationMapping, "1.0"); !errors.Is(err, ErrInvalidRuleVersion) {
		t.Fatalf("version: %v", err)
	}
	if err := ValidateRuleRef(RuleKindRelationMapping, "v1.0.0"); !errors.Is(err, ErrInvalidRuleVersion) {
		t.Fatalf("version prefix: %v", err)
	}
}

func TestCanonicalRuleContent(t *testing.T) {
	canonical, hash, err := canonicalRuleContent(json.RawMessage(`{ "b": 1, "a": [2] }`))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"b":1,"a":[2]}` {
		t.Fatalf("not compacted: %s", canonical)
	}
	_, hash2, err := canonicalRuleContent(json.RawMessage(`{"b":1,"a":[2]}`))
	if err != nil {
		t.Fatal(err)
	}
	if hash != hash2 {
		t.Fatal("formatting changed the content hash")
	}
	if len(hash) != 64 {
		t.Fatalf("hash length: %d", len(hash))
	}
	if _, _, err := canonicalRuleContent(json.RawMessage(`not json`)); !errors.Is(err, ErrEmptyRuleContent) {
		t.Fatalf("invalid json: %v", err)
	}
	if _, _, err := canonicalRuleContent(json.RawMessage(`[1,2]`)); !errors.Is(err, ErrEmptyRuleContent) {
		t.Fatalf("non-object: %v", err)
	}
}

func TestAssembleSingleEntityDegradation(t *testing.T) {
	org := "3f6b0f90-1c2b-4d3e-8f4a-5b6c7d8e9f0a"
	from := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	counts := map[string]int{"actor": 5, "target": 2}

	// Relation lookup failure: features still produced, marked degraded.
	out := assembleSingleEntity(org, "ent:1", from, to, counts, nil, errors.New("relation store down"))
	if !out.RelationsDegraded || out.RelationError == "" {
		t.Fatalf("expected degraded result: %+v", out)
	}
	if out.ResolvedTotal != 7 || out.ResolvedByRole["actor"] != 5 {
		t.Fatalf("relation failure altered single-entity counts: %+v", out)
	}
	if out.Relations != nil {
		t.Fatalf("degraded result must not carry partial relations: %+v", out.Relations)
	}

	// No relations: normal result, not degraded.
	out = assembleSingleEntity(org, "ent:1", from, to, counts, []Relation{}, nil)
	if out.RelationsDegraded {
		t.Fatalf("empty relations must not be degraded: %+v", out)
	}
	if out.ResolvedTotal != 7 {
		t.Fatalf("counts: %+v", out)
	}

	// Relations present: enrichment attached.
	rels := []Relation{{RelationID: "rel:1", RelationType: RelAssignedTo}}
	out = assembleSingleEntity(org, "ent:1", from, to, counts, rels, nil)
	if out.RelationsDegraded || len(out.Relations) != 1 {
		t.Fatalf("enrichment: %+v", out)
	}
}
