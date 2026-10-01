package entity

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func strongCandidate(id, key string) Candidate {
	return Candidate{EntityID: id, CanonicalKey: key, Strength: StrengthStrong, Revision: 1, ValidFrom: t0}
}

func weakCandidate(id, key string, from time.Time, to *time.Time) Candidate {
	return Candidate{EntityID: id, CanonicalKey: key, Strength: StrengthWeak, Revision: 1, ValidFrom: from, ValidTo: to}
}

func evidenceOf(evs ...IdentifierEvidence) Evidence {
	return Evidence{Identifiers: evs}
}

func adjudicated(t *testing.T, role RoleObservation, ev Evidence) RoleAttribution {
	t.Helper()
	ra := adjudicate("evt:test", t0.Add(time.Hour), role, ev, RoleMappingVersionV1)
	if ra.AttributionID == "" || !strings.HasPrefix(ra.AttributionID, "att:") {
		t.Fatalf("attribution id missing or malformed: %q", ra.AttributionID)
	}
	if ra.EventID != "evt:test" {
		t.Fatalf("event id rewritten: %q", ra.EventID)
	}
	if ra.RuleVersion != RoleMappingVersionV1 {
		t.Fatalf("rule version: %q", ra.RuleVersion)
	}
	return ra
}

func accountRole(ids ...Identifier) RoleObservation {
	return RoleObservation{Role: RoleActor, EntityType: TypeAccount, Space: "corp.example", Identifiers: ids}
}

func TestAdjudicateResolvedStrong(t *testing.T) {
	role := accountRole(Identifier{KindSID, "S-1-5-21-1-2-3-100"})
	ra := adjudicated(t, role, evidenceOf(IdentifierEvidence{
		Kind: KindSID, Value: "S-1-5-21-1-2-3-100", Canonical: "s-1-5-21-1-2-3-100", Strength: StrengthStrong,
		Matched: []Candidate{strongCandidate("ent:aaa", "sid:s-1-5-21-1-2-3-100")},
	}))
	if ra.State != StateResolved || ra.EntityID != "ent:aaa" {
		t.Fatalf("state=%s entity=%q", ra.State, ra.EntityID)
	}
	if ra.Confidence != 1.0 {
		t.Fatalf("confidence=%v", ra.Confidence)
	}
	if !ra.ValidFrom.Equal(t0) || ra.ValidTo != nil {
		t.Fatalf("valid interval not carried: %v %v", ra.ValidFrom, ra.ValidTo)
	}
	assertAdjudication(t, ra, "resolved_by_strong_identifier")
	// Evidence must contain the normalization input and output.
	ie := ra.Evidence.Identifiers[0]
	if ie.Value != "S-1-5-21-1-2-3-100" || ie.Canonical != "s-1-5-21-1-2-3-100" || ie.Strength != StrengthStrong {
		t.Fatalf("evidence incomplete: %+v", ie)
	}
}

func TestAdjudicateResolvedWeakOccurrence(t *testing.T) {
	role := accountRole(Identifier{KindUsername, "jsmith"})
	ra := adjudicated(t, role, evidenceOf(IdentifierEvidence{
		Kind: KindUsername, Value: "jsmith", Canonical: "jsmith", Strength: StrengthWeak,
		Matched: []Candidate{weakCandidate("ent:w1", "username:jsmith", t0, nil)},
	}))
	if ra.State != StateResolved || ra.EntityID != "ent:w1" {
		t.Fatalf("state=%s entity=%q", ra.State, ra.EntityID)
	}
	if ra.Confidence != 0.8 {
		t.Fatalf("confidence=%v", ra.Confidence)
	}
	assertAdjudication(t, ra, "resolved_by_weak_identifier_occurrence_at_event_time")
}

func TestAdjudicateResolvedStrongPriorityOverWeakSameEntity(t *testing.T) {
	role := accountRole(Identifier{KindSID, "S-1-5-21-1-2-3-100"}, Identifier{KindUsername, "jsmith"})
	ra := adjudicated(t, role, evidenceOf(
		IdentifierEvidence{Kind: KindSID, Value: "S-1-5-21-1-2-3-100", Canonical: "s-1-5-21-1-2-3-100", Strength: StrengthStrong,
			Matched: []Candidate{strongCandidate("ent:aaa", "sid:s-1-5-21-1-2-3-100")}},
		IdentifierEvidence{Kind: KindUsername, Value: "jsmith", Canonical: "jsmith", Strength: StrengthWeak},
	))
	if ra.State != StateResolved || ra.EntityID != "ent:aaa" {
		t.Fatalf("state=%s entity=%q", ra.State, ra.EntityID)
	}
	assertAdjudication(t, ra, "strong_identifier_priority_over_weak")
}

func TestAdjudicateUnresolvedNoMatch(t *testing.T) {
	role := accountRole(Identifier{KindUsername, "ghost"})
	ra := adjudicated(t, role, evidenceOf(IdentifierEvidence{
		Kind: KindUsername, Value: "ghost", Canonical: "ghost", Strength: StrengthWeak,
	}))
	if ra.State != StateUnresolved || ra.EntityID != "" {
		t.Fatalf("state=%s entity=%q", ra.State, ra.EntityID)
	}
	if ra.Evidence.Reason != ReasonNoMatchingEntity {
		t.Fatalf("reason=%q", ra.Evidence.Reason)
	}
	if ra.Confidence != 0 {
		t.Fatalf("confidence=%v", ra.Confidence)
	}
}

func TestAdjudicateUnresolvedOccurrenceOutsideEventTime(t *testing.T) {
	// The weak identity has occurrences, but none covers the event time.
	closedTo := t0.Add(30 * time.Minute)
	role := accountRole(Identifier{KindUsername, "jsmith"})
	ra := adjudicated(t, role, evidenceOf(IdentifierEvidence{
		Kind: KindUsername, Value: "jsmith", Canonical: "jsmith", Strength: StrengthWeak,
		Matched: []Candidate{
			weakCandidate("ent:w1", "username:jsmith", t0.Add(-2*time.Hour), &closedTo),
		},
	}))
	if ra.State != StateUnresolved || ra.Evidence.Reason != ReasonNoActiveOccurrence {
		t.Fatalf("state=%s reason=%q", ra.State, ra.Evidence.Reason)
	}
}

func TestAdjudicateUnresolvedAllIdentifiersInvalid(t *testing.T) {
	role := accountRole(Identifier{KindSID, "not-a-sid"})
	ra := adjudicated(t, role, evidenceOf(IdentifierEvidence{
		Kind: KindSID, Value: "not-a-sid", LookupError: "identifier failed normalization: sid \"not-a-sid\"",
	}))
	if ra.State != StateUnresolved || ra.Evidence.Reason != ReasonIdentifierInvalid {
		t.Fatalf("state=%s reason=%q", ra.State, ra.Evidence.Reason)
	}
}

func TestAdjudicateUnresolvedNoIdentifiers(t *testing.T) {
	role := accountRole()
	ra := adjudicated(t, role, evidenceOf())
	if ra.State != StateUnresolved || ra.Evidence.Reason != ReasonNoIdentifiers {
		t.Fatalf("state=%s reason=%q", ra.State, ra.Evidence.Reason)
	}
}

func TestAdjudicateAmbiguousStrongWeakConflict(t *testing.T) {
	// The SID resolves to one entity, the username to another: the conflict is
	// preserved with both candidates, never resolved by "latest wins".
	role := accountRole(Identifier{KindSID, "S-1-5-21-1-2-3-100"}, Identifier{KindUsername, "jsmith"})
	ra := adjudicated(t, role, evidenceOf(
		IdentifierEvidence{Kind: KindSID, Value: "S-1-5-21-1-2-3-100", Canonical: "s-1-5-21-1-2-3-100", Strength: StrengthStrong,
			Matched: []Candidate{strongCandidate("ent:aaa", "sid:s-1-5-21-1-2-3-100")}},
		IdentifierEvidence{Kind: KindUsername, Value: "jsmith", Canonical: "jsmith", Strength: StrengthWeak,
			Matched: []Candidate{weakCandidate("ent:w1", "username:jsmith", t0, nil)}},
	))
	if ra.State != StateAmbiguous || ra.EntityID != "" {
		t.Fatalf("state=%s entity=%q", ra.State, ra.EntityID)
	}
	if ra.Evidence.Reason != ReasonStrongWeakConflict {
		t.Fatalf("reason=%q", ra.Evidence.Reason)
	}
	cands := candidatesOf(ra.Evidence)
	if len(cands) != 2 {
		t.Fatalf("candidates not preserved: %+v", cands)
	}
	assertAdjudication(t, ra, "candidates_preserved_not_overwritten")
}

func TestAdjudicateAmbiguousMultipleActiveCandidates(t *testing.T) {
	role := accountRole(Identifier{KindUsername, "shared"})
	ra := adjudicated(t, role, evidenceOf(IdentifierEvidence{
		Kind: KindUsername, Value: "shared", Canonical: "shared", Strength: StrengthWeak,
		Matched: []Candidate{
			weakCandidate("ent:w1", "username:shared", t0.Add(-time.Hour), nil),
			weakCandidate("ent:w2", "username:shared#2", t0.Add(-time.Minute), nil),
		},
	}))
	if ra.State != StateAmbiguous || ra.Evidence.Reason != ReasonMultipleActiveOccurrences {
		t.Fatalf("state=%s reason=%q", ra.State, ra.Evidence.Reason)
	}
	if len(candidatesOf(ra.Evidence)) != 2 {
		t.Fatal("candidate list incomplete")
	}
}

func TestAttributionIDDeterministicAndScoped(t *testing.T) {
	snap := "actor|resolved|ent:aaa|sid:s-1-5-21-1-2-3-100|"
	first := AttributionID("evt:1", snap, RoleMappingVersionV1)
	if first != AttributionID("evt:1", snap, RoleMappingVersionV1) {
		t.Fatal("attribution id not deterministic")
	}
	if !strings.HasPrefix(first, "att:") || len(first) != 4+64 {
		t.Fatalf("attribution id format: %q", first)
	}
	if AttributionID("evt:2", snap, RoleMappingVersionV1) == first {
		t.Fatal("attribution id not scoped by event id")
	}
	if AttributionID("evt:1", "target"+snap[5:], RoleMappingVersionV1) == first {
		t.Fatal("attribution id not scoped by role snapshot")
	}
	if AttributionID("evt:1", snap, "2.0.0") == first {
		t.Fatal("attribution id not scoped by role mapping version")
	}
}

func assertAdjudication(t *testing.T, ra RoleAttribution, entry string) {
	t.Helper()
	for _, e := range ra.Evidence.Adjudication {
		if e == entry {
			return
		}
	}
	t.Fatalf("adjudication %q missing in %v", entry, ra.Evidence.Adjudication)
}

func sampleUIMEvent() map[string]any {
	return map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:abc", "dataset": "authentication"},
		"user": map[string]any{
			"id":   "S-1-5-21-100-200-300-1001",
			"name": "CORP\\JSmith",
			"target": map[string]any{
				"id":   "S-1-5-21-100-200-300-2002",
				"name": "svc-backup@corp.example",
			},
		},
		"host":        map[string]any{"name": "WEB01"},
		"source":      map[string]any{"ip": "10.0.0.5"},
		"destination": map[string]any{"ip": "10.0.0.9"},
	}
}

func TestRolesFromUIMExtractsEveryRole(t *testing.T) {
	event := sampleUIMEvent()
	eventID, at, roles, err := RolesFromUIM(event, "corp.example", "host-local")
	if err != nil {
		t.Fatal(err)
	}
	if eventID != "evt:abc" {
		t.Fatalf("event id: %q", eventID)
	}
	if !at.Equal(time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)) {
		t.Fatalf("event time: %v", at)
	}
	byRole := map[string]RoleObservation{}
	for _, r := range roles {
		byRole[r.Role] = r
	}
	actor := byRole[RoleActor]
	if actor.EntityType != TypeAccount || actor.Space != "corp.example" {
		t.Fatalf("actor mapping: %+v", actor)
	}
	if actor.Identifiers[0].Kind != KindSID || actor.Identifiers[1].Kind != KindNTName {
		t.Fatalf("actor identifiers: %+v", actor.Identifiers)
	}
	target := byRole[RoleTarget]
	if target.Identifiers[1].Kind != KindUPN {
		t.Fatalf("target identifiers: %+v", target.Identifiers)
	}
	if byRole[RoleHost].EntityType != TypeDevice || byRole[RoleHost].Space != "host-local" {
		t.Fatalf("host mapping: %+v", byRole[RoleHost])
	}
	if byRole[RoleSourceDevice].Identifiers[0].Kind != KindIP || byRole[RoleDestinationDevice].Identifiers[0].Value != "10.0.0.9" {
		t.Fatalf("device roles: %+v", byRole)
	}
}

func TestRolesFromUIMFailClosed(t *testing.T) {
	if _, _, _, err := RolesFromUIM(map[string]any{}, "a", "d"); err == nil {
		t.Fatal("missing event.id accepted")
	}
	bad := sampleUIMEvent()
	bad["@timestamp"] = "not-a-time"
	if _, _, _, err := RolesFromUIM(bad, "a", "d"); err == nil {
		t.Fatal("invalid @timestamp accepted")
	}
}

func TestEventNeverMutatedByAttribution(t *testing.T) {
	event := sampleUIMEvent()
	before, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	eventID, at, roles, err := RolesFromUIM(event, "corp.example", "host-local")
	if err != nil {
		t.Fatal(err)
	}
	// Run the pure adjudication core over the extracted roles.
	for _, role := range roles {
		ra := adjudicate(eventID, at, role, Evidence{Identifiers: []IdentifierEvidence{}}, RoleMappingVersionV1)
		if ra.EventID != eventID {
			t.Fatalf("event id rewritten: %q != %q", ra.EventID, eventID)
		}
	}
	after, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("event mutated by attribution:\n%s\n%s", before, after)
	}
}
