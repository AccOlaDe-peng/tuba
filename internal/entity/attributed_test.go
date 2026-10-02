package entity

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var contributionTime = time.Date(2026, 10, 12, 1, 0, 0, 0, time.UTC)

func resolvedAttr(role, entityID string) RoleAttribution {
	return RoleAttribution{
		AttributionID: "att:" + strings.Repeat("a", 64),
		EventID:       "evt:abc", Role: role, State: StateResolved, EntityID: entityID,
		Confidence: 1.0, RuleVersion: RoleMappingVersionV1, ValidFrom: contributionTime.Add(-time.Hour),
		Evidence: Evidence{Identifiers: []IdentifierEvidence{{Kind: KindSID, Value: "S-1-5-21-1", Canonical: "s-1-5-21-1", Strength: StrengthStrong}}, Adjudication: []string{"resolved_by_strong_identifier"}},
	}
}

func unresolvedAttr(role, reason string) RoleAttribution {
	return RoleAttribution{
		AttributionID: "att:" + strings.Repeat("b", 64),
		EventID:       "evt:abc", Role: role, State: StateUnresolved,
		Confidence: 0, RuleVersion: RoleMappingVersionV1,
		Evidence: Evidence{Identifiers: []IdentifierEvidence{{Kind: KindIP, Value: "10.0.0.5", Canonical: "10.0.0.5", Strength: StrengthWeak}}, Adjudication: []string{"no_registered_entity_matches"}, Reason: reason},
	}
}

func TestContributionKeyResolvedSameEntitySamePartition(t *testing.T) {
	key1, err := ContributionKey("org1", resolvedAttr(RoleActor, "ent:"+strings.Repeat("1", 64)))
	if err != nil {
		t.Fatal(err)
	}
	key2, err := ContributionKey("org1", resolvedAttr(RoleTarget, "ent:"+strings.Repeat("1", 64)))
	if err != nil {
		t.Fatal(err)
	}
	if key1 != key2 {
		t.Fatalf("same entity must share one partition key: %q vs %q", key1, key2)
	}
	other, err := ContributionKey("org1", resolvedAttr(RoleActor, "ent:"+strings.Repeat("2", 64)))
	if err != nil {
		t.Fatal(err)
	}
	if other == key1 {
		t.Fatal("different entities must be able to land in different partitions")
	}
	if !strings.HasPrefix(key1, "org1:ent:") {
		t.Fatalf("resolved key format: %q", key1)
	}
	otherOrg, err := ContributionKey("org2", resolvedAttr(RoleActor, "ent:"+strings.Repeat("1", 64)))
	if err != nil {
		t.Fatal(err)
	}
	if otherOrg == key1 {
		t.Fatal("tenant must be part of the key")
	}
}

func TestContributionKeyUnresolvedNeverPollutesEntityPartitions(t *testing.T) {
	unresolved, err := ContributionKey("org1", unresolvedAttr(RoleSourceDevice, ReasonNoMatchingEntity))
	if err != nil {
		t.Fatal(err)
	}
	if want := "org1:unresolved:unresolved:no_matching_entity"; unresolved != want {
		t.Fatalf("unresolved key: %q", unresolved)
	}
	ambiguous := unresolvedAttr(RoleActor, ReasonStrongWeakConflict)
	ambiguous.State = StateAmbiguous
	ambKey, err := ContributionKey("org1", ambiguous)
	if err != nil {
		t.Fatal(err)
	}
	if want := "org1:unresolved:ambiguous:strong_weak_conflict"; ambKey != want {
		t.Fatalf("ambiguous key: %q", ambKey)
	}
	resolved, err := ContributionKey("org1", resolvedAttr(RoleActor, "ent:"+strings.Repeat("1", 64)))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{unresolved, ambKey} {
		if key == resolved || !strings.Contains(key, ":unresolved:") {
			t.Fatalf("unresolved key %q collides with entity keyspace", key)
		}
	}
	if unresolved == ambKey {
		t.Fatal("different states/reasons must have independent keys")
	}
	noReason := unresolvedAttr(RoleGroup, "")
	key, err := ContributionKey("org1", noReason)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(key, ":unknown_reason") {
		t.Fatalf("missing reason must still key deterministically: %q", key)
	}
}

func TestContributionKeyFailClosed(t *testing.T) {
	if _, err := ContributionKey("", resolvedAttr(RoleActor, "ent:"+strings.Repeat("1", 64))); err == nil {
		t.Fatal("empty organization accepted")
	}
	bad := resolvedAttr(RoleActor, "not-an-entity")
	if _, err := ContributionKey("org1", bad); err == nil {
		t.Fatal("resolved attribution without ent: id accepted")
	}
	polluted := unresolvedAttr(RoleGroup, ReasonNoMatchingEntity)
	polluted.EntityID = "ent:" + strings.Repeat("3", 64)
	if _, err := ContributionKey("org1", polluted); err == nil {
		t.Fatal("unresolved attribution carrying an entity id accepted")
	}
	unknown := unresolvedAttr(RoleGroup, ReasonNoMatchingEntity)
	unknown.State = "weird"
	if _, err := ContributionKey("org1", unknown); err == nil {
		t.Fatal("unknown state accepted")
	}
}

func TestContributionsFanOutKeepsEveryRole(t *testing.T) {
	attrs := []RoleAttribution{
		resolvedAttr(RoleActor, "ent:"+strings.Repeat("1", 64)),
		resolvedAttr(RoleTarget, "ent:"+strings.Repeat("2", 64)),
		resolvedAttr(RoleHost, "ent:"+strings.Repeat("1", 64)),
		unresolvedAttr(RoleSourceDevice, ReasonNoMatchingEntity),
		unresolvedAttr(RoleDestinationDevice, ReasonNoMatchingEntity),
	}
	contributions, err := Contributions("org1", "authentication", "evt:abc", contributionTime, attrs)
	if err != nil {
		t.Fatal(err)
	}
	if len(contributions) != len(attrs) {
		t.Fatalf("every role must fan out: got %d of %d", len(contributions), len(attrs))
	}
	roles := map[string]Contribution{}
	for _, c := range contributions {
		roles[c.Role] = c
		if c.EventID != "evt:abc" || !c.EventTime.Equal(contributionTime) {
			t.Fatalf("event identity not carried verbatim: %+v", c)
		}
		if c.SchemaVersion != AttributedSchemaVersion || c.PartitionKey == "" || c.Domain != "authentication" {
			t.Fatalf("envelope incomplete: %+v", c)
		}
	}
	if roles[RoleActor].PartitionKey != roles[RoleHost].PartitionKey {
		t.Fatal("two roles on one entity must share the partition key")
	}
	for _, role := range []string{RoleSourceDevice, RoleDestinationDevice} {
		c := roles[role]
		if c.State != StateUnresolved || c.Reason != ReasonNoMatchingEntity || c.EntityID != "" {
			t.Fatalf("unresolved contribution damaged: %+v", c)
		}
		if !strings.Contains(c.PartitionKey, ":unresolved:") {
			t.Fatalf("unresolved contribution keyed as entity: %q", c.PartitionKey)
		}
	}
	if _, err := Contributions("org1", "authentication", "", contributionTime, attrs); err == nil {
		t.Fatal("empty event id accepted")
	}
	if _, err := Contributions("org1", "authentication", "evt:abc", time.Time{}, attrs); err == nil {
		t.Fatal("zero event time accepted")
	}
	if _, err := Contributions("", "authentication", "evt:abc", contributionTime, attrs); err == nil {
		t.Fatal("empty organization accepted")
	}
}

func TestContributionJSONRoundTrip(t *testing.T) {
	contributions, err := Contributions("org1", "authentication", "evt:abc", contributionTime,
		[]RoleAttribution{resolvedAttr(RoleActor, "ent:"+strings.Repeat("1", 64)), unresolvedAttr(RoleSourceDevice, ReasonNoMatchingEntity)})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range contributions {
		body, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		var back Contribution
		if err := json.Unmarshal(body, &back); err != nil {
			t.Fatal(err)
		}
		again, err := json.Marshal(back)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != string(again) {
			t.Fatalf("round trip mismatch:\n%s\n%s", body, again)
		}
		var doc map[string]any
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatal(err)
		}
		if doc["partition_key"] != c.PartitionKey {
			t.Fatal("partition key must be visible in the message body")
		}
		if c.State == StateResolved {
			if doc["entity_id"] != c.EntityID {
				t.Fatal("resolved contribution lost entity id")
			}
		} else if _, present := doc["entity_id"]; present {
			t.Fatal("unresolved contribution must not carry entity id")
		}
	}
}
