package analysis

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestParseResultEnforcesTenant(t *testing.T) {
	raw := []byte(`{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_a"}}}`)
	if _, err := Parse(raw, "tenant_a", "tenant_a"); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(raw, "tenant_b", "tenant_b"); err == nil {
		t.Fatal("expected cross-tenant result to be rejected")
	}
}

const validObjectJSON = `{"contract_version":"2.0.0","object_type":"anomaly","object_id":"anomaly:tenant_a:user-42:2026-09-26","revision":2,"operation":"upsert","generation":"g1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"failed-login-burst","rule_version":"2.1.0","run_id":"run-1","window_start":"2026-09-26T10:00:00Z","date_key":"2026-09-26","input_refs":[{"object_type":"feature","object_id":"feature:1","revision":4,"content_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"document":{"organization":{"id":"tenant_a"}}}`

func mutateObjectJSON(t *testing.T, old, new string) []byte {
	t.Helper()
	if !strings.Contains(validObjectJSON, old) {
		t.Fatalf("fixture does not contain %q", old)
	}
	return []byte(strings.Replace(validObjectJSON, old, new, 1))
}

func TestParseObjectValidEnvelope(t *testing.T) {
	object, err := ParseObject([]byte(validObjectJSON), "tenant_a", "tenant_a")
	if err != nil {
		t.Fatal(err)
	}
	if object.ObjectType != ObjectTypeAnomaly || object.Revision != 2 || object.Operation != OperationUpsert {
		t.Fatalf("unexpected object %+v", object)
	}
	if object.WindowStart != time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC) {
		t.Fatalf("window start %s", object.WindowStart)
	}
	if len(object.InputRefs) != 1 || object.InputRefs[0].Revision != 4 {
		t.Fatalf("input refs %+v", object.InputRefs)
	}
}

func TestParseObjectAcceptsEveryTypeAndRetraction(t *testing.T) {
	for _, objectType := range []string{"feature", "baseline", "anomaly", "risk_event", "entity_risk"} {
		raw := mutateObjectJSON(t, `"object_type":"anomaly"`, fmt.Sprintf(`"object_type":%q`, objectType))
		if _, err := ParseObject(raw, "tenant_a", "tenant_a"); err != nil {
			t.Fatalf("%s: %v", objectType, err)
		}
	}
	raw := mutateObjectJSON(t, `"operation":"upsert"`, `"operation":"retracted"`)
	object, err := ParseObject(raw, "tenant_a", "tenant_a")
	if err != nil || object.Operation != OperationRetracted {
		t.Fatalf("retracted: %v %+v", err, object)
	}
}

func TestParseObjectRejectsFailClosed(t *testing.T) {
	cases := map[string][]byte{
		"unknown type":       mutateObjectJSON(t, `"object_type":"anomaly"`, `"object_type":"threat"`),
		"zero revision":      mutateObjectJSON(t, `"revision":2`, `"revision":0`),
		"bad operation":      mutateObjectJSON(t, `"operation":"upsert"`, `"operation":"delete"`),
		"empty generation":   mutateObjectJSON(t, `"generation":"g1"`, `"generation":""`),
		"cross tenant":       mutateObjectJSON(t, `"organization_id":"tenant_a"`, `"organization_id":"tenant_b"`),
		"non semver rule":    mutateObjectJSON(t, `"rule_version":"2.1.0"`, `"rule_version":"2.1"`),
		"missing run id":     mutateObjectJSON(t, `"run_id":"run-1"`, `"run_id":""`),
		"bad window":         mutateObjectJSON(t, `"window_start":"2026-09-26T10:00:00Z"`, `"window_start":"yesterday"`),
		"malformed date key": mutateObjectJSON(t, `"date_key":"2026-09-26"`, `"date_key":"2026/09/26"`),
		// The fixed date key must be the UTC date of window_start; any drift
		// is refused instead of partitioning the replay into the wrong day.
		"date key drift": mutateObjectJSON(t, `"date_key":"2026-09-26"`, `"date_key":"2026-09-27"`),
		"missing refs": mutateObjectJSON(t,
			`"input_refs":[{"object_type":"feature","object_id":"feature:1","revision":4,"content_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]`,
			`"input_refs":null`),
		"bad ref hash":   mutateObjectJSON(t, `"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`, `"zzzz"`),
		"array document": mutateObjectJSON(t, `"document":{"organization":{"id":"tenant_a"}}`, `"document":[1,2]`),
		"wrong contract": mutateObjectJSON(t, `"contract_version":"2.0.0"`, `"contract_version":"1.0.0"`),
	}
	for name, raw := range cases {
		if _, err := ParseObject(raw, "tenant_a", "tenant_a"); err == nil {
			t.Fatalf("%s: expected rejection", name)
		}
	}
	if _, err := ParseObject(mutateObjectJSON(t, `"organization_id":"tenant_a"`, `"organization_id":"tenant_b"`), "tenant_b", "tenant_b"); err == nil {
		t.Fatal("namespace must match the service identity too")
	}
	if _, err := ParseObject([]byte(validObjectJSON+" {}"), "tenant_a", "tenant_a"); err == nil {
		t.Fatal("trailing data must be rejected")
	}
	if _, err := ParseObject([]byte(`{"contract_version":"2.0.0","object_type":"anomaly"}`), "tenant_a", "tenant_a"); err == nil {
		t.Fatal("missing required fields must be rejected")
	}
}

func TestMigrateLegacyUsesExplicitWindowOnly(t *testing.T) {
	legacy := Result{
		ResultID:       "anom:1",
		OrganizationID: "tenant_a",
		Namespace:      "tenant_a",
		RuleID:         "r1",
		RuleVersion:    "1.0.0",
		RunID:          "run-1",
		Document:       []byte(`{"organization":{"id":"tenant_a"},"detection":{"window":{"start":"2026-09-26T10:00:00Z"}}}`),
	}
	object, err := MigrateLegacy(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if object.ObjectType != ObjectTypeAnomaly || object.ObjectID != "anom:1" || object.Revision != 1 ||
		object.Operation != OperationUpsert || object.Generation != LegacyGeneration || len(object.InputRefs) != 0 ||
		object.DateKey != "2026-09-26" {
		t.Fatalf("unexpected migrated object %+v", object)
	}
	// generation/input_refs are never inferred from rule_version or run_id.
	if object.Generation == legacy.RuleVersion || object.Generation == legacy.RunID {
		t.Fatal("legacy migration inferred a generation from untrustworthy fields")
	}
}

func TestMigrateLegacyWithoutWindowRefusesFabrication(t *testing.T) {
	legacy := Result{ResultID: "anom:1", OrganizationID: "tenant_a", Namespace: "tenant_a", RuleID: "r1", RuleVersion: "1.0.0", RunID: "run-1"}
	for _, document := range []string{
		`{"organization":{"id":"tenant_a"}}`,
		`{"organization":{"id":"tenant_a"},"detection":{"window":{"start":"not a time"}}}`,
		`{"organization":{"id":"tenant_a"},"analysis":{"generated_at":"2026-09-27T00:00:00Z"}}`,
	} {
		legacy.Document = []byte(document)
		if _, err := MigrateLegacy(legacy); !errors.Is(err, ErrLegacyWindowMissing) {
			t.Fatalf("%s: expected ErrLegacyWindowMissing, got %v", document, err)
		}
	}
}
