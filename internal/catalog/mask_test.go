package catalog

import (
	"reflect"
	"testing"
)

func authDoc() map[string]any {
	return map[string]any{
		"@timestamp":   "2026-10-12T00:00:00Z",
		"organization": map[string]any{"id": "tenant_a"},
		"event":        map[string]any{"id": "evt-1", "outcome": "failure", "original": "raw-secret-payload"},
		"user":         map[string]any{"id": "u-1", "name": "alice"},
		"source":       map[string]any{"ip": "10.0.0.1", "port": float64(4625)},
	}
}

func TestMaskEventRedactionMatrix(t *testing.T) {
	dataset, ok := Find("authentication")
	if !ok {
		t.Fatal("authentication dataset missing")
	}
	get := func(doc map[string]any, path string) (any, bool) { return LookupPath(doc, path) }

	// No sensitive:read, no raw:read: sensitive values never appear, original removed.
	masked := MaskEvent(authDoc(), dataset, MaskingPolicy{})
	if v, _ := get(masked, "user.name"); v != RedactedValue {
		t.Fatalf("user.name without sensitive:read = %v", v)
	}
	if v, _ := get(masked, "user.id"); v != RedactedValue {
		t.Fatalf("user.id without sensitive:read = %v", v)
	}
	if _, ok := get(masked, "event.original"); ok {
		t.Fatal("event.original present without raw:read")
	}
	if v, _ := get(masked, "source.ip"); v != "10.0.0.1" {
		t.Fatalf("internal field must pass through: %v", v)
	}
	if v, _ := get(masked, "event.outcome"); v != "failure" {
		t.Fatalf("public field must pass through: %v", v)
	}

	// sensitive:read only: sensitive values visible, original still removed.
	masked = MaskEvent(authDoc(), dataset, MaskingPolicy{IncludeSensitive: true})
	if v, _ := get(masked, "user.name"); v != "alice" {
		t.Fatalf("user.name with sensitive:read = %v", v)
	}
	if _, ok := get(masked, "event.original"); ok {
		t.Fatal("event.original present without raw:read")
	}

	// sensitive:read + raw:read: full content.
	masked = MaskEvent(authDoc(), dataset, MaskingPolicy{IncludeSensitive: true, IncludeRaw: true})
	if v, _ := get(masked, "event.original"); v != "raw-secret-payload" {
		t.Fatalf("event.original with raw:read = %v", v)
	}
	if v, _ := get(masked, "user.name"); v != "alice" {
		t.Fatalf("user.name = %v", v)
	}

	// raw:read without sensitive:read: original present, sensitive still redacted.
	masked = MaskEvent(authDoc(), dataset, MaskingPolicy{IncludeRaw: true})
	if v, _ := get(masked, "event.original"); v != "raw-secret-payload" {
		t.Fatalf("event.original = %v", v)
	}
	if v, _ := get(masked, "user.name"); v != RedactedValue {
		t.Fatalf("user.name without sensitive:read = %v", v)
	}
}

func TestMaskEventDoesNotMutateInput(t *testing.T) {
	dataset, _ := Find("authentication")
	doc := authDoc()
	_ = MaskEvent(doc, dataset, MaskingPolicy{})
	if doc["user"].(map[string]any)["name"] != "alice" {
		t.Fatal("input mutated")
	}
	if doc["event"].(map[string]any)["original"] != "raw-secret-payload" {
		t.Fatal("input mutated (event.original)")
	}
}

func TestMaskEventNestedSensitivePaths(t *testing.T) {
	dataset, _ := Find("iam")
	doc := map[string]any{
		"user":  map[string]any{"id": "u-1", "target": map[string]any{"id": "u-2", "name": "bob"}},
		"group": map[string]any{"id": "g-1", "name": "admins"},
	}
	masked := MaskEvent(doc, dataset, MaskingPolicy{})
	for _, path := range []string{"user.id", "user.target.id", "user.target.name", "group.id", "group.name"} {
		if v, _ := LookupPath(masked, path); v != RedactedValue {
			t.Fatalf("%s = %v, want redacted", path, v)
		}
	}
}

func TestExportColumns(t *testing.T) {
	raw, _ := Find("raw")
	withRaw := ExportColumns(raw, MaskingPolicy{IncludeRaw: true})
	withoutRaw := ExportColumns(raw, MaskingPolicy{})
	if reflect.DeepEqual(withRaw, withoutRaw) {
		t.Fatal("raw policy must change columns")
	}
	for _, c := range withoutRaw {
		if c == "event.original" {
			t.Fatal("event.original column present without raw:read")
		}
	}
	found := false
	for _, c := range withRaw {
		if c == "event.original" {
			found = true
		}
	}
	if !found {
		t.Fatal("event.original column missing with raw:read")
	}
	// Column order must follow the catalog declaration.
	authentication, _ := Find("authentication")
	columns := ExportColumns(authentication, MaskingPolicy{})
	if len(columns) != len(authentication.Fields) {
		t.Fatalf("columns %d != fields %d", len(columns), len(authentication.Fields))
	}
	for i, f := range authentication.Fields {
		if columns[i] != f.Name {
			t.Fatalf("column %d = %s, want %s", i, columns[i], f.Name)
		}
	}
}

func TestLookupPath(t *testing.T) {
	doc := authDoc()
	if v, ok := LookupPath(doc, "source.port"); !ok || v != float64(4625) {
		t.Fatalf("source.port = %v %v", v, ok)
	}
	if _, ok := LookupPath(doc, "source.missing"); ok {
		t.Fatal("missing path resolved")
	}
	if _, ok := LookupPath(doc, "source.ip.deeper"); ok {
		t.Fatal("path through scalar resolved")
	}
}
