package catalog

// Q03 export redaction (design baseline §7): export content is masked by the
// creator's permission snapshot. Without sensitive:read, values of catalog
// fields declared sensitivity=sensitive never appear — they are replaced by
// the RedactedValue marker so the column layout stays stable and the
// redaction is explicit. Original payloads (event.original, the raw dataset
// content) require raw:read; without it the field is removed entirely.
type MaskingPolicy struct {
	IncludeSensitive bool
	IncludeRaw       bool
}

const RedactedValue = "[redacted]"

// MaskEvent returns a redacted deep copy of an event document for export.
// The input is never mutated. Redaction is fail-closed: any field the
// catalog declares sensitive is masked when the policy lacks
// sensitive:read, regardless of the document's own shape.
func MaskEvent(source map[string]any, dataset DatasetDecl, policy MaskingPolicy) map[string]any {
	masked := deepCopyMap(source)
	if !policy.IncludeRaw {
		if event, ok := masked["event"].(map[string]any); ok {
			delete(event, "original")
		}
	}
	if !policy.IncludeSensitive {
		for _, f := range dataset.Fields {
			if f.Sensitivity == SensitivitySensitive {
				redactPath(masked, f.Name)
			}
		}
	}
	return masked
}

// ExportColumns returns the CSV column list for a dataset under the policy:
// every declared field in catalog order, except event.original when the
// policy lacks raw:read. Sensitive columns remain (their values are
// redacted by MaskEvent) so the schema is stable across policies.
func ExportColumns(dataset DatasetDecl, policy MaskingPolicy) []string {
	columns := make([]string, 0, len(dataset.Fields))
	for _, f := range dataset.Fields {
		if f.Name == "event.original" && !policy.IncludeRaw {
			continue
		}
		columns = append(columns, f.Name)
	}
	return columns
}

// LookupPath resolves a dotted catalog field path against a document.
func LookupPath(source map[string]any, path string) (any, bool) {
	current := source
	parts := splitPath(path)
	for i, part := range parts {
		value, ok := current[part]
		if !ok {
			return nil, false
		}
		if i == len(parts)-1 {
			return value, true
		}
		next, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return nil, false
}

func redactPath(doc map[string]any, path string) {
	parts := splitPath(path)
	current := doc
	for i, part := range parts {
		if i == len(parts)-1 {
			if _, ok := current[part]; ok {
				current[part] = RedactedValue
			}
			return
		}
		next, ok := current[part].(map[string]any)
		if !ok {
			return
		}
		current = next
	}
}

func splitPath(path string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	return append(parts, path[start:])
}

func deepCopyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = deepCopyValue(value)
	}
	return out
}

func deepCopyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return deepCopyMap(v)
	case []any:
		copied := make([]any, len(v))
		for i, item := range v {
			copied[i] = deepCopyValue(item)
		}
		return copied
	default:
		return value
	}
}
