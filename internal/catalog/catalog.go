// Package catalog holds the authoritative machine-readable Data Model /
// Dataset registry (Q01). It lives in its own package so both the API (query
// and catalog endpoints) and the control worker (Q03 export executor)
// validate and compile against the same declarations. It mirrors
// contracts/uim/domain-catalog.v1.yaml — the cross-check test pins the two
// together so they cannot drift apart silently.
package catalog

import "sort"

// Sensitivity levels (design baseline §7 敏感级别).
const (
	SensitivityPublic    = "public"
	SensitivityInternal  = "internal"
	SensitivitySensitive = "sensitive"
)

type FieldDecl struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Sensitivity string `json:"sensitivity"`
	Searchable  bool   `json:"searchable"`
	Aggregable  bool   `json:"aggregable"`
}

type DatasetDecl struct {
	Name             string      `json:"name"`
	Kind             string      `json:"kind"` // uim-domain | raw | quarantine
	ActiveGeneration string      `json:"active_generation"`
	IndexPattern     string      `json:"index_pattern"` // <prefix>-<namespace>
	QualityStatuses  []string    `json:"quality_statuses,omitempty"`
	Fields           []FieldDecl `json:"fields,omitempty"`
}

func field(name, typ, sensitivity string, searchable, aggregable bool) FieldDecl {
	return FieldDecl{Name: name, Type: typ, Sensitivity: sensitivity, Searchable: searchable, Aggregable: aggregable}
}

func commonUIMFields() []FieldDecl {
	return []FieldDecl{
		field("@timestamp", "date", SensitivityPublic, true, true),
		field("organization.id", "keyword", SensitivityInternal, true, true),
		field("event.id", "keyword", SensitivityPublic, true, true),
		field("event.kind", "keyword", SensitivityPublic, true, true),
		field("event.category", "keyword[]", SensitivityPublic, true, true),
		field("event.dataset", "keyword", SensitivityPublic, true, true),
		field("event.action", "keyword", SensitivityPublic, true, true),
		field("event.outcome", "keyword", SensitivityPublic, true, true),
		field("vendor.name", "keyword", SensitivityPublic, true, true),
		field("vendor.product", "keyword", SensitivityPublic, true, true),
		field("vendor.dataset", "keyword", SensitivityInternal, true, true),
		field("ueba.schema.version", "keyword", SensitivityPublic, true, true),
		field("ueba.quality.status", "keyword", SensitivityPublic, true, true),
		field("ueba.quality.reasons", "keyword[]", SensitivityInternal, true, true),
		field("ueba.quality.usable_for", "keyword[]", SensitivityInternal, true, true),
		field("ueba.route.domain", "keyword", SensitivityPublic, true, true),
		field("ueba.route.generation", "keyword", SensitivityPublic, true, true),
		field("ueba.provenance.raw_event_id", "keyword", SensitivityInternal, true, true),
		field("ueba.provenance.release_id", "keyword", SensitivityInternal, true, true),
	}
}

var domainFields = map[string][]FieldDecl{
	"authentication": {
		field("user.id", "keyword", SensitivitySensitive, true, true),
		field("user.name", "keyword", SensitivitySensitive, true, true),
		field("source.ip", "ip", SensitivityInternal, true, true),
		field("source.port", "integer", SensitivityInternal, true, true),
	},
	"session": {
		field("user.id", "keyword", SensitivitySensitive, true, true),
		field("user.name", "keyword", SensitivitySensitive, true, true),
	},
	"iam": {
		field("user.id", "keyword", SensitivitySensitive, true, true),
		field("user.target.id", "keyword", SensitivitySensitive, true, true),
		field("user.target.name", "keyword", SensitivitySensitive, true, true),
		field("group.id", "keyword", SensitivitySensitive, true, true),
		field("group.name", "keyword", SensitivitySensitive, true, true),
	},
	"directory": {
		field("user.id", "keyword", SensitivitySensitive, true, true),
		field("user.target.id", "keyword", SensitivitySensitive, true, true),
	},
	"network": {
		field("source.ip", "ip", SensitivityInternal, true, true),
		field("source.port", "integer", SensitivityInternal, true, true),
		field("destination.ip", "ip", SensitivityInternal, true, true),
		field("destination.port", "integer", SensitivityInternal, true, true),
		field("network.transport", "keyword", SensitivityPublic, true, true),
		field("network.protocol", "keyword", SensitivityPublic, true, true),
		field("network.bytes", "long", SensitivityPublic, false, true),
		field("network.packets", "long", SensitivityPublic, false, true),
	},
	"dns": {
		field("dns.question.name", "keyword", SensitivityInternal, true, true),
		field("source.ip", "ip", SensitivityInternal, true, true),
	},
	"web": {
		field("url.domain", "keyword", SensitivityInternal, true, true),
		field("source.ip", "ip", SensitivityInternal, true, true),
	},
	"tls": {
		field("tls.version", "keyword", SensitivityPublic, true, true),
		field("tls.cipher", "keyword", SensitivityPublic, true, true),
		field("source.ip", "ip", SensitivityInternal, true, true),
	},
}

const ActiveGenerationV1 = "g1"

// Datasets returns the dataset registry in deterministic order.
func Datasets() []DatasetDecl {
	domains := make([]string, 0, len(domainFields))
	for domain := range domainFields {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	datasets := make([]DatasetDecl, 0, len(domains)+2)
	for _, domain := range domains {
		fields := append(commonUIMFields(), domainFields[domain]...)
		datasets = append(datasets, DatasetDecl{
			Name:             domain,
			Kind:             "uim-domain",
			ActiveGeneration: ActiveGenerationV1,
			IndexPattern:     "logs-ueba." + domain + "-<namespace>",
			QualityStatuses:  []string{"qualified", "partial"},
			Fields:           fields,
		})
	}
	datasets = append(datasets,
		DatasetDecl{
			Name:             "raw",
			Kind:             "raw",
			ActiveGeneration: ActiveGenerationV1,
			IndexPattern:     "logs-ueba.raw-<namespace>",
			Fields: []FieldDecl{
				field("@timestamp", "date", SensitivityPublic, true, true),
				field("organization.id", "keyword", SensitivityInternal, true, true),
				field("event.id", "keyword", SensitivityPublic, true, true),
				field("event.original", "text", SensitivitySensitive, true, false),
				field("ueba.provenance.source_context_id", "keyword", SensitivityInternal, true, true),
			},
		},
		DatasetDecl{
			Name:             "quarantine",
			Kind:             "quarantine",
			ActiveGeneration: ActiveGenerationV1,
			IndexPattern:     "logs-ueba.quarantine-<namespace>",
			Fields: []FieldDecl{
				field("@timestamp", "date", SensitivityPublic, true, true),
				field("organization.id", "keyword", SensitivityInternal, true, true),
				field("quarantine.stage", "keyword", SensitivityInternal, true, true),
				field("quarantine.reason", "keyword", SensitivityInternal, true, true),
			},
		},
	)
	return datasets
}

func Find(name string) (DatasetDecl, bool) {
	for _, dataset := range Datasets() {
		if dataset.Name == name {
			return dataset, true
		}
	}
	return DatasetDecl{}, false
}

// QualityStatusAllowed reports whether the dataset declares the quality
// status as queryable (design baseline §7 质量条件).
func QualityStatusAllowed(dataset DatasetDecl, status string) bool {
	for _, declared := range dataset.QualityStatuses {
		if declared == status {
			return true
		}
	}
	return false
}
