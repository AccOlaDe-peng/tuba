package analysis

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

// Result is the versioned boundary between analytical workers and Go sinks.
type Result struct {
	ContractVersion string          `json:"contract_version"`
	ResultType      string          `json:"result_type"`
	ResultID        string          `json:"result_id"`
	OrganizationID  string          `json:"organization_id"`
	Namespace       string          `json:"namespace"`
	RuleID          string          `json:"rule_id"`
	RuleVersion     string          `json:"rule_version"`
	RunID           string          `json:"run_id"`
	Document        json.RawMessage `json:"document"`
}

func Parse(raw []byte, organization, namespace string) (Result, error) {
	var result Result
	if len(raw) == 0 || len(raw) > 1<<20 {
		return result, errors.New("analysis result size must be 1 byte to 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("invalid analysis result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return result, errors.New("trailing data after analysis result")
	}
	if result.ContractVersion != "1.0.0" || result.ResultType != "anomaly" {
		return result, errors.New("unsupported analysis result contract")
	}
	if result.ResultID == "" || len(result.ResultID) > 256 || result.RuleID == "" || result.RuleVersion == "" || result.RunID == "" || len(result.Document) == 0 {
		return result, errors.New("required analysis result fields are missing")
	}
	if result.OrganizationID != organization || result.Namespace != namespace {
		return result, errors.New("analysis result tenant does not match service identity")
	}
	var document struct {
		Organization struct {
			ID string `json:"id"`
		} `json:"organization"`
	}
	if json.Unmarshal(result.Document, &document) != nil || document.Organization.ID != organization {
		return result, errors.New("analysis document tenant mismatch")
	}
	return result, nil
}

// F07: analysis-result v2 (contracts/events/analysis-result/2) — the
// multi-object sink boundary. Every derived object (feature, baseline,
// anomaly/finding, risk_event, entity_risk) carries the producer-assigned
// external revision of its business object, an operation (upsert/retracted),
// the generation, and the fixed event-time date key of its stable detection
// window. The sink routes each object by type and applies the E05 external
// version semantics: order is decided by revision, never by arrival.

const (
	ContractVersionV2 = "2.0.0"

	ObjectTypeFeature    = "feature"
	ObjectTypeBaseline   = "baseline"
	ObjectTypeAnomaly    = "anomaly"
	ObjectTypeRiskEvent  = "risk_event"
	ObjectTypeEntityRisk = "entity_risk"

	OperationUpsert    = "upsert"
	OperationRetracted = "retracted"

	// LegacyGeneration marks objects migrated from the v1 contract, which has
	// no trustworthy generation or input references (see contract.md).
	LegacyGeneration = "legacy-v1"
)

var (
	tenantPattern      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	semverPattern      = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
	dateKeyPattern     = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	contentHashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// InputRef identifies one exact upstream object (and optionally its revision
// and content hash) used to derive the result.
type InputRef struct {
	ObjectType  string `json:"object_type"`
	ObjectID    string `json:"object_id"`
	Revision    int64  `json:"revision,omitempty"`
	ContentHash string `json:"content_hash,omitempty"`
}

// ObjectResult is one analysis-result v2 envelope.
type ObjectResult struct {
	ContractVersion string          `json:"contract_version"`
	ObjectType      string          `json:"object_type"`
	ObjectID        string          `json:"object_id"`
	Revision        int64           `json:"revision"`
	Operation       string          `json:"operation"`
	Generation      string          `json:"generation"`
	OrganizationID  string          `json:"organization_id"`
	Namespace       string          `json:"namespace"`
	RuleID          string          `json:"rule_id"`
	RuleVersion     string          `json:"rule_version"`
	RunID           string          `json:"run_id"`
	WindowStart     time.Time       `json:"-"`
	WindowStartRaw  string          `json:"window_start"`
	DateKey         string          `json:"date_key"`
	InputRefs       []InputRef      `json:"input_refs"`
	Document        json.RawMessage `json:"document"`
}

// ValidObjectType reports whether the object type is routable by the sink.
func ValidObjectType(objectType string) bool {
	switch objectType {
	case ObjectTypeFeature, ObjectTypeBaseline, ObjectTypeAnomaly, ObjectTypeRiskEvent, ObjectTypeEntityRisk:
		return true
	}
	return false
}

// ParseObject decodes and validates one analysis-result v2 envelope
// fail-closed. The date key must be the UTC calendar date of window_start:
// the producer derives it from event time, and the sink refuses any envelope
// where the two disagree rather than drifting a replay into the wrong day
// partition.
func ParseObject(raw []byte, organization, namespace string) (ObjectResult, error) {
	var result ObjectResult
	if len(raw) == 0 || len(raw) > 1<<20 {
		return result, errors.New("analysis object size must be 1 byte to 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("invalid analysis object: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return result, errors.New("trailing data after analysis object")
	}
	if result.ContractVersion != ContractVersionV2 {
		return result, fmt.Errorf("unsupported analysis object contract %q", result.ContractVersion)
	}
	if !ValidObjectType(result.ObjectType) {
		return result, fmt.Errorf("unsupported analysis object type %q", result.ObjectType)
	}
	if result.ObjectID == "" || len(result.ObjectID) > 256 {
		return result, errors.New("analysis object id must be 1 to 256 bytes")
	}
	if result.Revision < 1 {
		return result, errors.New("analysis object revision must be >= 1")
	}
	if result.Operation != OperationUpsert && result.Operation != OperationRetracted {
		return result, fmt.Errorf("unsupported analysis object operation %q", result.Operation)
	}
	if result.Generation == "" || len(result.Generation) > 128 {
		return result, errors.New("analysis object generation must be 1 to 128 bytes")
	}
	if !tenantPattern.MatchString(result.OrganizationID) || !tenantPattern.MatchString(result.Namespace) {
		return result, errors.New("analysis object tenant or namespace is malformed")
	}
	if result.OrganizationID != organization || result.Namespace != namespace {
		return result, errors.New("analysis object tenant does not match service identity")
	}
	if result.RuleID == "" || len(result.RuleID) > 256 {
		return result, errors.New("analysis object rule id must be 1 to 256 bytes")
	}
	if !semverPattern.MatchString(result.RuleVersion) {
		return result, errors.New("analysis object rule version must be semver")
	}
	if result.RunID == "" || len(result.RunID) > 256 {
		return result, errors.New("analysis object run id must be 1 to 256 bytes")
	}
	windowStart, err := time.Parse(time.RFC3339Nano, result.WindowStartRaw)
	if err != nil {
		return result, errors.New("analysis object window_start must be an RFC3339 timestamp")
	}
	result.WindowStart = windowStart.UTC()
	if !dateKeyPattern.MatchString(result.DateKey) {
		return result, errors.New("analysis object date_key must be YYYY-MM-DD")
	}
	if result.DateKey != result.WindowStart.Format("2006-01-02") {
		return result, errors.New("analysis object date_key does not match the UTC date of window_start")
	}
	if result.InputRefs == nil {
		return result, errors.New("analysis object input_refs is required (empty array allowed)")
	}
	for i, ref := range result.InputRefs {
		if ref.ObjectType == "" || ref.ObjectID == "" {
			return result, fmt.Errorf("analysis object input_refs[%d] requires object_type and object_id", i)
		}
		if ref.Revision < 0 {
			return result, fmt.Errorf("analysis object input_refs[%d] revision must be >= 1 when present", i)
		}
		if ref.ContentHash != "" && !contentHashPattern.MatchString(ref.ContentHash) {
			return result, fmt.Errorf("analysis object input_refs[%d] content_hash must be a sha256 hex digest", i)
		}
	}
	if len(result.Document) == 0 || !json.Valid(result.Document) {
		return result, errors.New("analysis object document is required and must be JSON")
	}
	var documentShape any
	if err := json.Unmarshal(result.Document, &documentShape); err != nil {
		return result, errors.New("analysis object document is required and must be JSON")
	}
	if _, isObject := documentShape.(map[string]any); !isObject {
		return result, errors.New("analysis object document must be a JSON object")
	}
	return result, nil
}

// ErrLegacyWindowMissing reports a v1 result whose document has no explicit
// stable window time. The migration contract forbids substituting generated_at,
// @timestamp, or the current time; the message must go to a migration
// quarantine instead.
var ErrLegacyWindowMissing = errors.New("legacy v1 result has no explicit window start; migration to v2 requires the stable window time")

// MigrateLegacy adapts a validated v1 anomaly result to the v2 object shape:
// object_type=anomaly, revision=1, operation=upsert, generation=legacy-v1 and
// input_refs=[] (v1 has no trustworthy generation or input references; they
// are never inferred from rule_version or run_id). The fixed date key comes
// from the document's explicit detection.window.start (or top-level
// window_start); a missing or invalid window time is ErrLegacyWindowMissing.
func MigrateLegacy(result Result) (ObjectResult, error) {
	var document struct {
		Detection struct {
			Window struct {
				Start string `json:"start"`
			} `json:"window"`
		} `json:"detection"`
		WindowStart string `json:"window_start"`
	}
	_ = json.Unmarshal(result.Document, &document)
	raw := document.Detection.Window.Start
	if raw == "" {
		raw = document.WindowStart
	}
	windowStart, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return ObjectResult{}, ErrLegacyWindowMissing
	}
	windowStart = windowStart.UTC()
	return ObjectResult{
		ContractVersion: ContractVersionV2,
		ObjectType:      ObjectTypeAnomaly,
		ObjectID:        result.ResultID,
		Revision:        1,
		Operation:       OperationUpsert,
		Generation:      LegacyGeneration,
		OrganizationID:  result.OrganizationID,
		Namespace:       result.Namespace,
		RuleID:          result.RuleID,
		RuleVersion:     result.RuleVersion,
		RunID:           result.RunID,
		WindowStart:     windowStart,
		WindowStartRaw:  windowStart.Format(time.RFC3339Nano),
		DateKey:         windowStart.Format("2006-01-02"),
		InputRefs:       []InputRef{},
		Document:        result.Document,
	}, nil
}
