package sink

// F07 analysis-object sink: multi-object dispatcher for analysis-result v2
// (contracts/events/analysis-result/2). Every object type is written in two
// layers, mirroring the E05 projection conventions:
//
//   - state projection (latest state per business object): sink-managed
//     strict index ueba-analysis-<type>-<namespace>, keyed by object_id and
//     guarded by Elasticsearch external versioning with the producer-assigned
//     revision. A write whose revision is older than the stored one is
//     rejected by ES and reported as StaleRevisionError — an out-of-order or
//     late write never overwrites a newer value. A same-revision replay with
//     identical content is idempotent; the same revision with different
//     content is PermanentIndexError{ANALYSIS_REVISION_CONFLICT} (never a
//     silent overwrite).
//   - history projection (traceability): append-only dated index
//     tuba-v1-analysis-<type>-<namespace>-g1-<date> (read alias
//     logs-ueba.analysis-<type>-<namespace>) keyed by "<object_id>:<revision>".
//     The date partition is derived from the envelope's fixed event-time
//     date_key (validated against window_start at the parse boundary), never
//     from processing time — replays and late messages land in their original
//     day partition.
//
// operation=retracted is a tombstone applied in revision order: the state
// document records operation=retracted and history keeps the frame.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tuba/product/internal/analysis"
)

// PutAnalysis writes one legacy v1 anomaly document to the long-standing
// anomaly index. Kept for the v1 compatibility path; v2 objects go through
// PutAnalysisObject.
func (s *Elasticsearch) PutAnalysis(ctx context.Context, result analysis.Result) error {
	index := "ueba-anomalies-" + s.Namespace
	endpoint := s.URL + "/" + index + "/_doc/" + url.PathEscape(result.ResultID) + "?op_type=index"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(result.Document))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("elasticsearch returned %d: %s", resp.StatusCode, body)
	}
	return nil
}

func analysisObjectStateIndex(objectType, namespace string) string {
	return "ueba-analysis-" + objectType + "-" + namespace
}

func analysisObjectHistoryIndex(objectType, namespace, dateKey string) string {
	return fmt.Sprintf("tuba-v1-analysis-%s-%s-g1-%s", objectType, namespace, strings.ReplaceAll(dateKey, "-", "."))
}

func analysisObjectHistoryAlias(objectType, namespace string) string {
	return "logs-ueba.analysis-" + objectType + "-" + namespace
}

// analysisObjectBody renders the state/history document. The content hash is
// computed over the document without content_hash, then embedded (same
// convention as the E05 projections), so same-revision replays can be told
// apart from conflicts.
func analysisObjectBody(object analysis.ObjectResult) ([]byte, error) {
	var document any
	if err := json.Unmarshal(object.Document, &document); err != nil {
		return nil, PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object document is not valid JSON"}
	}
	inputRefs := object.InputRefs
	if inputRefs == nil {
		inputRefs = []analysis.InputRef{}
	}
	doc := map[string]any{
		"@timestamp":   object.WindowStart.UTC().Format(time.RFC3339Nano),
		"organization": map[string]any{"id": object.OrganizationID},
		"namespace":    object.Namespace,
		"object": map[string]any{
			"type": object.ObjectType, "id": object.ObjectID,
			"revision": object.Revision, "operation": object.Operation,
			"generation": object.Generation,
		},
		"rule":         map[string]any{"id": object.RuleID, "version": object.RuleVersion},
		"run_id":       object.RunID,
		"window_start": object.WindowStart.UTC().Format(time.RFC3339Nano),
		"date_key":     object.DateKey,
		"input_refs":   inputRefs,
		"document":     document,
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	doc["content_hash"] = contentHash(encoded)
	return json.Marshal(doc)
}

func validateAnalysisObject(object analysis.ObjectResult) error {
	switch {
	case object.OrganizationID == "" || object.Namespace == "" || !analysis.ValidObjectType(object.ObjectType):
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "missing object tenant, namespace, or supported object type"}
	case object.ObjectID == "" || object.RuleID == "" || object.RuleVersion == "" || object.RunID == "":
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "missing object id, rule, or run identity"}
	case object.Revision < 1:
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object revision must be >= 1"}
	case object.Operation != analysis.OperationUpsert && object.Operation != analysis.OperationRetracted:
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object operation must be upsert or retracted"}
	case object.Generation == "":
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object generation is required"}
	case object.WindowStart.IsZero() || object.DateKey == "":
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "missing object window start or fixed date key"}
	case object.DateKey != object.WindowStart.UTC().Format("2006-01-02"):
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object date key does not match the UTC date of window start"}
	case len(object.Document) == 0 || !json.Valid(object.Document):
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object document is required and must be JSON"}
	}
	return nil
}

// PutAnalysisObject routes one analysis-result v2 object to its per-type state
// index (external version = producer revision) and appends the same frame to
// the dated history index. A stale revision returns StaleRevisionError (state
// untouched, history not appended); a same-revision identical replay is a
// no-op success; the same revision with different content is a permanent
// conflict and never overwrites.
func (s *Elasticsearch) PutAnalysisObject(ctx context.Context, object analysis.ObjectResult) error {
	if err := validateAnalysisObject(object); err != nil {
		return err
	}
	if object.Namespace != s.Namespace {
		return PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object namespace does not match sink namespace"}
	}
	body, err := analysisObjectBody(object)
	if err != nil {
		return err
	}
	state := analysisObjectStateIndex(object.ObjectType, s.Namespace)
	if err := s.ensureAnalysisStateIndex(ctx, state); err != nil {
		return err
	}
	if err := s.putVersioned(ctx, state, object.ObjectID, object.Revision, body); err != nil {
		return translateAnalysisError(err)
	}
	historyID := fmt.Sprintf("%s:%d", object.ObjectID, object.Revision)
	return translateAnalysisError(s.putHistory(ctx, analysisObjectHistoryIndex(object.ObjectType, s.Namespace, object.DateKey),
		analysisObjectHistoryAlias(object.ObjectType, s.Namespace), historyID, body, analysisObjectHistoryMapping))
}

// translateAnalysisError re-codes the shared projection-layer permanent
// errors so a dead-lettered analysis object carries an analysis-specific,
// queryable reason code.
func translateAnalysisError(err error) error {
	var permanent PermanentIndexError
	if !errors.As(err, &permanent) {
		return err
	}
	switch permanent.Code {
	case "PROJECTION_REVISION_CONFLICT":
		permanent.Code = "ANALYSIS_REVISION_CONFLICT"
	case "PROJECTION_STATE_REJECTED":
		permanent.Code = "ANALYSIS_STATE_REJECTED"
	case "PROJECTION_HISTORY_REJECTED":
		permanent.Code = "ANALYSIS_HISTORY_REJECTED"
	case "PROJECTION_HISTORY_CONFLICT":
		permanent.Code = "ANALYSIS_HISTORY_CONFLICT"
	case "PROJECTION_INDEX_CREATE_FAILED":
		permanent.Code = "ANALYSIS_INDEX_CREATE_FAILED"
	default:
		return err
	}
	return permanent
}

// ensureAnalysisStateIndex creates the per-type state index with a strict
// bounded mapping on first use (same convention as the quarantine sink).
func (s *Elasticsearch) ensureAnalysisStateIndex(ctx context.Context, index string) error {
	if _, ok := s.rawIndices.Load(index); ok {
		return nil
	}
	body := []byte(fmt.Sprintf(`{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"dynamic":"strict","properties":%s}}`, analysisObjectStateMapping))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.URL+"/"+url.PathEscape(index), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	status := resp.StatusCode
	resp.Body.Close()
	if status >= 200 && status < 300 || status == http.StatusBadRequest && strings.Contains(string(data), "resource_already_exists_exception") {
		s.rawIndices.Store(index, struct{}{})
		return nil
	}
	if status == 429 || status >= 500 {
		return fmt.Errorf("create analysis state index returned %d: %s", status, data)
	}
	return PermanentIndexError{"ANALYSIS_INDEX_CREATE_FAILED", string(data)}
}

const analysisObjectStateMapping = `{
	"@timestamp":{"type":"date"},
	"organization":{"properties":{"id":{"type":"keyword"}}},
	"namespace":{"type":"keyword"},
	"object":{"properties":{"type":{"type":"keyword"},"id":{"type":"keyword"},"revision":{"type":"long"},"operation":{"type":"keyword"},"generation":{"type":"keyword"}}},
	"rule":{"properties":{"id":{"type":"keyword"},"version":{"type":"keyword"}}},
	"run_id":{"type":"keyword"},
	"window_start":{"type":"date"},
	"date_key":{"type":"keyword"},
	"input_refs":{"type":"object","enabled":false},
	"document":{"type":"object","enabled":false},
	"content_hash":{"type":"keyword"}
}`

const analysisObjectHistoryMapping = analysisObjectStateMapping
