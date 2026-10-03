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

// PutAnalysisObject writes one analysis-result v2 object through the batch
// path (a batch of one). See PutAnalysisObjectBatch for the semantics.
func (s *Elasticsearch) PutAnalysisObject(ctx context.Context, object analysis.ObjectResult) error {
	results, err := s.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{object})
	if err != nil {
		return err
	}
	return results[0]
}

// PutAnalysisObjectBatch routes a batch of analysis-result v2 objects with two
// Elasticsearch _bulk requests: one for the per-type state indices (external
// version = producer revision) and one for the dated history indices. ES
// applies bulk items in request order, so same-key objects inside one batch
// are applied in batch order. Per-object results mirror the single-write
// semantics exactly:
//   - a stale revision is StaleRevisionError (state untouched, history not
//     appended);
//   - a same-revision identical replay is an idempotent no-op success;
//   - the same revision with different content is a permanent
//     ANALYSIS_REVISION_CONFLICT and never overwrites;
//   - transport/429/5xx failures stay retryable.
//
// The history frame of an object is appended only when its state write was
// accepted (applied or identical replay), matching PutAnalysisObject. A
// batch-level error is returned only for failures that prevented the item
// results from being known (transport loss, malformed response); the caller
// must treat it as retryable unless it is a PermanentIndexError.
func (s *Elasticsearch) PutAnalysisObjectBatch(ctx context.Context, objects []analysis.ObjectResult) ([]error, error) {
	if len(objects) == 0 {
		return nil, nil
	}
	results := make([]error, len(objects))
	type prepared struct {
		position int
		object   analysis.ObjectResult
		state    string
		body     []byte
	}
	ready := make([]prepared, 0, len(objects))
	for i, object := range objects {
		if err := validateAnalysisObject(object); err != nil {
			results[i] = err
			continue
		}
		if object.Namespace != s.Namespace {
			results[i] = PermanentIndexError{"ANALYSIS_CONTRACT_INVALID", "object namespace does not match sink namespace"}
			continue
		}
		body, err := analysisObjectBody(object)
		if err != nil {
			results[i] = err
			continue
		}
		state := analysisObjectStateIndex(object.ObjectType, s.Namespace)
		if err := s.ensureAnalysisStateIndex(ctx, state); err != nil {
			return nil, translateAnalysisError(err)
		}
		ready = append(ready, prepared{i, object, state, body})
	}
	if len(ready) == 0 {
		return results, nil
	}

	// Phase 1: state bulk, external version = revision.
	var stateBulk bytes.Buffer
	enc := json.NewEncoder(&stateBulk)
	for _, p := range ready {
		meta := map[string]any{"index": map[string]any{
			"_index": p.state, "_id": p.object.ObjectID,
			"version_type": "external", "version": p.object.Revision,
		}}
		if err := enc.Encode(meta); err != nil {
			return nil, err
		}
		stateBulk.Write(p.body)
		stateBulk.WriteByte('\n')
	}
	items, err := s.doBulk(ctx, &stateBulk, len(ready))
	if err != nil {
		return nil, err
	}
	accepted := make([]prepared, 0, len(ready))
	for i, itemResult := range items {
		p := ready[i]
		item := itemResult["index"]
		switch {
		case item.Status >= 200 && item.Status < 300:
			accepted = append(accepted, p)
		case item.Status == http.StatusConflict && item.Error != nil && item.Error.Type == "version_conflict_engine_exception":
			conflictErr := translateAnalysisError(s.resolveVersionConflict(ctx, p.state, p.object.ObjectID, p.object.Revision, p.body))
			if conflictErr == nil {
				// same-revision identical replay: no-op success, history frame
				// is still appended (its deterministic id absorbs the replay).
				accepted = append(accepted, p)
			} else {
				results[p.position] = conflictErr
			}
		case item.Status == 429 || item.Status >= 500:
			results[p.position] = fmt.Errorf("analysis state write returned %d: %s", item.Status, item.errorReason())
		default:
			results[p.position] = translateAnalysisError(PermanentIndexError{"PROJECTION_STATE_REJECTED", item.errorReason()})
		}
	}
	if len(accepted) == 0 {
		return results, nil
	}

	// Phase 2: history bulk with deterministic "<object_id>:<revision>" ids.
	var historyBulk bytes.Buffer
	henc := json.NewEncoder(&historyBulk)
	for _, p := range accepted {
		index := analysisObjectHistoryIndex(p.object.ObjectType, s.Namespace, p.object.DateKey)
		if err := s.ensureHistoryIndex(ctx, index, analysisObjectHistoryAlias(p.object.ObjectType, s.Namespace), analysisObjectHistoryMapping); err != nil {
			return nil, translateAnalysisError(err)
		}
		if err := henc.Encode(map[string]any{"create": map[string]string{"_index": index, "_id": fmt.Sprintf("%s:%d", p.object.ObjectID, p.object.Revision)}}); err != nil {
			return nil, err
		}
		historyBulk.Write(p.body)
		historyBulk.WriteByte('\n')
	}
	historyItems, err := s.doBulk(ctx, &historyBulk, len(accepted))
	if err != nil {
		// State writes already succeeded; only the history outcomes are
		// unknown. Classify per item so the caller retries the affected
		// objects (replay is idempotent) instead of failing the batch.
		for _, p := range accepted {
			results[p.position] = err
		}
		return results, nil
	}
	for i, itemResult := range historyItems {
		p := accepted[i]
		item := itemResult["create"]
		switch {
		case item.Status >= 200 && item.Status < 300:
		case item.Status == http.StatusConflict:
			index := analysisObjectHistoryIndex(p.object.ObjectType, s.Namespace, p.object.DateKey)
			results[p.position] = translateAnalysisError(s.verifyHistoryDuplicate(ctx, index, fmt.Sprintf("%s:%d", p.object.ObjectID, p.object.Revision), p.body))
		case item.Status == 429 || item.Status >= 500:
			results[p.position] = fmt.Errorf("analysis history write returned %d: %s", item.Status, item.errorReason())
		default:
			results[p.position] = translateAnalysisError(PermanentIndexError{"PROJECTION_HISTORY_REJECTED", item.errorReason()})
		}
	}
	return results, nil
}

// doBulk posts one NDJSON bulk body and returns the per-item results. The
// request body must contain exactly want items; a mismatched or undecodable
// response is a retryable batch-level error because the per-item outcomes are
// unknown.
func (s *Elasticsearch) doBulk(ctx context.Context, body *bytes.Buffer, want int) ([]map[string]rawBulkItem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/_bulk", body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return nil, fmt.Errorf("analysis bulk returned %d: %s", resp.StatusCode, message)
		}
		return nil, PermanentIndexError{"PROJECTION_BULK_REJECTED", string(message)}
	}
	var result struct {
		Items []map[string]rawBulkItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Items) != want {
		return nil, fmt.Errorf("analysis bulk response item count %d, want %d", len(result.Items), want)
	}
	return result.Items, nil
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
