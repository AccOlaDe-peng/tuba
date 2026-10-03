package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
	"tuba/product/internal/es"
)

type anomalyDocument struct {
	Timestamp    string `json:"@timestamp"`
	Organization struct {
		ID string `json:"id"`
	} `json:"organization"`
	Entity struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"entity"`
	Anomaly struct {
		ID       string  `json:"id"`
		Type     string  `json:"type"`
		Severity string  `json:"severity"`
		Status   string  `json:"status"`
		Score    float64 `json:"score"`
	} `json:"anomaly"`
	Evidence struct {
		EventIDs []string `json:"event_ids"`
		Count    int      `json:"count"`
	} `json:"evidence"`
	Detection struct {
		RuleID      string `json:"rule_id"`
		RuleVersion string `json:"rule_version"`
	} `json:"detection"`
	Explanation struct {
		ReasonCodes []string `json:"reason_codes"`
		Summary     string   `json:"summary"`
	} `json:"explanation"`
}

func decodeAnomaly(raw json.RawMessage, id string) (anomalyDocument, error) {
	var document anomalyDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return document, err
	}
	if document.Anomaly.ID == "" {
		document.Anomaly.ID = id
	}
	if document.Organization.ID == "" || document.Anomaly.ID == "" || document.Timestamp == "" {
		return document, errors.New("invalid anomaly document")
	}
	return document, nil
}

func anomalySummary(source map[string]any, id string) map[string]any {
	raw, _ := json.Marshal(source)
	document, err := decodeAnomaly(raw, id)
	if err != nil {
		return map[string]any{"id": id, "type": "invalid", "severity": "medium", "status": "open", "timestamp": ""}
	}
	return map[string]any{
		"id":             document.Anomaly.ID,
		"type":           document.Anomaly.Type,
		"severity":       document.Anomaly.Severity,
		"status":         document.Anomaly.Status,
		"timestamp":      document.Timestamp,
		"score":          document.Anomaly.Score,
		"entity":         map[string]any{"id": document.Entity.ID, "type": document.Entity.Type},
		"rule_id":        document.Detection.RuleID,
		"rule_version":   document.Detection.RuleVersion,
		"summary":        document.Explanation.Summary,
		"evidence_count": document.Evidence.Count,
	}
}

func anomalyDetailBody(document anomalyDocument) map[string]any {
	return map[string]any{
		"id":                 document.Anomaly.ID,
		"type":               document.Anomaly.Type,
		"severity":           document.Anomaly.Severity,
		"status":             document.Anomaly.Status,
		"timestamp":          document.Timestamp,
		"score":              document.Anomaly.Score,
		"entity":             map[string]any{"id": document.Entity.ID, "type": document.Entity.Type},
		"rule_id":            document.Detection.RuleID,
		"rule_version":       document.Detection.RuleVersion,
		"summary":            document.Explanation.Summary,
		"reason_codes":       document.Explanation.ReasonCodes,
		"evidence_event_ids": document.Evidence.EventIDs,
		"evidence_count":     document.Evidence.Count,
	}
}

func (s Server) getAnomaly(ctx context.Context, id string, principal auth.Principal) (es.Hit, anomalyDocument, int, error) {
	var hit es.Hit
	var document anomalyDocument
	status, err := s.ES.Do(ctx, http.MethodGet, "/ueba-anomalies-"+principal.Namespace+"/_doc/"+es.EscapeID(id), nil, &hit)
	if err != nil {
		return hit, document, http.StatusServiceUnavailable, err
	}
	if status == http.StatusNotFound {
		if s.OrgIDs == nil {
			return hit, document, http.StatusNotFound, nil
		}
		// v2 findings are only in the F07 state index, never in the v1
		// mirror index: fall through to the v2 projection before giving up.
		orgUUID, resolveErr := s.OrgIDs.Resolve(ctx, principal.Organization)
		if resolveErr != nil {
			return hit, document, http.StatusServiceUnavailable, resolveErr
		}
		return s.getAnomalyV2(ctx, id, principal, orgUUID)
	}
	if status != http.StatusOK {
		return hit, document, http.StatusServiceUnavailable, fmt.Errorf("elasticsearch HTTP %d", status)
	}
	document, err = decodeAnomaly(hit.Source, id)
	if err != nil {
		return hit, document, http.StatusServiceUnavailable, err
	}
	if document.Organization.ID != principal.Organization {
		return hit, document, http.StatusNotFound, nil
	}
	return hit, document, http.StatusOK, nil
}

// v2AnomalyState is the F07 state-projection envelope (ueba-analysis-anomaly-
// <namespace>) around a finding document. The tenant key is the control-plane
// organization UUID; the finding payload itself is the opaque document field.
type v2AnomalyState struct {
	Timestamp    string `json:"@timestamp"`
	Organization struct {
		ID string `json:"id"`
	} `json:"organization"`
	Object struct {
		ID        string `json:"id"`
		Operation string `json:"operation"`
	} `json:"object"`
	Document json.RawMessage `json:"document"`
}

func decodeV2AnomalyState(raw json.RawMessage) (v2AnomalyState, error) {
	var state v2AnomalyState
	err := json.Unmarshal(raw, &state)
	return state, err
}

// getAnomalyV2 reads one finding from the v2 state index. The projection is
// keyed by object_id, which for findings is the anomaly id; the org check uses
// the resolved UUID, and a retracted tombstone reads as not found.
func (s Server) getAnomalyV2(ctx context.Context, id string, principal auth.Principal, orgUUID string) (es.Hit, anomalyDocument, int, error) {
	var hit es.Hit
	var document anomalyDocument
	var raw struct {
		Found  bool            `json:"found"`
		Source json.RawMessage `json:"_source"`
	}
	status, err := s.ES.Do(ctx, http.MethodGet, "/ueba-analysis-anomaly-"+principal.Namespace+"/_doc/"+es.EscapeID(id), nil, &raw)
	if err != nil {
		return hit, document, http.StatusServiceUnavailable, err
	}
	if status == http.StatusNotFound || (status == http.StatusOK && !raw.Found) {
		return hit, document, http.StatusNotFound, nil
	}
	if status != http.StatusOK {
		return hit, document, http.StatusServiceUnavailable, fmt.Errorf("elasticsearch HTTP %d", status)
	}
	state, err := decodeV2AnomalyState(raw.Source)
	if err != nil {
		return hit, document, http.StatusServiceUnavailable, err
	}
	if state.Organization.ID != orgUUID || state.Object.Operation != "upsert" {
		return hit, document, http.StatusNotFound, nil
	}
	document, err = decodeAnomaly(state.Document, id)
	if err != nil {
		return hit, document, http.StatusServiceUnavailable, err
	}
	if document.Timestamp == "" {
		document.Timestamp = state.Timestamp
	}
	hit.ID = state.Object.ID
	return hit, document, http.StatusOK, nil
}

// searchV2Anomalies queries the v2 anomaly state index for the organization
// UUID. The finding payload (document) is stored with enabled:false, so only
// envelope fields (organization, operation, @timestamp) are filterable in
// Elasticsearch; severity/status/entity filters are applied in memory after
// decoding. maybeMore reports that the state index returned a full page, in
// which case more v2 findings may exist beyond it.
func (s Server) searchV2Anomalies(ctx context.Context, principal auth.Principal, orgUUID string, limit int, severity, status, entity string, from, to time.Time) (items []map[string]any, total int64, maybeMore bool, err error) {
	query := map[string]any{
		"size":             limit,
		"track_total_hits": true,
		"sort":             []any{map[string]any{"@timestamp": "desc"}, map[string]any{"object.id": "asc"}},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"organization.id": orgUUID}},
			map[string]any{"term": map[string]any{"object.operation": "upsert"}},
			map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": from.Format(time.RFC3339), "lte": to.Format(time.RFC3339)}}},
		}}},
	}
	var result es.SearchResult
	httpStatus, err := s.ES.Do(ctx, http.MethodPost, "/ueba-analysis-anomaly-"+principal.Namespace+"/_search", query, &result)
	if err == nil && httpStatus == http.StatusNotFound {
		return []map[string]any{}, 0, false, nil
	}
	if err != nil || httpStatus != http.StatusOK {
		return nil, 0, false, fmt.Errorf("elasticsearch HTTP %d", httpStatus)
	}
	items = make([]map[string]any, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		state, decodeErr := decodeV2AnomalyState(hit.Source)
		if decodeErr != nil {
			return nil, 0, false, decodeErr
		}
		if state.Organization.ID != orgUUID || state.Object.Operation != "upsert" {
			continue
		}
		var inner map[string]any
		if json.Unmarshal(state.Document, &inner) != nil {
			return nil, 0, false, errors.New("invalid v2 anomaly document")
		}
		summary := anomalySummary(inner, state.Object.ID)
		if summary["timestamp"] == "" {
			summary["timestamp"] = state.Timestamp
		}
		if severity != "" && summary["severity"] != severity {
			continue
		}
		if status != "" && summary["status"] != status {
			continue
		}
		if entity != "" {
			entityMap, _ := summary["entity"].(map[string]any)
			if entityMap["id"] != entity {
				continue
			}
		}
		items = append(items, summary)
	}
	return items, result.Hits.Total.Value, len(result.Hits.Hits) == limit, nil
}

// mergeAnomalyItems combines the v1 and v2 summary pages: findings present in
// both (the F07 sink mirrors v1 anomalies into v2, possibly under a different
// document id but the same anomaly.id) appear once, ordered newest first with
// the anomaly id as the tiebreak, truncated to limit.
func mergeAnomalyItems(v1, v2 []map[string]any, limit int) []map[string]any {
	combined := make([]map[string]any, 0, len(v1)+len(v2))
	combined = append(combined, v1...)
	combined = append(combined, v2...)
	timestamp := func(item map[string]any) time.Time {
		raw, _ := item["timestamp"].(string)
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}
		}
		return parsed
	}
	sort.SliceStable(combined, func(i, j int) bool {
		ti, tj := timestamp(combined[i]), timestamp(combined[j])
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		idi, _ := combined[i]["id"].(string)
		idj, _ := combined[j]["id"].(string)
		return idi < idj
	})
	seen := map[string]bool{}
	merged := make([]map[string]any, 0, minInt(len(combined), limit))
	for _, item := range combined {
		id, _ := item["id"].(string)
		if seen[id] {
			continue
		}
		seen[id] = true
		merged = append(merged, item)
		if len(merged) == limit {
			break
		}
	}
	return merged
}


func (s Server) anomalyDetail(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	id := r.PathValue("id")
	if !validResourceID(id) {
		http.Error(w, "invalid anomaly ID", http.StatusBadRequest)
		return
	}
	_, document, status, err := s.getAnomaly(r.Context(), id, principal)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	if status != http.StatusOK {
		http.Error(w, http.StatusText(status), status)
		return
	}
	writeJSON(w, http.StatusOK, anomalyDetailBody(document))
}

func (s Server) anomalyEvidence(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	id := r.PathValue("id")
	if !validResourceID(id) {
		http.Error(w, "invalid anomaly ID", http.StatusBadRequest)
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 200 {
			http.Error(w, "limit must be 1..200", http.StatusBadRequest)
			return
		}
		limit = value
	}
	_, document, status, err := s.getAnomaly(r.Context(), id, principal)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	if status != http.StatusOK {
		http.Error(w, http.StatusText(status), status)
		return
	}
	eventIDs := uniqueIDs(document.Evidence.EventIDs, limit)
	if len(eventIDs) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "total": 0, "truncated": false})
		return
	}
	query := map[string]any{
		"size":             len(eventIDs),
		"track_total_hits": true,
		"sort":             []any{map[string]any{"@timestamp": "asc"}, map[string]any{"event.id": "asc"}},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"organization.id": principal.Organization}},
			map[string]any{"terms": map[string]any{"event.id": eventIDs}},
		}}},
	}
	var result es.SearchResult
	status, err = s.ES.Do(r.Context(), http.MethodPost, "/logs-ueba.authentication-*/_search", query, &result)
	if err != nil || status != http.StatusOK {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	items := make([]map[string]any, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		var source map[string]any
		if json.Unmarshal(hit.Source, &source) != nil {
			http.Error(w, "invalid stored event", http.StatusServiceUnavailable)
			return
		}
		items = append(items, evidenceEvent(source, hit.ID))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":     items,
		"total":     result.Hits.Total.Value,
		"truncated": len(document.Evidence.EventIDs) > len(eventIDs),
	})
}

func evidenceEvent(source map[string]any, id string) map[string]any {
	event, _ := source["event"].(map[string]any)
	user, _ := source["user"].(map[string]any)
	ueba, _ := source["ueba"].(map[string]any)
	return map[string]any{
		"id":        id,
		"timestamp": source["@timestamp"],
		"event":     event,
		"user":      user,
		"ueba":      ueba,
	}
}

func uniqueIDs(ids []string, limit int) []string {
	seen := map[string]bool{}
	output := make([]string, 0, minInt(len(ids), limit))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		output = append(output, id)
		if len(output) == limit {
			break
		}
	}
	return output
}

func validResourceID(id string) bool {
	return id != "" && len(id) <= 256 && !strings.ContainsAny(id, "/\\")
}

func (s Server) overview(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	openAnomalies, highRisk, latest, err := s.anomalyOverview(r.Context(), principal)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	caseOverview := control.Overview{}
	if s.Control != nil {
		caseOverview, err = s.Control.Overview(r.Context(), principal)
		if err != nil {
			http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"open_anomalies": openAnomalies,
		"high_risk":      highRisk,
		"latest_anomaly": latest,
		"cases":          caseOverview,
		"generated_at":   time.Now().UTC(),
	})
}

func (s Server) anomalyOverview(ctx context.Context, principal auth.Principal) (int64, int64, *time.Time, error) {
	count := func(extra []any) (int64, error) {
		filters := []any{map[string]any{"term": map[string]any{"organization.id": principal.Organization}}}
		filters = append(filters, extra...)
		var result struct {
			Count int64 `json:"count"`
		}
		status, err := s.ES.Do(ctx, http.MethodPost, "/ueba-anomalies-"+principal.Namespace+"/_count", map[string]any{
			"query": map[string]any{"bool": map[string]any{"filter": filters}},
		}, &result)
		if err == nil && status == http.StatusNotFound {
			// A tenant without any detected anomalies has no index yet. Treat that
			// initial state as an empty result instead of failing the whole overview.
			return 0, nil
		}
		if err != nil || status != http.StatusOK {
			return 0, fmt.Errorf("elasticsearch count failed")
		}
		return result.Count, nil
	}
	open, err := count([]any{map[string]any{"term": map[string]any{"anomaly.status": "open"}}})
	if err != nil {
		return 0, 0, nil, err
	}
	high, err := count([]any{
		map[string]any{"term": map[string]any{"anomaly.status": "open"}},
		map[string]any{"terms": map[string]any{"anomaly.severity": []string{"high", "critical"}}},
	})
	if err != nil {
		return 0, 0, nil, err
	}
	var latestResult es.SearchResult
	status, err := s.ES.Do(ctx, http.MethodPost, "/ueba-anomalies-"+principal.Namespace+"/_search", map[string]any{
		"size":             1,
		"sort":             []any{map[string]any{"@timestamp": "desc"}},
		"_source":          []string{"@timestamp"},
		"query":            map[string]any{"term": map[string]any{"organization.id": principal.Organization}},
		"track_total_hits": false,
	}, &latestResult)
	latestSearchFailed := err != nil || status != http.StatusOK
	if status == http.StatusNotFound {
		// No v1 index yet: v1 contributes nothing, but v2 findings may still
		// exist, so fall through to the v2 merge instead of returning early.
		latestResult = es.SearchResult{}
		latestSearchFailed = false
	}
	if latestSearchFailed {
		return 0, 0, nil, fmt.Errorf("elasticsearch search failed")
	}
	var latest *time.Time
	if len(latestResult.Hits.Hits) > 0 {
		var source struct {
			Timestamp time.Time `json:"@timestamp"`
		}
		if json.Unmarshal(latestResult.Hits.Hits[0].Source, &source) == nil && !source.Timestamp.IsZero() {
			latest = &source.Timestamp
		}
	}
	if s.OrgIDs != nil {
		orgUUID, err := s.OrgIDs.Resolve(ctx, principal.Organization)
		if err != nil {
			return 0, 0, nil, err
		}
		// v2 envelope fields cannot filter status/severity in Elasticsearch
		// (document is enabled:false), so a bounded page is classified in
		// memory, mirroring the /anomalies list merge. A full page means more
		// v2 findings exist; counts stay a lower bound in that case.
		v2Items, _, _, err := s.searchV2Anomalies(ctx, principal, orgUUID, 1000, "", "", "", time.Time{}, time.Now().UTC())
		if err != nil {
			return 0, 0, nil, err
		}
		for _, item := range v2Items {
			if item["status"] != "open" {
				continue
			}
			open++
			if sev, _ := item["severity"].(string); sev == "high" || sev == "critical" {
				high++
			}
			if ts, _ := item["timestamp"].(string); ts != "" {
				if t, err := time.Parse(time.RFC3339, ts); err == nil && (latest == nil || t.After(*latest)) {
					latest = &t
				}
			}
		}
	}
	return open, high, latest, nil
}

type dependencyStatus struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Detail    string `json:"detail"`
	LatencyMS int64  `json:"latency_ms,omitempty"`
}

func (s Server) operations(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	checkedAt := time.Now().UTC()
	startedAt := s.StartedAt
	if startedAt.IsZero() {
		startedAt = checkedAt
	}
	dependencies := []dependencyStatus{{
		Name:   "api",
		Status: "ok",
		Detail: "HTTP API 可访问",
	}}
	overall := "ok"

	if s.Control != nil {
		started := time.Now()
		err := func() error {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			_, err := s.Control.Ping(ctx)
			return err
		}()
		dependency := dependencyStatus{Name: "postgresql", Status: "ok", Detail: "控制面连接正常", LatencyMS: time.Since(started).Milliseconds()}
		if err != nil {
			dependency.Status = "unavailable"
			dependency.Detail = "控制面连接失败"
			overall = "degraded"
		}
		dependencies = append(dependencies, dependency)
	} else {
		dependencies = append(dependencies, dependencyStatus{Name: "postgresql", Status: "unknown", Detail: "未配置控制面"})
	}

	var health struct {
		Status           string `json:"status"`
		NumberOfNodes    int    `json:"number_of_nodes"`
		ActiveShards     int    `json:"active_shards"`
		UnassignedShards int    `json:"unassigned_shards"`
	}
	started := time.Now()
	status, err := s.ES.Do(r.Context(), http.MethodGet, "/_cluster/health", nil, &health)
	elastic := dependencyStatus{Name: "elasticsearch", Status: "ok", Detail: "集群状态 healthy", LatencyMS: time.Since(started).Milliseconds()}
	if err != nil || status != http.StatusOK {
		elastic.Status = "unavailable"
		elastic.Detail = "集群不可用"
		overall = "degraded"
	} else if health.Status != "green" {
		elastic.Status = "degraded"
		elastic.Detail = fmt.Sprintf("集群状态 %s，未分配分片 %d", health.Status, health.UnassignedShards)
		overall = "degraded"
	} else {
		elastic.Detail = fmt.Sprintf("%d 节点，%d 活跃分片", health.NumberOfNodes, health.ActiveShards)
	}
	dependencies = append(dependencies, elastic)

	_, _, latest, freshnessErr := s.anomalyOverview(r.Context(), principal)
	freshness := map[string]any{"status": "unknown", "latest_at": nil, "age_seconds": nil}
	if freshnessErr == nil && latest != nil {
		age := maxInt64(0, int64(checkedAt.Sub(*latest).Seconds()))
		status := "ok"
		if age > 300 {
			status = "degraded"
			overall = "degraded"
		}
		freshness = map[string]any{"status": status, "latest_at": latest, "age_seconds": age}
	}
	if s.Control != nil {
		runtimeStatus, runtimeErr := s.Control.AnalysisRuntime(r.Context())
		if runtimeErr == nil {
			freshness["runtime"] = runtimeStatus
		} else {
			freshness["runtime_error"] = "analysis runtime metadata unavailable"
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":           overall,
		"checked_at":       checkedAt,
		"uptime_seconds":   int64(checkedAt.Sub(startedAt).Seconds()),
		"dependencies":     dependencies,
		"analysis":         freshness,
		"kafka_configured": s.KafkaConfigured,
	})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
