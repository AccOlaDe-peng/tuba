package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
		return hit, document, http.StatusNotFound, nil
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
	if err == nil && status == http.StatusNotFound {
		return 0, 0, nil, nil
	}
	if err != nil || status != http.StatusOK {
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
