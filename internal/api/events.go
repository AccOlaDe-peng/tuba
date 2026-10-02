package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

// eventDomains 保留供其他调用方做快速域判定；查询路径的权威判定是
// catalog 注册表（findDataset），二者由 catalog_test.go 保持一致。
var eventDomains = map[string]bool{
	"authentication": true, "session": true, "iam": true, "directory": true,
	"network": true, "dns": true, "web": true, "tls": true,
}

func (s Server) events(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	domain := r.URL.Query().Get("domain")
	dataset, ok := findDataset(domain)
	if !ok || dataset.Kind != "uim-domain" {
		http.Error(w, "domain must be one of the supported event domains", http.StatusBadRequest)
		return
	}
	// Q01 质量条件：可声明 quality 过滤，取值必须在 catalog 声明的集合内。
	quality := r.URL.Query().Get("quality")
	if quality != "" && !qualityStatusAllowed(dataset, quality) {
		http.Error(w, "quality must be one of the dataset's declared quality statuses", http.StatusBadRequest)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := parseBoundedInt(raw, 1, 100)
		if err != nil {
			http.Error(w, "limit must be 1..100", http.StatusBadRequest)
			return
		}
		limit = n
	}

	now := time.Now().UTC()
	to := now
	if raw := r.URL.Query().Get("to"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			http.Error(w, "invalid to", http.StatusBadRequest)
			return
		}
		to = parsed.UTC()
	}
	from := to.Add(-24 * time.Hour)
	if raw := r.URL.Query().Get("from"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			http.Error(w, "invalid from", http.StatusBadRequest)
			return
		}
		from = parsed.UTC()
	}
	if !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
		http.Error(w, "time range must be within 31 days", http.StatusBadRequest)
		return
	}

	query := map[string]any{
		"size":             limit,
		"track_total_hits": true,
		"sort":             []any{map[string]any{"@timestamp": "desc"}, map[string]any{"event.id": "asc"}},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"organization.id": principal.Organization}},
			map[string]any{"term": map[string]any{"ueba.route.domain": domain}},
			// Q01 强制 active generation：只查目录声明的当前代次，旧代次数据
			// 不进入查询视图。
			map[string]any{"term": map[string]any{"ueba.route.generation": dataset.ActiveGeneration}},
			map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": from.Format(time.RFC3339Nano), "lte": to.Format(time.RFC3339Nano)}}},
		}}},
	}
	if quality != "" {
		filters := query["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
		query["query"].(map[string]any)["bool"].(map[string]any)["filter"] = append(filters,
			map[string]any{"term": map[string]any{"ueba.quality.status": quality}})
	}
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		if len(cursor) > 2048 {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		var sort []any
		if err != nil || json.Unmarshal(decoded, &sort) != nil || len(sort) != 2 {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		query["search_after"] = sort
	}

	var result es.SearchResult
	path := "/logs-ueba." + domain + "-" + principal.Namespace + "/_search"
	status, err := s.ES.Do(r.Context(), http.MethodPost, path, query, &result)
	if err != nil || status != http.StatusOK {
		http.Error(w, "event store unavailable", http.StatusServiceUnavailable)
		return
	}
	items := make([]map[string]any, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		var source map[string]any
		if json.Unmarshal(hit.Source, &source) != nil {
			http.Error(w, "invalid stored event", http.StatusServiceUnavailable)
			return
		}
		item, ok := publicEvent(source, principal.Organization, hit.ID)
		if ok {
			items = append(items, item)
		}
	}
	next := ""
	if len(result.Hits.Hits) == limit {
		last := result.Hits.Hits[len(result.Hits.Hits)-1]
		if len(last.Sort) == 2 {
			if encoded, err := json.Marshal(last.Sort); err == nil {
				next = base64.RawURLEncoding.EncodeToString(encoded)
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next, "total": result.Hits.Total.Value})
}

func parseBoundedInt(value string, min, max int) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("integer must be between %d and %d", min, max)
	}
	return n, nil
}

// publicEvent returns the approved UIM fields. Raw vendor payload remains available
// only through the separately authorized raw-evidence flow.
func publicEvent(source map[string]any, organizationID, fallbackID string) (map[string]any, bool) {
	organization := eventObject(source["organization"])
	if eventString(organization["id"]) != organizationID {
		return nil, false
	}
	event := eventObject(source["event"])
	id := eventString(event["id"])
	if id == "" {
		id = fallbackID
	}
	if id == "" {
		return nil, false
	}
	vendor := eventObject(source["vendor"])
	publicVendor := map[string]any{}
	for _, key := range []string{"name", "product", "dataset"} {
		if value, ok := vendor[key]; ok {
			publicVendor[key] = value
		}
	}
	item := map[string]any{"id": id, "@timestamp": source["@timestamp"], "organization": map[string]any{"id": organizationID}, "event": event}
	if len(publicVendor) > 0 {
		item["vendor"] = publicVendor
	}
	for _, key := range []string{"user", "group", "host", "source", "destination", "network", "dns", "http", "url", "tls", "ueba"} {
		if value, ok := source[key]; ok {
			item[key] = value
		}
	}
	// Keep query parameters out of response values if a malformed document contains them.
	if value, ok := item["event"].(map[string]any); ok {
		delete(value, "original")
	}
	return item, true
}

func eventObject(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func eventString(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
