package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

func queryServer(t *testing.T, esResponse string, captured *map[string]any, capturedPath *string) Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if capturedPath != nil {
			*capturedPath = r.URL.Path
		}
		if captured != nil {
			if err := json.NewDecoder(r.Body).Decode(captured); err != nil {
				t.Error(err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, esResponse)
	}))
	t.Cleanup(server.Close)
	client, err := es.New(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	return Server{ES: client}
}

func postQuery(t *testing.T, s Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/query", bytes.NewBufferString(body))
	recorder := httptest.NewRecorder()
	s.query(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "tenant_a"})
	return recorder
}

func TestQueryEventsEndToEnd(t *testing.T) {
	var body map[string]any
	var path string
	s := queryServer(t, `{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"es-1","sort":["2026-09-26T10:00:00Z","evt-1"],"_source":{"@timestamp":"2026-09-26T10:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"evt-1","kind":"event","dataset":"authentication","original":"must-not-return"},"user":{"name":"alice"},"ueba":{"quality":{"status":"qualified"}}}}]}}`, &body, &path)
	recorder := postQuery(t, s, `{"query":"search authentication where event.outcome = \"failure\" | sort -@timestamp | head 10","from":"2026-09-25T00:00:00Z","to":"2026-09-27T00:00:00Z"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if path != "/logs-ueba.authentication-tenant_a/_search" {
		t.Fatalf("path=%q", path)
	}
	// Four enforced elements + timeout + limit.
	if body["timeout"] != "30s" || body["size"] != float64(10) {
		t.Fatalf("timeout/size: %+v", body)
	}
	filters := body["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	joined, _ := json.Marshal(filters)
	for _, want := range []string{`"organization.id":"tenant_a"`, `"ueba.route.domain":"authentication"`, `"ueba.route.generation":"g1"`, `"@timestamp"`, `"event.outcome":"failure"`} {
		if !strings.Contains(string(joined), want) {
			t.Fatalf("missing %s in %s", want, joined)
		}
	}
	var response struct {
		Mode       string           `json:"mode"`
		Items      []map[string]any `json:"items"`
		Total      int64            `json:"total"`
		NextCursor string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Mode != "events" || response.Total != 1 || len(response.Items) != 1 || response.Items[0]["id"] != "evt-1" {
		t.Fatalf("response=%+v", response)
	}
	if strings.Contains(recorder.Body.String(), "must-not-return") {
		t.Fatalf("sensitive field leaked: %s", recorder.Body.String())
	}
}

func TestQueryStatsEndToEnd(t *testing.T) {
	var body map[string]any
	s := queryServer(t, `{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]},"aggregations":{"buckets":{"after_key":{"event.outcome":"x"},"buckets":[{"key":{"event.outcome":"failure"},"doc_count":42,"count":{"value":42}}]}}}`, &body, nil)
	recorder := postQuery(t, s, `{"query":"search authentication | stats count by event.outcome"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if body["size"] != float64(0) {
		t.Fatalf("agg size: %+v", body)
	}
	comp := body["aggs"].(map[string]any)["buckets"].(map[string]any)["composite"].(map[string]any)
	if comp["size"] != float64(10000) {
		t.Fatalf("bucket budget: %+v", comp)
	}
	if !strings.Contains(recorder.Body.String(), `"mode":"stats"`) || !strings.Contains(recorder.Body.String(), `"doc_count":42`) {
		t.Fatalf("response=%s", recorder.Body.String())
	}
}

func TestQueryRejectsArbitraryDSL(t *testing.T) {
	s := queryServer(t, `{}`, nil, nil)
	cases := []string{
		// raw ES DSL as the whole body
		`{"query":{"match_all":{}}}`,
		// extra DSL-ish fields alongside the SPL text
		`{"query":"search authentication","dsl":{"query":{"match_all":{}}}}`,
		`{"query":"search authentication","query_string":"user.name:*"`,
		`{"query":"search authentication","script":"return 1"`,
		`{"query":"search authentication","aggs":{}}`,
		`{"query":"search authentication","es_query":{"match_all":{}}}`,
		// DSL / forbidden constructs inside the SPL text
		`{"query":"search authentication where user.name = {\"match_all\":{}}"}`,
		`{"query":"search authentication [search raw]"}`,
		`{"query":"search authentication where user.name = /a.*/"}`,
		`{"query":"search authentication where user.name = \"adm*\""}`,
		`{"query":"search authentication | eval x = 1"}`,
		`{"query":"search authentication | join user.name"}`,
	}
	for _, body := range cases {
		recorder := postQuery(t, s, body)
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s status=%d body=%s", body, recorder.Code, recorder.Body.String())
		}
	}
}

func TestQueryEnforcesWhitelistAndLimits(t *testing.T) {
	s := queryServer(t, `{}`, nil, nil)
	cases := map[string]string{
		// whitelist: undeclared / capability-violating fields
		`{"query":"search authentication where not.in.catalog = \"x\""}`:            "spl_field:",
		`{"query":"search authentication | stats count by event.original"}`:         "spl_field:",
		`{"query":"search authentication | stats sum(user.name) by user.name"}`:     "spl_field:",
		`{"query":"search authentication | top 5 not.in.catalog"}`:                  "spl_field:",
		`{"query":"search unknown_dataset"}`:                                        "spl_field:",
		// pagination limit
		`{"query":"search authentication","limit":1001}`:                            "limit must be 1..1000",
		// time range cap 31d
		`{"query":"search authentication","from":"2026-08-01T00:00:00Z","to":"2026-09-27T00:00:00Z"}`: "time range",
		// aggregation bucket budget
		`{"query":"search authentication | timechart span=1m count by event.outcome","from":"2026-08-27T00:00:00Z","to":"2026-09-27T00:00:00Z"}`: "budget_exceeded:",
		// cursor in agg mode
		`{"query":"search authentication | stats count by event.outcome","cursor":"W10="}`: "cursor",
	}
	for body, want := range cases {
		recorder := postQuery(t, s, body)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), want) {
			t.Errorf("%s status=%d body=%s (want %q)", body, recorder.Code, recorder.Body.String(), want)
		}
	}
}

func TestQueryDefaultsAndHead(t *testing.T) {
	var body map[string]any
	s := queryServer(t, `{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`, &body, nil)
	recorder := postQuery(t, s, `{"query":"search authentication"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if body["size"] != float64(100) {
		t.Fatalf("default limit: %+v", body)
	}
	recorder = postQuery(t, s, `{"query":"search authentication | head 500","limit":50}`)
	if body["size"] != float64(50) {
		t.Fatalf("explicit limit wins over head: %+v", body)
	}
}
