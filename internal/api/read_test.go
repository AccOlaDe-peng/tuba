package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

func testAnomalyResponse() string {
	return `{
		"_id":"anom-1",
		"_source":{
			"@timestamp":"2026-09-24T10:42:16Z",
			"organization":{"id":"tenant_a"},
			"entity":{"id":"zhang.wei","type":"account"},
			"anomaly":{"id":"anom-1","type":"auth.failure-then-success","severity":"high","status":"open","score":0.98},
			"evidence":{"event_ids":["event-2","event-1"],"count":2},
			"detection":{"rule_id":"auth.failure-then-success","rule_version":"1.0.0"},
			"explanation":{"reason_codes":["AUTH_FAILURE_BURST_THEN_SUCCESS"],"summary":"5 failures followed by success"}
		}
	}`
}

func TestAnomalyListReturnsNormalizedSummary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[` + testAnomalyResponse() + `]}}`))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	handler := Server{ES: client}.list("anomalies")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies?severity=high&status=open", nil)
	recorder := httptest.NewRecorder()
	handler(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Items []map[string]any `json:"items"`
		Total int64            `json:"total"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || response.Items[0]["severity"] != "high" || response.Items[0]["type"] != "auth.failure-then-success" {
		t.Fatalf("unexpected response: %+v", response)
	}
	if response.Items[0]["entity"].(map[string]any)["id"] != "zhang.wei" {
		t.Fatal("entity was not normalized")
	}
}

func TestAnomalyEvidenceScopesTenantAndEventIDs(t *testing.T) {
	var searchQuery map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/ueba-anomalies-ns_a/_doc/"):
			_, _ = w.Write([]byte(testAnomalyResponse()))
		case r.URL.Path == "/logs-ueba.authentication-*/_search":
			if err := json.NewDecoder(r.Body).Decode(&searchQuery); err != nil {
				t.Error(err)
			}
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"event-1","_source":{"@timestamp":"2026-09-24T10:24:03Z","organization":{"id":"tenant_a"},"event":{"id":"event-1","action":"logon","outcome":"failure"},"user":{"id":"zhang.wei"},"ueba":{"quality":{"status":"qualified"}},"secret":"must-not-leak"}}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies/anom-1/evidence", nil)
	request.SetPathValue("id", "anom-1")
	recorder := httptest.NewRecorder()
	Server{ES: client}.anomalyEvidence(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	filters := searchQuery["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	tenantFilter := filters[0].(map[string]any)["term"].(map[string]any)
	if tenantFilter["organization.id"] != "tenant_a" {
		t.Fatal("missing tenant filter")
	}
	terms := filters[1].(map[string]any)["terms"].(map[string]any)["event.id"].([]any)
	if len(terms) != 2 {
		t.Fatalf("unexpected evidence IDs: %+v", terms)
	}
	if strings.Contains(recorder.Body.String(), "must-not-leak") {
		t.Fatal("unapproved source field leaked")
	}
}

func TestAnomalyDetailRejectsCrossTenantDocument(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := strings.Replace(testAnomalyResponse(), `"id":"tenant_a"`, `"id":"tenant_b"`, 1)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies/anom-1", nil)
	request.SetPathValue("id", "anom-1")
	recorder := httptest.NewRecorder()
	Server{ES: client}.anomalyDetail(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestOperationsReportsDependenciesAndFreshness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_cluster/health":
			_, _ = w.Write([]byte(`{"status":"green","number_of_nodes":3,"active_shards":12,"unassigned_shards":0}`))
		case "/ueba-anomalies-ns_a/_count":
			_, _ = w.Write([]byte(`{"count":4}`))
		case "/ueba-anomalies-ns_a/_search":
			latest := time.Now().UTC().Add(-30 * time.Second).Format(time.RFC3339)
			_, _ = fmt.Fprintf(w, `{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"anom-1","_source":{"@timestamp":"%s"}}]}}`, latest)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/operations/status", nil)
	recorder := httptest.NewRecorder()
	Server{ES: client, StartedAt: time.Now().Add(-2 * time.Hour)}.operations(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Status       string `json:"status"`
		Dependencies []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"dependencies"`
		Analysis struct {
			Status string `json:"status"`
		} `json:"analysis"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != "ok" || response.Analysis.Status != "ok" {
		t.Fatalf("unexpected operations response: %+v", response)
	}
	foundES := false
	for _, dependency := range response.Dependencies {
		if dependency.Name == "elasticsearch" && dependency.Status == "ok" {
			foundES = true
		}
	}
	if !foundES {
		t.Fatal("elasticsearch dependency missing")
	}
}
