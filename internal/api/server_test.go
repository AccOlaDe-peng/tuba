package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

func TestReadinessRequiresDatabaseAndElasticsearch(t *testing.T) {
	response := httptest.NewRecorder()
	Server{}.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestListInjectsTenantScope(t *testing.T) {
	var path string
	var query map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Error(err)
		}
		_, _ = w.Write([]byte(`{"hits":{"hits":[]}}`))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client}
	req := httptest.NewRequest("GET", "/api/v1/cases?limit=5", nil)
	w := httptest.NewRecorder()
	s.list("cases")(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if path != "/ueba-cases-ns_a/_search" {
		t.Fatal("wrong index", path)
	}
	filters := query["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	filter := filters[0].(map[string]any)["term"].(map[string]any)
	if filter["organization.id"] != "tenant_a" {
		t.Fatal("missing tenant filter")
	}
}

func TestCaseActionAuditedWithVersionCheck(t *testing.T) {
	var putPath string
	var saved map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(`{"_id":"c1","_seq_no":3,"_primary_term":1,"_source":{"organization":{"id":"tenant_a"},"case":{"id":"c1","status":"open"},"version":1,"timeline":[]}}`))
			return
		}
		putPath = r.URL.String()
		_ = json.NewDecoder(r.Body).Decode(&saved)
		_, _ = w.Write([]byte(`{"result":"updated"}`))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client}
	req := httptest.NewRequest("POST", "/api/v1/cases/c1/actions", strings.NewReader(`{"status":"in_progress","reason":"investigating"}`))
	req.SetPathValue("id", "c1")
	w := httptest.NewRecorder()
	s.caseAction(w, req, auth.Principal{Subject: "analyst-1", Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(putPath, "if_seq_no=3") || !strings.Contains(putPath, "if_primary_term=1") {
		t.Fatal("missing optimistic version", putPath)
	}
	timeline := saved["timeline"].([]any)
	if timeline[0].(map[string]any)["actor"] != "analyst-1" {
		t.Fatal("missing actor audit")
	}
}

func TestInvalidTransition(t *testing.T) {
	doc := map[string]any{"case": map[string]any{"status": "closed"}}
	if err := applyAction(doc, "actor", CaseAction{Status: "in_progress"}); err == nil {
		t.Fatal("invalid transition accepted")
	}
}
