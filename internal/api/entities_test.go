package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/entity"
	"tuba/product/internal/es"
)

var testEntityID = "ent:" + strings.Repeat("a", 64)

func TestEntityEndpointsFailClosedWithoutStore(t *testing.T) {
	s := Server{}
	for _, path := range []string{
		"/api/v1/entities",
		"/api/v1/entities/" + testEntityID,
		"/api/v1/entities/" + testEntityID + "/attributions",
		"/api/v1/entities/" + testEntityID + "/relations",
		"/api/v1/entities/" + testEntityID + "/features",
		"/api/v1/entities/" + testEntityID + "/baseline",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.SetPathValue("id", testEntityID)
		w := httptest.NewRecorder()
		switch {
		case strings.HasSuffix(path, "/attributions"):
			s.entityAttributions(w, req, auth.Principal{})
		case strings.HasSuffix(path, "/relations"):
			s.entityRelations(w, req, auth.Principal{})
		case strings.HasSuffix(path, "/features"):
			s.entityFeatures(w, req, auth.Principal{})
		case strings.HasSuffix(path, "/baseline"):
			s.entityBaseline(w, req, auth.Principal{})
		case strings.HasSuffix(path, testEntityID):
			s.entityDetail(w, req, auth.Principal{})
		default:
			s.listEntities(w, req, auth.Principal{})
		}
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status=%d, want 503", path, w.Code)
		}
	}
}

func TestEntityIDValidation(t *testing.T) {
	s := Server{Entities: entity.NewQueries(nil)}
	for _, id := range []string{"", "x", "ent:zz", "../" + testEntityID, testEntityID + "/x", strings.ToUpper(testEntityID)} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+id, nil)
		req.SetPathValue("id", id)
		w := httptest.NewRecorder()
		s.entityDetail(w, req, auth.Principal{})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("id %q status=%d, want 400", id, w.Code)
		}
	}
}

func riskStubServer(t *testing.T, status int, body string) *es.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestEntityRiskReturnsProjection(t *testing.T) {
	client := riskStubServer(t, 200, `{"found":true,"_source":{
		"organization":{"id":"tenant_a"},
		"object":{"type":"entity_risk","id":"`+testEntityID+`","revision":3,"operation":"upsert"},
		"document":{"entity_id":"`+testEntityID+`","risk_score":12.5,"compute_version":"1.0.0","contributions":[{"contribution_id":"rc:1"}]}}}`)
	s := Server{ES: client}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	projection, ok := body["projection"].(map[string]any)
	if !ok || projection["risk_score"] != 12.5 {
		t.Fatalf("unexpected body %s", w.Body.String())
	}
	if body["revision"] != float64(3) {
		t.Fatalf("missing projection revision %s", w.Body.String())
	}
}

func TestEntityRiskNoProjectionIsNotAnError(t *testing.T) {
	client := riskStubServer(t, 404, `{"error":"index_not_found_exception"}`)
	s := Server{ES: client}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["projection"] != nil {
		t.Fatalf("expected null projection, got %s", w.Body.String())
	}
}

func TestEntityRiskRejectsCrossTenantDocument(t *testing.T) {
	client := riskStubServer(t, 200, `{"found":true,"_source":{
		"organization":{"id":"tenant_b"},
		"object":{"revision":1,"operation":"upsert"},
		"document":{"risk_score":1}}}`)
	s := Server{ES: client}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("cross-tenant document accepted: status=%d", w.Code)
	}
}

func TestEntityRiskIndexErrorFailsClosed(t *testing.T) {
	client := riskStubServer(t, 500, `{}`)
	s := Server{ES: client}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", w.Code)
	}
}

func TestEntityListLimitValidation(t *testing.T) {
	s := Server{Entities: entity.NewQueries(nil)}
	for _, raw := range []string{"0", "101", "abc"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/entities?limit="+raw, nil)
		w := httptest.NewRecorder()
		s.listEntities(w, req, auth.Principal{})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("limit=%s status=%d, want 400", raw, w.Code)
		}
	}
}
