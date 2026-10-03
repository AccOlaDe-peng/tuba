package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

const testOrgUUID = "d9e836f8-3f52-49dd-9c15-32bad37a5bef"

// v2Envelope renders one F07 state-projection document around a finding
// payload (the v1-shaped anomaly document stored as the opaque document field).
func v2Envelope(objectID string, revision int, operation string, timestamp string, inner string) string {
	return `{
		"@timestamp":"` + timestamp + `",
		"organization":{"id":"` + testOrgUUID + `"},
		"namespace":"ns_a",
		"object":{"type":"anomaly","id":"` + objectID + `","revision":` + itoa(revision) + `,"operation":"` + operation + `","generation":"g1"},
		"rule":{"id":"auth.failure-burst","version":"1.0.0"},
		"run_id":"run-1",
		"window_start":"` + timestamp + `",
		"date_key":"2026-10-11",
		"input_refs":[],
		"document":` + inner + `
	}`
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

const v2FindingInner = `{
	"@timestamp":"2026-10-11T14:05:00Z",
	"organization":{"id":"` + testOrgUUID + `"},
	"entity":{"id":"tuba-v09-probe","type":"account"},
	"anomaly":{"id":"anom:v2-1","type":"auth.failure-burst","severity":"high","status":"open","score":0.9},
	"evidence":{"event_ids":["event-9","event-8"],"count":2},
	"detection":{"rule_id":"auth.failure-burst","rule_version":"1.0.0"},
	"explanation":{"reason_codes":["AUTH_FAILURE_BURST"],"summary":"7 failures in 10 minutes"}
}`

func v2TestServer(t *testing.T, v1Search string, v2Search string) *es.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ueba-anomalies-ns_a/_search":
			_, _ = w.Write([]byte(v1Search))
		case "/ueba-analysis-anomaly-ns_a/_search":
			_, _ = w.Write([]byte(v2Search))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func v2Resolver() *OrgResolver {
	return NewOrgResolver(&fakeOrgSource{id: testOrgUUID})
}

func TestAnomalyListMergesV2FindingsAndScopesTenant(t *testing.T) {
	var v2Query map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ueba-anomalies-ns_a/_search":
			// v1 holds a mirror of the same finding (same anomaly.id, older
			// timestamp shape) plus one v1-only finding.
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":2,"relation":"eq"},"hits":[` +
				`{"_id":"legacy-1","_source":{"@timestamp":"2026-10-11T13:00:00Z","organization":{"id":"tenant_a"},"entity":{"id":"li.na","type":"account"},"anomaly":{"id":"anom:v1-1","type":"auth.failure-then-success","severity":"medium","status":"open","score":0.5},"evidence":{"event_ids":["event-1"],"count":1},"detection":{"rule_id":"auth.failure-then-success","rule_version":"1.0.0"},"explanation":{"reason_codes":["X"],"summary":"v1 only"}}},` +
				`{"_id":"legacy-mirror","_source":{"@timestamp":"2026-10-11T14:05:00Z","organization":{"id":"tenant_a"},"entity":{"id":"tuba-v09-probe","type":"account"},"anomaly":{"id":"anom:v2-1","type":"auth.failure-burst","severity":"high","status":"open","score":0.9},"evidence":{"event_ids":["event-9"],"count":1},"detection":{"rule_id":"auth.failure-burst","rule_version":"1.0.0"},"explanation":{"reason_codes":["AUTH_FAILURE_BURST"],"summary":"mirror"}}}` +
				`]}}`))
		case "/ueba-analysis-anomaly-ns_a/_search":
			if err := json.NewDecoder(r.Body).Decode(&v2Query); err != nil {
				t.Error(err)
			}
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"anom:v2-1","_source":` +
				v2Envelope("anom:v2-1", 3, "upsert", "2026-10-11T14:05:00Z", v2FindingInner) + `}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	handler := Server{ES: client, OrgIDs: v2Resolver()}.list("anomalies")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies", nil)
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
	if len(response.Items) != 2 {
		t.Fatalf("expected 2 deduplicated items, got %d: %s", len(response.Items), recorder.Body.String())
	}
	// Newest first: the v2 finding (14:05) before the v1-only finding (13:00).
	if response.Items[0]["id"] != "anom:v2-1" || response.Items[1]["id"] != "anom:v1-1" {
		t.Fatalf("unexpected order/dedup: %s", recorder.Body.String())
	}
	if response.Total != 3 {
		t.Fatalf("total should sum both sources, got %d", response.Total)
	}
	// The v2 query must carry the organization UUID term, never the slug.
	filters := v2Query["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	orgTerm := filters[0].(map[string]any)["term"].(map[string]any)["organization.id"]
	if orgTerm != testOrgUUID {
		t.Fatalf("v2 query missing UUID tenant filter: %+v", filters[0])
	}
	// The duplicated id appears exactly once regardless of which mirror wins.
	body := recorder.Body.String()
	if strings.Count(body, "anom:v2-1") != 1 {
		t.Fatalf("duplicated anomaly id in merged list: %s", body)
	}
	if strings.Contains(body, "mirror") == strings.Contains(body, "7 failures") {
		t.Fatalf("exactly one mirror of the duplicated finding may survive: %s", body)
	}
}

func TestAnomalyListV2InMemoryFilters(t *testing.T) {
	client := v2TestServer(t,
		`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`,
		`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"anom:v2-1","_source":`+v2Envelope("anom:v2-1", 1, "upsert", "2026-10-11T14:05:00Z", v2FindingInner)+`}]}}`)
	handler := Server{ES: client, OrgIDs: v2Resolver()}.list("anomalies")
	// severity matches
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies?severity=high", nil)
	recorder := httptest.NewRecorder()
	handler(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "anom:v2-1") {
		t.Fatalf("severity=high should keep v2 finding: %d %s", recorder.Code, recorder.Body.String())
	}
	// severity mismatch filters the v2 finding out
	request = httptest.NewRequest(http.MethodGet, "/api/v1/anomalies?severity=low", nil)
	recorder = httptest.NewRecorder()
	handler(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "anom:v2-1") {
		t.Fatalf("severity=low should drop v2 finding: %d %s", recorder.Code, recorder.Body.String())
	}
	// entity mismatch filters the v2 finding out
	request = httptest.NewRequest(http.MethodGet, "/api/v1/anomalies?entity=someone-else", nil)
	recorder = httptest.NewRecorder()
	handler(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if strings.Contains(recorder.Body.String(), "anom:v2-1") {
		t.Fatalf("entity filter should drop v2 finding: %s", recorder.Body.String())
	}
}

func TestAnomalyListResolverFailureFailsClosed(t *testing.T) {
	client := v2TestServer(t,
		`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`,
		`{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`)
	resolver := NewOrgResolver(&fakeOrgSource{err: errors.New("pg down")})
	handler := Server{ES: client, OrgIDs: resolver}.list("anomalies")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies", nil)
	recorder := httptest.NewRecorder()
	handler(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("resolver failure must fail closed, got %d", recorder.Code)
	}
}

func TestAnomalyListWithoutV1IndexStillReturnsV2(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ueba-anomalies-ns_a/_search":
			http.NotFound(w, r)
		case "/ueba-analysis-anomaly-ns_a/_search":
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"anom:v2-1","_source":` + v2Envelope("anom:v2-1", 1, "upsert", "2026-10-11T14:05:00Z", v2FindingInner) + `}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	handler := Server{ES: client, OrgIDs: v2Resolver()}.list("anomalies")
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies", nil)
	recorder := httptest.NewRecorder()
	handler(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "anom:v2-1") {
		t.Fatalf("missing v1 index must not hide v2 findings: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestAnomalyDetailFallsBackToV2Projection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ueba-analysis-anomaly-ns_a/_doc/anom:v2-1":
			_, _ = w.Write([]byte(`{"found":true,"_source":` + v2Envelope("anom:v2-1", 2, "upsert", "2026-10-11T14:05:00Z", v2FindingInner) + `}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client, OrgIDs: v2Resolver()}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies/anom:v2-1", nil)
	request.SetPathValue("id", "anom:v2-1")
	recorder := httptest.NewRecorder()
	s.anomalyDetail(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["id"] != "anom:v2-1" || body["rule_id"] != "auth.failure-burst" || body["summary"] != "7 failures in 10 minutes" {
		t.Fatalf("unexpected detail body: %s", recorder.Body.String())
	}
	if codes, ok := body["reason_codes"].([]any); !ok || len(codes) != 1 {
		t.Fatalf("missing five-element reason codes: %s", recorder.Body.String())
	}
	if ids, ok := body["evidence_event_ids"].([]any); !ok || len(ids) != 2 {
		t.Fatalf("missing evidence event ids: %s", recorder.Body.String())
	}
}

func TestAnomalyDetailV2RejectsCrossTenantAndRetracted(t *testing.T) {
	crossTenant := strings.Replace(v2Envelope("anom:v2-1", 1, "upsert", "2026-10-11T14:05:00Z", v2FindingInner), testOrgUUID, "00000000-0000-0000-0000-000000000000", 1)
	retracted := v2Envelope("anom:v2-1", 2, "retracted", "2026-10-11T14:05:00Z", v2FindingInner)
	for name, source := range map[string]string{"cross-tenant": crossTenant, "retracted": retracted} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ueba-analysis-anomaly-ns_a/_doc/anom:v2-1" {
					_, _ = w.Write([]byte(`{"found":true,"_source":` + source + `}`))
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			client, err := es.New(server.URL, "key")
			if err != nil {
				t.Fatal(err)
			}
			s := Server{ES: client, OrgIDs: v2Resolver()}
			request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies/anom:v2-1", nil)
			request.SetPathValue("id", "anom:v2-1")
			recorder := httptest.NewRecorder()
			s.anomalyDetail(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("%s projection must read as not found, got %d", name, recorder.Code)
			}
		})
	}
}

func TestAnomalyEvidenceReadsV2Projection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ueba-analysis-anomaly-ns_a/_doc/anom:v2-1":
			_, _ = w.Write([]byte(`{"found":true,"_source":` + v2Envelope("anom:v2-1", 1, "upsert", "2026-10-11T14:05:00Z", v2FindingInner) + `}`))
		case "/logs-ueba.authentication-*/_search":
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"event-9","_source":{"@timestamp":"2026-10-11T14:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"event-9","outcome":"failure"},"user":{"id":"tuba-v09-probe"}}}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client, OrgIDs: v2Resolver()}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/anomalies/anom:v2-1/evidence", nil)
	request.SetPathValue("id", "anom:v2-1")
	recorder := httptest.NewRecorder()
	s.anomalyEvidence(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "event-9") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestEntityRiskValidatesOrganizationUUID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"found":true,"_source":{
			"organization":{"id":"` + testOrgUUID + `"},
			"object":{"type":"entity_risk","id":"` + testEntityID + `","revision":4,"operation":"upsert"},
			"document":{"entity_id":"` + testEntityID + `","risk_score":2.997,"compute_version":"1.0.0"}}}`))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client, OrgIDs: v2Resolver()}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	projection, _ := body["projection"].(map[string]any)
	if projection["risk_score"] != 2.997 {
		t.Fatalf("unexpected body %s", w.Body.String())
	}
}

func TestEntityRiskUUIDMismatchFailsClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"found":true,"_source":{
			"organization":{"id":"00000000-0000-0000-0000-000000000000"},
			"object":{"revision":1,"operation":"upsert"},
			"document":{"risk_score":1}}}`))
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client, OrgIDs: v2Resolver()}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("cross-tenant UUID accepted: status=%d", w.Code)
	}
}

func TestEntityRiskResolverFailureFailsClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("ES must not be queried when org resolution fails")
	}))
	defer server.Close()
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewOrgResolver(&fakeOrgSource{err: errors.New("pg down")})
	s := Server{ES: client, OrgIDs: resolver}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/entities/"+testEntityID+"/risk", nil)
	req.SetPathValue("id", testEntityID)
	w := httptest.NewRecorder()
	s.entityRisk(w, req, auth.Principal{Organization: "tenant_a", Namespace: "ns_a"})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", w.Code)
	}
}
