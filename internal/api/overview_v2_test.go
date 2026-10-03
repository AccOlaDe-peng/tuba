package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

// TestAnomalyOverviewMergesV2Counts: the overview open/high counts and latest
// timestamp must include v2 analysis findings (organization UUID scope,
// upsert-only), not just the v1 index. status/severity are classified in
// memory because the v2 document field is not indexed.
func TestAnomalyOverviewMergesV2Counts(t *testing.T) {
	v2Hit := func(id, status, severity, ts string) string {
		inner := `{"@timestamp":"` + ts + `","organization":{"id":"` + testOrgUUID + `"},` +
			`"entity":{"id":"ent-1","type":"account"},` +
			`"anomaly":{"id":"` + id + `","type":"auth.failure-burst","severity":"` + severity + `","status":"` + status + `","score":0.9},` +
			`"evidence":{"event_ids":[],"count":0},` +
			`"detection":{"rule_id":"auth.failure-burst","rule_version":"1.0.0"},` +
			`"explanation":{"reason_codes":["AUTH_FAILURE_BURST"],"summary":"s"}}`
		return `{"_id":"` + id + `","_source":` + v2Envelope(id, 1, "upsert", ts, inner) + `}`
	}
	var sawV2OrgTerm bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ueba-anomalies-ns_a/_count":
			var query map[string]any
			_ = json.NewDecoder(r.Body).Decode(&query)
			body, _ := json.Marshal(query)
			if strings.Contains(string(body), `"high"`) {
				_, _ = w.Write([]byte(`{"count":1}`))
				return
			}
			_, _ = w.Write([]byte(`{"count":2}`))
		case "/ueba-anomalies-ns_a/_search":
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"v1-1","_source":{"@timestamp":"2026-10-10T00:00:00Z"}}]}}`))
		case "/ueba-analysis-anomaly-ns_a/_search":
			var query map[string]any
			_ = json.NewDecoder(r.Body).Decode(&query)
			body, _ := json.Marshal(query)
			sawV2OrgTerm = strings.Contains(string(body), testOrgUUID)
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":2,"relation":"eq"},"hits":[` +
				v2Hit("anom:v2-open-high", "open", "high", "2026-10-11T14:05:00Z") + "," +
				v2Hit("anom:v2-closed", "closed", "critical", "2026-10-11T15:05:00Z") + `]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client, OrgIDs: v2Resolver()}
	principal := auth.Principal{Organization: "tenant_a", Namespace: "ns_a"}

	open, high, latest, err := s.anomalyOverview(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	if !sawV2OrgTerm {
		t.Fatal("v2 overview query must filter by organization UUID")
	}
	if open != 3 {
		t.Fatalf("open = %d, want 3 (2 v1 + 1 v2 open)", open)
	}
	if high != 2 {
		t.Fatalf("high = %d, want 2 (1 v1 + 1 v2 open high; closed v2 critical excluded)", high)
	}
	if latest == nil || latest.Format("2006-01-02T15:04:05Z") != "2026-10-11T14:05:00Z" {
		t.Fatalf("latest = %v, want v2 open finding timestamp", latest)
	}
}

// TestAnomalyOverviewV2WithoutV1Index: a tenant with no v1 anomaly index at
// all must still report its v2 findings instead of an empty overview.
func TestAnomalyOverviewV2WithoutV1Index(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ueba-analysis-anomaly-ns_a/_search" {
			inner := `{"@timestamp":"2026-10-11T14:05:00Z","organization":{"id":"` + testOrgUUID + `"},` +
				`"entity":{"id":"ent-1","type":"account"},` +
				`"anomaly":{"id":"anom:v2-only","type":"auth.failure-burst","severity":"medium","status":"open","score":0.5},` +
				`"evidence":{"event_ids":[],"count":0},` +
				`"detection":{"rule_id":"auth.failure-burst","rule_version":"1.0.0"},` +
				`"explanation":{"reason_codes":["AUTH_FAILURE_BURST"],"summary":"s"}}`
			_, _ = w.Write([]byte(`{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"anom:v2-only","_source":` +
				v2Envelope("anom:v2-only", 1, "upsert", "2026-10-11T14:05:00Z", inner) + `}]}}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	client, err := es.New(server.URL, "key")
	if err != nil {
		t.Fatal(err)
	}
	s := Server{ES: client, OrgIDs: v2Resolver()}
	principal := auth.Principal{Organization: "tenant_a", Namespace: "ns_a"}

	open, high, latest, err := s.anomalyOverview(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	if open != 1 || high != 0 {
		t.Fatalf("open=%d high=%d, want 1/0", open, high)
	}
	if latest == nil {
		t.Fatal("latest must come from the v2 finding when no v1 index exists")
	}
}
