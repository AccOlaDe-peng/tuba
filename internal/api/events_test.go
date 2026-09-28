package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/es"
)

func TestEventsScopesQueryAndOmitsVendorPayload(t *testing.T) {
	var path string
	var query map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&query); err != nil {
			t.Error(err)
		}
		_, _ = fmt.Fprint(w, `{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"es-id","sort":["2026-09-26T10:00:00Z","zeek-event-1"],"_source":{"@timestamp":"2026-09-26T10:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"zeek-event-1","kind":"event","dataset":"network","original":"must-not-return"},"vendor":{"name":"zeek","product":"zeek","dataset":"zeek.conn","payload":{"uid":"must-not-return"}},"source":{"ip":"192.0.2.10"},"destination":{"ip":"198.51.100.20"},"network":{"transport":"tcp"},"ueba":{"quality":{"status":"qualified"}},"unapproved":"must-not-return"}}]}}`)
	}))
	defer server.Close()
	client, err := es.New(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	cursor := base64.RawURLEncoding.EncodeToString([]byte(`[` + `"2026-09-25T10:00:00Z","event-0"` + `]`))
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events?domain=network&from=2026-09-25T00:00:00Z&to=2026-09-27T00:00:00Z&limit=10&cursor="+cursor, nil)
	recorder := httptest.NewRecorder()
	Server{ES: client}.events(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "tenant_a"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if path != "/logs-ueba.network-tenant_a/_search" {
		t.Fatalf("unexpected tenant alias path %q", path)
	}
	filters := query["query"].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
	if filters[0].(map[string]any)["term"].(map[string]any)["organization.id"] != "tenant_a" ||
		filters[1].(map[string]any)["term"].(map[string]any)["ueba.route.domain"] != "network" {
		t.Fatalf("missing tenant/domain filters: %+v", filters)
	}
	if len(query["search_after"].([]any)) != 2 || query["size"] != float64(10) {
		t.Fatalf("cursor or limit not applied: %+v", query)
	}
	var response struct {
		Items      []map[string]any `json:"items"`
		Total      int              `json:"total"`
		NextCursor string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Total != 1 || len(response.Items) != 1 || response.Items[0]["id"] != "zeek-event-1" {
		t.Fatalf("unexpected event page: %+v", response)
	}
	if strings.Contains(recorder.Body.String(), "must-not-return") || strings.Contains(recorder.Body.String(), "unapproved") {
		t.Fatalf("raw or unknown fields leaked: %s", recorder.Body.String())
	}
}

func TestEventsRejectsUnsafeQuery(t *testing.T) {
	client, err := es.New("http://127.0.0.1:9200", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/api/v1/events?domain=network*",
		"/api/v1/events?domain=network&limit=101",
		"/api/v1/events?domain=network&from=2026-09-27T00:00:00Z&to=2026-09-26T00:00:00Z",
		"/api/v1/events?domain=network&cursor=not-valid",
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		Server{ES: client}.events(recorder, request, auth.Principal{Organization: "tenant_a", Namespace: "tenant_a"})
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
}
