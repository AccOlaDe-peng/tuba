package sink

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"tuba/product/internal/rawevent"
)

func TestPutRawMappingIncludesEveryTrustedEnvelopeField(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tuba-v1-raw-") && !strings.HasSuffix(r.URL.Path, "/_mapping"):
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `"source_context_id":{"type":"keyword"}`) || !strings.Contains(string(body), `"delivery_position":{"type":"keyword"}`) {
				t.Errorf("raw mapping omits trusted fields: %s", body)
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
			io.WriteString(w, `{"acknowledged":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			lines := strings.Split(strings.TrimSpace(readBody(t, r)), "\n")
			var document map[string]any
			if err := json.Unmarshal([]byte(lines[1]), &document); err != nil {
				t.Fatal(err)
			}
			if _, exists := document["delivery_position"]; exists {
				t.Error("optional delivery_position should remain in receipt/Kafka metadata, not strict-mapped Raw ES documents")
			}
			io.WriteString(w, `{"items":[{"create":{"status":201}}]}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	event := rawevent.Envelope{
		RawEventID:       "raw-test-id",
		DeliveryPosition: "kafka-v1:topic:0:42",
		ReceivedAt:       time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC),
		PayloadHash:      "payload-hash",
		Payload:          []byte(`{"ts":"2026-09-26T00:00:00Z"}`),
	}
	if err := New(server.URL, "limited", "tenant_a").PutRaw(context.Background(), event); err != nil {
		t.Fatal(err)
	}
}

func TestPutRawDuplicateFromDifferentDeliveryOffsetKeepsStableDocumentID(t *testing.T) {
	bulkCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tuba-v1-raw-"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
			io.WriteString(w, `{"acknowledged":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkCalls++
			lines := strings.Split(strings.TrimSpace(readBody(t, r)), "\n")
			var metadata map[string]map[string]string
			if err := json.Unmarshal([]byte(lines[0]), &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata["create"]["_id"] != "raw:stable-source-record" {
				t.Errorf("Raw document ID changed across Kafka redelivery: %s", lines[0])
			}
			if bulkCalls == 1 {
				io.WriteString(w, `{"items":[{"create":{"status":201}}]}`)
			} else {
				io.WriteString(w, `{"items":[{"create":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"document already exists"}}}]}`)
			}
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_doc/raw:stable-source-record"):
			io.WriteString(w, `{"_source":{"payload_hash":"same-source-payload"}}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	indexer := New(server.URL, "limited", "tenant_a")
	event := rawevent.Envelope{
		RawEventID: "raw:stable-source-record", SourcePosition: "filebeat-v1:2053:8926348:10118640",
		DeliveryPosition: "kafka-v1:topic:0:42", ReceivedAt: time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC),
		PayloadHash: "same-source-payload", Payload: []byte(`{"message":"zeek record"}`),
	}
	if err := indexer.PutRaw(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	event.DeliveryPosition = "kafka-v1:topic:0:43"
	if err := indexer.PutRaw(context.Background(), event); err != nil {
		t.Fatalf("same source record redelivered at a new Kafka offset should verify as idempotent: %v", err)
	}
	if bulkCalls != 2 {
		t.Fatalf("bulk calls=%d, want 2", bulkCalls)
	}
}

func TestPutStandardCreatesValidStrictMappingAndIndexesEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tuba-v1-uim-network-"):
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("invalid UIM index mapping JSON: %v", err)
			}
			mappings := body["mappings"].(map[string]any)
			if mappings["dynamic"] != "strict" {
				t.Fatalf("UIM mapping should be strict: %+v", mappings)
			}
			properties := mappings["properties"].(map[string]any)
			source := properties["source"].(map[string]any)["properties"].(map[string]any)
			if source["ip"].(map[string]any)["type"] != "ip" {
				t.Fatal("UIM source.ip mapping should be IP")
			}
			aliases := body["aliases"].(map[string]any)
			if _, ok := aliases["logs-ueba.network-tenant_a"]; !ok {
				t.Fatalf("UIM read alias missing: %+v", aliases)
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
			io.WriteString(w, `{"acknowledged":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			io.WriteString(w, `{"items":[{"create":{"status":201}}]}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	event := map[string]any{
		"@timestamp":   "2026-09-26T10:00:00Z",
		"organization": map[string]any{"id": "tenant_a"},
		"event":        map[string]any{"id": "zeek-event-1", "dataset": "network"},
		"ueba":         map[string]any{"route": map[string]any{"domain": "network"}},
	}
	if err := New(server.URL, "limited", "tenant_a").PutStandard(context.Background(), event); err != nil {
		t.Fatal(err)
	}
}

func TestPutStandardRoutesAllSupportedDomains(t *testing.T) {
	domains := []string{"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"}
	created := make(map[string]bool, len(domains))
	aliased := make(map[string]bool, len(domains))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tuba-v1-uim-"):
			for _, domain := range domains {
				if strings.HasPrefix(r.URL.Path, "/tuba-v1-uim-"+domain+"-") {
					created[domain] = true
					break
				}
			}
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
			var body struct {
				Actions []map[string]map[string]string `json:"actions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode alias actions: %v", err)
			}
			for _, action := range body.Actions {
				if add, ok := action["add"]; ok {
					for _, domain := range domains {
						if add["alias"] == "logs-ueba."+domain+"-tenant_a" {
							aliased[domain] = true
						}
					}
				}
			}
			io.WriteString(w, `{"acknowledged":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			io.WriteString(w, `{"items":[{"create":{"status":201}}]}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	indexer := New(server.URL, "limited", "tenant_a")
	for _, domain := range domains {
		event := map[string]any{
			"@timestamp":   "2026-09-26T10:00:00Z",
			"organization": map[string]any{"id": "tenant_a"},
			"event":        map[string]any{"id": "event-" + domain, "dataset": domain},
			"ueba":         map[string]any{"route": map[string]any{"domain": domain}},
		}
		if err := indexer.PutStandard(context.Background(), event); err != nil {
			t.Fatalf("index domain %s: %v", domain, err)
		}
	}
	for _, domain := range domains {
		if !created[domain] || !aliased[domain] {
			t.Errorf("domain %s: physical index created=%t alias added=%t", domain, created[domain], aliased[domain])
		}
	}
}

func TestPutStandardBatchUsesBulkAndReturnsItemLevelFailures(t *testing.T) {
	bulkCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tuba-v1-uim-network-"):
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
			io.WriteString(w, `{"acknowledged":true}`)
		case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
			bulkCalls++
			lines := strings.Split(strings.TrimSpace(readBody(t, r)), "\n")
			if len(lines) != 4 {
				t.Fatalf("NDJSON lines=%d, want metadata+document for 2 events: %q", len(lines), lines)
			}
			var first map[string]map[string]string
			if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
				t.Fatal(err)
			}
			if first["create"]["_index"] != "tuba-v1-uim-network-tenant_a-g1-2026.09.26" || first["create"]["_id"] != "event-1" {
				t.Fatalf("wrong first bulk metadata: %+v", first)
			}
			io.WriteString(w, `{"items":[{"create":{"status":201}},{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad event"}}}]}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	makeEvent := func(id string) map[string]any {
		return map[string]any{
			"@timestamp":   "2026-09-26T10:00:00Z",
			"organization": map[string]any{"id": "tenant_a"},
			"event":        map[string]any{"id": id, "dataset": "network"},
			"ueba":         map[string]any{"route": map[string]any{"domain": "network"}},
		}
	}
	got, err := New(server.URL, "limited", "tenant_a").PutStandardBatch(context.Background(), []map[string]any{makeEvent("event-1"), makeEvent("event-2")})
	if err != nil {
		t.Fatal(err)
	}
	var permanent PermanentIndexError
	if len(got) != 2 || got[0] != nil || !errors.As(got[1], &permanent) || bulkCalls != 1 {
		t.Fatalf("results=%v bulk_calls=%d", got, bulkCalls)
	}
}

func TestPutStandardBatchVerifiesCreateConflicts(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    bool
		wantError string
	}{
		{name: "equivalent event is idempotent"},
		{name: "different content is isolated", mutate: true, wantError: "EVENT_ID_CONFLICT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := map[string]any{
				"@timestamp":   "2026-09-26T10:00:00Z",
				"organization": map[string]any{"id": "tenant_a"},
				"event":        map[string]any{"id": "event-1", "dataset": "network"},
				"ueba":         map[string]any{"route": map[string]any{"domain": "network"}},
			}
			existing := map[string]any{}
			encodedEvent, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encodedEvent, &existing); err != nil {
				t.Fatal(err)
			}
			if test.mutate {
				existing["event"].(map[string]any)["dataset"] = "different"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/tuba-v1-uim-network-"):
					w.WriteHeader(http.StatusBadRequest)
					io.WriteString(w, `{"error":{"type":"resource_already_exists_exception"}}`)
				case r.Method == http.MethodPost && r.URL.Path == "/_aliases":
					io.WriteString(w, `{"acknowledged":true}`)
				case r.Method == http.MethodPost && r.URL.Path == "/_bulk":
					io.WriteString(w, `{"items":[{"create":{"status":409,"error":{"type":"version_conflict_engine_exception","reason":"document already exists"}}}]}`)
				case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_doc/event-1"):
					body, marshalErr := json.Marshal(map[string]any{"_source": existing})
					if marshalErr != nil {
						t.Errorf("marshal stored source: %v", marshalErr)
						http.Error(w, "marshal failed", http.StatusInternalServerError)
						return
					}
					w.Write(body)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected request", http.StatusNotFound)
				}
			}))
			defer server.Close()

			results, err := New(server.URL, "limited", "tenant_a").PutStandardBatch(context.Background(), []map[string]any{event})
			if err != nil {
				t.Fatal(err)
			}
			if test.wantError == "" {
				if results[0] != nil {
					t.Fatalf("equivalent duplicate returned error: %v", results[0])
				}
				return
			}
			var permanent PermanentIndexError
			if !errors.As(results[0], &permanent) || permanent.Code != test.wantError {
				t.Fatalf("conflict error=%v, want code %s", results[0], test.wantError)
			}
		})
	}
}

func readBody(t *testing.T, r *http.Request) string {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
