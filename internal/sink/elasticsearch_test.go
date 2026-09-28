package sink

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"tuba/product/internal/event"
	"tuba/product/internal/indexing"
)

func TestBulkUsesStableIDsAndClassifiesItems(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/_bulk" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		if r.Header.Get("Authorization") != "ApiKey limited" {
			t.Error("missing API key")
		}
		b, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(b), `"_id":"event-1"`) {
			t.Errorf("stable id missing: %s", b)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"items":[{"create":{"status":201}},{"create":{"status":429,"error":{"type":"es_rejected_execution_exception","reason":"busy"}}},{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"bad field"}}}]}`)
	}))
	defer srv.Close()
	mk := func(id string) indexing.Document {
		e := event.Authentication{}
		e.Event.ID = id
		return indexing.Document{Event: e, Raw: []byte(`{}`)}
	}
	got, err := New(srv.URL, "limited", "tenant_a").PutBatch(context.Background(), []indexing.Document{mk("event-1"), mk("event-2"), mk("event-3")})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != 201 || !got[1].Retryable || got[2].Retryable {
		t.Fatalf("bad classification: %+v", got)
	}
}
