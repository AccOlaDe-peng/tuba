package sink

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"tuba/product/internal/analysis"
)

func TestAnalysisUpsertUsesStableID(t *testing.T) {
	var path string
	var query string
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		query = r.URL.RawQuery
		value, _ := io.ReadAll(r.Body)
		body = string(value)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"result":"created"}`))
	}))
	defer server.Close()
	client := New(server.URL, "key", "tenant_a")
	result := analysis.Result{ResultID: "anom:1", Document: []byte(`{"organization":{"id":"tenant_a"}}`)}
	if err := client.PutAnalysis(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if path != "/ueba-anomalies-tenant_a/_doc/anom:1" || query != "op_type=index" {
		t.Fatalf("unexpected path %s", path)
	}
	if body == "" {
		t.Fatal("document was not sent")
	}
}
