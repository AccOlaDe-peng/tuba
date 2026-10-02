package sink

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func analysisObjectFixture(objectType string, revision int64) analysis.ObjectResult {
	window := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	return analysis.ObjectResult{
		ContractVersion: analysis.ContractVersionV2,
		ObjectType:      objectType,
		ObjectID:        objectType + ":tenant_a:entity-1:2026-09-26",
		Revision:        revision,
		Operation:       analysis.OperationUpsert,
		Generation:      "g1",
		OrganizationID:  "tenant_a",
		Namespace:       "test-ns",
		RuleID:          "rule-1",
		RuleVersion:     "2.1.0",
		RunID:           "run-1",
		WindowStart:     window,
		WindowStartRaw:  window.Format(time.RFC3339Nano),
		DateKey:         "2026-09-26",
		InputRefs:       []analysis.InputRef{},
		Document:        []byte(`{"organization":{"id":"tenant_a"},"value":1}`),
	}
}

// TestAnalysisObjectRoutingCoversEveryType asserts each object type lands in
// its own state index and event-time-dated history index with the external
// revision in the request.
func TestAnalysisObjectRoutingCoversEveryType(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	for _, objectType := range []string{"feature", "baseline", "anomaly", "risk_event", "entity_risk"} {
		object := analysisObjectFixture(objectType, 1)
		if err := client.PutAnalysisObject(context.Background(), object); err != nil {
			t.Fatalf("%s: %v", objectType, err)
		}
		state := "ueba-analysis-" + objectType + "-test-ns"
		history := "tuba-v1-analysis-" + objectType + "-test-ns-g1-2026.09.26"
		doc := fake.stored(t, state, object.ObjectID)
		if doc.version != 1 {
			t.Fatalf("%s: stored external version %d, want 1", objectType, doc.version)
		}
		frame := fake.stored(t, history, object.ObjectID+":1")
		if frame.source["date_key"] != "2026-09-26" {
			t.Fatalf("%s: history frame lost the fixed date key", objectType)
		}
		if got := fake.aliases["logs-ueba.analysis-"+objectType+"-test-ns"]; len(got) != 1 || got[0] != history {
			t.Fatalf("%s: read alias missing: %v", objectType, fake.aliases)
		}
		found := false
		fake.mu.Lock()
		for _, request := range fake.requests {
			if strings.Contains(request, state+"/_doc/") && strings.Contains(request, "version_type=external&version=1") {
				found = true
			}
		}
		fake.mu.Unlock()
		if !found {
			t.Fatalf("%s: state write did not use external versioning", objectType)
		}
	}
}

// TestAnalysisObjectOutOfOrderRevisionNeverOverwrites is the F07 red line: a
// late rev2 after rev5 is stale — the stored rev5 stays authoritative and the
// stale frame never reaches the history index.
func TestAnalysisObjectOutOfOrderRevisionNeverOverwrites(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	rev5 := analysisObjectFixture("anomaly", 5)
	if err := client.PutAnalysisObject(ctx, rev5); err != nil {
		t.Fatal(err)
	}
	rev2 := analysisObjectFixture("anomaly", 2)
	err := client.PutAnalysisObject(ctx, rev2)
	if !IsStaleRevision(err) {
		t.Fatalf("expected StaleRevisionError, got %v", err)
	}
	doc := fake.stored(t, "ueba-analysis-anomaly-test-ns", rev5.ObjectID)
	if doc.version != 5 || doc.source["object"].(map[string]any)["revision"].(float64) != 5 {
		t.Fatalf("stored state was overwritten by the stale revision: %+v", doc)
	}
	history := "tuba-v1-analysis-anomaly-test-ns-g1-2026.09.26"
	fake.mu.Lock()
	_, polluted := fake.docs[history][rev2.ObjectID+":2"]
	fake.mu.Unlock()
	if polluted {
		t.Fatal("stale revision frame must not be appended to the history index")
	}
}

// TestAnalysisObjectSameRevisionReplayAndConflict locks the same-revision
// semantics: identical content is an idempotent no-op; different content is a
// permanent conflict and the stored document is never silently overwritten.
func TestAnalysisObjectSameRevisionReplayAndConflict(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	object := analysisObjectFixture("anomaly", 3)
	if err := client.PutAnalysisObject(ctx, object); err != nil {
		t.Fatal(err)
	}
	before := fake.requestCount()
	if err := client.PutAnalysisObject(ctx, object); err != nil {
		t.Fatalf("identical replay must be idempotent: %v", err)
	}
	if fake.requestCount() == before {
		t.Fatal("replay must re-verify against the stored document")
	}
	fake.mu.Lock()
	frames := len(fake.docs["tuba-v1-analysis-anomaly-test-ns-g1-2026.09.26"])
	fake.mu.Unlock()
	if frames != 1 {
		t.Fatalf("same-revision replay appended a second history frame: %d", frames)
	}
	conflicting := object
	conflicting.Document = []byte(`{"organization":{"id":"tenant_a"},"value":2}`)
	err := client.PutAnalysisObject(ctx, conflicting)
	permanent, ok := err.(PermanentIndexError)
	if !ok || permanent.Code != "ANALYSIS_REVISION_CONFLICT" {
		t.Fatalf("expected ANALYSIS_REVISION_CONFLICT, got %v", err)
	}
	doc := fake.stored(t, "ueba-analysis-anomaly-test-ns", object.ObjectID)
	if doc.source["document"].(map[string]any)["value"].(float64) != 1 {
		t.Fatal("same-revision conflict silently overwrote the stored document")
	}
}

// TestAnalysisObjectFixedDateKeyByEventTime: a late message reprocessed days
// later must land in its original event-time day partition, not in a
// processing-time partition.
func TestAnalysisObjectFixedDateKeyByEventTime(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	object := analysisObjectFixture("feature", 1)
	object.WindowStart = time.Date(2026, 8, 1, 0, 30, 0, 0, time.UTC)
	object.DateKey = "2026-08-01"
	if err := client.PutAnalysisObject(context.Background(), object); err != nil {
		t.Fatal(err)
	}
	fake.stored(t, "tuba-v1-analysis-feature-test-ns-g1-2026.08.01", object.ObjectID+":1")
	fake.mu.Lock()
	for index := range fake.indices {
		if strings.HasPrefix(index, "tuba-v1-analysis-feature-") && index != "tuba-v1-analysis-feature-test-ns-g1-2026.08.01" {
			t.Fatalf("unexpected date partition drifted from the fixed date key: %s", index)
		}
	}
	fake.mu.Unlock()
}

// TestAnalysisObjectRetractionIsAppliedInRevisionOrder: the retracted
// tombstone becomes the current state at the next revision; a late re-upsert
// at the older revision is stale and never resurrects the object.
func TestAnalysisObjectRetractionIsAppliedInRevisionOrder(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	upsert := analysisObjectFixture("anomaly", 1)
	if err := client.PutAnalysisObject(ctx, upsert); err != nil {
		t.Fatal(err)
	}
	retracted := upsert
	retracted.Revision = 2
	retracted.Operation = analysis.OperationRetracted
	if err := client.PutAnalysisObject(ctx, retracted); err != nil {
		t.Fatal(err)
	}
	doc := fake.stored(t, "ueba-analysis-anomaly-test-ns", upsert.ObjectID)
	if doc.source["object"].(map[string]any)["operation"] != "retracted" {
		t.Fatal("retraction tombstone was not applied as the current state")
	}
	late := upsert
	if err := client.PutAnalysisObject(ctx, late); !IsStaleRevision(err) {
		t.Fatalf("late re-upsert after retraction must be stale, got %v", err)
	}
	doc = fake.stored(t, "ueba-analysis-anomaly-test-ns", upsert.ObjectID)
	if doc.source["object"].(map[string]any)["operation"] != "retracted" {
		t.Fatal("late upsert resurrected a retracted object")
	}
	history := "tuba-v1-analysis-anomaly-test-ns-g1-2026.09.26"
	fake.stored(t, history, upsert.ObjectID+":1")
	fake.stored(t, history, upsert.ObjectID+":2")
}

// TestAnalysisObjectContractInvalidFailsClosed: malformed objects are rejected
// before any HTTP request leaves the sink.
func TestAnalysisObjectContractInvalidFailsClosed(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	cases := map[string]func(analysis.ObjectResult) analysis.ObjectResult{
		"unknown type":     func(o analysis.ObjectResult) analysis.ObjectResult { o.ObjectType = "threat"; return o },
		"zero revision":    func(o analysis.ObjectResult) analysis.ObjectResult { o.Revision = 0; return o },
		"bad operation":    func(o analysis.ObjectResult) analysis.ObjectResult { o.Operation = "delete"; return o },
		"empty generation": func(o analysis.ObjectResult) analysis.ObjectResult { o.Generation = ""; return o },
		"missing window":   func(o analysis.ObjectResult) analysis.ObjectResult { o.WindowStart = time.Time{}; return o },
		"date key drift": func(o analysis.ObjectResult) analysis.ObjectResult {
			o.DateKey = "2026-09-27"
			return o
		},
		"namespace mismatch": func(o analysis.ObjectResult) analysis.ObjectResult { o.Namespace = "other"; return o },
		"missing document":   func(o analysis.ObjectResult) analysis.ObjectResult { o.Document = nil; return o },
	}
	for name, mutate := range cases {
		before := fake.requestCount()
		err := client.PutAnalysisObject(context.Background(), mutate(analysisObjectFixture("anomaly", 1)))
		permanent, ok := err.(PermanentIndexError)
		if !ok || permanent.Code != "ANALYSIS_CONTRACT_INVALID" {
			t.Fatalf("%s: expected ANALYSIS_CONTRACT_INVALID, got %v", name, err)
		}
		if fake.requestCount() != before {
			t.Fatalf("%s: contract-invalid object reached Elasticsearch", name)
		}
	}
}

// TestAnalysisObjectRetryableFailures: transport loss and 503 stay retryable
// and a retry succeeds with exactly one state version and one history frame.
func TestAnalysisObjectRetryableFailures(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	object := analysisObjectFixture("baseline", 1)
	fake.mu.Lock()
	fake.failNext = 1
	fake.mu.Unlock()
	if err := client.PutAnalysisObject(context.Background(), object); err == nil || IsStaleRevision(err) {
		t.Fatalf("503 must stay retryable, got %v", err)
	}
	if err := client.PutAnalysisObject(context.Background(), object); err != nil {
		t.Fatal(err)
	}
	doc := fake.stored(t, "ueba-analysis-baseline-test-ns", object.ObjectID)
	if doc.version != 1 {
		t.Fatalf("version %d after retry", doc.version)
	}
	fake.stored(t, "tuba-v1-analysis-baseline-test-ns-g1-2026.09.26", object.ObjectID+":1")
	fake.mu.Lock()
	fake.closeDown = true
	fake.mu.Unlock()
	if err := client.PutAnalysisObject(context.Background(), object); err == nil {
		t.Fatal("transport failure must surface as a retryable error")
	} else if IsStaleRevision(err) {
		t.Fatal("transport failure must not be misclassified as stale")
	}
}
