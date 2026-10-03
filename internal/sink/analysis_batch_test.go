package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"tuba/product/internal/analysis"
)

// TestAnalysisObjectBatchMixedItemResults: one bulk batch carries a fresh
// write, a stale revision and a same-revision conflict; every item outcome is
// classified independently and only the accepted object reaches history.
func TestAnalysisObjectBatchMixedItemResults(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()

	fresh := analysisObjectFixture("anomaly", 1)
	fresh.ObjectID = "anomaly:tenant_a:fresh:2026-09-26"
	stored := analysisObjectFixture("anomaly", 5)
	stored.ObjectID = "anomaly:tenant_a:stored:2026-09-26"
	if err := client.PutAnalysisObject(ctx, stored); err != nil {
		t.Fatal(err)
	}
	stale := stored
	stale.Revision = 2
	conflict := stored
	conflict.Revision = 5
	conflict.Document = []byte(`{"organization":{"id":"tenant_a"},"value":99}`)

	results, err := client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{fresh, stale, conflict})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("results=%d, want 3", len(results))
	}
	if results[0] != nil {
		t.Fatalf("fresh write: %v", results[0])
	}
	if !IsStaleRevision(results[1]) {
		t.Fatalf("stale write: expected StaleRevisionError, got %v", results[1])
	}
	var permanent PermanentIndexError
	if !errors.As(results[2], &permanent) || permanent.Code != "ANALYSIS_REVISION_CONFLICT" {
		t.Fatalf("conflict write: expected ANALYSIS_REVISION_CONFLICT, got %v", results[2])
	}

	state := "ueba-analysis-anomaly-test-ns"
	if doc := fake.stored(t, state, fresh.ObjectID); doc.version != 1 {
		t.Fatalf("fresh state version %d", doc.version)
	}
	if doc := fake.stored(t, state, stored.ObjectID); doc.version != 5 {
		t.Fatalf("stored state overwritten: %+v", doc)
	}
	history := "tuba-v1-analysis-anomaly-test-ns-g1-2026.09.26"
	fake.mu.Lock()
	_, stalePolluted := fake.docs[history][stale.ObjectID+":2"]
	frames := len(fake.docs[history])
	fake.mu.Unlock()
	if stalePolluted {
		t.Fatal("rejected stale revision must not reach the history index")
	}
	if frames != 2 {
		t.Fatalf("history frames=%d, want 2 (initial write + fresh)", frames)
	}
}

// TestAnalysisObjectBatchSameKeyAppliesInBatchOrder: bulk item order is
// preserved, so revisions of one object inside a single batch apply in order;
// a same-batch regression is stale, never a silent overwrite.
func TestAnalysisObjectBatchSameKeyAppliesInBatchOrder(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	state := "ueba-analysis-anomaly-test-ns"

	rev1 := analysisObjectFixture("anomaly", 1)
	rev2 := rev1
	rev2.Revision = 2
	rev2.Document = []byte(`{"organization":{"id":"tenant_a"},"value":2}`)
	results, err := client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{rev1, rev2})
	if err != nil {
		t.Fatal(err)
	}
	if results[0] != nil || results[1] != nil {
		t.Fatalf("ascending revisions must both apply: %v, %v", results[0], results[1])
	}
	if doc := fake.stored(t, state, rev1.ObjectID); doc.version != 2 {
		t.Fatalf("final version %d, want 2", doc.version)
	}
	fake.mu.Lock()
	frames := len(fake.docs["tuba-v1-analysis-anomaly-test-ns-g1-2026.09.26"])
	fake.mu.Unlock()
	if frames != 2 {
		t.Fatalf("both revisions must be in history: %d", frames)
	}

	rev3 := rev2
	rev3.Revision = 3
	rev3.Document = []byte(`{"organization":{"id":"tenant_a"},"value":3}`)
	regressed := rev1
	results, err = client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{rev3, regressed})
	if err != nil {
		t.Fatal(err)
	}
	if results[0] != nil {
		t.Fatalf("rev3: %v", results[0])
	}
	if !IsStaleRevision(results[1]) {
		t.Fatalf("same-batch regression must be stale, got %v", results[1])
	}
	if doc := fake.stored(t, state, rev1.ObjectID); doc.version != 3 {
		t.Fatalf("stale same-batch write overwrote newer state: %+v", doc)
	}
}

// TestAnalysisObjectBatchIdempotentReplay: replaying an already-stored batch
// succeeds as no-ops and history keeps exactly one frame per revision.
func TestAnalysisObjectBatchIdempotentReplay(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	objects := []analysis.ObjectResult{
		analysisObjectFixture("feature", 1),
		analysisObjectFixture("baseline", 4),
	}
	for i := range objects {
		objects[i].ObjectID = fmt.Sprintf("%s:tenant_a:entity-%d:2026-09-26", objects[i].ObjectType, i)
	}
	results, err := client.PutAnalysisObjectBatch(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	if results[0] != nil || results[1] != nil {
		t.Fatalf("first batch: %v, %v", results[0], results[1])
	}
	results, err = client.PutAnalysisObjectBatch(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	if results[0] != nil || results[1] != nil {
		t.Fatalf("identical replay must succeed: %v, %v", results[0], results[1])
	}
	if doc := fake.stored(t, "ueba-analysis-baseline-test-ns", objects[1].ObjectID); doc.version != 4 {
		t.Fatalf("replay moved the version: %+v", doc)
	}
	fake.mu.Lock()
	featureFrames := len(fake.docs["tuba-v1-analysis-feature-test-ns-g1-2026.09.26"])
	fake.mu.Unlock()
	if featureFrames != 1 {
		t.Fatalf("replay duplicated history frames: %d", featureFrames)
	}
}

// TestAnalysisObjectBatchRetryableBulkFailure: a 503 on the bulk request is a
// batch-level retryable error; nothing is partially classified and a retry of
// the whole batch succeeds idempotently.
func TestAnalysisObjectBatchRetryableBulkFailure(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	object := analysisObjectFixture("feature", 1)
	fake.mu.Lock()
	fake.failNext = 1
	fake.mu.Unlock()
	results, err := client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{object})
	if err == nil || IsStaleRevision(err) {
		t.Fatalf("503 must surface as retryable batch error, got results=%v err=%v", results, err)
	}
	var permanent PermanentIndexError
	if errors.As(err, &permanent) {
		t.Fatalf("503 must not be permanent: %v", err)
	}
	results, err = client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{object})
	if err != nil || results[0] != nil {
		t.Fatalf("retry: results=%v err=%v", results, err)
	}
	fake.stored(t, "ueba-analysis-feature-test-ns", object.ObjectID)
}

// TestAnalysisObjectBatchContractInvalidStaysLocal: invalid objects are
// rejected per item before any HTTP request and do not block valid batchmates.
func TestAnalysisObjectBatchContractInvalidStaysLocal(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	valid := analysisObjectFixture("anomaly", 1)
	invalid := analysisObjectFixture("anomaly", 1)
	invalid.ObjectID = "anomaly:tenant_a:invalid:2026-09-26"
	invalid.Revision = 0
	before := fake.requestCount()
	results, err := client.PutAnalysisObjectBatch(context.Background(), []analysis.ObjectResult{invalid, valid})
	if err != nil {
		t.Fatal(err)
	}
	var permanent PermanentIndexError
	if !errors.As(results[0], &permanent) || permanent.Code != "ANALYSIS_CONTRACT_INVALID" {
		t.Fatalf("invalid object: %v", results[0])
	}
	if results[1] != nil {
		t.Fatalf("valid batchmate blocked by invalid item: %v", results[1])
	}
	fake.stored(t, "ueba-analysis-anomaly-test-ns", valid.ObjectID)
	fake.mu.Lock()
	for _, bulk := range fake.bulkBodies {
		if strings.Contains(bulk, invalid.ObjectID) {
			t.Fatal("contract-invalid object reached the bulk request")
		}
	}
	fake.mu.Unlock()
	if fake.requestCount() == before {
		t.Fatal("valid object was never written")
	}
}

// TestAnalysisObjectBatchItemCountMismatchIsRetryable: a truncated bulk
// response means the item outcomes are unknown — the batch fails retryable so
// the caller replays it (idempotent) instead of guessing.
func TestAnalysisObjectBatchItemCountMismatchIsRetryable(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	fake.mu.Lock()
	fake.truncateBulkItems = true
	fake.mu.Unlock()
	_, err := client.PutAnalysisObjectBatch(context.Background(), []analysis.ObjectResult{analysisObjectFixture("anomaly", 1)})
	if err == nil {
		t.Fatal("item count mismatch must fail the batch")
	}
	var permanent PermanentIndexError
	if errors.As(err, &permanent) {
		t.Fatalf("mismatch must stay retryable, got %v", err)
	}
}

// TestAnalysisObjectBatchHistoryFailureDoesNotLoseObject: a retryable history
// failure classifies only that item; the state write already succeeded and a
// batch retry completes idempotently.
func TestAnalysisObjectBatchHistoryFailureDoesNotLoseObject(t *testing.T) {
	client, fake := projectionFixtureServer(t)
	ctx := context.Background()
	object := analysisObjectFixture("anomaly", 1)
	fake.mu.Lock()
	fake.failNextHistoryBulk = true
	fake.mu.Unlock()
	results, err := client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{object})
	if err != nil {
		t.Fatal(err)
	}
	if results[0] == nil || IsStaleRevision(results[0]) {
		t.Fatalf("history 503 must be a retryable item error, got %v", results[0])
	}
	if doc := fake.stored(t, "ueba-analysis-anomaly-test-ns", object.ObjectID); doc.version != 1 {
		t.Fatalf("state write lost: %+v", doc)
	}
	results, err = client.PutAnalysisObjectBatch(ctx, []analysis.ObjectResult{object})
	if err != nil || results[0] != nil {
		t.Fatalf("retry must be an idempotent no-op: results=%v err=%v", results, err)
	}
	fake.stored(t, "tuba-v1-analysis-anomaly-test-ns-g1-2026.09.26", object.ObjectID+":1")
}
