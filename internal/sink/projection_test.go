package sink

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeProjectionES emulates the Elasticsearch APIs the projection sink uses:
// index creation, alias updates, bulk create, versioned single-doc index and
// document readback. Documents are stored per index/id with their external
// version so version-conflict semantics are real, not scripted.
type fakeProjectionES struct {
	mu        sync.Mutex
	indices   map[string]bool
	aliases   map[string][]string
	docs      map[string]map[string]fakeStoredDoc // index -> id -> doc
	requests  []string
	failNext  int // next N requests return 503
	closeDown bool
}

type fakeStoredDoc struct {
	version int64
	source  map[string]any
}

func newFakeProjectionES() *fakeProjectionES {
	return &fakeProjectionES{
		indices: map[string]bool{},
		aliases: map[string][]string{},
		docs:    map[string]map[string]fakeStoredDoc{},
	}
}

func (f *fakeProjectionES) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.closeDown {
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					conn.Close()
					return
				}
			}
		}
		f.requests = append(f.requests, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		if f.failNext > 0 {
			f.failNext--
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/")
		switch {
		case path == "_bulk" && r.Method == http.MethodPost:
			f.serveBulk(w, body)
		case path == "_aliases" && r.Method == http.MethodPost:
			var req struct {
				Actions []struct {
					Add struct {
						Index string `json:"index"`
						Alias string `json:"alias"`
					} `json:"add"`
				} `json:"actions"`
			}
			_ = json.Unmarshal(body, &req)
			for _, a := range req.Actions {
				f.aliases[a.Add.Alias] = append(f.aliases[a.Add.Alias], a.Add.Index)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
		case r.Method == http.MethodPut && !strings.Contains(path, "/"):
			// create index
			if f.indices[path] {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"type":"resource_already_exists_exception"}}`))
				return
			}
			f.indices[path] = true
			f.docs[path] = map[string]fakeStoredDoc{}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))
		case r.Method == http.MethodPut && strings.Contains(path, "/_doc/"):
			f.serveVersionedIndex(w, r, path, body)
		case r.Method == http.MethodGet && strings.Contains(path, "/_doc/"):
			f.serveGet(w, path)
		default:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"unhandled_fake_request","reason":"` + r.Method + " " + path + `"}}`))
		}
	})
}

func (f *fakeProjectionES) serveBulk(w http.ResponseWriter, body []byte) {
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	var items []map[string]any
	for i := 0; i < len(lines); i += 2 {
		var meta struct {
			Create struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			} `json:"create"`
		}
		if err := json.Unmarshal([]byte(lines[i]), &meta); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var source map[string]any
		if err := json.Unmarshal([]byte(lines[i+1]), &source); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if !f.indices[meta.Create.Index] {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"index_not_found_exception"}}`))
			return
		}
		status := http.StatusCreated
		if _, exists := f.docs[meta.Create.Index][meta.Create.ID]; exists {
			status = http.StatusConflict
		} else {
			f.docs[meta.Create.Index][meta.Create.ID] = fakeStoredDoc{version: 1, source: source}
		}
		items = append(items, map[string]any{"create": map[string]any{"_index": meta.Create.Index, "_id": meta.Create.ID, "status": status}})
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
}

func (f *fakeProjectionES) serveVersionedIndex(w http.ResponseWriter, r *http.Request, path string, body []byte) {
	parts := strings.SplitN(path, "/_doc/", 2)
	index, id := parts[0], parts[1]
	if !f.indices[index] {
		// index templates auto-create state indices on first write
		f.indices[index] = true
		f.docs[index] = map[string]fakeStoredDoc{}
	}
	q := r.URL.Query()
	if q.Get("version_type") != "external" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"type":"fake_expected_external_version"}}`))
		return
	}
	version, err := strconv.ParseInt(q.Get("version"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if existing, ok := f.docs[index][id]; ok && version <= existing.version {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"type": "version_conflict_engine_exception", "reason": "version conflict, current version [" + strconv.FormatInt(existing.version, 10) + "] is higher or equal to the one provided [" + strconv.FormatInt(version, 10) + "]"},
		})
		return
	}
	var source map[string]any
	if err := json.Unmarshal(body, &source); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.docs[index][id] = fakeStoredDoc{version: version, source: source}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"_index": index, "_id": id, "_version": version, "result": "updated"})
}

func (f *fakeProjectionES) serveGet(w http.ResponseWriter, path string) {
	parts := strings.SplitN(path, "/_doc/", 2)
	index, id := parts[0], parts[1]
	doc, ok := f.docs[index][id]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"found":false}`))
		return
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"_index": index, "_id": id, "_version": doc.version, "found": true, "_source": doc.source})
}

func (f *fakeProjectionES) stored(t *testing.T, index, id string) fakeStoredDoc {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	doc, ok := f.docs[index][id]
	if !ok {
		t.Fatalf("expected stored document %s/%s", index, id)
	}
	return doc
}

func (f *fakeProjectionES) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func projectionFixtureServer(t *testing.T) (*Elasticsearch, *fakeProjectionES) {
	t.Helper()
	fake := newFakeProjectionES()
	server := httptest.NewServer(fake.handler())
	t.Cleanup(server.Close)
	return New(server.URL, "test-key", "test-ns"), fake
}

func entityProjectionFixture(revision int64) EntityProjection {
	return EntityProjection{
		OrganizationID:          "11111111-1111-1111-1111-111111111111",
		EntityID:                "ent:aaaa",
		EntityType:              "account",
		Authority:               "ad",
		CanonicalKey:            "corp\\alice",
		Strength:                "weak",
		Revision:                revision,
		Document:                json.RawMessage(`{"upn":"alice@corp.example"}`),
		ValidFrom:               time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		LastAttributionID:       "att:bbbb",
		LastAttributionRevision: 7,
		Source:                  SourceRef{EventID: "evt-1", RawEventID: "raw-1"},
		UpdatedAt:               time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC),
	}
}

func relationProjectionFixture(revision int64) RelationProjection {
	return RelationProjection{
		OrganizationID: "11111111-1111-1111-1111-111111111111",
		RelationID:     "rel:cccc",
		FromEntityID:   "ent:aaaa",
		RelationType:   "member_of",
		ToEntityID:     "ent:dddd",
		RuleVersion:    "1.0.0",
		Confidence:     1.0,
		Revision:       revision,
		ValidFrom:      time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		Evidence:       json.RawMessage(`{"via":"event"}`),
		Source:         SourceRef{EventID: "evt-2", RawEventID: "raw-2"},
		UpdatedAt:      time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC),
	}
}

func attributionProjectionFixture() AttributionProjection {
	return AttributionProjection{
		OrganizationID: "11111111-1111-1111-1111-111111111111",
		AttributionID:  "att:bbbb",
		EventID:        "evt-1",
		RawEventID:     "raw-1",
		EventTime:      time.Date(2026, 10, 12, 7, 59, 0, 0, time.UTC),
		Domain:         "authentication",
		Role:           "actor",
		State:          "resolved",
		EntityID:       "ent:aaaa",
		Confidence:     0.8,
		RuleVersion:    "1.0.0",
		PartitionKey:   "11111111-1111-1111-1111-111111111111:ent:aaaa",
		ValidFrom:      time.Date(2026, 10, 12, 7, 59, 0, 0, time.UTC),
		Evidence:       json.RawMessage(`{"adjudication":["resolved_by_weak_identifier"]}`),
	}
}

func TestPutEntityProjectionWritesStateAndHistory(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	p := entityProjectionFixture(3)
	if err := s.PutEntityProjection(context.Background(), p); err != nil {
		t.Fatalf("put entity projection: %v", err)
	}
	state := fake.stored(t, "ueba-entities-test-ns", "ent:aaaa")
	if state.version != 3 {
		t.Fatalf("state external version = %d, want 3", state.version)
	}
	entity := state.source["entity"].(map[string]any)
	if entity["canonical_key"] != "corp\\alice" || entity["type"] != "account" || entity["authority"] != "ad" {
		t.Fatalf("state entity master fields wrong: %v", entity)
	}
	last := state.source["last_attribution"].(map[string]any)
	if last["id"] != "att:bbbb" || last["revision"].(float64) != 7 {
		t.Fatalf("last attribution reference wrong: %v", last)
	}
	source := state.source["source"].(map[string]any)
	if source["event_id"] != "evt-1" || source["raw_event_id"] != "raw-1" {
		t.Fatalf("traceability references missing: %v", source)
	}
	if state.source["content_hash"] == "" {
		t.Fatal("content hash missing")
	}
	history := fake.stored(t, "tuba-v1-entity-history-test-ns-g1-2026.10.12", "ent:aaaa:3")
	if history.source["content_hash"] != state.source["content_hash"] {
		t.Fatal("history snapshot must equal state snapshot")
	}
	fake.mu.Lock()
	aliases := fake.aliases["logs-ueba.entity-history-test-ns"]
	fake.mu.Unlock()
	if len(aliases) != 1 || aliases[0] != "tuba-v1-entity-history-test-ns-g1-2026.10.12" {
		t.Fatalf("history read alias wrong: %v", aliases)
	}
}

func TestPutEntityProjectionStaleRevisionDoesNotOverwrite(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	if err := s.PutEntityProjection(context.Background(), entityProjectionFixture(5)); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	older := entityProjectionFixture(2)
	older.CanonicalKey = "corp\\old"
	err := s.PutEntityProjection(context.Background(), older)
	var stale StaleRevisionError
	if !errors.As(err, &stale) {
		t.Fatalf("expected StaleRevisionError, got %v", err)
	}
	if stale.Existing != 5 || stale.Incoming != 2 {
		t.Fatalf("stale error versions wrong: %+v", stale)
	}
	state := fake.stored(t, "ueba-entities-test-ns", "ent:aaaa")
	if state.version != 5 || state.source["entity"].(map[string]any)["canonical_key"] != "corp\\alice" {
		t.Fatalf("stale write overwrote newer state: %+v", state)
	}
	// the stale snapshot must not reach the history index either
	if _, ok := fake.docs["tuba-v1-entity-history-test-ns-g1-2026.10.12"]["ent:aaaa:2"]; ok {
		t.Fatal("stale snapshot leaked into history index")
	}
}

func TestPutEntityProjectionSameRevisionIdempotent(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	p := entityProjectionFixture(4)
	if err := s.PutEntityProjection(context.Background(), p); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := s.PutEntityProjection(context.Background(), p); err != nil {
		t.Fatalf("idempotent replay must succeed, got %v", err)
	}
	state := fake.stored(t, "ueba-entities-test-ns", "ent:aaaa")
	if state.version != 4 {
		t.Fatalf("version moved on replay: %d", state.version)
	}
	// history replay must not duplicate either: one create + one conflict-
	// verified readback leaves exactly one document.
	if len(fake.docs["tuba-v1-entity-history-test-ns-g1-2026.10.12"]) != 1 {
		t.Fatalf("history duplicated on replay: %d docs", len(fake.docs["tuba-v1-entity-history-test-ns-g1-2026.10.12"]))
	}
}

func TestPutEntityProjectionSameRevisionDifferentContentConflicts(t *testing.T) {
	s, _ := projectionFixtureServer(t)
	if err := s.PutEntityProjection(context.Background(), entityProjectionFixture(4)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	conflicting := entityProjectionFixture(4)
	conflicting.CanonicalKey = "corp\\changed"
	err := s.PutEntityProjection(context.Background(), conflicting)
	var permanent PermanentIndexError
	if !errors.As(err, &permanent) || permanent.Code != "PROJECTION_REVISION_CONFLICT" {
		t.Fatalf("expected PROJECTION_REVISION_CONFLICT, got %v", err)
	}
}

func TestPutRelationProjectionWritesStateAndHistory(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	if err := s.PutRelationProjection(context.Background(), relationProjectionFixture(1)); err != nil {
		t.Fatalf("put relation projection: %v", err)
	}
	state := fake.stored(t, "ueba-relations-test-ns", "rel:cccc")
	relation := state.source["relation"].(map[string]any)
	if relation["from_entity_id"] != "ent:aaaa" || relation["to_entity_id"] != "ent:dddd" || relation["type"] != "member_of" {
		t.Fatalf("relation fields wrong: %v", relation)
	}
	if state.source["valid_from"] == "" {
		t.Fatal("temporal interval missing from relation projection")
	}
	source := state.source["source"].(map[string]any)
	if source["event_id"] != "evt-2" || source["raw_event_id"] != "raw-2" {
		t.Fatalf("traceability references missing: %v", source)
	}
	if _, ok := fake.docs["tuba-v1-relation-history-test-ns-g1-2026.10.12"]["rel:cccc:1"]; !ok {
		t.Fatal("relation history snapshot missing")
	}
	// close the interval: revision 2 wins over the open revision 1
	closed := relationProjectionFixture(2)
	to := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	closed.ValidTo = &to
	if err := s.PutRelationProjection(context.Background(), closed); err != nil {
		t.Fatalf("close relation: %v", err)
	}
	state = fake.stored(t, "ueba-relations-test-ns", "rel:cccc")
	if state.version != 2 || state.source["valid_to"] == nil {
		t.Fatalf("closed revision not applied: %+v", state)
	}
	// a late duplicate of the open interval must not reopen the relation
	err := s.PutRelationProjection(context.Background(), relationProjectionFixture(1))
	if !IsStaleRevision(err) {
		t.Fatalf("expected stale rejection of reopened relation, got %v", err)
	}
	state = fake.stored(t, "ueba-relations-test-ns", "rel:cccc")
	if state.source["valid_to"] == nil {
		t.Fatal("stale write reopened a closed relation")
	}
	if len(fake.docs["tuba-v1-relation-history-test-ns-g1-2026.10.12"]) != 2 {
		t.Fatal("both lifecycle revisions must stay in relation history")
	}
}

func TestPutAttributionProjectionTraceableAndImmutable(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	p := attributionProjectionFixture()
	if err := s.PutAttributionProjection(context.Background(), p); err != nil {
		t.Fatalf("put attribution projection: %v", err)
	}
	doc := fake.stored(t, "tuba-v1-attributions-test-ns-g1-2026.10.12", "att:bbbb")
	attr := doc.source["attribution"].(map[string]any)
	if attr["entity_id"] != "ent:aaaa" || attr["role"] != "actor" || attr["state"] != "resolved" {
		t.Fatalf("attribution mapping wrong: %v", attr)
	}
	source := doc.source["source"].(map[string]any)
	if source["event_id"] != "evt-1" || source["raw_event_id"] != "raw-1" {
		t.Fatalf("event.id/raw_event_id traceability missing: %v", source)
	}
	if doc.source["event"].(map[string]any)["id"] != "evt-1" {
		t.Fatal("event reference missing")
	}
	// replay of the same decision is idempotent
	if err := s.PutAttributionProjection(context.Background(), p); err != nil {
		t.Fatalf("attribution replay must succeed, got %v", err)
	}
	if len(fake.docs["tuba-v1-attributions-test-ns-g1-2026.10.12"]) != 1 {
		t.Fatal("attribution replay duplicated the document")
	}
	// same attribution.id with different content is a permanent conflict
	changed := p
	changed.EntityID = "ent:zzzz"
	err := s.PutAttributionProjection(context.Background(), changed)
	var permanent PermanentIndexError
	if !errors.As(err, &permanent) || permanent.Code != "PROJECTION_HISTORY_CONFLICT" {
		t.Fatalf("expected PROJECTION_HISTORY_CONFLICT, got %v", err)
	}
}

func TestProjectionValidationFailClosed(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	cases := []error{
		s.PutEntityProjection(context.Background(), EntityProjection{}),
		s.PutRelationProjection(context.Background(), RelationProjection{}),
		s.PutAttributionProjection(context.Background(), AttributionProjection{}),
	}
	badRevision := entityProjectionFixture(0)
	cases = append(cases, s.PutEntityProjection(context.Background(), badRevision))
	noSource := relationProjectionFixture(1)
	noSource.Source = SourceRef{}
	cases = append(cases, s.PutRelationProjection(context.Background(), noSource))
	for i, err := range cases {
		var permanent PermanentIndexError
		if !errors.As(err, &permanent) || permanent.Code != "PROJECTION_CONTRACT_INVALID" {
			t.Fatalf("case %d: expected fail-closed contract error, got %v", i, err)
		}
	}
	if fake.requestCount() != 0 {
		t.Fatalf("invalid projections must not reach Elasticsearch (%d requests)", fake.requestCount())
	}
}

func TestProjectionElasticsearchUnreachableRetryable(t *testing.T) {
	fake := newFakeProjectionES()
	server := httptest.NewServer(fake.handler())
	s := New(server.URL, "test-key", "test-ns")
	server.Close() // nothing listening: connection refused
	err := s.PutEntityProjection(context.Background(), entityProjectionFixture(1))
	if err == nil {
		t.Fatal("expected error when Elasticsearch is unreachable")
	}
	var permanent PermanentIndexError
	if errors.As(err, &permanent) {
		t.Fatalf("transport failure must stay retryable, got permanent %v", err)
	}
	if IsStaleRevision(err) {
		t.Fatalf("transport failure must not be reported as stale: %v", err)
	}
}

func TestProjectionServerOverloadedRetryable(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	fake.failNext = 1
	err := s.PutEntityProjection(context.Background(), entityProjectionFixture(1))
	if err == nil {
		t.Fatal("expected error on 503")
	}
	var permanent PermanentIndexError
	if errors.As(err, &permanent) || IsStaleRevision(err) {
		t.Fatalf("503 must stay retryable, got %v", err)
	}
	// next attempt succeeds against the same sink
	if err := s.PutEntityProjection(context.Background(), entityProjectionFixture(1)); err != nil {
		t.Fatalf("retry after overload must succeed, got %v", err)
	}
}

func TestPutEntityProjectionVersionQueryIsExternal(t *testing.T) {
	s, fake := projectionFixtureServer(t)
	if err := s.PutEntityProjection(context.Background(), entityProjectionFixture(9)); err != nil {
		t.Fatalf("put: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	found := false
	for _, r := range fake.requests {
		if strings.HasPrefix(r, "PUT /ueba-entities-test-ns/_doc/ent:aaaa") &&
			strings.Contains(r, "version_type=external") && strings.Contains(r, "version=9") {
			found = true
		}
	}
	if !found {
		t.Fatalf("state write must use external versioning with the projection revision: %v", fake.requests)
	}
}
