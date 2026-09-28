package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestZeekToDurableReceiptLoop(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "conn.log")
	lines := []string{
		`{"ts":1721000000.01,"uid":"Ctest001","id.orig_h":"192.0.2.10","id.orig_p":51000,"id.resp_h":"198.51.100.20","id.resp_p":80,"proto":"tcp","service":"http","duration":0.2,"conn_state":"SF"}`,
		`{"ts":1721000001.02,"uid":"Ctest002","id.orig_h":"192.0.2.11","id.orig_p":51001,"id.resp_h":"198.51.100.53","id.resp_p":53,"proto":"udp","service":"dns","duration":0.01,"conn_state":"SF"}`,
	}
	data := []byte(lines[0] + "\n" + lines[1] + "\n")
	if err := os.WriteFile(logPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	spoolPath := filepath.Join(dir, "spool.sqlite")
	store, err := OpenStore(spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if runtime.GOOS != "windows" {
		info, err := os.Stat(spoolPath)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0600 {
			t.Fatalf("spool permissions=%#o, want 0600", got)
		}
	}

	var accepted atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("X-API-Key") != "tuba_src_test_key" || r.Header.Get("X-Source-Context") != "ctx_0123456789abcdef0123456789abcdef" {
			http.Error(w, "bad request contract", http.StatusUnauthorized)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["uid"] == nil {
			http.Error(w, "invalid Zeek payload", http.StatusBadRequest)
			return
		}
		accepted.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprint(w, `{"receipt_id":"raw:`+fmt.Sprintf("%064x", accepted.Load())+`","raw_event_id":"raw:`+fmt.Sprintf("%064x", accepted.Load())+`","status":"accepted"}`)
	}))
	defer server.Close()

	source := ZeekSource{
		ID: "src_0123456789abcdef0123456789abcdef", ContextID: "ctx_0123456789abcdef0123456789abcdef",
		StreamID: "zeek.conn", Pattern: logPath,
		Filter: FilterPolicy{Version: "shadow-test-v1", Mode: "shadow", DropEquals: map[string]string{"service": "http"}},
	}
	if err := source.Poll(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	var cursor fileCursor
	encoded, err := store.Cursor(context.Background(), source.ID, "zeek.conn:"+fileGenerationForTest(t, logPath))
	if err != nil || CursorDecode(encoded, &cursor) != nil {
		t.Fatalf("read cursor unavailable: %v", err)
	}
	if cursor.Offset != int64(len(data)) {
		t.Fatalf("read cursor=%d, want %d", cursor.Offset, len(data))
	}
	var shadowCount int64
	if err := store.db.QueryRow(`SELECT COALESCE(sum(count),0) FROM filter_counts WHERE source_id=?`, source.ID).Scan(&shadowCount); err != nil {
		t.Fatal(err)
	}
	if shadowCount != 1 {
		t.Fatalf("shadow hit count=%d, want 1", shadowCount)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		(&Sender{Store: store, Endpoint: server.URL, Sources: map[string]Credential{source.ID: {APIKey: "tuba_src_test_key", ContextID: source.ContextID}}}).Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var acked int64
		if err := store.db.QueryRow(`SELECT count(*) FROM queue_items WHERE state='acked'`).Scan(&acked); err != nil {
			t.Fatal(err)
		}
		if acked == 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	<-done
	var acked, payloadBytes int64
	if err := store.db.QueryRow(`SELECT count(*),COALESCE(sum(length(payload)),0) FROM queue_items WHERE state='acked'`).Scan(&acked, &payloadBytes); err != nil {
		t.Fatal(err)
	}
	if acked != 2 || payloadBytes != 0 || accepted.Load() != 2 {
		t.Fatalf("acked=%d payload_bytes=%d accepted=%d; want 2, 0, 2", acked, payloadBytes, accepted.Load())
	}
}

func fileGenerationForTest(t *testing.T, path string) string {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	value, err := fileGeneration(info, path)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
