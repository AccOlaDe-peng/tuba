package sourceadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/telemetry"
)

type flakyConsumer struct {
	mu          sync.Mutex
	message     kafka.Message
	fetchCalls  int
	commitCalls int
	cancel      context.CancelFunc
}

func (c *flakyConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fetchCalls++
	switch c.fetchCalls {
	case 1:
		return kafka.Message{}, errors.New("temporary broker disconnect")
	case 2:
		return c.message, nil
	default:
		<-ctx.Done()
		return kafka.Message{}, ctx.Err()
	}
}

func (c *flakyConsumer) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commitCalls++
	if c.commitCalls == 1 {
		return errors.New("temporary offset commit failure")
	}
	c.cancel()
	return nil
}

type unusedDeadLetter struct{}

func (unusedDeadLetter) WriteMessages(context.Context, ...kafka.Message) error {
	return errors.New("unexpected DLQ write")
}

func TestRunRetriesKafkaFetchAndCommitWithoutRedeliveringAcceptedRecord(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	body := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/conn.log"},"offset":10118640}}`)
	message := kafka.Message{Topic: topic, Partition: 0, Offset: 42, Value: body}
	digest := sha256.Sum256(body)
	var ingestCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ingestCalls++
		if got := r.Header.Get("X-Source-Offset"); got != "42" {
			t.Errorf("source offset header=%q, want 42", got)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"receipt_id":        "raw:accepted-42",
			"raw_event_id":      "raw:accepted-42",
			"source_context_id": "ctx_0123456789abcdef0123456789abcdef",
			"source_position":   "filebeat-v1:2053:8926348:10118640",
			"delivery_position": "kafka-v1:" + topic + ":0:42",
			"payload_hash":      hex.EncodeToString(digest[:]),
			"status":            "accepted",
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &flakyConsumer{message: message, cancel: cancel}
	metrics := telemetry.New()
	adapter := Adapter{
		Binding:      Binding{Topic: topic},
		IngestURL:    server.URL + "/api/v1/internal/ingest/beat-events",
		AdapterToken: "test-adapter-token-with-more-than-32-characters",
		Consumer:     consumer,
		DeadLetter:   unusedDeadLetter{},
		HTTPClient:   server.Client(),
		RetryBackoff: time.Millisecond,
		Metrics:      metrics,
	}
	if err := adapter.Run(ctx); err != nil {
		t.Fatalf("adapter Run() error = %v", err)
	}
	if consumer.fetchCalls != 2 {
		t.Fatalf("FetchMessage calls = %d, want 2 (one transient failure and one message)", consumer.fetchCalls)
	}
	if consumer.commitCalls != 2 {
		t.Fatalf("CommitMessages calls = %d, want 2 (retry the same source offset)", consumer.commitCalls)
	}
	if ingestCalls != 1 {
		t.Fatalf("ingest calls = %d, want 1; a commit retry must not redeliver an accepted record", ingestCalls)
	}

	metricsText := httptest.NewRecorder()
	metrics.RuntimeHandler().ServeHTTP(metricsText, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metricsText.Body.String(), "tuba_source_adapter_kafka_fetch_retries_total 1") {
		t.Fatalf("fetch retry metric missing: %s", metricsText.Body.String())
	}
	if !strings.Contains(metricsText.Body.String(), "tuba_source_adapter_offset_commit_failures_total 1") {
		t.Fatalf("commit retry metric missing: %s", metricsText.Body.String())
	}
}
