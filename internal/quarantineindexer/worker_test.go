package quarantineindexer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/sink"
	"tuba/product/internal/telemetry"
	"tuba/product/internal/uim"
)

type queueConsumer struct {
	messages    []kafka.Message
	committed   []kafka.Message
	cancel      context.CancelFunc
	cancelAfter int
}

func (c *queueConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	default:
	}
	if len(c.messages) == 0 {
		return kafka.Message{}, errors.New("no test message available")
	}
	message := c.messages[0]
	c.messages = c.messages[1:]
	return message, nil
}

func (c *queueConsumer) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	c.committed = append(c.committed, messages...)
	if c.cancelAfter > 0 && len(c.committed) >= c.cancelAfter {
		c.cancel()
	}
	return nil
}

type dlqWriter struct {
	messages []kafka.Message
}

func (w *dlqWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	return nil
}

type quarantineSink struct {
	calls int
	apply func(int) error
}

func (s *quarantineSink) PutQuarantine(_ context.Context, _ uim.Quarantine) error {
	s.calls++
	return s.apply(s.calls)
}

func quarantineMessage(t *testing.T, id string, offset int64) kafka.Message {
	t.Helper()
	record := uim.Quarantine{
		ID:             id,
		OrganizationID: "tenant_a",
		Namespace:      "tenant_a",
		RawEventID:     "raw-" + id,
		Stage:          "UIM",
		Code:           "UIM_TLS_SERVER_NAME_REQUIRED",
		OccurredAt:     time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "tuba.collector.tenant_a.quarantine.v1", Partition: 0, Offset: offset, Value: encoded}
}

func assertMetrics(t *testing.T, metrics *telemetry.Registry, want ...string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, line := range want {
		if !strings.Contains(body, line) {
			t.Fatalf("metric %q missing from:\n%s", line, body)
		}
	}
}

func TestRunIndexesValidRecordAndDLQsInvalidWithMetrics(t *testing.T) {
	messages := []kafka.Message{
		quarantineMessage(t, "quar-1", 1),
		{Topic: "tuba.collector.tenant_a.quarantine.v1", Partition: 0, Offset: 2, Value: []byte("not-json")},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := &queueConsumer{messages: messages, cancel: cancel, cancelAfter: 2}
	dead := &dlqWriter{}
	indexer := &quarantineSink{apply: func(int) error { return nil }}
	metrics := telemetry.New()
	worker := Worker{Organization: "tenant_a", Namespace: "tenant_a", Consumer: input, DeadLetter: dead, Sink: indexer, Metrics: metrics}
	if err := worker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if indexer.calls != 1 || len(dead.messages) != 1 || len(input.committed) != 2 {
		t.Fatalf("sink calls=%d DLQ=%d commits=%d", indexer.calls, len(dead.messages), len(input.committed))
	}
	assertMetrics(t, metrics, "tuba_quarantine_indexer_indexed_total 1", "tuba_quarantine_indexer_invalid_total 1")
}

func TestRunRecordsRetryAndPermanentFailureMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := &queueConsumer{messages: []kafka.Message{quarantineMessage(t, "quar-1", 1)}, cancel: cancel, cancelAfter: 1}
	dead := &dlqWriter{}
	indexer := &quarantineSink{apply: func(call int) error {
		if call == 1 {
			return errors.New("temporary overload")
		}
		return sink.PermanentIndexError{Code: "QUARANTINE_INDEX_CONFLICT", Message: "conflict"}
	}}
	metrics := telemetry.New()
	worker := Worker{Organization: "tenant_a", Namespace: "tenant_a", Consumer: input, DeadLetter: dead, Sink: indexer, Metrics: metrics}
	if err := worker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if indexer.calls != 2 || len(dead.messages) != 1 || len(input.committed) != 1 {
		t.Fatalf("sink calls=%d DLQ=%d commits=%d", indexer.calls, len(dead.messages), len(input.committed))
	}
	assertMetrics(t, metrics, "tuba_quarantine_indexer_retry_total 1", "tuba_quarantine_indexer_index_failed_total 1")
}
