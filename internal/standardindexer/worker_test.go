package standardindexer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/rawevent"
	"tuba/product/internal/sink"
	"tuba/product/internal/uim"
)

type batchSink struct {
	calls [][]map[string]any
	apply func(int, []map[string]any) ([]error, error)
}

func (s *batchSink) PutStandardBatch(_ context.Context, events []map[string]any) ([]error, error) {
	copyEvents := append([]map[string]any(nil), events...)
	s.calls = append(s.calls, copyEvents)
	return s.apply(len(s.calls), events)
}

type dlqWriter struct {
	messages []kafka.Message
	err      error
}

func (w *dlqWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	if w.err != nil {
		return w.err
	}
	w.messages = append(w.messages, messages...)
	return nil
}

type consumer struct {
	committed []kafka.Message
	err       error
}

func (c *consumer) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, errors.New("unused")
}
func (c *consumer) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	if c.err != nil {
		return c.err
	}
	c.committed = append(c.committed, messages...)
	return nil
}

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

func TestRunBoundsBatchByBytesAndCarriesNextMessage(t *testing.T) {
	messages := []kafka.Message{
		toMessage(t, standardEvent(t, "event-001"), 1),
		toMessage(t, standardEvent(t, "event-002"), 2),
		toMessage(t, standardEvent(t, "event-003"), 3),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := &queueConsumer{messages: messages, cancel: cancel, cancelAfter: 2}
	indexer := &batchSink{apply: func(_ int, events []map[string]any) ([]error, error) {
		return make([]error, len(events)), nil
	}}
	worker := Worker{
		Domain: "network", Organization: "tenant_a", Consumer: input,
		DeadLetter: &dlqWriter{}, Sink: indexer, BatchSize: 10,
		MaxBatchBytes: len(messages[0].Value), BatchWait: time.Hour,
	}
	if err := worker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(indexer.calls) != 2 || len(indexer.calls[0]) != 1 || len(indexer.calls[1]) != 1 {
		t.Fatalf("batch calls were not byte bounded: sizes=%v", []int{len(indexer.calls[0]), len(indexer.calls[1])})
	}
	if len(input.committed) != 2 || input.committed[0].Offset != 1 || input.committed[1].Offset != 2 {
		t.Fatalf("carry message offset/order changed: committed=%v", input.committed)
	}
}

func TestProcessBatchBulkIndexesValidEventsAndDLQsScopeMismatch(t *testing.T) {
	first := standardEvent(t, "event-1")
	second := standardEvent(t, "event-2")
	mismatch := standardEvent(t, "event-other-tenant")
	mismatch["organization"].(map[string]any)["id"] = "tenant_b"
	messages := []kafka.Message{toMessage(t, first, 1), toMessage(t, second, 2), toMessage(t, mismatch, 3)}
	dead := &dlqWriter{}
	commits := &consumer{}
	indexer := &batchSink{apply: func(_ int, events []map[string]any) ([]error, error) {
		if len(events) != 2 {
			t.Fatalf("bulk event count=%d, want 2", len(events))
		}
		return []error{nil, nil}, nil
	}}
	worker := Worker{Domain: "network", Organization: "tenant_a", Consumer: commits, DeadLetter: dead, Sink: indexer}
	if err := worker.processBatch(context.Background(), messages); err != nil {
		t.Fatal(err)
	}
	if len(indexer.calls) != 1 || len(dead.messages) != 1 || len(commits.committed) != 3 {
		t.Fatalf("bulk calls=%d DLQ=%d commits=%d", len(indexer.calls), len(dead.messages), len(commits.committed))
	}
}

func TestProcessBatchRetriesOnlyRetryableItem(t *testing.T) {
	messages := []kafka.Message{toMessage(t, standardEvent(t, "event-1"), 1), toMessage(t, standardEvent(t, "event-2"), 2)}
	dead := &dlqWriter{}
	commits := &consumer{}
	indexer := &batchSink{apply: func(call int, events []map[string]any) ([]error, error) {
		if call == 1 {
			return []error{nil, errors.New("temporary overload")}, nil
		}
		if len(events) != 1 {
			t.Fatalf("retry should contain only one event: %+v", events)
		}
		return []error{nil}, nil
	}}
	worker := Worker{Domain: "network", Organization: "tenant_a", Consumer: commits, DeadLetter: dead, Sink: indexer, RetryBackoff: time.Nanosecond}
	if err := worker.processBatch(context.Background(), messages); err != nil {
		t.Fatal(err)
	}
	if len(indexer.calls) != 2 || len(commits.committed) != 2 || len(dead.messages) != 0 {
		t.Fatalf("bulk calls=%d commits=%d DLQ=%d", len(indexer.calls), len(commits.committed), len(dead.messages))
	}
}

func TestProcessBatchDoesNotCommitWhenDeadLetterFails(t *testing.T) {
	dead := &dlqWriter{err: errors.New("DLQ unavailable")}
	commits := &consumer{}
	worker := Worker{Domain: "network", Organization: "tenant_a", Consumer: commits, DeadLetter: dead, Sink: &batchSink{apply: func(_ int, events []map[string]any) ([]error, error) {
		return make([]error, len(events)), nil
	}}}
	message := kafka.Message{Topic: "events", Partition: 0, Offset: 1, Value: []byte("not-json")}
	if err := worker.processBatch(context.Background(), []kafka.Message{message}); err == nil {
		t.Fatal("expected DLQ failure")
	}
	if len(commits.committed) != 0 {
		t.Fatal("offset committed although DLQ write failed")
	}
}

func TestProcessBatchDLQsEventIDContentConflict(t *testing.T) {
	dead := &dlqWriter{}
	commits := &consumer{}
	worker := Worker{Domain: "network", Organization: "tenant_a", Consumer: commits, DeadLetter: dead, Sink: &batchSink{apply: func(_ int, events []map[string]any) ([]error, error) {
		return []error{sink.PermanentIndexError{Code: "EVENT_ID_CONFLICT", Message: "same ID has different content"}}, nil
	}}}
	message := toMessage(t, standardEvent(t, "event-1"), 1)
	if err := worker.processBatch(context.Background(), []kafka.Message{message}); err != nil {
		t.Fatal(err)
	}
	if len(dead.messages) != 1 || len(commits.committed) != 1 {
		t.Fatalf("DLQ=%d commits=%d", len(dead.messages), len(commits.committed))
	}
	var failure struct {
		Failure struct {
			Code string `json:"code"`
		} `json:"failure"`
		Payload map[string]any `json:"payload"`
	}
	if err := json.Unmarshal(dead.messages[0].Value, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Failure.Code != "EVENT_ID_CONFLICT" {
		t.Fatalf("DLQ code=%q", failure.Failure.Code)
	}
	if _, leaksRaw := failure.Payload["vendor.payload"]; leaksRaw {
		t.Fatal("DLQ must not contain raw vendor payload")
	}
}

func standardEvent(t *testing.T, id string) map[string]any {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"ts": 1790416800.25, "uid": id, "id.orig_h": "192.0.2.10", "id.resp_h": "198.51.100.20", "proto": "tcp",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := rawevent.New(rawevent.TrustedSource{
		OrganizationID: "tenant_a", Namespace: "tenant_a", SourceInstanceID: "zeek-01",
		SourceContextID: "ctx_0123456789abcdef0123456789abcdef", SourceEpoch: "epoch-1",
		VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek.conn", ReleaseID: "zeek-v1",
	}, "file:conn.log:"+id, payload, time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	event, err := uim.Normalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	event["event"].(map[string]any)["id"] = id
	return event
}

func toMessage(t *testing.T, event map[string]any, offset int64) kafka.Message {
	t.Helper()
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "tuba.events.network.v1", Partition: 0, Offset: offset, Value: encoded}
}
