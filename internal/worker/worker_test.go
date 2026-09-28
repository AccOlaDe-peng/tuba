package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/indexing"
)

type fakeConsumer struct{ commits int }

func (f *fakeConsumer) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, nil
}
func (f *fakeConsumer) CommitMessages(context.Context, ...kafka.Message) error {
	f.commits++
	return nil
}

type fakeWriter struct {
	writes int
	err    error
}

func (f *fakeWriter) WriteMessages(context.Context, ...kafka.Message) error { f.writes++; return f.err }

type fakeSink struct {
	writes  int
	err     error
	results []indexing.Result
}

func (f *fakeSink) PutBatch(_ context.Context, docs []indexing.Document) ([]indexing.Result, error) {
	f.writes++
	if f.err != nil {
		return nil, f.err
	}
	if f.results != nil {
		return f.results, nil
	}
	out := make([]indexing.Result, len(docs))
	for i := range out {
		out[i].Status = 201
	}
	return out, nil
}

func TestCommitOnlyAfterSinkSuccess(t *testing.T) {
	consumer := &fakeConsumer{}
	sink := &fakeSink{err: errors.New("ES unavailable")}
	dlq := &fakeWriter{err: errors.New("Kafka unavailable")}
	w := Worker{Organization: "tenant_a", Consumer: consumer, DeadLetter: dlq, Sink: sink, MaxAttempts: 1}
	msg := kafka.Message{Value: []byte(`{"@timestamp":"2026-09-23T02:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"event-1","kind":"event","category":["authentication"],"action":"logon","outcome":"failure","dataset":"authentication"},"vendor":{"dataset":"windows.security"},"ueba":{"schema":{"version":"1.0.0"},"quality":{"status":"qualified"},"route":{"domain":"authentication"}}}`)}
	if err := w.process(context.Background(), msg); err == nil {
		t.Fatal("sink failure ignored")
	}
	if consumer.commits != 0 {
		t.Fatal("offset committed before Elasticsearch")
	}
	sink.err = nil
	dlq.err = nil
	if err := w.process(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if consumer.commits != 1 {
		t.Fatal("offset not committed after Elasticsearch")
	}
}

func TestDeadLetterBeforeCommit(t *testing.T) {
	consumer := &fakeConsumer{}
	dlq := &fakeWriter{err: errors.New("Kafka unavailable")}
	w := Worker{Organization: "tenant_a", Consumer: consumer, DeadLetter: dlq, Sink: &fakeSink{}}
	msg := kafka.Message{Value: []byte(`bad json`)}
	if err := w.process(context.Background(), msg); err == nil {
		t.Fatal("DLQ failure ignored")
	}
	if consumer.commits != 0 {
		t.Fatal("offset committed before DLQ")
	}
	dlq.err = nil
	if err := w.process(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if consumer.commits != 1 {
		t.Fatal("offset not committed after DLQ")
	}
}
func TestPartialBulkFailureRetriesOnlyRetryableItems(t *testing.T) {
	consumer := &fakeConsumer{}
	dlq := &fakeWriter{}
	sink := &sequenceSink{}
	w := Worker{Organization: "tenant_a", Consumer: consumer, DeadLetter: dlq, Sink: sink, MaxAttempts: 2, RetryBackoff: time.Nanosecond}
	valid := func(id string) kafka.Message {
		return kafka.Message{Topic: "events", Value: []byte(strings.Replace(`{"@timestamp":"2026-09-23T02:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"ID","kind":"event","category":["authentication"],"action":"logon","outcome":"failure","dataset":"authentication"},"vendor":{"dataset":"windows.security"},"ueba":{"schema":{"version":"1.0.0"},"quality":{"status":"qualified"},"route":{"domain":"authentication"}}}`, "ID", id, 1))}
	}
	if err := w.processBatch(context.Background(), []kafka.Message{valid("one"), valid("two")}); err != nil {
		t.Fatal(err)
	}
	if len(sink.sizes) != 2 || sink.sizes[0] != 2 || sink.sizes[1] != 1 {
		t.Fatalf("retry batch sizes = %v", sink.sizes)
	}
	if dlq.writes != 0 || consumer.commits != 1 {
		t.Fatalf("dlq=%d commits=%d", dlq.writes, consumer.commits)
	}
}

type sequenceSink struct{ sizes []int }

func (s *sequenceSink) PutBatch(_ context.Context, d []indexing.Document) ([]indexing.Result, error) {
	s.sizes = append(s.sizes, len(d))
	if len(s.sizes) == 1 {
		return []indexing.Result{{Status: 201}, {Status: 429, Retryable: true}}, nil
	}
	return []indexing.Result{{Status: 201}}, nil
}
