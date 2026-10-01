package controlworker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/telemetry"
)

type fakeWriter struct {
	mu    sync.Mutex
	fn    func(kafka.Message) error
	seen  []kafka.Message
	calls int
}

func (w *fakeWriter) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	w.mu.Lock()
	w.calls++
	w.seen = append(w.seen, messages...)
	fn := w.fn
	w.mu.Unlock()
	for _, message := range messages {
		if fn != nil {
			if err := fn(message); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *fakeWriter) snapshot() []kafka.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]kafka.Message(nil), w.seen...)
}

func TestNormalizeOutboxConfigDefaults(t *testing.T) {
	c := normalizeOutboxConfig(OutboxConfig{})
	if c.BatchSize != 100 || c.PollInterval != 500*time.Millisecond || c.LeaseDuration != 30*time.Second ||
		c.PublishTimeout != 10*time.Second || c.MaxAttempts != 8 || c.MaxConcurrentKeys != 4 || c.StuckThreshold != 5*time.Minute {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	custom := normalizeOutboxConfig(OutboxConfig{BatchSize: 5, MaxAttempts: 2, MaxConcurrentKeys: 1, StuckThreshold: time.Second})
	if custom.BatchSize != 5 || custom.MaxAttempts != 2 || custom.MaxConcurrentKeys != 1 || custom.StuckThreshold != time.Second {
		t.Fatalf("explicit values must be kept: %+v", custom)
	}
}

func TestGroupByAggregateKey(t *testing.T) {
	record := func(producer, key string, sequence int64) outboxRecord {
		return outboxRecord{Producer: producer, AggregateKey: key, Sequence: sequence}
	}
	sorted := []outboxRecord{
		record("p", "a", 1), record("p", "a", 2), record("p", "a", 3),
		record("p", "b", 1),
		record("q", "a", 7), record("q", "a", 8),
	}
	groups := groupByAggregateKey(sorted)
	if len(groups) != 3 {
		t.Fatalf("groups=%d, want 3", len(groups))
	}
	var sizes []int
	for _, group := range groups {
		sizes = append(sizes, len(group))
	}
	if !reflect.DeepEqual(sizes, []int{3, 1, 2}) {
		t.Fatalf("group sizes=%v, want [3 1 2]", sizes)
	}
	// Sequence order must be preserved inside every group.
	for _, group := range groups {
		for i := 1; i < len(group); i++ {
			if group[i].Sequence <= group[i-1].Sequence {
				t.Fatalf("sequence order violated in group %+v", group)
			}
		}
	}
	if got := groups[2][0].Producer; got != "q" {
		t.Fatalf("producer is part of the grouping key: got %q", got)
	}
	if groupByAggregateKey(nil) != nil {
		t.Fatal("empty batch must yield no groups")
	}
}

func TestOutboxPublishRejectsInvalidJSON(t *testing.T) {
	writer := &fakeWriter{}
	p := OutboxPublisher{Config: normalizeOutboxConfig(OutboxConfig{Writer: writer})}
	err := p.publish(context.Background(), p.Config, outboxRecord{ID: 42, Payload: []byte("{not json")})
	if err == nil {
		t.Fatal("invalid payload must be rejected")
	}
	if writer.calls != 0 {
		t.Fatalf("writer must not be called for invalid payload, got %d calls", writer.calls)
	}
}

func TestOutboxPublishCarriesOrderingHeadersAndTimeout(t *testing.T) {
	writer := &fakeWriter{fn: func(m kafka.Message) error { return errors.New("broker down") }}
	cfg := normalizeOutboxConfig(OutboxConfig{Writer: writer, PublishTimeout: time.Second})
	p := OutboxPublisher{Config: cfg}
	record := outboxRecord{ID: 7, Producer: "detector", AggregateKey: "ent:abc", Sequence: 11, Topic: "tuba.test", MessageKey: []byte("k"), Payload: []byte(`{"ok":true}`)}
	if err := p.publish(context.Background(), cfg, record); err == nil {
		t.Fatal("writer error must propagate")
	}
	seen := writer.snapshot()
	if len(seen) != 1 {
		t.Fatalf("deliveries=%d, want 1", len(seen))
	}
	headers := map[string]string{}
	for _, h := range seen[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	want := map[string]string{"outbox-id": "7", "producer": "detector", "aggregate-key": "ent:abc", "aggregate-sequence": "11"}
	if !reflect.DeepEqual(headers, want) {
		t.Fatalf("headers=%v, want %v", headers, want)
	}
	if seen[0].Topic != "tuba.test" || string(seen[0].Key) != "k" || string(seen[0].Value) != `{"ok":true}` {
		t.Fatalf("message mismatch: %+v", seen[0])
	}
}

func TestOutboxMetricsAreNilSafe(t *testing.T) {
	p := OutboxPublisher{Config: OutboxConfig{}}
	p.inc("outbox_claimed_total")
	p.add("outbox_claimed_total", 3)
	p.set("outbox_pending_records", 9)

	metrics := telemetry.New()
	p = OutboxPublisher{Config: OutboxConfig{Metrics: metrics}}
	p.inc("outbox_dead_total")
	p.set("outbox_dead_records", 2)
	if metrics.Value("outbox_dead_total") != 1 || metrics.Value("outbox_dead_records") != 2 {
		t.Fatalf("metrics not recorded: dead_total=%d dead_records=%d",
			metrics.Value("outbox_dead_total"), metrics.Value("outbox_dead_records"))
	}
}
