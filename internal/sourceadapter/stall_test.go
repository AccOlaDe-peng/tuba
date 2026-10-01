package sourceadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/rawevent"
	"tuba/product/internal/telemetry"
)

// blockedConsumer reproduces the kafka-go reader state after its topic was
// deleted: FetchMessage returns neither a message nor an error until the
// reader is closed, which is what made the adapter silent in COL-07b 4/5.
type blockedConsumer struct {
	done   chan struct{}
	mu     sync.Mutex
	closed bool
}

func newBlockedConsumer() *blockedConsumer {
	return &blockedConsumer{done: make(chan struct{})}
}

func (c *blockedConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case <-c.done:
		return kafka.Message{}, errors.New("consumer closed")
	}
}

func (c *blockedConsumer) CommitMessages(context.Context, ...kafka.Message) error {
	return nil
}

func (c *blockedConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		close(c.done)
	}
	return nil
}

func (c *blockedConsumer) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// revivedConsumer is the fresh consumer the factory builds once the topic has
// been recreated: it delivers from FirstOffset and then goes idle.
type revivedConsumer struct {
	message kafka.Message
	cancel  context.CancelFunc
	mu      sync.Mutex
	fetches int
}

func (c *revivedConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	c.mu.Lock()
	c.fetches++
	c.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (c *revivedConsumer) CommitMessages(_ context.Context, _ ...kafka.Message) error {
	c.cancel()
	return nil
}

// deliverMessage arranges for the first factory-built fetch to return the
// message by closing a gate the test controls.
type gatedRevivedConsumer struct {
	revivedConsumer
	gate <-chan struct{}
}

func (c *gatedRevivedConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	c.mu.Lock()
	c.fetches++
	fetches := c.fetches
	c.mu.Unlock()
	if fetches == 1 {
		select {
		case <-ctx.Done():
			return kafka.Message{}, ctx.Err()
		case <-c.gate:
			return c.message, nil
		}
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

type scriptedProber struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (p *scriptedProber) Probe(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.err
}

func (p *scriptedProber) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

func (p *scriptedProber) probeCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type logRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (l *logRecorder) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logRecorder) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func acceptingIngest(t *testing.T, topic string, body []byte) *httptest.Server {
	t.Helper()
	payloadHash, err := rawevent.CanonicalPayloadHash(body)
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"receipt_id":        "raw:accepted-0",
			"raw_event_id":      "raw:accepted-0",
			"source_context_id": "ctx_0123456789abcdef0123456789abcdef",
			"source_position":   "filebeat-v1:2053:8926348:10118640",
			"delivery_position": "kafka-v1:" + topic + ":0:0",
			"payload_hash":      payloadHash,
			"status":            "accepted",
		})
	}))
}

// COL-07b 4/5: deleting and recreating the source topic must not wedge the
// adapter until a restart. The watchdog notices the silent fetch, the probe
// confirms the topic is gone, the consumer is replaced, and once the topic is
// back the fresh reader delivers from FirstOffset without any process restart.
func TestRunSelfHealsAfterTopicDeletedAndRecreated(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	body := beatEventBody()
	message := kafka.Message{Topic: topic, Partition: 0, Offset: 0, Value: body}
	server := acceptingIngest(t, topic, body)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dead := newBlockedConsumer()
	gate := make(chan struct{})
	revived := &gatedRevivedConsumer{revivedConsumer{message: message, cancel: cancel}, gate}
	var factoryCalls int
	var factoryMu sync.Mutex
	factory := func(context.Context) (Consumer, error) {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		factoryCalls++
		return revived, nil
	}
	prober := &scriptedProber{err: kafka.UnknownTopicOrPartition}
	logs := &logRecorder{}
	metrics := telemetry.New()
	adapter := Adapter{
		Binding: Binding{Topic: topic}, IngestURL: server.URL + "/api/v1/internal/ingest/beat-events",
		AdapterToken: "test-adapter-token-with-more-than-32-characters",
		Consumer:     dead, DeadLetter: unusedDeadLetter{}, HTTPClient: server.Client(),
		RetryBackoff: time.Millisecond, StallWatchdog: 5 * time.Millisecond, StallLogInterval: 5 * time.Millisecond,
		Probe: prober, NewConsumer: factory, Metrics: metrics, Logf: logs.logf,
	}

	done := make(chan error, 1)
	go func() { done <- adapter.Run(ctx) }()

	// While the topic is missing the dead consumer is closed but the factory
	// must NOT run: a reader that joins the group before the recreate would get
	// a zero-partition assignment that nothing rebalances.
	deadline := time.Now().Add(2 * time.Second)
	for !dead.isClosed() {
		if time.Now().After(deadline) {
			t.Fatal("stalled consumer was not closed while the topic was missing")
		}
		time.Sleep(time.Millisecond)
	}
	factoryMu.Lock()
	early := factoryCalls
	factoryMu.Unlock()
	if early != 0 {
		t.Fatalf("factory ran %d times while the topic was still missing, want 0", early)
	}

	// The topic is "recreated": the probe sees it again and the fresh consumer
	// can now fetch from offset 0.
	prober.setErr(nil)
	deadline = time.Now().Add(2 * time.Second)
	for {
		factoryMu.Lock()
		replaced := factoryCalls
		factoryMu.Unlock()
		if replaced > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer was not replaced after the topic returned")
		}
		time.Sleep(time.Millisecond)
	}
	close(gate)

	if err := <-done; err != nil {
		t.Fatalf("adapter Run() error = %v", err)
	}
	if revived.fetches == 0 {
		t.Fatal("replacement consumer never fetched")
	}
	if logs.count("consumption stalled") == 0 {
		t.Fatal("stall was never logged; the adapter must not be silent")
	}
	if logs.count("consumer replaced") != 1 {
		t.Fatalf("replacement logs = %d, want 1", logs.count("consumer replaced"))
	}
	if logs.count("consumption recovered") != 1 {
		t.Fatalf("recovery logs = %d, want 1", logs.count("consumption recovered"))
	}
	metricsText := httptest.NewRecorder()
	metrics.RuntimeHandler().ServeHTTP(metricsText, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		"tuba_source_adapter_stall_events_total 1",
		"tuba_source_adapter_consumer_replacements_total 1",
		"tuba_source_adapter_events_accepted_total 1",
	} {
		if !strings.Contains(metricsText.Body.String(), want) {
			t.Fatalf("metric %q missing: %s", want, metricsText.Body.String())
		}
	}
	if !strings.Contains(metricsText.Body.String(), "tuba_source_adapter_topic_missing_events_total") {
		t.Fatalf("topic-missing metric missing: %s", metricsText.Body.String())
	}
}

// While the topic stays missing the adapter closes the dead consumer, does
// not build a replacement early (it would join with a zero-partition
// assignment), and keeps logging so an operator can see the binding is stuck.
func TestRunLogsPeriodicallyWhileTopicMissing(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	dead := newBlockedConsumer()
	var factoryCalls int
	var factoryMu sync.Mutex
	factory := func(context.Context) (Consumer, error) {
		factoryMu.Lock()
		defer factoryMu.Unlock()
		factoryCalls++
		return newBlockedConsumer(), nil
	}
	prober := &scriptedProber{err: kafka.UnknownTopicOrPartition}
	logs := &logRecorder{}
	adapter := Adapter{
		Binding: Binding{Topic: topic}, IngestURL: "http://127.0.0.1/api/v1/internal/ingest/beat-events",
		AdapterToken: "test-adapter-token-with-more-than-32-characters",
		Consumer:     dead, DeadLetter: unusedDeadLetter{}, HTTPClient: http.DefaultClient,
		RetryBackoff: time.Millisecond, StallWatchdog: 5 * time.Millisecond, StallLogInterval: 20 * time.Millisecond,
		Probe: prober, NewConsumer: factory, Metrics: telemetry.New(), Logf: logs.logf,
	}
	if err := adapter.Run(ctx); err != nil {
		t.Fatalf("adapter Run() error = %v", err)
	}
	if got := logs.count("consumption stalled"); got < 2 {
		t.Fatalf("stall log lines = %d, want periodic repeats while the topic is missing", got)
	}
	if !dead.isClosed() {
		t.Fatal("stalled consumer was not closed while the topic was missing")
	}
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if factoryCalls != 0 {
		t.Fatalf("factory calls = %d, want 0: no replacement may join before the topic returns", factoryCalls)
	}
}

// The error-shaped variant of topic deletion: fetch keeps returning
// UnknownTopicOrPartition, so the adapter closes the dead reader and replaces
// it once the probe sees the topic again, instead of retrying it forever.
func TestRunReplacesConsumerOnUnknownTopicFetchError(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	body := beatEventBody()
	message := kafka.Message{Topic: topic, Partition: 0, Offset: 0, Value: body}
	server := acceptingIngest(t, topic, body)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gate := make(chan struct{})
	close(gate)
	revived := &gatedRevivedConsumer{revivedConsumer{message: message, cancel: cancel}, gate}
	factoryCalls := 0
	factory := func(context.Context) (Consumer, error) {
		factoryCalls++
		return revived, nil
	}
	logs := &logRecorder{}
	metrics := telemetry.New()
	adapter := Adapter{
		Binding: Binding{Topic: topic}, IngestURL: server.URL + "/api/v1/internal/ingest/beat-events",
		AdapterToken: "test-adapter-token-with-more-than-32-characters",
		Consumer:     &unknownTopicConsumer{}, DeadLetter: unusedDeadLetter{}, HTTPClient: server.Client(),
		RetryBackoff: time.Millisecond, StallWatchdog: 5 * time.Millisecond, StallLogInterval: time.Millisecond,
		Probe: &scriptedProber{}, NewConsumer: factory, Metrics: metrics, Logf: logs.logf,
	}
	if err := adapter.Run(ctx); err != nil {
		t.Fatalf("adapter Run() error = %v", err)
	}
	if factoryCalls != 1 {
		t.Fatalf("factory calls = %d, want 1", factoryCalls)
	}
	if logs.count("consumption recovered") != 1 {
		t.Fatalf("recovery logs = %d, want 1", logs.count("consumption recovered"))
	}
}

type unknownTopicConsumer struct{}

func (unknownTopicConsumer) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, kafka.UnknownTopicOrPartition
}

func (unknownTopicConsumer) CommitMessages(context.Context, ...kafka.Message) error {
	return nil
}

func (unknownTopicConsumer) Close() error { return nil }

// The prober must only act on a definitive UnknownTopicOrPartition answer:
// treating a transient broker hiccup as deletion would swap healthy consumers.
func TestTopicProberProbe(t *testing.T) {
	present := TopicProber{Topic: "t", ReadPartitions: func(context.Context, string) ([]kafka.Partition, error) {
		return []kafka.Partition{{Topic: "t", ID: 0}}, nil
	}}
	if err := present.Probe(context.Background()); err != nil {
		t.Fatalf("present topic probe error = %v, want nil", err)
	}
	transient := TopicProber{Topic: "t", ReadPartitions: func(context.Context, string) ([]kafka.Partition, error) {
		return nil, errors.New("broker unreachable")
	}}
	if err := transient.Probe(context.Background()); err != nil {
		t.Fatalf("transient failure probe error = %v, want nil", err)
	}
	deleted := TopicProber{Topic: "t", ReadPartitions: func(context.Context, string) ([]kafka.Partition, error) {
		return nil, kafka.UnknownTopicOrPartition
	}}
	if err := deleted.Probe(context.Background()); !errors.Is(err, kafka.UnknownTopicOrPartition) {
		t.Fatalf("deleted topic probe error = %v, want UnknownTopicOrPartition", err)
	}
}
