package entityworker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/entity"
)

type fakeConsumer struct {
	mu        sync.Mutex
	messages  []kafka.Message
	committed []kafka.Message
	events    *[]string
}

func (f *fakeConsumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	if len(f.messages) > 0 {
		m := f.messages[0]
		f.messages = f.messages[1:]
		f.mu.Unlock()
		return m, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *fakeConsumer) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	*f.events = append(*f.events, "commit")
	f.committed = append(f.committed, msgs...)
	return nil
}

type fakeWriter struct {
	mu       sync.Mutex
	messages []kafka.Message
	err      error
	events   *[]string
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	*f.events = append(*f.events, "write")
	f.messages = append(f.messages, msgs...)
	return nil
}

type fakeProcessor struct {
	attrs []entity.RoleAttribution
	err   error
}

func (f fakeProcessor) Attribute(context.Context, string, map[string]any) ([]entity.RoleAttribution, error) {
	return f.attrs, f.err
}

func uimMessage(t *testing.T) kafka.Message {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:abc"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: body}
}

func sampleAttrs() []entity.RoleAttribution {
	ent := func(s string) string { return "ent:" + strings.Repeat(s, 64) }
	att := func(s string) string { return "att:" + strings.Repeat(s, 64) }
	ev := entity.Evidence{Identifiers: []entity.IdentifierEvidence{{Kind: entity.KindSID, Value: "S-1-5-21-1"}}}
	return []entity.RoleAttribution{
		{AttributionID: att("a"), EventID: "evt:abc", Role: entity.RoleActor, State: entity.StateResolved, EntityID: ent("1"), Confidence: 1, RuleVersion: entity.RoleMappingVersionV1, Evidence: ev},
		{AttributionID: att("b"), EventID: "evt:abc", Role: entity.RoleTarget, State: entity.StateResolved, EntityID: ent("2"), Confidence: 1, RuleVersion: entity.RoleMappingVersionV1, Evidence: ev},
		{AttributionID: att("c"), EventID: "evt:abc", Role: entity.RoleSourceDevice, State: entity.StateUnresolved, RuleVersion: entity.RoleMappingVersionV1, Evidence: entity.Evidence{Reason: entity.ReasonNoMatchingEntity}},
	}
}

func runOnce(t *testing.T, org string, consumer *fakeConsumer, writer *fakeWriter, processor Processor) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	worker := Worker{Organization: org, Domain: "authentication", Consumer: consumer, Processor: processor, Output: writer}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		writer.mu.Lock()
		n := len(writer.messages)
		writer.mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("worker exited before publishing: %v", err)
		case <-deadline:
			t.Fatal("worker produced nothing")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestWorkerPublishesOneMessagePerRoleThenCommits(t *testing.T) {
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{uimMessage(t)}, events: &events}
	writer := &fakeWriter{events: &events}
	runOnce(t, "org1", consumer, writer, fakeProcessor{attrs: sampleAttrs()})

	if len(writer.messages) != 3 {
		t.Fatalf("expected 3 contributions, got %d", len(writer.messages))
	}
	keys := map[string]string{}
	for _, m := range writer.messages {
		var c entity.Contribution
		if err := json.Unmarshal(m.Value, &c); err != nil {
			t.Fatal(err)
		}
		if string(m.Key) != c.PartitionKey {
			t.Fatalf("kafka key %q != body partition_key %q", m.Key, c.PartitionKey)
		}
		keys[c.Role] = string(m.Key)
		headers := map[string]string{}
		for _, h := range m.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers["schema-version"] != entity.AttributedSchemaVersion || headers["event-id"] != "evt:abc" ||
			headers["attribution-id"] != c.AttributionID || headers["role"] != c.Role || headers["state"] != c.State {
			t.Fatalf("headers incomplete: %v", headers)
		}
	}
	if keys[entity.RoleActor] != "org1:ent:"+strings.Repeat("1", 64) {
		t.Fatalf("actor key: %q", keys[entity.RoleActor])
	}
	if keys[entity.RoleTarget] == keys[entity.RoleActor] {
		t.Fatal("different entities must key independently")
	}
	if !strings.Contains(keys[entity.RoleSourceDevice], ":unresolved:") {
		t.Fatalf("unresolved contribution missing or keyed wrong: %q", keys[entity.RoleSourceDevice])
	}
	// commit happens only after the write was accepted
	if len(events) < 2 || events[0] != "write" || events[1] != "commit" {
		t.Fatalf("write must precede commit: %v", events)
	}
	if len(consumer.committed) != 1 {
		t.Fatalf("input not committed exactly once: %d", len(consumer.committed))
	}
}

func TestWorkerDoesNotCommitWhenWriteFails(t *testing.T) {
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{uimMessage(t)}, events: &events}
	writer := &fakeWriter{err: errors.New("broker down"), events: &events}
	worker := Worker{Organization: "org1", Domain: "authentication", Consumer: consumer, Processor: fakeProcessor{attrs: sampleAttrs()}, Output: writer}
	if err := worker.Run(context.Background()); err == nil {
		t.Fatal("write failure must surface")
	}
	if len(consumer.committed) != 0 {
		t.Fatal("offset committed despite failed publish")
	}
}

func TestWorkerDoesNotPublishWhenAttributionFails(t *testing.T) {
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{uimMessage(t)}, events: &events}
	writer := &fakeWriter{events: &events}
	worker := Worker{Organization: "org1", Domain: "authentication", Consumer: consumer,
		Processor: fakeProcessor{err: errors.New("registry unavailable")}, Output: writer}
	if err := worker.Run(context.Background()); err == nil {
		t.Fatal("attribution failure must surface")
	}
	if len(writer.messages) != 0 || len(consumer.committed) != 0 {
		t.Fatal("failure must not publish or commit")
	}
}

func TestWorkerRejectsUndecodableEvent(t *testing.T) {
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{{Value: []byte("not json")}}, events: &events}
	writer := &fakeWriter{events: &events}
	worker := Worker{Organization: "org1", Domain: "authentication", Consumer: consumer, Processor: fakeProcessor{attrs: sampleAttrs()}, Output: writer}
	if err := worker.Run(context.Background()); err == nil {
		t.Fatal("garbage input must fail closed")
	}
	if len(writer.messages) != 0 || len(consumer.committed) != 0 {
		t.Fatal("garbage input must not publish or commit")
	}
}

func TestEventSummaryEmbeddedOnOutboundContributions(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"@timestamp": "2026-10-12T01:00:00Z",
		"event":      map[string]any{"id": "evt:auth1", "outcome": "failure"},
		"host":       map[string]any{"id": "web01"},
		"source":     map[string]any{"ip": "10.0.0.7"},
		"ueba":       map[string]any{"quality": map[string]any{"status": "qualified"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	consumer := &fakeConsumer{messages: []kafka.Message{{Value: body}}, events: &events}
	writer := &fakeWriter{events: &events}
	runOnce(t, "org1", consumer, writer, fakeProcessor{attrs: sampleAttrs()})
	if len(writer.messages) == 0 {
		t.Fatal("no contributions published")
	}
	for _, m := range writer.messages {
		var c entity.Contribution
		if err := json.Unmarshal(m.Value, &c); err != nil {
			t.Fatal(err)
		}
		if c.Evidence.Event["outcome"] != "failure" || c.Evidence.Event["host_id"] != "web01" ||
			c.Evidence.Event["source_ip"] != "10.0.0.7" || c.Evidence.Event["quality"] != "qualified" {
			t.Fatalf("outbound contribution missing event summary: %v", c.Evidence.Event)
		}
	}
}

func TestEventSummaryOmittedWhenEventCarriesNoSemantics(t *testing.T) {
	if summary := EventSummary(map[string]any{"event": map[string]any{"id": "evt:abc"}}); summary != nil {
		t.Fatalf("expected nil summary, got %v", summary)
	}
}
