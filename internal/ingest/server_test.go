package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/rawevent"
)

func TestReadinessTracksDependencyRecovery(t *testing.T) {
	dependencyErr := errors.New("dependency unavailable")
	dependencyReady := false
	server := Server{
		RequestTimeout: time.Second,
		ReadyCheck: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("readiness dependency check has no deadline")
			}
			if !dependencyReady {
				return dependencyErr
			}
			return nil
		},
	}
	check := func(want int) {
		t.Helper()
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
		if response.Code != want {
			t.Fatalf("readiness status=%d, want %d", response.Code, want)
		}
	}

	check(http.StatusServiceUnavailable)
	dependencyReady = true
	check(http.StatusOK)
	dependencyReady = false
	check(http.StatusServiceUnavailable)
}

func TestReadinessWithoutDependencyCheckIsUnavailable(t *testing.T) {
	response := httptest.NewRecorder()
	Server{}.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

type staticTopicResolver map[string]RawSource

func (r staticTopicResolver) ResolveTopic(_ context.Context, topic string) (RawSource, error) {
	source, ok := r[topic]
	if !ok {
		return RawSource{}, errors.New("topic has no active source binding")
	}
	return source, nil
}

type memoryReceipt struct {
	hash    string
	encoded []byte
	acked   bool
}

type memoryRawReceipts struct{ items map[string]memoryReceipt }

func (s *memoryRawReceipts) GetOrCreate(_ context.Context, organization, source, rawID, hash, _ string, encoded []byte) ([]byte, bool, error) {
	if s.items == nil {
		s.items = make(map[string]memoryReceipt)
	}
	key := organization + "/" + source + "/" + rawID
	if existing, ok := s.items[key]; ok {
		if existing.hash != hash {
			return nil, false, ErrRawIDConflict
		}
		return existing.encoded, existing.acked, nil
	}
	s.items[key] = memoryReceipt{hash: hash, encoded: append([]byte(nil), encoded...)}
	return encoded, false, nil
}

func (s *memoryRawReceipts) MarkKafkaAcked(_ context.Context, organization, source, rawID string, _ time.Time) error {
	key := organization + "/" + source + "/" + rawID
	item, ok := s.items[key]
	if !ok {
		return errors.New("receipt not found")
	}
	item.acked = true
	s.items[key] = item
	return nil
}

type recordingProducer struct {
	topics   []string
	messages []kafka.Message
}

func (p *recordingProducer) WriteMessages(_ context.Context, topic string, messages ...kafka.Message) error {
	p.topics = append(p.topics, topic)
	p.messages = append(p.messages, messages...)
	return nil
}

// A raw envelope is only accepted downstream by an indexer whose configured
// namespace matches the envelope's. Sources in different namespaces therefore
// have to land on different physical raw topics; one shared topic cannot be
// consumed by both, and whichever indexer reads it rejects the other tenant.
func TestAcceptRawRoutesEachNamespaceToItsOwnRawTopic(t *testing.T) {
	const adapterToken = "test-adapter-token-with-more-than-32-characters"
	zeekTopic := "tuba.source.ctx_11111111111111111111111111111111.v1"
	windowsTopic := "tuba.source.ctx_22222222222222222222222222222222.v1"
	zeekSource := RawSource{
		OrganizationID: "zeek_ns", Namespace: "zeek_ns", SourceInstanceID: "src_11111111111111111111111111111111",
		SourceEpoch: "1", VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek.conn",
		ReleaseID: "release-1", SourceContextID: "ctx_11111111111111111111111111111111",
	}
	windowsSource := RawSource{
		OrganizationID: "tenant_a", Namespace: "tenant_a", SourceInstanceID: "src_22222222222222222222222222222222",
		SourceEpoch: "1", VendorName: "Microsoft", VendorProduct: "windows", VendorDataset: "windows.security",
		ReleaseID: "release-2", SourceContextID: "ctx_22222222222222222222222222222222",
	}
	producer := &recordingProducer{}
	server := Server{
		RawProducer: producer, RawTopicPattern: "tuba.collector.{namespace}.raw.v1",
		TopicResolver: staticTopicResolver{zeekTopic: zeekSource, windowsTopic: windowsSource},
		AdapterToken:  adapterToken, RawReceipts: &memoryRawReceipts{}, RequestTimeout: time.Second,
	}
	event := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/conn.log"},"offset":10118640}}`)
	for i, topic := range []string{zeekTopic, windowsTopic} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/ingest/beat-events", bytes.NewReader(event))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Source-Adapter-Token", adapterToken)
		req.Header.Set("X-Source-Topic", topic)
		req.Header.Set("X-Source-Partition", "0")
		req.Header.Set("X-Source-Offset", strconv.Itoa(100+i))
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, req)
		if response.Code != http.StatusAccepted {
			t.Fatalf("%s: status=%d body=%s", topic, response.Code, response.Body.String())
		}
	}
	want := []string{"tuba.collector.zeek_ns.raw.v1", "tuba.collector.tenant_a.raw.v1"}
	if len(producer.topics) != len(want) {
		t.Fatalf("raw writes=%d (%v), want %d", len(producer.topics), producer.topics, len(want))
	}
	for i := range want {
		if producer.topics[i] != want[i] {
			t.Fatalf("write %d went to %q, want %q", i, producer.topics[i], want[i])
		}
	}
}

// An ingest with no configured raw topic pattern must refuse the event. Falling
// through would produce to a nameless topic, turning a deployment mistake into
// silent data loss instead of a loud 503.
func TestAcceptRawRejectsUnconfiguredTopicPattern(t *testing.T) {
	const adapterToken = "test-adapter-token-with-more-than-32-characters"
	topic := "tuba.source.ctx_11111111111111111111111111111111.v1"
	source := RawSource{
		OrganizationID: "zeek_ns", Namespace: "zeek_ns", SourceInstanceID: "src_11111111111111111111111111111111",
		SourceEpoch: "1", VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek.conn",
		ReleaseID: "release-1", SourceContextID: "ctx_11111111111111111111111111111111",
	}
	producer := &recordingProducer{}
	server := Server{
		RawProducer: producer, RawTopicPattern: "",
		TopicResolver: staticTopicResolver{topic: source},
		AdapterToken:  adapterToken, RawReceipts: &memoryRawReceipts{}, RequestTimeout: time.Second,
	}
	event := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/conn.log"},"offset":10118640}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/ingest/beat-events", bytes.NewReader(event))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Source-Adapter-Token", adapterToken)
	req.Header.Set("X-Source-Topic", topic)
	req.Header.Set("X-Source-Partition", "0")
	req.Header.Set("X-Source-Offset", "42")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, req)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s; want 503", response.Code, response.Body.String())
	}
	if len(producer.messages) != 0 || len(producer.topics) != 0 {
		t.Fatalf("wrote %v despite an unconfigured topic pattern", producer.topics)
	}
}

// COL-03 的五个验收场景之一是“超大消息”。上限必须按大小拒绝，而不是截断
// 成一次成功：截断后的正文仍然会进 Raw，等于悄悄改了证据。
func TestBeatIngressRejectsAnOversizedPayload(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	const adapterToken = "test-adapter-token-with-more-than-32-characters"
	source := RawSource{
		OrganizationID: "tenant-a", Namespace: "tenant-a", SourceInstanceID: "src_0123456789abcdef0123456789abcdef",
		SourceEpoch: "epoch-1", VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek.conn",
		ReleaseID: "release-1", SourceContextID: "ctx_0123456789abcdef0123456789abcdef",
	}
	producer := &recordingProducer{}
	server := Server{RawProducer: producer, RawTopicPattern: "tuba.collector.raw.v1", TopicResolver: staticTopicResolver{topic: source},
		AdapterToken: adapterToken, RawReceipts: &memoryRawReceipts{}, RequestTimeout: time.Second}

	// Padded inside a string value so the body is over the ceiling *and* still
	// well-formed: a malformed body would be rejected for the other reason.
	body := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/conn.log"},"offset":10118640},"pad":"` +
		strings.Repeat("a", rawevent.MaxPayloadBytes) + `"}`)
	if len(body) <= rawevent.MaxPayloadBytes {
		t.Fatalf("test body is %d bytes, not over the %d byte ceiling", len(body), rawevent.MaxPayloadBytes)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/ingest/beat-events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Source-Adapter-Token", adapterToken)
	req.Header.Set("X-Source-Topic", topic)
	req.Header.Set("X-Source-Partition", "0")
	req.Header.Set("X-Source-Offset", "43")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, req)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d body=%s; want 413", response.Code, response.Body.String())
	}
	if len(producer.messages) != 0 {
		t.Fatalf("oversized payload produced %d Kafka writes; it must not be truncated into a success", len(producer.messages))
	}
}

func TestBeatIngressUsesRegisteredTopicBindingAndDeduplicatesReceipt(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	const adapterToken = "test-adapter-token-with-more-than-32-characters"
	source := RawSource{
		OrganizationID: "tenant-a", Namespace: "tenant-a", SourceInstanceID: "src_0123456789abcdef0123456789abcdef",
		SourceEpoch: "epoch-1", VendorName: "zeek", VendorProduct: "zeek", VendorDataset: "zeek.conn",
		ReleaseID: "release-1", SourceContextID: "ctx_0123456789abcdef0123456789abcdef",
	}
	producer := &recordingProducer{}
	receipts := &memoryRawReceipts{}
	server := Server{RawProducer: producer, RawTopicPattern: "tuba.collector.raw.v1", TopicResolver: staticTopicResolver{topic: source},
		AdapterToken: adapterToken, RawReceipts: receipts, RequestTimeout: time.Second}
	beatEvent := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/conn.log"},"offset":10118640},"organization":{"id":"attacker-controlled"}}`)
	request := func(body []byte, topicValue string, token string, offset string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/ingest/beat-events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Source-Adapter-Token", token)
		req.Header.Set("X-Source-Topic", topicValue)
		req.Header.Set("X-Source-Partition", "0")
		req.Header.Set("X-Source-Offset", offset)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, req)
		return response
	}

	first := request(beatEvent, topic, adapterToken, "42")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first receipt status=%d body=%s", first.Code, first.Body.String())
	}
	duplicate := request(beatEvent, topic, adapterToken, "42")
	if duplicate.Code != http.StatusAccepted || len(producer.messages) != 1 {
		t.Fatalf("duplicate status=%d writes=%d body=%s; want 202 and one Kafka write", duplicate.Code, len(producer.messages), duplicate.Body.String())
	}
	var envelope rawevent.Envelope
	if err := json.Unmarshal(producer.messages[0].Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Organization.ID != source.OrganizationID || envelope.SourceContextID != source.SourceContextID || envelope.SourcePosition != "filebeat-v1:2053:8926348:10118640" || envelope.DeliveryPosition != fmt.Sprintf("kafka-v1:%s:0:42", topic) {
		t.Fatalf("trusted envelope scope/position = %q/%q/%q", envelope.Organization.ID, envelope.SourceContextID, envelope.SourcePosition)
	}
	if got := string(envelope.Payload); !strings.Contains(got, "attacker-controlled") {
		t.Fatalf("raw payload was not retained: %s", got)
	}

	conflictEvent := append([]byte(nil), beatEvent...)
	conflictEvent = bytes.Replace(conflictEvent, []byte("attacker-controlled"), []byte("different-payload"), 1)
	conflict := request(conflictEvent, topic, adapterToken, "43")
	if conflict.Code != http.StatusConflict || len(producer.messages) != 1 {
		t.Fatalf("position conflict status=%d writes=%d; want 409 and no extra Kafka write", conflict.Code, len(producer.messages))
	}
	redelivery := request(beatEvent, topic, adapterToken, "43")
	if redelivery.Code != http.StatusAccepted || len(producer.messages) != 1 {
		t.Fatalf("cross-offset redelivery status=%d writes=%d; want idempotent 202 and one Kafka write", redelivery.Code, len(producer.messages))
	}
	var redeliveryReceipt map[string]any
	if err := json.Unmarshal(redelivery.Body.Bytes(), &redeliveryReceipt); err != nil {
		t.Fatal(err)
	}
	if redeliveryReceipt["source_position"] != envelope.SourcePosition || redeliveryReceipt["delivery_position"] != fmt.Sprintf("kafka-v1:%s:0:43", topic) {
		t.Fatalf("redelivery receipt positions=%v/%v", redeliveryReceipt["source_position"], redeliveryReceipt["delivery_position"])
	}
	missingCoordinates := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"}}`)
	if response := request(missingCoordinates, topic, adapterToken, "44"); response.Code != http.StatusBadRequest || len(producer.messages) != 1 {
		t.Fatalf("missing source coordinates status=%d writes=%d; want 400 without a Raw write", response.Code, len(producer.messages))
	}
	if unauthorized := request(beatEvent, topic, "wrong-token", "42"); unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("wrong adapter token status=%d, want 401", unauthorized.Code)
	}
	if unbound := request(beatEvent, "tuba.source.ctx_ffffffffffffffffffffffffffffffff.v1", adapterToken, "42"); unbound.Code != http.StatusServiceUnavailable {
		t.Fatalf("unbound topic status=%d, want 503", unbound.Code)
	}
}

func TestWindowsSecurityIngressRequiresStablePositionAndDeduplicatesAcrossOffsets(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	const adapterToken = "test-adapter-token-with-more-than-32-characters"
	source := RawSource{
		OrganizationID: "tenant-a", Namespace: "tenant-a", SourceInstanceID: "src_0123456789abcdef0123456789abcdef",
		SourceEpoch: "epoch-1", VendorName: "Microsoft", VendorProduct: "windows", VendorDataset: "windows.security",
		ReleaseID: "release-1", SourceContextID: "ctx_0123456789abcdef0123456789abcdef",
	}
	producer := &recordingProducer{}
	receipts := &memoryRawReceipts{}
	server := Server{RawProducer: producer, RawTopicPattern: "tuba.collector.raw.v1", TopicResolver: staticTopicResolver{topic: source},
		AdapterToken: adapterToken, RawReceipts: receipts, RequestTimeout: time.Second}
	beatEvent := []byte(`{"@timestamp":"2026-09-29T03:14:15.1234567Z","agent":{"type":"winlogbeat","version":"8.19.0","id":"beat-win139"},"event":{"dataset":"windows.security","original":"<Event/>"},"winlog":{"computer_name":"WIN-139","channel":"Security","record_id":"928144","event_id":"4624"}}`)
	request := func(body []byte, offset string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/internal/ingest/beat-events", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Source-Adapter-Token", adapterToken)
		req.Header.Set("X-Source-Topic", topic)
		req.Header.Set("X-Source-Partition", "0")
		req.Header.Set("X-Source-Offset", offset)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, req)
		return response
	}
	if response := request(beatEvent, "51"); response.Code != http.StatusAccepted {
		t.Fatalf("first receipt status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(beatEvent, "52"); response.Code != http.StatusAccepted || len(producer.messages) != 1 {
		t.Fatalf("cross-offset redelivery status=%d writes=%d body=%s", response.Code, len(producer.messages), response.Body.String())
	}
	var envelope rawevent.Envelope
	if err := json.Unmarshal(producer.messages[0].Value, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.SourcePosition != "winlogbeat-v1:77696e2d313339:7365637572697479:928144:2026-09-29T03:14:15.1234567Z" {
		t.Fatalf("Windows source position = %q", envelope.SourcePosition)
	}
	missingCoordinates := []byte(`{"@timestamp":"2026-09-29T03:14:15Z","agent":{"type":"winlogbeat","version":"8.19.0","id":"beat-win139"},"event":{"dataset":"windows.security"}}`)
	if response := request(missingCoordinates, "53"); response.Code != http.StatusBadRequest || len(producer.messages) != 1 {
		t.Fatalf("missing Windows source identity status=%d writes=%d", response.Code, len(producer.messages))
	}
}

// The ingest and the consumers of the raw topic read the same config struct. A
// pattern default that differs from the fixed-topic default would send every
// event to a topic nothing reads, so an unconfigured deployment has to fall back
// to the fixed topic rather than to a per-namespace name.
func TestResolveRawTopicPatternFallsBackToTheFixedTopic(t *testing.T) {
	got, err := ResolveRawTopicPattern("", "tuba.raw.events.v1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "tuba.raw.events.v1" {
		t.Fatalf("resolved %q, want the fixed topic so producers and consumers agree", got)
	}
}

// A pattern without the placeholder collapses every namespace onto one topic,
// which each namespace's indexers then reject. Failing at startup is louder than
// a config string that looks valid.
func TestResolveRawTopicPatternRejectsAPatternWithoutThePlaceholder(t *testing.T) {
	for _, pattern := range []string{"tuba.collector.raw.v1", "tuba.collector.${namespace}.raw.v1"} {
		if _, err := ResolveRawTopicPattern(pattern, "tuba.raw.events.v1"); err == nil {
			t.Fatalf("pattern %q was accepted without the {namespace} placeholder", pattern)
		}
	}
}

func TestResolveRawTopicPatternAcceptsAPerNamespacePattern(t *testing.T) {
	got, err := ResolveRawTopicPattern("tuba.collector.{namespace}.raw.live2.v1", "unused")
	if err != nil {
		t.Fatal(err)
	}
	if got != "tuba.collector.{namespace}.raw.live2.v1" {
		t.Fatalf("resolved %q, want the configured pattern", got)
	}
}
