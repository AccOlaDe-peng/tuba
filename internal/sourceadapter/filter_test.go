package sourceadapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/rawevent"
	"tuba/product/internal/telemetry"
)

func shadowPolicy() *FilterPolicy {
	return &FilterPolicy{
		PolicyID: "zeek_noise", Version: "2026.10.02a", Mode: "shadow",
		Rules: []FilterRule{
			{ID: "dns_internal", Reason: "NOISE_INTERNAL_DNS", Equals: map[string]string{"event.dataset": "zeek.dns", "zeek.dns.query": "internal.example"}},
			{ID: "conn_icmp", Reason: "NOISE_ICMP", Equals: map[string]string{"event.dataset": "zeek.conn", "network.transport": "icmp"}},
		},
		Protected: []FilterRule{
			{ID: "log_clear", Reason: "PROTECTED_LOG_CLEAR", Equals: map[string]string{"event.dataset": "zeek.notice"}},
		},
	}
}

func TestFilterPolicyValidateRejectsAnythingButShadow(t *testing.T) {
	for _, mode := range []string{"", "keep", "enforce", "drop"} {
		policy := shadowPolicy()
		policy.Mode = mode
		err := policy.Validate()
		if err == nil {
			t.Fatalf("mode %q accepted; only shadow is publishable without shadow-count evidence", mode)
		}
		if !strings.Contains(err.Error(), "shadow") {
			t.Fatalf("mode %q rejection does not name the shadow gate: %v", mode, err)
		}
	}
}

func TestFilterPolicyValidateRejectsMalformedPolicies(t *testing.T) {
	cases := map[string]func(*FilterPolicy){
		"bad policy id":     func(p *FilterPolicy) { p.PolicyID = "Zeek Noise" },
		"empty version":     func(p *FilterPolicy) { p.Version = "" },
		"duplicate rule id": func(p *FilterPolicy) { p.Rules = append(p.Rules, p.Rules[0]) },
		"lowercase reason":  func(p *FilterPolicy) { p.Rules[0].Reason = "noise_internal_dns" },
		"no clauses":        func(p *FilterPolicy) { p.Rules[0].Equals = nil },
		"blank path":        func(p *FilterPolicy) { p.Rules[0].Equals = map[string]string{"  ": "x"} },
		"oversized value":   func(p *FilterPolicy) { p.Rules[0].Equals = map[string]string{"event.dataset": strings.Repeat("x", 513)} },
		"bad protected id":  func(p *FilterPolicy) { p.Protected[0].ID = "BAD" },
	}
	for name, mutate := range cases {
		policy := shadowPolicy()
		mutate(policy)
		if err := policy.Validate(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := shadowPolicy().Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
}

func TestFilterPolicyEvaluateMatchesFirstRuleInConfigOrder(t *testing.T) {
	policy := shadowPolicy()
	event := []byte(`{"event":{"dataset":"zeek.dns"},"zeek":{"dns":{"query":"internal.example"}},"network":{"transport":"udp"}}`)
	decision, err := policy.Evaluate(event)
	if err != nil {
		t.Fatal(err)
	}
	if decision.RuleID != "dns_internal" || decision.Reason != "NOISE_INTERNAL_DNS" || decision.Protected {
		t.Fatalf("decision=%+v, want first matching rule dns_internal", decision)
	}
}

func TestFilterPolicyEvaluateRequiresEveryClause(t *testing.T) {
	policy := shadowPolicy()
	event := []byte(`{"event":{"dataset":"zeek.dns"},"zeek":{"dns":{"query":"other.example"}}}`)
	decision, err := policy.Evaluate(event)
	if err != nil {
		t.Fatal(err)
	}
	if decision.RuleID != "" {
		t.Fatalf("decision=%+v, want no match when one clause differs", decision)
	}
}

// Beat payloads mix nested objects (event.dataset) with literal dotted keys
// (id.orig_h); both shapes must resolve.
func TestFilterPolicyEvaluateResolvesLiteralDottedKeys(t *testing.T) {
	policy := &FilterPolicy{
		PolicyID: "zeek_noise", Version: "v1", Mode: "shadow",
		Rules: []FilterRule{
			{ID: "dns_ptr", Reason: "NOISE_DNS_PTR", Equals: map[string]string{"event.dataset": "zeek.dns", "id.resp_h": "8.8.8.8", "qtype_name": "PTR"}},
		},
	}
	event := []byte(`{"event":{"dataset":"zeek.dns"},"id.orig_h":"10.6.69.144","id.resp_h":"8.8.8.8","qtype_name":"PTR"}`)
	decision, err := policy.Evaluate(event)
	if err != nil {
		t.Fatal(err)
	}
	if decision.RuleID != "dns_ptr" {
		t.Fatalf("decision=%+v, want dns_ptr via literal dotted keys", decision)
	}
	miss := []byte(`{"event":{"dataset":"zeek.dns"},"id.resp_h":"1.1.1.1","qtype_name":"PTR"}`)
	decision, err = policy.Evaluate(miss)
	if err != nil {
		t.Fatal(err)
	}
	if decision.RuleID != "" {
		t.Fatalf("decision=%+v, want no match for a different resolver", decision)
	}
}

func TestFilterPolicyEvaluateProtectedScenariosWinOverRules(t *testing.T) {
	policy := shadowPolicy()
	policy.Rules = append([]FilterRule{{ID: "catch_all", Reason: "NOISE_CATCH_ALL", Equals: map[string]string{"agent.type": "filebeat"}}}, policy.Rules...)
	event := []byte(`{"agent":{"type":"filebeat"},"event":{"dataset":"zeek.notice"}}`)
	decision, err := policy.Evaluate(event)
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Protected || decision.RuleID != "log_clear" || decision.Reason != "PROTECTED_LOG_CLEAR" {
		t.Fatalf("decision=%+v, want protected log_clear to beat the catch_all rule", decision)
	}
}

func TestFilterPolicyEvaluateRejectsNonObjectPayloads(t *testing.T) {
	policy := shadowPolicy()
	for _, payload := range [][]byte{[]byte(`not json`), []byte(`[1,2]`), nil} {
		if _, err := policy.Evaluate(payload); err == nil {
			t.Fatalf("payload %q evaluated without error", payload)
		}
	}
}

// The whole point of shadow mode: a matching event is counted against its rule
// and reason, and the delivery path — one ingest call, one commit, no DLQ — is
// byte-for-byte what it would be without the policy.
func TestShadowFilterCountsMatchAndDeliversUnchanged(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	body := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.conn"},"network":{"transport":"icmp"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/conn.log"},"offset":10118640}}`)
	message := kafka.Message{Topic: topic, Partition: 0, Offset: 42, Value: body}
	payloadHash, err := rawevent.CanonicalPayloadHash(body)
	if err != nil {
		t.Fatal(err)
	}
	var ingestCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ingestCalls++
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"receipt_id":        "raw:accepted-42",
			"raw_event_id":      "raw:accepted-42",
			"source_context_id": "ctx_0123456789abcdef0123456789abcdef",
			"source_position":   "filebeat-v1:2053:8926348:10118640",
			"delivery_position": "kafka-v1:" + topic + ":0:42",
			"payload_hash":      payloadHash,
			"status":            "accepted",
		})
	}))
	defer server.Close()

	consumer := &recordingConsumer{}
	dead := &recordingDeadLetter{}
	metrics := telemetry.New()
	adapter := Adapter{Binding: Binding{Topic: topic}, IngestURL: server.URL, AdapterToken: "adapter-token",
		Consumer: consumer, DeadLetter: dead, RetryBackoff: time.Millisecond, Metrics: metrics,
		HTTPClient: http.DefaultClient, Filter: shadowPolicy()}

	if err := adapter.processUntilCommitted(context.Background(), consumer, message); err != nil {
		t.Fatalf("deliver matched event: %v", err)
	}
	if ingestCalls != 1 {
		t.Fatalf("ingest calls=%d, want 1: a shadow match must still be delivered", ingestCalls)
	}
	if len(dead.messages) != 0 {
		t.Fatalf("DLQ writes=%d, want 0: shadow mode must not quarantine", len(dead.messages))
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commits=%d, want 1", consumer.commitCalls)
	}
	metricsText := httptest.NewRecorder()
	metrics.RuntimeHandler().ServeHTTP(metricsText, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	want := `tuba_source_adapter_filter_shadow_matches_total{policy_id="zeek_noise",version="2026.10.02a",rule_id="conn_icmp",reason="NOISE_ICMP"} 1`
	if !strings.Contains(metricsText.Body.String(), want) {
		t.Fatalf("shadow metric missing:\nwant: %s\ngot:\n%s", want, metricsText.Body.String())
	}
	if !strings.Contains(metricsText.Body.String(), "tuba_source_adapter_events_accepted_total 1") {
		t.Fatalf("accepted metric missing: %s", metricsText.Body.String())
	}
}

func TestShadowFilterCountsProtectedEventsSeparately(t *testing.T) {
	topic := "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
	body := []byte(`{"@timestamp":"2026-09-28T10:00:00Z","agent":{"type":"filebeat","version":"8.19.0","id":"beat-a"},"event":{"dataset":"zeek.notice"},"log":{"file":{"device_id":"2053","inode":"8926348","path":"/var/log/notice.log"},"offset":7}}`)
	message := kafka.Message{Topic: topic, Partition: 0, Offset: 9, Value: body}
	payloadHash, err := rawevent.CanonicalPayloadHash(body)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"receipt_id":        "raw:accepted-9",
			"raw_event_id":      "raw:accepted-9",
			"source_context_id": "ctx_0123456789abcdef0123456789abcdef",
			"source_position":   "filebeat-v1:2053:8926348:7",
			"delivery_position": "kafka-v1:" + topic + ":0:9",
			"payload_hash":      payloadHash,
			"status":            "accepted",
		})
	}))
	defer server.Close()

	metrics := telemetry.New()
	consumer := &recordingConsumer{}
	adapter := Adapter{Binding: Binding{Topic: topic}, IngestURL: server.URL, AdapterToken: "adapter-token",
		Consumer: consumer, DeadLetter: unusedDeadLetter{}, RetryBackoff: time.Millisecond, Metrics: metrics,
		HTTPClient: http.DefaultClient, Filter: shadowPolicy()}

	if err := adapter.processUntilCommitted(context.Background(), consumer, message); err != nil {
		t.Fatalf("deliver protected event: %v", err)
	}
	metricsText := httptest.NewRecorder()
	metrics.RuntimeHandler().ServeHTTP(metricsText, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	want := `tuba_source_adapter_filter_protected_total{policy_id="zeek_noise",version="2026.10.02a",rule_id="log_clear",reason="PROTECTED_LOG_CLEAR"} 1`
	if !strings.Contains(metricsText.Body.String(), want) {
		t.Fatalf("protected metric missing:\nwant: %s\ngot:\n%s", want, metricsText.Body.String())
	}
	if strings.Contains(metricsText.Body.String(), "filter_shadow_matches_total") {
		t.Fatalf("a protected event must not count as a shadow match: %s", metricsText.Body.String())
	}
}

func TestConfigValidateChecksEmbeddedFilterPolicy(t *testing.T) {
	cfg := Config{
		IngestURL: "http://127.0.0.1:9000/api/v1/internal/ingest/beat-events",
		Bindings:  []Binding{{Topic: "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"}},
		Filter:    &FilterPolicy{PolicyID: "zeek_noise", Version: "v1", Mode: "enforce", Rules: []FilterRule{{ID: "x", Reason: "NOISE_X", Equals: map[string]string{"a": "b"}}}},
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "filter policy") {
		t.Fatalf("config with an unpublishable filter accepted: %v", err)
	}
}
