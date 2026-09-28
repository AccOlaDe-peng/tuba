package sourceadapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/deadletter"
	"tuba/product/internal/telemetry"
)

const MaxEventBytes = 1 << 20

var contextPattern = regexp.MustCompile(`^ctx_[a-f0-9]{32}$`)

type Binding struct {
	Topic string `json:"topic"`
}

type Config struct {
	IngestURL string    `json:"ingest_url"`
	Bindings  []Binding `json:"bindings"`
}

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type Adapter struct {
	Binding      Binding
	IngestURL    string
	AdapterToken string
	Consumer     Consumer
	DeadLetter   Writer
	HTTPClient   *http.Client
	RetryBackoff time.Duration
	Metrics      *telemetry.Registry
}

func (c Config) Validate() error {
	u, err := url.Parse(c.IngestURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || u.Path != "/api/v1/internal/ingest/beat-events" {
		return errors.New("ingest_url must target /api/v1/internal/ingest/beat-events over HTTP(S), without query, userinfo or fragment")
	}
	if len(c.Bindings) == 0 {
		return errors.New("at least one source topic binding is required")
	}
	seen := make(map[string]struct{}, len(c.Bindings))
	for _, binding := range c.Bindings {
		if !validSourceTopic(binding.Topic) {
			return fmt.Errorf("topic %q is not a canonical source context topic", binding.Topic)
		}
		if _, ok := seen[binding.Topic]; ok {
			return fmt.Errorf("duplicate source topic binding %q", binding.Topic)
		}
		seen[binding.Topic] = struct{}{}
	}
	return nil
}

func (a Adapter) Run(ctx context.Context) error {
	if a.Consumer == nil || a.DeadLetter == nil {
		return errors.New("source adapter consumer and durable dead-letter writer are required")
	}
	if len(a.AdapterToken) < 32 {
		return errors.New("source adapter token must contain at least 32 characters")
	}
	if a.HTTPClient == nil {
		a.HTTPClient = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if a.RetryBackoff <= 0 {
		a.RetryBackoff = 200 * time.Millisecond
	}
	for ctx.Err() == nil {
		message, err := a.Consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch source event: %w", err)
		}
		a.inc("tuba_source_adapter_events_fetched_total")
		if message.Topic != "" && message.Topic != a.Binding.Topic {
			return errors.New("consumer returned a message outside its source topic binding")
		}
		if err := a.processUntilCommitted(ctx, message); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

func (a Adapter) processUntilCommitted(ctx context.Context, message kafka.Message) error {
	for attempt := 1; ctx.Err() == nil; attempt++ {
		permanent, code, err := a.deliver(ctx, message)
		if err == nil {
			if permanent {
				dead, buildErr := deadletter.Message(message, "source_adapter", code, code, false, attempt)
				if buildErr != nil {
					return fmt.Errorf("encode permanent source rejection: %w", buildErr)
				}
				if writeErr := a.DeadLetter.WriteMessages(ctx, dead); writeErr != nil {
					a.inc("tuba_source_adapter_dlq_write_failures_total")
					return fmt.Errorf("persist permanent source rejection before offset commit: %w", writeErr)
				}
				a.inc("tuba_source_adapter_dlq_written_total")
			}
			if commitErr := a.Consumer.CommitMessages(ctx, message); commitErr != nil {
				a.inc("tuba_source_adapter_offset_commit_failures_total")
				return fmt.Errorf("commit source topic offset after durable receipt: %w", commitErr)
			}
			a.inc("tuba_source_adapter_offsets_committed_total")
			if permanent {
				a.inc("tuba_source_adapter_events_rejected_total")
			} else {
				a.inc("tuba_source_adapter_events_accepted_total")
			}
			return nil
		}
		a.inc("tuba_source_adapter_delivery_retries_total")
		timer := time.NewTimer(a.RetryBackoff << min(attempt-1, 7))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func (a Adapter) inc(name string) {
	if a.Metrics != nil {
		a.Metrics.Inc(name)
	}
}

// deliver returns permanent=true only for locally detected invalid input. All HTTP
// failures keep the source offset uncommitted so authorization/configuration failures
// cannot silently skip events into a dead-letter stream.
func (a Adapter) deliver(ctx context.Context, message kafka.Message) (permanent bool, code string, err error) {
	if err := validateBeatEvent(message.Value); err != nil {
		return true, "BEAT_EVENT_INVALID", nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.IngestURL, bytes.NewReader(message.Value))
	if err != nil {
		return false, "", fmt.Errorf("build ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Source-Adapter-Token", a.AdapterToken)
	req.Header.Set("X-Source-Topic", a.Binding.Topic)
	req.Header.Set("X-Source-Partition", fmt.Sprintf("%d", message.Partition))
	req.Header.Set("X-Source-Offset", fmt.Sprintf("%d", message.Offset))
	response, err := a.HTTPClient.Do(req)
	if err != nil {
		return false, "", fmt.Errorf("post source event to ingest: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusAccepted {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
		if readErr != nil {
			return false, "", fmt.Errorf("read ingest receipt: %w", readErr)
		}
		if len(body) > 64<<10 {
			return false, "", errors.New("ingest receipt exceeds 64 KiB")
		}
		var receipt struct {
			ReceiptID       string `json:"receipt_id"`
			RawEventID      string `json:"raw_event_id"`
			SourceContextID string `json:"source_context_id"`
			SourcePosition  string `json:"source_position"`
			PayloadHash     string `json:"payload_hash"`
			Status          string `json:"status"`
		}
		if err := json.Unmarshal(body, &receipt); err != nil {
			return false, "", fmt.Errorf("decode ingest receipt: %w", err)
		}
		digest := sha256.Sum256(message.Value)
		contextID, ok := sourceContextFromTopic(a.Binding.Topic)
		position := fmt.Sprintf("kafka-v1:%s:%d:%d", a.Binding.Topic, message.Partition, message.Offset)
		if !ok || receipt.Status != "accepted" || receipt.ReceiptID == "" || receipt.ReceiptID != receipt.RawEventID ||
			receipt.SourceContextID != contextID || receipt.SourcePosition != position || receipt.PayloadHash != hex.EncodeToString(digest[:]) {
			return false, "", errors.New("ingest receipt does not match this source context, position and payload")
		}
		return false, "", nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return false, "", fmt.Errorf("ingest returned status %d; source offset remains uncommitted", response.StatusCode)
}

func validSourceTopic(topic string) bool {
	_, ok := sourceContextFromTopic(topic)
	return ok
}

// GroupID is stable for one immutable ingress topic and shared by all
// instances of its adapter consumer group.
func GroupID(topic string, suffix ...string) string {
	digest := sha256.Sum256([]byte(topic))
	groupID := "tuba-source-adapter-" + hex.EncodeToString(digest[:8])
	if len(suffix) > 0 && suffix[0] != "" {
		groupID += "-" + suffix[0]
	}
	return groupID
}

func sourceContextFromTopic(topic string) (string, bool) {
	const prefix, suffix = "tuba.source.", ".v1"
	if len(topic) <= len(prefix)+len(suffix) || topic[:len(prefix)] != prefix || topic[len(topic)-len(suffix):] != suffix {
		return "", false
	}
	contextID := topic[len(prefix) : len(topic)-len(suffix)]
	return contextID, contextPattern.MatchString(contextID)
}

func validateBeatEvent(value []byte) error {
	if len(value) == 0 || len(value) > MaxEventBytes {
		return errors.New("event size is outside the 1 MiB contract")
	}
	var event map[string]json.RawMessage
	if err := json.Unmarshal(value, &event); err != nil || event == nil {
		return errors.New("event must be one JSON object")
	}
	var timestamp string
	if err := json.Unmarshal(event["@timestamp"], &timestamp); err != nil {
		return errors.New("event @timestamp is required")
	}
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		return errors.New("event @timestamp must be RFC3339")
	}
	var agent struct {
		Type    string `json:"type"`
		Version string `json:"version"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal(event["agent"], &agent); err != nil || (agent.Type != "filebeat" && agent.Type != "winlogbeat") || agent.Version == "" || len(agent.Version) > 64 || agent.ID == "" || len(agent.ID) > 128 {
		return errors.New("event agent.type, agent.version and agent.id are required")
	}
	var sourceEvent struct {
		Dataset string `json:"dataset"`
	}
	if err := json.Unmarshal(event["event"], &sourceEvent); err != nil || sourceEvent.Dataset == "" || len(sourceEvent.Dataset) > 128 {
		return errors.New("event.dataset is required")
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
