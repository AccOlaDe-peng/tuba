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
	"log"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/deadletter"
	"tuba/product/internal/rawevent"
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

// Prober reports whether the binding's source topic definitively does not
// exist in the broker metadata. It must return nil unless a broker answered
// UnknownTopicOrPartition — reachability problems are not proof of deletion
// and swapping the consumer cannot fix them.
type Prober interface {
	Probe(ctx context.Context) error
}

// TopicProber confirms a source topic was deleted by asking the broker
// metadata. It reports an error only on a definitive UnknownTopicOrPartition
// answer; reachability problems return nil because replacing the consumer
// cannot fix them and must not be triggered by a transient broker hiccup.
type TopicProber struct {
	Topic          string
	ReadPartitions func(ctx context.Context, topic string) ([]kafka.Partition, error)
}

func (p TopicProber) Probe(ctx context.Context) error {
	if p.ReadPartitions == nil {
		return nil
	}
	_, err := p.ReadPartitions(ctx, p.Topic)
	if errors.Is(err, kafka.UnknownTopicOrPartition) {
		return err
	}
	return nil
}

// ConsumerFactory builds a fresh consumer for the same binding. The adapter
// uses it to replace a consumer whose topic was deleted underneath it, which
// mirrors the process-restart recovery path without a restart.
type ConsumerFactory func(ctx context.Context) (Consumer, error)

type Adapter struct {
	Binding      Binding
	IngestURL    string
	AdapterToken string
	Consumer     Consumer
	DeadLetter   Writer
	HTTPClient   *http.Client
	RetryBackoff time.Duration
	Metrics      *telemetry.Registry
	// Probe, NewConsumer and the stall knobs detect and recover from the
	// kafka-go reader going permanently silent after its topic is deleted:
	// FetchMessage then returns neither a message nor an error, so nothing
	// retries, logs or reconnects until the process is restarted.
	Probe            Prober
	NewConsumer      ConsumerFactory
	StallWatchdog    time.Duration
	StallLogInterval time.Duration
	Logf             func(format string, args ...any)
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
	if a.StallWatchdog <= 0 {
		a.StallWatchdog = time.Minute
	}
	if a.StallLogInterval <= 0 {
		a.StallLogInterval = time.Minute
	}
	if a.Logf == nil {
		a.Logf = log.Printf
	}
	consumer := a.Consumer
	for ctx.Err() == nil {
		message, current, err := a.fetchUntilAvailable(ctx, consumer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		// The consumer may have been replaced while recovering from a deleted
		// topic; the offset has to be committed through the reader that fetched
		// the message, because kafka-go ties commit state to the reader.
		consumer = current
		a.inc("tuba_source_adapter_events_fetched_total")
		if message.Topic != "" && message.Topic != a.Binding.Topic {
			return errors.New("consumer returned a message outside its source topic binding")
		}
		if err := a.processUntilCommitted(ctx, consumer, message); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
	return nil
}

func (a Adapter) processUntilCommitted(ctx context.Context, consumer Consumer, message kafka.Message) error {
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
			if commitErr := a.commitUntilSuccessful(ctx, consumer, message); commitErr != nil {
				return commitErr
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

type fetchResult struct {
	message kafka.Message
	err     error
}

// fetchAsync runs a blocking FetchMessage in the background so the caller can
// also watch the clock: after a topic deletion kafka-go can block here forever
// without ever returning an error, and a plain retry loop around FetchMessage
// never gets a chance to notice.
func fetchAsync(ctx context.Context, consumer Consumer) <-chan fetchResult {
	result := make(chan fetchResult, 1)
	go func() {
		message, err := consumer.FetchMessage(ctx)
		result <- fetchResult{message: message, err: err}
	}()
	return result
}

type stallTracker struct {
	active  bool
	since   time.Time
	lastLog time.Time
}

// fetchUntilAvailable waits for the next message and recovers from a deleted
// source topic at runtime. A deleted topic shows up in two shapes: fetch
// errors (UnknownTopicOrPartition) or a fetch that returns nothing at all.
// The watchdog catches the silent shape, the probe confirms the topic is
// really gone (an idle topic must not trigger anything), and the consumer is
// then closed and only re-created once the probe sees the topic again — a
// reader that joins the group while the topic is absent gets a zero-partition
// assignment that nothing rebalances, so the replacement has to wait for the
// recreate, after which it starts at FirstOffset like a process restart.
func (a Adapter) fetchUntilAvailable(ctx context.Context, consumer Consumer) (kafka.Message, Consumer, error) {
	delay := a.RetryBackoff
	awaitingReturn := false
	var stall stallTracker
	result := fetchAsync(ctx, consumer)
	watchdog := time.NewTimer(a.StallWatchdog)
	defer watchdog.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return kafka.Message{}, nil, ctx.Err()
		case res := <-result:
			if res.err == nil {
				if stall.active {
					a.logf("source adapter topic=%s consumption recovered after %s stalled", a.Binding.Topic, time.Since(stall.since).Round(time.Second))
				}
				return res.message, consumer, nil
			}
			if ctx.Err() != nil {
				return kafka.Message{}, nil, ctx.Err()
			}
			a.inc("tuba_source_adapter_kafka_fetch_retries_total")
			a.noteStall(&stall, fmt.Sprintf("fetch error: %v", res.err))
			if errors.Is(res.err, kafka.UnknownTopicOrPartition) {
				a.dropMissingTopicConsumer(consumer, &awaitingReturn)
			}
			if err := waitRetry(ctx, delay); err != nil {
				return kafka.Message{}, nil, err
			}
			delay = nextRetryDelay(delay)
			if awaitingReturn {
				result = nil
			} else {
				result = fetchAsync(ctx, consumer)
			}
			resetTimer(watchdog, a.StallWatchdog)
		case <-watchdog.C:
			if a.Probe != nil {
				probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				probeErr := a.Probe.Probe(probeCtx)
				cancel()
				if probeErr != nil {
					a.inc("tuba_source_adapter_topic_missing_events_total")
					a.noteStall(&stall, fmt.Sprintf("topic does not exist in broker metadata: %v", probeErr))
					a.dropMissingTopicConsumer(consumer, &awaitingReturn)
				} else if awaitingReturn {
					if next, ok := a.recreateConsumer(ctx, &stall); ok {
						consumer = next
						awaitingReturn = false
						result = fetchAsync(ctx, consumer)
					}
				}
			}
			watchdog.Reset(a.StallWatchdog)
		}
	}
	return kafka.Message{}, nil, ctx.Err()
}

// dropMissingTopicConsumer closes the reader whose topic was deleted so its
// blocked FetchMessage returns, once per stall. No fetch runs while the topic
// is missing; the watchdog keeps probing the metadata instead.
func (a Adapter) dropMissingTopicConsumer(consumer Consumer, awaitingReturn *bool) {
	if *awaitingReturn {
		return
	}
	if closer, ok := consumer.(io.Closer); ok {
		_ = closer.Close()
	}
	*awaitingReturn = true
	a.logf("source adapter topic=%s consumer closed; probing broker metadata until the topic is recreated", a.Binding.Topic)
}

// recreateConsumer builds the replacement reader once the topic exists again.
// Because KRaft deleted the group's committed offsets together with the topic,
// the fresh reader starts at FirstOffset exactly like a restarted process.
func (a Adapter) recreateConsumer(ctx context.Context, stall *stallTracker) (Consumer, bool) {
	if a.NewConsumer == nil {
		return nil, false
	}
	fresh, err := a.NewConsumer(ctx)
	if err != nil {
		a.logf("source adapter topic=%s cannot replace stalled consumer: %v", a.Binding.Topic, err)
		return nil, false
	}
	a.inc("tuba_source_adapter_consumer_replacements_total")
	a.logf("source adapter topic=%s topic is back; consumer replaced after %s stalled, resuming at FirstOffset", a.Binding.Topic, time.Since(stall.since).Round(time.Second))
	return fresh, true
}

func (a Adapter) noteStall(stall *stallTracker, reason string) {
	now := time.Now()
	if !stall.active {
		stall.active, stall.since, stall.lastLog = true, now, now
		a.inc("tuba_source_adapter_stall_events_total")
		a.logf("source adapter topic=%s consumption stalled: %s", a.Binding.Topic, reason)
		return
	}
	if now.Sub(stall.lastLog) >= a.StallLogInterval {
		stall.lastLog = now
		a.logf("source adapter topic=%s consumption stalled: %s, waiting %s", a.Binding.Topic, reason, now.Sub(stall.since).Round(time.Second))
	}
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func (a Adapter) logf(format string, args ...any) {
	if a.Logf != nil {
		a.Logf(format, args...)
	}
}

func (a Adapter) commitUntilSuccessful(ctx context.Context, consumer Consumer, message kafka.Message) error {
	delay := a.RetryBackoff
	for ctx.Err() == nil {
		if err := consumer.CommitMessages(ctx, message); err == nil {
			return nil
		}
		a.inc("tuba_source_adapter_offset_commit_failures_total")
		if err := waitRetry(ctx, delay); err != nil {
			return err
		}
		delay = nextRetryDelay(delay)
	}
	return ctx.Err()
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = 200 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextRetryDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 200 * time.Millisecond
	}
	if delay >= 30*time.Second {
		return 30 * time.Second
	}
	delay *= 2
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
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
			ReceiptID        string `json:"receipt_id"`
			RawEventID       string `json:"raw_event_id"`
			SourceContextID  string `json:"source_context_id"`
			SourcePosition   string `json:"source_position"`
			DeliveryPosition string `json:"delivery_position"`
			PayloadHash      string `json:"payload_hash"`
			Status           string `json:"status"`
		}
		if err := json.Unmarshal(body, &receipt); err != nil {
			return false, "", fmt.Errorf("decode ingest receipt: %w", err)
		}
		// The receipt carries the canonical hash, so the check has to recompute
		// it the same way rather than over the raw bytes.
		payloadHash, hashErr := rawevent.CanonicalPayloadHash(message.Value)
		if hashErr != nil {
			return false, "", errors.New("source event is not valid JSON")
		}
		contextID, ok := sourceContextFromTopic(a.Binding.Topic)
		deliveryPosition := fmt.Sprintf("kafka-v1:%s:%d:%d", a.Binding.Topic, message.Partition, message.Offset)
		position := rawevent.StableBeatPosition(message.Value, deliveryPosition)
		if !ok || receipt.Status != "accepted" || receipt.ReceiptID == "" || receipt.ReceiptID != receipt.RawEventID ||
			receipt.SourceContextID != contextID || receipt.SourcePosition != position || receipt.DeliveryPosition != deliveryPosition ||
			receipt.PayloadHash != payloadHash {
			return false, "", errors.New("ingest receipt does not match this source context, position and payload")
		}
		return false, "", nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if permanentIngestRejection(response.StatusCode) {
		return true, fmt.Sprintf("INGEST_REJECTED_%d", response.StatusCode), nil
	}
	return false, "", fmt.Errorf("ingest returned status %d; source offset remains uncommitted", response.StatusCode)
}

// permanentIngestRejection reports whether the ingest examined this event and
// will never accept it. Retrying such a record holds the source offset forever
// and stalls every later event on the partition, so it is quarantined instead.
// Routing and throttling failures are excluded: those are deployment conditions
// a retry can outlive, and quarantining them would drop the whole stream rather
// than the one bad record.
//
// 403 is permanent. It carries one specific meaning from the ingest — the
// platform identified this source and refused it — and no amount of retrying
// changes that answer. Holding it instead means the records sit in the topic
// until the 24-hour retention deletes them, with nothing anywhere recording that
// they were refused, which is how a revoked source used to disappear silently.
// Quarantining keeps the bytes and raises events_rejected_total, so the refusal
// is loud and replayable. 401 stays retryable: an unidentified source may be a
// credential rotation in flight, which a retry does outlive.
//
// A 400 from the ingest covers both a single unacceptable record and a collector
// that has not been configured to emit the coordinates its source kind requires.
// Both are quarantined rather than retried: retrying a record whose bytes cannot
// change stalls the partition behind it, which is the failure this classification
// exists to prevent. Quarantining keeps the bytes in the 24-hour dead-letter
// topic and raises events_rejected_total, so a misconfigured collector is loud
// and recoverable rather than silent, and the partition keeps moving.
func permanentIngestRejection(status int) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusNotFound,
		http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return status >= 400 && status < 500
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
