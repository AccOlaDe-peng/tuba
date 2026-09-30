package ingest

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tuba/product/internal/rawevent"
	"tuba/product/internal/telemetry"

	"github.com/segmentio/kafka-go"
)

// Producer writes one batch to an explicit topic. The topic travels with the
// call because a single ingest serves every namespace, and the raw topic is
// scoped per namespace.
type Producer interface {
	WriteMessages(context.Context, string, ...kafka.Message) error
}

type Server struct {
	RawProducer Producer
	// RawTopicPattern is the physical raw topic, with {namespace} standing in
	// for the source's namespace (tuba.collector.{namespace}.raw.v1). A pattern
	// without the placeholder keeps the older fixed-topic behaviour.
	RawTopicPattern string
	SourceResolver  SourceResolver
	TopicResolver   TopicSourceResolver
	AdapterToken    string
	RawReceipts     RawReceiptStore
	Limiter         *Limiter
	SourceLimiters  *SourceLimiters
	Metrics         *telemetry.Registry
	ReadyCheck      func(context.Context) error
	RequestTimeout  time.Duration
}

// ResolveRawTopicPattern validates the configured raw topic pattern and supplies
// the fallback an unconfigured deployment needs.
//
// The ingest and the consumers of the raw topic are separate binaries sharing one
// config struct. A pattern default that differs from the fixed-topic default
// would send every event to a namespace-scoped topic while every consumer still
// read the fixed one, so an unconfigured deployment falls back to the fixed
// topic instead. A configured pattern must contain the placeholder: without it
// every namespace collapses onto one topic, which each namespace's own indexers
// then reject, and nothing surfaces the mistake until the dead-letter stream
// fills.
func ResolveRawTopicPattern(pattern, fixedTopic string) (string, error) {
	if pattern == "" {
		if fixedTopic == "" {
			return "", errors.New("neither KAFKA_RAW_TOPIC nor KAFKA_RAW_TOPIC_PATTERN is configured")
		}
		return fixedTopic, nil
	}
	// ${namespace} contains {namespace}, so a plain substring check would pass and
	// substitution would leave a stray dollar in the topic name -- a name no
	// consumer reads. Only the bare placeholder is accepted.
	if !strings.Contains(pattern, "{namespace}") || strings.Contains(pattern, "${namespace}") {
		return "", errors.New("KAFKA_RAW_TOPIC_PATTERN must contain the {namespace} placeholder and no other form of it")
	}
	return pattern, nil
}

// rawTopicFor resolves the raw topic for one source namespace. Envelopes carry
// the namespace they were produced under, and each consumer validates against
// its own, so routing them to a shared topic makes every downstream indexer
// reject the namespaces it does not own.
func rawTopicFor(pattern, namespace string) (string, error) {
	if pattern == "" || namespace == "" {
		return "", errors.New("raw topic pattern or namespace is empty")
	}
	topic := strings.ReplaceAll(pattern, "{namespace}", namespace)
	// Kafka caps topic names at 249 characters.
	if len(topic) > 249 {
		return "", errors.New("resolved raw topic is too long")
	}
	return topic, nil
}

type RawSource struct {
	OrganizationID, Namespace, SourceInstanceID, SourceEpoch string
	VendorName, VendorProduct, VendorDataset                 string
	ReleaseID                                                string
	SourceContextID                                          string
	RateLimit                                                int
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if s.ReadyCheck == nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := s.ReadyCheck(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.HandleFunc("POST /api/v1/ingest/events", s.ingestRaw)
	mux.HandleFunc("POST /api/v1/internal/ingest/beat-events", s.ingestBeat)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Handler())
	}
	return telemetry.RequestTimeout(mux, s.RequestTimeout)
}

func (s Server) ingestRaw(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	contextID := r.Header.Get("X-Source-Context")
	if contextID == "" || len(contextID) > 128 {
		http.Error(w, "X-Source-Context is required", http.StatusBadRequest)
		return
	}
	var source RawSource
	if s.SourceResolver != nil {
		resolved, err := s.SourceResolver.ResolveSource(r.Context(), r.Header.Get("X-API-Key"), contextID)
		switch {
		case errors.Is(err, ErrSourceRevoked):
			// The platform identified this source and refused it. Retrying cannot
			// change that answer, so the caller must quarantine the record and
			// advance; holding it would hand it to the topic's retention window
			// with nothing recorded about the refusal.
			http.Error(w, "source has been revoked", http.StatusForbidden)
			return
		case errors.Is(err, ErrSourcePaused):
			// Known and deliberately held. This answer does change — when the
			// operator resumes it — so it stays retryable.
			http.Error(w, "source is paused", http.StatusServiceUnavailable)
			return
		case errors.Is(err, ErrSourceUnauthorized):
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		case err != nil:
			http.Error(w, "source registry unavailable", http.StatusServiceUnavailable)
			return
		}
		source = resolved
	} else {
		http.Error(w, "source registry unavailable", http.StatusServiceUnavailable)
		return
	}
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		http.Error(w, "application/json required", http.StatusUnsupportedMediaType)
		return
	}
	position := r.Header.Get("X-Source-Position")
	if position == "" || len(position) > 512 {
		http.Error(w, "X-Source-Position is required", http.StatusBadRequest)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, rawevent.MaxPayloadBytes)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "event too large", http.StatusRequestEntityTooLarge)
		return
	}
	s.acceptRaw(w, r, source, position, "", payload)
}

func (s Server) ingestBeat(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if len(s.AdapterToken) < 32 {
		http.Error(w, "source adapter ingress is not configured", http.StatusServiceUnavailable)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Source-Adapter-Token")), []byte(s.AdapterToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	topic := r.Header.Get("X-Source-Topic")
	partition, partitionErr := strconv.Atoi(r.Header.Get("X-Source-Partition"))
	offset, offsetErr := strconv.ParseInt(r.Header.Get("X-Source-Offset"), 10, 64)
	if _, ok := sourceContextFromTopic(topic); !ok || partitionErr != nil || partition < 0 || offsetErr != nil || offset < 0 {
		http.Error(w, "invalid source topic position", http.StatusBadRequest)
		return
	}
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		http.Error(w, "application/json required", http.StatusUnsupportedMediaType)
		return
	}
	if s.TopicResolver == nil {
		http.Error(w, "source topic registry unavailable", http.StatusServiceUnavailable)
		return
	}
	source, err := s.TopicResolver.ResolveTopic(r.Context(), topic)
	switch {
	case errors.Is(err, ErrSourceRevoked):
		// A revoked source's topic is answered definitively: the adapter
		// quarantines the record and advances rather than holding the partition
		// until retention deletes it.
		http.Error(w, "source has been revoked", http.StatusForbidden)
		return
	case err != nil:
		// Unknown or paused. An unknown topic may simply be mid-registration, and
		// a paused one is expected to resume, so both keep the offset.
		http.Error(w, "source topic is not currently bound to an active source", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, rawevent.MaxPayloadBytes)
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "event too large", http.StatusRequestEntityTooLarge)
		return
	}
	// One decode serves the source-kind guards and the position itself. A decode
	// per call made this the most repeated work in the request path.
	payloadDecoder := json.NewDecoder(bytes.NewReader(payload))
	payloadDecoder.UseNumber()
	var beatEvent map[string]any
	if payloadDecoder.Decode(&beatEvent) != nil {
		beatEvent = nil
	}
	if source.VendorProduct == "zeek" && rawevent.StableBeatPositionFromEvent(beatEvent, "") == "" {
		http.Error(w, "Zeek Filebeat event requires stable file device, inode and log offset", http.StatusBadRequest)
		return
	}
	if (source.VendorProduct == "windows" || source.VendorDataset == "windows.security") && rawevent.StableBeatPositionFromEvent(beatEvent, "") == "" {
		http.Error(w, "Windows Security Winlogbeat event requires computer, channel, record ID and timestamp", http.StatusBadRequest)
		return
	}
	deliveryPosition := fmt.Sprintf("kafka-v1:%s:%d:%d", topic, partition, offset)
	position := rawevent.StableBeatPositionFromEvent(beatEvent, deliveryPosition)
	s.acceptRaw(w, r, source, position, deliveryPosition, payload)
}

func (s Server) acceptRaw(w http.ResponseWriter, r *http.Request, source RawSource, position, deliveryPosition string, payload []byte) {
	if s.Limiter != nil && !s.Limiter.Allow() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	if s.RawProducer == nil || s.RawReceipts == nil || source.SourceInstanceID == "" || source.SourceContextID == "" || source.VendorName == "" || source.VendorProduct == "" || source.VendorDataset == "" || source.ReleaseID == "" {
		http.Error(w, "raw ingestion is not configured", http.StatusServiceUnavailable)
		return
	}
	rawTopic, err := rawTopicFor(s.RawTopicPattern, source.Namespace)
	if err != nil {
		http.Error(w, "raw ingestion is not configured", http.StatusServiceUnavailable)
		return
	}
	if source.RateLimit > 0 && s.SourceLimiters != nil && !s.SourceLimiters.Allow(source.SourceInstanceID, source.RateLimit) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "source rate limit exceeded", http.StatusTooManyRequests)
		return
	}
	trusted := rawevent.TrustedSource{
		OrganizationID: source.OrganizationID, Namespace: source.Namespace,
		SourceInstanceID: source.SourceInstanceID, SourceEpoch: source.SourceEpoch,
		SourceContextID: source.SourceContextID,
		VendorName:      source.VendorName, VendorProduct: source.VendorProduct, VendorDataset: source.VendorDataset,
		ReleaseID: source.ReleaseID,
	}
	envelope, err := rawevent.New(trusted, position, payload, time.Now().UTC())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	envelope.DeliveryPosition = deliveryPosition
	candidate, err := rawevent.Marshal(envelope)
	if err != nil {
		http.Error(w, "could not encode raw event", http.StatusInternalServerError)
		return
	}
	encoded, kafkaAcked, err := s.RawReceipts.GetOrCreate(r.Context(), envelope.Organization.ID, envelope.SourceInstanceID, envelope.RawEventID, envelope.PayloadHash, source.SourceContextID, candidate)
	if err != nil {
		if errors.Is(err, ErrRawIDConflict) {
			http.Error(w, "source position was already used for a different payload", http.StatusConflict)
			return
		}
		http.Error(w, "receipt store unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		http.Error(w, "receipt store returned invalid envelope", http.StatusInternalServerError)
		return
	}
	if kafkaAcked {
		if s.Metrics != nil {
			s.Metrics.Inc("tuba_raw_ingest_accepted_total")
		}
		writeAccepted(w, envelope, deliveryPosition)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.RawProducer.WriteMessages(ctx, rawTopic, kafka.Message{Key: []byte(envelope.Organization.ID + ":" + envelope.SourceInstanceID), Value: encoded}); err != nil {
		if s.Metrics != nil {
			s.Metrics.Inc("tuba_raw_ingest_kafka_failures_total")
		}
		http.Error(w, "Kafka unavailable", http.StatusServiceUnavailable)
		return
	}
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ackCancel()
	if err := s.RawReceipts.MarkKafkaAcked(ackCtx, envelope.Organization.ID, envelope.SourceInstanceID, envelope.RawEventID, time.Now().UTC()); err != nil {
		http.Error(w, "receipt acknowledgement unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.Metrics != nil {
		s.Metrics.Inc("tuba_raw_ingest_accepted_total")
	}
	writeAccepted(w, envelope, deliveryPosition)
}

func writeAccepted(w http.ResponseWriter, envelope rawevent.Envelope, deliveryPosition string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	receipt := map[string]any{
		"receipt_id": envelope.RawEventID, "raw_event_id": envelope.RawEventID,
		"source_context_id": envelope.SourceContextID, "source_position": envelope.SourcePosition,
		"payload_hash": envelope.PayloadHash, "accepted_at": envelope.ReceivedAt, "status": "accepted",
	}
	if deliveryPosition != "" {
		receipt["delivery_position"] = deliveryPosition
	}
	_ = json.NewEncoder(w).Encode(receipt)
}

func isJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && mediaType == "application/json"
}
