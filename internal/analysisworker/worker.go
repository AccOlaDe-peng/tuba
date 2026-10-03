package analysisworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysis"
	"tuba/product/internal/deadletter"
	"tuba/product/internal/sink"
	"tuba/product/internal/telemetry"
)

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type DeadLetter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type Sink interface {
	// PutAnalysis is the legacy v1 anomaly write (kept for the migration
	// window: v1 messages stay queryable in their existing index).
	PutAnalysis(context.Context, analysis.Result) error
	// PutAnalysisObject is the F07 multi-object v2 write with external
	// revision semantics, used by the single-message path.
	PutAnalysisObject(context.Context, analysis.ObjectResult) error
	// PutAnalysisObjectBatch applies a batch of v2 objects with ES _bulk
	// writes and returns one result per object (nil = applied or idempotent
	// replay, StaleRevisionError = dropped, PermanentIndexError = dead-letter,
	// anything else = retryable).
	PutAnalysisObjectBatch(context.Context, []analysis.ObjectResult) ([]error, error)
}

type Worker struct {
	Organization string
	Namespace    string
	Consumer     Consumer
	DeadLetter   DeadLetter
	Sink         Sink
	BatchSize    int
	MaxBatchBytes int
	BatchWait    time.Duration
	MaxAttempts  int
	RetryBackoff time.Duration
	Metrics      *telemetry.Registry
}

func (w Worker) defaults() Worker {
	if w.BatchSize <= 0 {
		w.BatchSize = 500
	}
	if w.MaxBatchBytes <= 0 {
		w.MaxBatchBytes = 16 << 20
	}
	if w.BatchWait <= 0 {
		w.BatchWait = time.Second
	}
	if w.MaxAttempts <= 0 {
		w.MaxAttempts = 5
	}
	if w.RetryBackoff <= 0 {
		w.RetryBackoff = 200 * time.Millisecond
	}
	return w
}

// Run fetches messages and accumulates them into batches bounded by
// BatchSize, MaxBatchBytes and BatchWait, then applies each batch with one
// set of ES bulk writes and commits the batch offsets once at the end.
func (w Worker) Run(ctx context.Context) error {
	w = w.defaults()
	var carry *kafka.Message
	for ctx.Err() == nil {
		var first kafka.Message
		if carry != nil {
			first = *carry
			carry = nil
		} else {
			var err error
			first, err = w.Consumer.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
		batch := []kafka.Message{first}
		batchBytes := len(first.Value)
		deadline := time.Now().Add(w.BatchWait)
		for len(batch) < w.BatchSize {
			fetchCtx, cancel := context.WithDeadline(ctx, deadline)
			message, fetchErr := w.Consumer.FetchMessage(fetchCtx)
			cancel()
			if fetchErr != nil {
				break
			}
			if batchBytes+len(message.Value) > w.MaxBatchBytes {
				carry = &message
				break
			}
			batch = append(batch, message)
			batchBytes += len(message.Value)
		}
		for ctx.Err() == nil {
			if err := w.processBatch(ctx, batch); err == nil {
				break
			} else {
				slog.Error("analysis result batch processing failed; offsets will not be committed", "messages", len(batch), "error", err)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(2 * time.Second):
				}
			}
		}
	}
	return nil
}

func (w Worker) inc(name string) {
	if w.Metrics != nil {
		w.Metrics.Inc(name)
	}
}

// deadEnvelope builds the standard queryable DLQ envelope for one poison or
// permanently-rejected message. The envelopes of a whole batch are published
// in one write before any offset is committed; a publish failure aborts the
// batch without committing (fail-closed).
func (w Worker) deadEnvelope(message kafka.Message, stage, code, reason string) (kafka.Message, error) {
	dead, err := deadletter.Message(message, stage, code, reason, false, 1)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("dead letter envelope: %w", err)
	}
	return dead, nil
}

// objectEntry pairs a consumed message with the v2 object it carries.
type objectEntry struct {
	message kafka.Message
	object  analysis.ObjectResult
}

// process keeps the single-message entry point used by the existing tests;
// it is a batch of one.
func (w Worker) process(ctx context.Context, message kafka.Message) error {
	return w.processBatch(ctx, []kafka.Message{message})
}

// processBatch applies one batch and commits every offset only after all
// outcomes are final:
//
//  1. parse/contract failures and migration refusals become DLQ envelopes;
//  2. legacy v1 messages keep their single-document v1 write, then mirror
//     into the v2 object machinery;
//  3. all v2 objects are written with PutAnalysisObjectBatch; per-item
//     permanent errors become DLQ envelopes, stale revisions are dropped,
//     transient failures are retried with backoff up to MaxAttempts (after
//     that the batch fails and no offset is committed);
//  4. all DLQ envelopes are published in one write — a publish failure
//     aborts the batch without committing;
//  5. only then are the batch offsets committed (at-least-once; crash replay
//     is absorbed by the idempotent revision/history-id semantics).
func (w Worker) processBatch(ctx context.Context, messages []kafka.Message) error {
	w = w.defaults()
	var entries []objectEntry
	var dead []kafka.Message
	collectDead := func(message kafka.Message, stage, code, reason string) error {
		envelope, err := w.deadEnvelope(message, stage, code, reason)
		if err != nil {
			return err
		}
		dead = append(dead, envelope)
		return nil
	}
	for _, message := range messages {
		w.inc("tuba_analysis_results_received_total")
		switch contractVersion(message.Value) {
		case analysis.ContractVersionV2:
			object, err := analysis.ParseObject(message.Value, w.Organization, w.Namespace)
			if err != nil {
				if err := collectDead(message, "analysis-sink-parse", "ANALYSIS_CONTRACT_INVALID", err.Error()); err != nil {
					return err
				}
				continue
			}
			entries = append(entries, objectEntry{message, object})
		case "1.0.0":
			result, err := analysis.Parse(message.Value, w.Organization, w.Namespace)
			if err != nil {
				if err := collectDead(message, "analysis-sink-parse", "ANALYSIS_CONTRACT_INVALID", err.Error()); err != nil {
					return err
				}
				continue
			}
			// Legacy write keeps v1 anomalies queryable in their existing index
			// during the migration window.
			if err := w.Sink.PutAnalysis(ctx, result); err != nil {
				return err
			}
			object, err := analysis.MigrateLegacy(result)
			if err != nil {
				// The v1 document has no explicit stable window time: the
				// contract forbids fabricating one from processing time, so the
				// message goes to the migration quarantine (DLQ) with the legacy
				// document already retained in the v1 index.
				if err := collectDead(message, "analysis-sink-migrate", "ANALYSIS_LEGACY_WINDOW_MISSING", err.Error()); err != nil {
					return err
				}
				continue
			}
			entries = append(entries, objectEntry{message, object})
		default:
			if err := collectDead(message, "analysis-sink-parse", "ANALYSIS_CONTRACT_INVALID", "unsupported or missing contract_version"); err != nil {
				return err
			}
		}
	}

	pending := entries
	for attempt := 1; len(pending) > 0; attempt++ {
		objects := make([]analysis.ObjectResult, len(pending))
		for i, entry := range pending {
			objects[i] = entry.object
		}
		results, batchErr := w.Sink.PutAnalysisObjectBatch(ctx, objects)
		if batchErr == nil && len(results) != len(objects) {
			batchErr = fmt.Errorf("analysis sink returned %d results for %d objects", len(results), len(objects))
		}
		if batchErr != nil {
			var permanent sink.PermanentIndexError
			if errors.As(batchErr, &permanent) {
				for _, entry := range pending {
					if err := collectDead(entry.message, "analysis-sink-index", permanent.Code, permanent.Message); err != nil {
						return err
					}
				}
				pending = nil
				break
			}
			if attempt >= w.MaxAttempts {
				return fmt.Errorf("bulk index analysis objects after %d attempts: %w", attempt, batchErr)
			}
			w.inc("tuba_analysis_sink_retry_total")
			if err := sleep(ctx, w.RetryBackoff<<min(attempt-1, 6)); err != nil {
				return err
			}
			continue
		}

		next := make([]objectEntry, 0, len(pending))
		var retrySamples []string
		for i, itemErr := range results {
			entry := pending[i]
			switch {
			case itemErr == nil:
				w.inc("tuba_analysis_results_written_total")
				w.inc("tuba_analysis_objects_written_total." + entry.object.ObjectType)
			case sink.IsStaleRevision(itemErr):
				// An out-of-order/late revision: the stored newer value stays
				// authoritative; the record is dropped (offset committed), not
				// retried and not dead-lettered — revision decides order.
				w.inc("tuba_analysis_objects_stale_dropped_total")
				slog.Info("stale analysis object revision dropped; stored newer revision stays authoritative",
					"object_type", entry.object.ObjectType, "object_id", entry.object.ObjectID, "revision", entry.object.Revision, "error", itemErr)
			default:
				var permanent sink.PermanentIndexError
				if errors.As(itemErr, &permanent) {
					if err := collectDead(entry.message, "analysis-sink-index", permanent.Code, permanent.Message); err != nil {
						return err
					}
					continue
				}
				next = append(next, entry)
				if len(retrySamples) < 3 {
					retrySamples = append(retrySamples, fmt.Sprintf("%s rev=%d: %v", entry.object.ObjectID, entry.object.Revision, itemErr))
				}
			}
		}
		pending = next
		if len(pending) > 0 {
			slog.Warn("analysis object batch has retryable item failures",
				"attempt", attempt, "failing", len(pending), "samples", retrySamples)
			if attempt >= w.MaxAttempts {
				return fmt.Errorf("index %d analysis object(s) still failing after %d attempts", len(pending), attempt)
			}
			w.inc("tuba_analysis_sink_retry_total")
			if err := sleep(ctx, w.RetryBackoff<<min(attempt-1, 6)); err != nil {
				return err
			}
		}
	}

	if len(dead) > 0 {
		if err := w.DeadLetter.WriteMessages(ctx, dead...); err != nil {
			return fmt.Errorf("dead letter publish: %w", err)
		}
		for range dead {
			w.inc("tuba_analysis_results_dead_letter_total")
		}
	}
	return w.Consumer.CommitMessages(ctx, messages...)
}

func contractVersion(raw []byte) string {
	var envelope struct {
		ContractVersion string `json:"contract_version"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	return envelope.ContractVersion
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
