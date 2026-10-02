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
	// revision semantics.
	PutAnalysisObject(context.Context, analysis.ObjectResult) error
}

type Worker struct {
	Organization string
	Namespace    string
	Consumer     Consumer
	DeadLetter   DeadLetter
	Sink         Sink
	Metrics      *telemetry.Registry
}

func (w Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		message, err := w.Consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for ctx.Err() == nil {
			if err := w.process(ctx, message); err == nil {
				break
			} else {
				slog.Error("analysis result processing failed; offset will not be committed", "partition", message.Partition, "offset", message.Offset, "error", err)
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

// deadLetters routes one poison/permanently-rejected message to the DLQ with
// the standard queryable envelope (stage/code/retryable). A DLQ write failure
// is returned so the input offset is NOT committed (fail-closed).
func (w Worker) deadLetters(ctx context.Context, message kafka.Message, stage, code, reason string) error {
	dead, err := deadletter.Message(message, stage, code, reason, false, 1)
	if err != nil {
		return fmt.Errorf("dead letter envelope: %w", err)
	}
	if err := w.DeadLetter.WriteMessages(ctx, dead); err != nil {
		return fmt.Errorf("dead letter publish: %w", err)
	}
	w.inc("tuba_analysis_results_dead_letter_total")
	return nil
}

func (w Worker) process(ctx context.Context, message kafka.Message) error {
	w.inc("tuba_analysis_results_received_total")
	version := contractVersion(message.Value)
	switch version {
	case analysis.ContractVersionV2:
		object, err := analysis.ParseObject(message.Value, w.Organization, w.Namespace)
		if err != nil {
			if err := w.deadLetters(ctx, message, "analysis-sink-parse", "ANALYSIS_CONTRACT_INVALID", err.Error()); err != nil {
				return err
			}
			return w.Consumer.CommitMessages(ctx, message)
		}
		if err := w.writeObject(ctx, message, object); err != nil {
			return err
		}
	case "1.0.0":
		result, err := analysis.Parse(message.Value, w.Organization, w.Namespace)
		if err != nil {
			if err := w.deadLetters(ctx, message, "analysis-sink-parse", "ANALYSIS_CONTRACT_INVALID", err.Error()); err != nil {
				return err
			}
			return w.Consumer.CommitMessages(ctx, message)
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
			if err := w.deadLetters(ctx, message, "analysis-sink-migrate", "ANALYSIS_LEGACY_WINDOW_MISSING", err.Error()); err != nil {
				return err
			}
			return w.Consumer.CommitMessages(ctx, message)
		}
		if err := w.writeObject(ctx, message, object); err != nil {
			return err
		}
	default:
		if err := w.deadLetters(ctx, message, "analysis-sink-parse", "ANALYSIS_CONTRACT_INVALID", "unsupported or missing contract_version"); err != nil {
			return err
		}
		return w.Consumer.CommitMessages(ctx, message)
	}
	return w.Consumer.CommitMessages(ctx, message)
}

// writeObject applies one v2 object and classifies the outcome:
//   - StaleRevisionError: an out-of-order/late revision. The stored newer
//     value stays authoritative; the record is dropped (offset committed), not
//     retried and not dead-lettered — revision decides order, not arrival.
//   - sink.PermanentIndexError: contract or same-revision content conflict.
//     The message is dead-lettered with the queryable reason code, never
//     silently overwritten, and the offset is committed only after the DLQ
//     write succeeds.
//   - any other error: retryable; the offset is not committed.
func (w Worker) writeObject(ctx context.Context, message kafka.Message, object analysis.ObjectResult) error {
	err := w.Sink.PutAnalysisObject(ctx, object)
	if err == nil {
		w.inc("tuba_analysis_results_written_total")
		w.inc("tuba_analysis_objects_written_total." + object.ObjectType)
		return nil
	}
	if sink.IsStaleRevision(err) {
		w.inc("tuba_analysis_objects_stale_dropped_total")
		slog.Info("stale analysis object revision dropped; stored newer revision stays authoritative",
			"object_type", object.ObjectType, "object_id", object.ObjectID, "revision", object.Revision, "error", err)
		return nil
	}
	var permanent sink.PermanentIndexError
	if errors.As(err, &permanent) {
		if err := w.deadLetters(ctx, message, "analysis-sink-index", permanent.Code, permanent.Message); err != nil {
			return err
		}
		return nil
	}
	return err
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
