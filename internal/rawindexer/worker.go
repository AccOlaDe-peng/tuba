package rawindexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/deadletter"
	"tuba/product/internal/rawevent"
	"tuba/product/internal/sink"
	"tuba/product/internal/telemetry"
)

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type Sink interface {
	PutRaw(context.Context, rawevent.Envelope) error
}

type Worker struct {
	Organization, Namespace string
	Consumer                Consumer
	DeadLetter              Writer
	Sink                    Sink
	MaxAttempts             int
	RetryBackoff            time.Duration
	Metrics                 *telemetry.Registry
}

func (w Worker) Run(ctx context.Context) error {
	if w.MaxAttempts <= 0 {
		w.MaxAttempts = 5
	}
	if w.RetryBackoff <= 0 {
		w.RetryBackoff = 200 * time.Millisecond
	}
	for ctx.Err() == nil {
		message, err := w.Consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		var envelope rawevent.Envelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			w.inc("tuba_raw_indexer_invalid_total")
			if err := w.toDLQ(ctx, message, "RAW_ENVELOPE_INVALID", err.Error()); err != nil {
				return err
			}
			if err := w.Consumer.CommitMessages(ctx, message); err != nil {
				return fmt.Errorf("commit invalid raw envelope: %w", err)
			}
			continue
		}
		if err := rawevent.Validate(envelope, w.Organization, w.Namespace); err != nil {
			w.inc("tuba_raw_indexer_invalid_total")
			if dlqErr := w.toDLQ(ctx, message, "RAW_ENVELOPE_INVALID", err.Error()); dlqErr != nil {
				return dlqErr
			}
			if commitErr := w.Consumer.CommitMessages(ctx, message); commitErr != nil {
				return fmt.Errorf("commit invalid raw envelope: %w", commitErr)
			}
			continue
		}

		var writeErr error
		for attempt := 1; attempt <= w.MaxAttempts; attempt++ {
			writeErr = w.Sink.PutRaw(ctx, envelope)
			if writeErr == nil {
				w.inc("tuba_raw_indexer_indexed_total")
				break
			}
			var permanent sink.PermanentIndexError
			if errors.As(writeErr, &permanent) {
				w.inc("tuba_raw_indexer_index_failed_total")
				if err := w.toDLQ(ctx, message, permanent.Code, permanent.Message); err != nil {
					return err
				}
				writeErr = nil
				break
			}
			if attempt < w.MaxAttempts {
				w.inc("tuba_raw_indexer_retry_total")
				timer := time.NewTimer(w.RetryBackoff << min(attempt-1, 6))
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
			}
		}
		if writeErr != nil {
			return fmt.Errorf("raw evidence write failed after %d attempts: %w", w.MaxAttempts, writeErr)
		}
		if err := w.Consumer.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit raw offset: %w", err)
		}
	}
	return nil
}

func (w Worker) toDLQ(ctx context.Context, message kafka.Message, code, text string) error {
	var envelope rawevent.Envelope
	_ = json.Unmarshal(message.Value, &envelope)
	dead, err := deadletter.RawReferenceMessage(message, envelope.RawEventID, envelope.PayloadHash, "raw_indexing", code, text)
	if err != nil {
		return err
	}
	if err := w.DeadLetter.WriteMessages(ctx, dead); err != nil {
		return fmt.Errorf("publish raw index DLQ: %w", err)
	}
	slog.Warn("raw evidence message sent to DLQ", "code", code, "partition", message.Partition, "offset", message.Offset)
	return nil
}

func (w Worker) inc(name string) {
	if w.Metrics != nil {
		w.Metrics.Inc(name)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
