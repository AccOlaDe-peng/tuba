package standardindexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/deadletter"
	"tuba/product/internal/sink"
	"tuba/product/internal/uim"
)

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}
type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}
type Sink interface {
	PutStandardBatch(context.Context, []map[string]any) ([]error, error)
}

type Worker struct {
	Domain, Organization string
	Consumer             Consumer
	DeadLetter           Writer
	Sink                 Sink
	BatchSize            int
	MaxBatchBytes        int
	BatchWait            time.Duration
	MaxAttempts          int
	RetryBackoff         time.Duration
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
		if err := w.processBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (w Worker) process(ctx context.Context, message kafka.Message) error {
	return w.processBatch(ctx, []kafka.Message{message})
}

func (w Worker) processBatch(ctx context.Context, messages []kafka.Message) error {
	w = w.defaults()
	events := make([]map[string]any, 0, len(messages))
	sources := make([]kafka.Message, 0, len(messages))
	for _, message := range messages {
		var event map[string]any
		if err := json.Unmarshal(message.Value, &event); err != nil {
			if err := w.toDLQ(ctx, message, "STANDARD_EVENT_INVALID", "invalid JSON UIM event"); err != nil {
				return err
			}
			continue
		}
		if err := uim.Validate(event); err != nil {
			if err := w.toDLQ(ctx, message, "UIM_CONTRACT_FAILED", err.Error()); err != nil {
				return err
			}
			continue
		}
		route := object(object(event["ueba"])["route"])
		organization := object(event["organization"])
		if value(route["domain"]) != w.Domain || value(organization["id"]) != w.Organization {
			if err := w.toDLQ(ctx, message, "TOPIC_SCOPE_MISMATCH", "event organization or domain does not match indexer subscription"); err != nil {
				return err
			}
			continue
		}
		events = append(events, event)
		sources = append(sources, message)
	}

	pendingEvents, pendingSources := events, sources
	for attempt := 1; len(pendingEvents) > 0; attempt++ {
		results, batchErr := w.Sink.PutStandardBatch(ctx, pendingEvents)
		if batchErr == nil && len(results) != len(pendingEvents) {
			batchErr = fmt.Errorf("standard indexer sink returned %d results for %d events", len(results), len(pendingEvents))
		}
		if batchErr != nil {
			var permanent sink.PermanentIndexError
			if errors.As(batchErr, &permanent) {
				for _, message := range pendingSources {
					if err := w.toDLQ(ctx, message, permanent.Code, permanent.Message); err != nil {
						return err
					}
				}
				pendingEvents, pendingSources = nil, nil
				break
			}
			if attempt >= w.MaxAttempts {
				return fmt.Errorf("bulk index UIM events after %d attempts: %w", attempt, batchErr)
			}
			if err := sleep(ctx, w.RetryBackoff<<min(attempt-1, 6)); err != nil {
				return err
			}
			continue
		}

		nextEvents := make([]map[string]any, 0, len(pendingEvents))
		nextSources := make([]kafka.Message, 0, len(pendingSources))
		for i, itemErr := range results {
			if itemErr == nil {
				continue
			}
			var permanent sink.PermanentIndexError
			if errors.As(itemErr, &permanent) {
				if err := w.toDLQ(ctx, pendingSources[i], permanent.Code, permanent.Message); err != nil {
					return err
				}
				continue
			}
			nextEvents = append(nextEvents, pendingEvents[i])
			nextSources = append(nextSources, pendingSources[i])
		}
		pendingEvents, pendingSources = nextEvents, nextSources
		if len(pendingEvents) > 0 {
			if attempt >= w.MaxAttempts {
				return fmt.Errorf("index %d UIM event(s) still failing after %d attempts", len(pendingEvents), attempt)
			}
			if err := sleep(ctx, w.RetryBackoff<<min(attempt-1, 6)); err != nil {
				return err
			}
		}
	}

	if err := w.Consumer.CommitMessages(ctx, messages...); err != nil {
		return fmt.Errorf("commit UIM event offsets: %w", err)
	}
	return nil
}

func (w Worker) toDLQ(ctx context.Context, source kafka.Message, code, reason string) error {
	var event map[string]any
	_ = json.Unmarshal(source.Value, &event)
	objectID := value(object(event["event"])["id"])
	digest := sha256.Sum256(source.Value)
	dead, err := deadletter.ReferenceMessage(source, objectID, hex.EncodeToString(digest[:]), "standard_indexing", code, reason)
	if err != nil {
		return err
	}
	if err := w.DeadLetter.WriteMessages(ctx, dead); err != nil {
		return fmt.Errorf("publish standard indexing DLQ: %w", err)
	}
	return nil
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

func object(input any) map[string]any { result, _ := input.(map[string]any); return result }
func value(input any) string {
	if input == nil {
		return ""
	}
	return fmt.Sprint(input)
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
