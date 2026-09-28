package worker

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"time"
	"tuba/product/internal/deadletter"
	"tuba/product/internal/event"
	"tuba/product/internal/indexing"
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
	PutBatch(context.Context, []indexing.Document) ([]indexing.Result, error)
}
type Worker struct {
	Organization string
	Consumer     Consumer
	DeadLetter   Writer
	Sink         Sink
	BatchSize    int
	BatchWait    time.Duration
	MaxAttempts  int
	RetryBackoff time.Duration
	Metrics      *telemetry.Registry
}

func (w Worker) defaults() Worker {
	if w.BatchSize <= 0 {
		w.BatchSize = 500
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
	for ctx.Err() == nil {
		first, err := w.Consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		batch := []kafka.Message{first}
		deadline := time.Now().Add(w.BatchWait)
		for len(batch) < w.BatchSize {
			c, cancel := context.WithDeadline(ctx, deadline)
			m, e := w.Consumer.FetchMessage(c)
			cancel()
			if e != nil {
				break
			}
			batch = append(batch, m)
		}
		if err := w.processBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}
func (w Worker) process(ctx context.Context, msg kafka.Message) error {
	return w.processBatch(ctx, []kafka.Message{msg})
}
func (w Worker) processBatch(ctx context.Context, msgs []kafka.Message) error {
	w = w.defaults()
	w.inc("tuba_indexer_events_received_total", uint64(len(msgs)))
	docs := make([]indexing.Document, 0, len(msgs))
	sources := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		e, err := event.Parse(m.Value, w.Organization)
		if err != nil {
			w.inc("tuba_indexer_events_validation_failed_total", 1)
			if e := w.toDLQ(ctx, m, "validation", "EVENT_INVALID", err.Error(), false, 1); e != nil {
				return e
			}
			continue
		}
		docs = append(docs, indexing.Document{Event: e, Raw: m.Value})
		sources = append(sources, m)
	}
	pd, ps := docs, sources
	for attempt := 1; len(pd) > 0; attempt++ {
		results, err := w.Sink.PutBatch(ctx, pd)
		if err != nil {
			if attempt >= w.MaxAttempts {
				return fmt.Errorf("bulk indexing unavailable after %d attempts: %w", attempt, err)
			}
			if err := sleep(ctx, w.RetryBackoff<<min(attempt-1, 6)); err != nil {
				return err
			}
			continue
		}
		nd := make([]indexing.Document, 0)
		ns := make([]kafka.Message, 0)
		successes := 0
		for i, r := range results {
			if r.Status >= 200 && r.Status < 300 || r.Status == 409 {
				successes++
				continue
			}
			if r.Retryable && attempt < w.MaxAttempts {
				nd = append(nd, pd[i])
				ns = append(ns, ps[i])
				continue
			}
			code := r.Code
			if code == "" {
				code = "ES_BULK_ITEM_FAILED"
			}
			if err := w.toDLQ(ctx, ps[i], "indexing", code, r.Message, r.Retryable, attempt); err != nil {
				return err
			}
			w.inc("tuba_indexer_events_index_failed_total", 1)
			continue
		}
		w.inc("tuba_indexer_events_indexed_total", uint64(successes))
		pd, ps = nd, ns
		if len(pd) > 0 {
			w.inc("tuba_indexer_bulk_retry_total", 1)
			if err := sleep(ctx, w.RetryBackoff<<min(attempt-1, 6)); err != nil {
				return err
			}
		}
	}
	if err := w.Consumer.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}
func (w Worker) inc(name string, n uint64) {
	if w.Metrics != nil {
		w.Metrics.Add(name, n)
	}
}
func (w Worker) toDLQ(ctx context.Context, m kafka.Message, stage, code, message string, retryable bool, attempts int) error {
	d, err := deadletter.Message(m, stage, code, message, retryable, attempts)
	if err != nil {
		return err
	}
	if err := w.DeadLetter.WriteMessages(ctx, d); err != nil {
		return fmt.Errorf("dead letter publish: %w", err)
	}
	slog.Warn("event sent to dead letter", "code", code, "partition", m.Partition, "offset", m.Offset)
	return nil
}
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
