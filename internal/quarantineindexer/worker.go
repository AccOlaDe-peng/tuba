package quarantineindexer

import (
	"context"
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
	PutQuarantine(context.Context, uim.Quarantine) error
}

type Worker struct {
	Organization, Namespace string
	Consumer                Consumer
	DeadLetter              Writer
	Sink                    Sink
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
		var record uim.Quarantine
		if err := json.Unmarshal(message.Value, &record); err != nil {
			if err := w.toDLQ(ctx, message, "QUARANTINE_INVALID", err.Error()); err != nil {
				return err
			}
		} else if record.OrganizationID != w.Organization || record.Namespace != w.Namespace || record.ID == "" || record.RawEventID == "" || record.OccurredAt.IsZero() {
			if err := w.toDLQ(ctx, message, "QUARANTINE_SCOPE_INVALID", "quarantine identity or tenant scope is invalid"); err != nil {
				return err
			}
		} else {
			var writeErr error
			for attempt := 0; attempt < 5; attempt++ {
				writeErr = w.Sink.PutQuarantine(ctx, record)
				if writeErr == nil {
					break
				}
				var permanent sink.PermanentIndexError
				if errors.As(writeErr, &permanent) {
					if err := w.toDLQ(ctx, message, permanent.Code, permanent.Message); err != nil {
						return err
					}
					writeErr = nil
					break
				}
				if attempt < 4 {
					timer := time.NewTimer(200 * time.Millisecond << min(attempt, 6))
					select {
					case <-ctx.Done():
						timer.Stop()
						return nil
					case <-timer.C:
					}
				}
			}
			if writeErr != nil {
				return fmt.Errorf("index quarantine record after retries: %w", writeErr)
			}
		}
		if err := w.Consumer.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit quarantine offset: %w", err)
		}
	}
	return nil
}

func (w Worker) toDLQ(ctx context.Context, source kafka.Message, code, reason string) error {
	var record uim.Quarantine
	_ = json.Unmarshal(source.Value, &record)
	dead, err := deadletter.ReferenceMessage(source, record.ID, "", "quarantine_indexing", code, reason)
	if err != nil {
		return err
	}
	if err := w.DeadLetter.WriteMessages(ctx, dead); err != nil {
		return fmt.Errorf("publish quarantine index DLQ: %w", err)
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
