package controlworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type MessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type OutboxConfig struct {
	Pool           *pgxpool.Pool
	Writer         MessageWriter
	WorkerID       string
	BatchSize      int
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	PublishTimeout time.Duration
}

type OutboxPublisher struct{ Config OutboxConfig }

type outboxRecord struct {
	ID           int64
	Producer     string
	AggregateKey string
	Sequence     int64
	Topic        string
	MessageKey   []byte
	Payload      []byte
	LeaseToken   int64
}

func (p OutboxPublisher) Run(ctx context.Context) error {
	c := p.Config
	if c.Pool == nil || c.Writer == nil || c.WorkerID == "" {
		return errors.New("outbox publisher requires PostgreSQL, Kafka writer, and worker ID")
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	for ctx.Err() == nil {
		batch, err := claimOutbox(ctx, c)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("claim outbox batch failed", "error", err)
			if !wait(ctx, c.PollInterval) {
				return nil
			}
			continue
		}
		if len(batch) == 0 {
			if !wait(ctx, c.PollInterval) {
				return nil
			}
			continue
		}
		sort.Slice(batch, func(i, j int) bool {
			if batch[i].Producer != batch[j].Producer {
				return batch[i].Producer < batch[j].Producer
			}
			if batch[i].AggregateKey != batch[j].AggregateKey {
				return batch[i].AggregateKey < batch[j].AggregateKey
			}
			return batch[i].Sequence < batch[j].Sequence
		})
		for _, record := range batch {
			if ctx.Err() != nil {
				return nil
			}
			if err := renewOutboxLease(ctx, c, record); err != nil {
				slog.Error("outbox lease lost before delivery", "outbox_id", record.ID, "error", err)
				continue
			}
			if err := p.publish(ctx, c, record); err != nil {
				slog.Error("outbox delivery failed", "outbox_id", record.ID, "topic", record.Topic, "error", err)
				if releaseErr := releaseOutbox(ctx, c, record, err); releaseErr != nil {
					slog.Error("release outbox lease failed", "outbox_id", record.ID, "error", releaseErr)
				}
				continue
			}
			if err := markOutboxSent(ctx, c, record); err != nil {
				slog.Error("mark outbox sent failed; delivery may be repeated", "outbox_id", record.ID, "error", err)
			}
		}
	}
	return nil
}

func renewOutboxLease(ctx context.Context, c OutboxConfig, record outboxRecord) error {
	result, err := c.Pool.Exec(ctx, `
		UPDATE processor_outbox
		SET lease_until = now() + make_interval(secs => $4::double precision)
		WHERE id=$1 AND lease_owner=$2 AND fencing_token=$3 AND sent_at IS NULL`,
		record.ID, c.WorkerID, record.LeaseToken, c.LeaseDuration.Seconds())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("outbox lease owner or fencing token changed")
	}
	return nil
}

func claimOutbox(ctx context.Context, c OutboxConfig) ([]outboxRecord, error) {
	rows, err := c.Pool.Query(ctx, `
		WITH eligible AS (
			SELECT o.id
			FROM processor_outbox o
			WHERE o.sent_at IS NULL
			  AND o.next_attempt_at <= now()
			  AND (o.lease_until IS NULL OR o.lease_until < now())
			  AND NOT EXISTS (
				SELECT 1 FROM processor_outbox previous
				WHERE previous.producer = o.producer
				  AND previous.aggregate_key = o.aggregate_key
				  AND previous.sequence < o.sequence
				  AND previous.sent_at IS NULL
			  )
			ORDER BY o.id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE processor_outbox o
		SET lease_owner = $2,
		    lease_until = now() + make_interval(secs => $3::double precision),
		    fencing_token = o.fencing_token + 1,
		    attempts = o.attempts + 1
		FROM eligible
		WHERE o.id = eligible.id
		RETURNING o.id,o.producer,o.aggregate_key,o.sequence,o.topic,o.message_key,o.payload,o.fencing_token`,
		c.BatchSize, c.WorkerID, c.LeaseDuration.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batch []outboxRecord
	for rows.Next() {
		var record outboxRecord
		if err := rows.Scan(&record.ID, &record.Producer, &record.AggregateKey, &record.Sequence, &record.Topic, &record.MessageKey, &record.Payload, &record.LeaseToken); err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, rows.Err()
}

func (p OutboxPublisher) publish(ctx context.Context, c OutboxConfig, record outboxRecord) error {
	var payload json.RawMessage = record.Payload
	if !json.Valid(payload) {
		return fmt.Errorf("outbox row %d contains invalid JSON payload", record.ID)
	}
	messageCtx, cancel := context.WithTimeout(ctx, c.PublishTimeout)
	defer cancel()
	return c.Writer.WriteMessages(messageCtx, kafka.Message{
		Topic: record.Topic,
		Key:   record.MessageKey,
		Value: payload,
		Headers: []kafka.Header{
			{Key: "outbox-id", Value: []byte(fmt.Sprint(record.ID))},
			{Key: "producer", Value: []byte(record.Producer)},
			{Key: "aggregate-key", Value: []byte(record.AggregateKey)},
			{Key: "aggregate-sequence", Value: []byte(fmt.Sprint(record.Sequence))},
		},
	})
}

func markOutboxSent(ctx context.Context, c OutboxConfig, record outboxRecord) error {
	result, err := c.Pool.Exec(ctx, `
		UPDATE processor_outbox
		SET sent_at = now(), lease_owner = NULL, lease_until = NULL, last_error = NULL
		WHERE id = $1 AND lease_owner = $2 AND fencing_token = $3 AND sent_at IS NULL`, record.ID, c.WorkerID, record.LeaseToken)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("outbox lease was lost before delivery could be acknowledged")
	}
	return nil
}

func releaseOutbox(ctx context.Context, c OutboxConfig, record outboxRecord, cause error) error {
	_, err := c.Pool.Exec(ctx, `
		UPDATE processor_outbox
		SET lease_owner = NULL,
		    lease_until = NULL,
		    next_attempt_at = now() + make_interval(secs => LEAST(300, power(2, LEAST(attempts, 8)))::double precision),
		    last_error = left($4, 1024)
		WHERE id = $1 AND lease_owner = $2 AND fencing_token = $3 AND sent_at IS NULL`, record.ID, c.WorkerID, record.LeaseToken, cause.Error())
	return err
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
