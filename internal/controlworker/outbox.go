package controlworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/telemetry"
)

type MessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type OutboxConfig struct {
	Pool           *pgxpool.Pool
	Writer         MessageWriter
	WorkerID       string
	Metrics        *telemetry.Registry
	BatchSize      int
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	PublishTimeout time.Duration
	// MaxAttempts bounds delivery retries per outbox row. A row whose attempts
	// reach the limit is never claimed again: it stays unsent, keeps its
	// aggregate key blocked (fail-closed ordering), and is surfaced through
	// the outbox_dead_records gauge until an operator requeues it with
	// UPDATE processor_outbox SET attempts = 0 WHERE id = ...
	MaxAttempts int
	// MaxConcurrentKeys bounds how many aggregate keys are delivered in
	// parallel. Records of one key are always delivered sequentially in
	// sequence order; distinct keys may proceed concurrently.
	MaxConcurrentKeys int
	// StuckThreshold is the age after which an unsent, still-retryable row is
	// reported via outbox_stall_events_total and a warning log.
	StuckThreshold time.Duration
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
	Attempts     int
}

func normalizeOutboxConfig(c OutboxConfig) OutboxConfig {
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
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 8
	}
	if c.MaxConcurrentKeys <= 0 {
		c.MaxConcurrentKeys = 4
	}
	if c.StuckThreshold <= 0 {
		c.StuckThreshold = 5 * time.Minute
	}
	return c
}

func (p OutboxPublisher) inc(name string) {
	if p.Config.Metrics != nil {
		p.Config.Metrics.Inc(name)
	}
}

func (p OutboxPublisher) add(name string, n int) {
	if p.Config.Metrics != nil && n > 0 {
		p.Config.Metrics.Add(name, uint64(n))
	}
}

func (p OutboxPublisher) set(name string, n uint64) {
	if p.Config.Metrics != nil {
		p.Config.Metrics.Set(name, n)
	}
}

func (p OutboxPublisher) Run(ctx context.Context) error {
	c := normalizeOutboxConfig(p.Config)
	if c.Pool == nil || c.Writer == nil || c.WorkerID == "" {
		return errors.New("outbox publisher requires PostgreSQL, Kafka writer, and worker ID")
	}
	stuckSeen := map[int64]bool{}
	for ctx.Err() == nil {
		p.scanOutboxHealth(ctx, c, stuckSeen)
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
		p.add("outbox_claimed_total", len(batch))
		p.deliverBatch(ctx, c, batch)
	}
	return nil
}

// deliverBatch fans the batch out per aggregate key: records within one key
// run strictly in sequence order while distinct keys proceed concurrently,
// bounded by MaxConcurrentKeys.
func (p OutboxPublisher) deliverBatch(ctx context.Context, c OutboxConfig, batch []outboxRecord) {
	sort.Slice(batch, func(i, j int) bool {
		if batch[i].Producer != batch[j].Producer {
			return batch[i].Producer < batch[j].Producer
		}
		if batch[i].AggregateKey != batch[j].AggregateKey {
			return batch[i].AggregateKey < batch[j].AggregateKey
		}
		return batch[i].Sequence < batch[j].Sequence
	})
	groups := groupByAggregateKey(batch)
	semaphore := make(chan struct{}, c.MaxConcurrentKeys)
	var wg sync.WaitGroup
	for _, group := range groups {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(group []outboxRecord) {
			defer wg.Done()
			defer func() { <-semaphore }()
			p.deliverGroup(ctx, c, group)
		}(group)
	}
	wg.Wait()
}

// groupByAggregateKey splits a batch sorted by (producer, aggregate_key,
// sequence) into contiguous per-key groups.
func groupByAggregateKey(batch []outboxRecord) [][]outboxRecord {
	var groups [][]outboxRecord
	for _, record := range batch {
		if n := len(groups); n > 0 {
			last := groups[n-1]
			if last[0].Producer == record.Producer && last[0].AggregateKey == record.AggregateKey {
				groups[n-1] = append(last, record)
				continue
			}
		}
		groups = append(groups, []outboxRecord{record})
	}
	return groups
}

func (p OutboxPublisher) deliverGroup(ctx context.Context, c OutboxConfig, group []outboxRecord) {
	for _, record := range group {
		if ctx.Err() != nil {
			return
		}
		if err := renewOutboxLease(ctx, c, record); err != nil {
			slog.Error("outbox lease lost before delivery", "outbox_id", record.ID, "error", err)
			return
		}
		if err := p.publish(ctx, c, record); err != nil {
			slog.Error("outbox delivery failed", "outbox_id", record.ID, "topic", record.Topic, "attempt", record.Attempts, "error", err)
			p.inc("outbox_publish_failures_total")
			if record.Attempts >= c.MaxAttempts {
				p.inc("outbox_dead_total")
				slog.Error("outbox delivery attempts exhausted; row stays unsent and blocks its aggregate key until requeued",
					"outbox_id", record.ID, "producer", record.Producer, "aggregate_key", record.AggregateKey, "sequence", record.Sequence)
			} else {
				p.inc("outbox_delivery_retries_total")
			}
			if releaseErr := releaseOutbox(ctx, c, record, err); releaseErr != nil {
				slog.Error("release outbox lease failed", "outbox_id", record.ID, "error", releaseErr)
			}
			return
		}
		if err := markOutboxSent(ctx, c, record); err != nil {
			p.inc("outbox_mark_sent_failures_total")
			slog.Error("mark outbox sent failed; delivery may be repeated", "outbox_id", record.ID, "error", err)
			return
		}
		p.inc("outbox_published_total")
	}
}

// scanOutboxHealth refreshes the queryable backlog gauges and raises the stall
// signal once per row and process for retryable rows older than the stuck
// threshold. Dead rows (attempts exhausted) are counted separately.
func (p OutboxPublisher) scanOutboxHealth(ctx context.Context, c OutboxConfig, stuckSeen map[int64]bool) {
	var pending, dead, oldestSeconds uint64
	err := c.Pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE attempts < $1),
		       count(*) FILTER (WHERE attempts >= $1),
		       COALESCE(EXTRACT(EPOCH FROM now() - min(created_at))::bigint, 0)
		FROM processor_outbox
		WHERE sent_at IS NULL`, c.MaxAttempts).Scan(&pending, &dead, &oldestSeconds)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("outbox health scan failed", "error", err)
		}
		return
	}
	p.set("outbox_pending_records", pending)
	p.set("outbox_dead_records", dead)
	p.set("outbox_oldest_unsent_seconds", oldestSeconds)
	rows, err := c.Pool.Query(ctx, `
		SELECT id, producer, aggregate_key, sequence, attempts
		FROM processor_outbox
		WHERE sent_at IS NULL AND attempts < $1 AND created_at < now() - make_interval(secs => $2::double precision)
		ORDER BY id
		LIMIT 100`, c.MaxAttempts, c.StuckThreshold.Seconds())
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("outbox stuck scan failed", "error", err)
		}
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, sequence int64
		var producer, key string
		var attempts int
		if err := rows.Scan(&id, &producer, &key, &sequence, &attempts); err != nil {
			slog.Error("outbox stuck scan row failed", "error", err)
			return
		}
		if stuckSeen[id] {
			continue
		}
		stuckSeen[id] = true
		p.inc("outbox_stall_events_total")
		slog.Warn("outbox record stuck beyond threshold",
			"outbox_id", id, "producer", producer, "aggregate_key", key, "sequence", sequence, "attempts", attempts)
	}
	if err := rows.Err(); err != nil && ctx.Err() == nil {
		slog.Error("outbox stuck scan failed", "error", err)
	}
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
			  AND o.attempts < $4
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
		RETURNING o.id,o.producer,o.aggregate_key,o.sequence,o.topic,o.message_key,o.payload,o.fencing_token,o.attempts`,
		c.BatchSize, c.WorkerID, c.LeaseDuration.Seconds(), c.MaxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batch []outboxRecord
	for rows.Next() {
		var record outboxRecord
		if err := rows.Scan(&record.ID, &record.Producer, &record.AggregateKey, &record.Sequence, &record.Topic, &record.MessageKey, &record.Payload, &record.LeaseToken, &record.Attempts); err != nil {
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
