package controlworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// T02 single-transaction processing protocol.
//
// One consumed input message is processed exactly once per consumer group:
// the inbox dedup record, the business checkpoint (the authoritative resume
// position), the handler's business state writes, and the result outbox rows
// all commit in a single PostgreSQL transaction. After a crash the only
// durable truth is what committed, so recovery always resumes from the
// PostgreSQL checkpoint — never from in-memory offsets.
//
// Fencing is enforced at two layers inside the same transaction: the job row
// must still be running and owned by the caller's fencing token (the primary
// guard, identical to finishJob), and a stored checkpoint may only be
// overwritten by an equal or higher fencing token (defense in depth against a
// stale holder that somehow passed the job guard). A rebalance therefore
// rejects the old lease holder's writes while the new owner proceeds.

// ErrStaleFencing is returned when the caller no longer holds the job lease
// or the checkpoint fence: a rebalance moved ownership elsewhere.
var ErrStaleFencing = errors.New("fencing token rejected: lease or checkpoint owned by a newer holder")

// ErrCheckpointRegression is returned when a commit would move a checkpoint
// backwards, which can never be correct.
var ErrCheckpointRegression = errors.New("checkpoint regression rejected")

var inboxPayloadHashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// InboxMessage identifies one consumed input message. MessageID is the
// dedup identity (e.g. raw_event_id); Topic/Partition/Offset are the
// transport position; PayloadHash is the canonical payload digest.
type InboxMessage struct {
	ConsumerGroup string
	MessageID     string
	Topic         string
	Partition     int32
	Offset        int64
	PayloadHash   string
}

// CheckpointUpdate advances the consumer position for one topic/partition.
// NextOffset is the next offset to consume (i.e. offset+1 semantics);
// recovery seeks to it.
type CheckpointUpdate struct {
	Topic      string
	Partition  int32
	NextOffset int64
	Watermark  *time.Time
}

// OutboxMessage is one result row to publish after commit. Sequence is
// assigned inside the transaction per (Producer, AggregateKey), preserving
// per-aggregate order.
type OutboxMessage struct {
	Producer     string
	AggregateKey string
	Topic        string
	MessageKey   []byte
	Payload      json.RawMessage
}

// AttemptOutcome is what the handler produced for one input message.
type AttemptOutcome struct {
	Checkpoint *CheckpointUpdate
	Outbox     []OutboxMessage
}

// TxResult reports how one ProcessInboxMessage call ended.
type TxResult struct {
	// Duplicate is true when the inbox already recorded this message ID: the
	// handler was not run and no outbox rows were written; only the
	// checkpoint was advanced (the durable offset commit for a redelivery).
	Duplicate bool
	// OutboxSequences are the per-aggregate sequences assigned to the
	// committed outbox rows, in the order the handler returned them.
	OutboxSequences []int64
}

// Checkpoint is a stored resume position. PG is the authority: a worker
// recovering from a crash seeks its input to NextOffset, nothing else.
type Checkpoint struct {
	Topic        string
	Partition    int32
	NextOffset   int64
	FencingToken int64
	Watermark    *time.Time
	UpdatedAt    time.Time
}

// TopicPartition keys a checkpoint or an input stream position.
type TopicPartition struct {
	Topic     string
	Partition int32
}

func validateInboxMessage(msg InboxMessage) error {
	if msg.ConsumerGroup == "" || msg.MessageID == "" || msg.Topic == "" {
		return errors.New("inbox message requires consumer group, message ID, and topic")
	}
	if msg.Partition < 0 {
		return fmt.Errorf("inbox message partition must be >= 0, got %d", msg.Partition)
	}
	if msg.Offset < 0 {
		return fmt.Errorf("inbox message offset must be >= 0, got %d", msg.Offset)
	}
	if !inboxPayloadHashPattern.MatchString(msg.PayloadHash) {
		return errors.New("inbox message payload hash must be 64 lowercase hex characters")
	}
	return nil
}

func validateCheckpoint(cp CheckpointUpdate) error {
	if cp.Topic == "" {
		return errors.New("checkpoint requires a topic")
	}
	if cp.Partition < 0 {
		return fmt.Errorf("checkpoint partition must be >= 0, got %d", cp.Partition)
	}
	if cp.NextOffset < 0 {
		return fmt.Errorf("checkpoint next offset must be >= 0, got %d", cp.NextOffset)
	}
	return nil
}

func validateOutboxMessage(msg OutboxMessage) error {
	if msg.Producer == "" || msg.AggregateKey == "" || msg.Topic == "" {
		return errors.New("outbox message requires producer, aggregate key, and topic")
	}
	if len(msg.MessageKey) == 0 {
		return errors.New("outbox message requires a message key")
	}
	if !json.Valid(msg.Payload) {
		return errors.New("outbox message payload must be valid JSON")
	}
	return nil
}

func outboxPayloadHash(payload json.RawMessage) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// ProcessInboxMessage commits one input message's inbox record, business
// state (written by work inside the same transaction), checkpoint advance,
// and outbox rows atomically under the job's fencing token. work receives
// the open transaction so business state writes share the commit; it must
// not start or commit transactions of its own. A duplicate message ID skips
// work entirely and only advances the checkpoint. Any error rolls the whole
// transaction back, so a crash anywhere before commit leaves no trace and
// the message is simply reprocessed after recovery.
func ProcessInboxMessage(ctx context.Context, pool *pgxpool.Pool, workerID string, job Job, msg InboxMessage, work func(context.Context, pgx.Tx) (AttemptOutcome, error)) (TxResult, error) {
	if work == nil {
		return TxResult{}, errors.New("transactional processing requires a work function")
	}
	if err := validateInboxMessage(msg); err != nil {
		return TxResult{}, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return TxResult{}, err
	}
	defer tx.Rollback(ctx)

	// Primary fencing guard: the job row must still be running and owned by
	// this worker's fencing token. A stale lease holder matches no row.
	var fencing int64
	err = tx.QueryRow(ctx, `
		SELECT fencing_token FROM processing_jobs
		WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND fencing_token = $3
		FOR UPDATE`, job.ID, workerID, job.FencingToken).Scan(&fencing)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TxResult{}, ErrStaleFencing
		}
		return TxResult{}, err
	}

	var recorded string
	err = tx.QueryRow(ctx, `
		INSERT INTO processor_inbox(consumer_group, message_id, topic, partition, message_offset, payload_hash)
		VALUES($1, $2, $3, $4, $5, $6)
		ON CONFLICT (consumer_group, message_id) DO NOTHING
		RETURNING message_id`,
		msg.ConsumerGroup, msg.MessageID, msg.Topic, msg.Partition, msg.Offset, msg.PayloadHash).Scan(&recorded)
	duplicate := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !duplicate {
		return TxResult{}, err
	}

	result := TxResult{Duplicate: duplicate}
	if duplicate {
		// Redelivery of an already-processed message: never re-run the handler
		// or re-emit outbox rows; only the checkpoint advances so recovery
		// does not see the message again.
		cp := &CheckpointUpdate{Topic: msg.Topic, Partition: msg.Partition, NextOffset: msg.Offset + 1}
		if err := advanceCheckpoint(ctx, tx, msg.ConsumerGroup, fencing, cp); err != nil {
			return TxResult{}, err
		}
		return result, tx.Commit(ctx)
	}

	outcome, err := work(ctx, tx)
	if err != nil {
		return TxResult{}, err
	}
	if outcome.Checkpoint != nil {
		if err := advanceCheckpoint(ctx, tx, msg.ConsumerGroup, fencing, outcome.Checkpoint); err != nil {
			return TxResult{}, err
		}
	}
	for _, out := range outcome.Outbox {
		if err := validateOutboxMessage(out); err != nil {
			return TxResult{}, err
		}
		sequence, err := nextOutboxSequence(ctx, tx, out.Producer, out.AggregateKey)
		if err != nil {
			return TxResult{}, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO processor_outbox(producer, aggregate_key, sequence, topic, message_key, payload, payload_hash)
			VALUES($1, $2, $3, $4, $5, $6, $7)`,
			out.Producer, out.AggregateKey, sequence, out.Topic, out.MessageKey, []byte(out.Payload), outboxPayloadHash(out.Payload)); err != nil {
			return TxResult{}, err
		}
		result.OutboxSequences = append(result.OutboxSequences, sequence)
	}
	return result, tx.Commit(ctx)
}

// advanceCheckpoint moves one partition's resume position forward under the
// fencing token. It runs inside the caller's transaction: a stored token
// newer than the writer's is a stale write, and any backwards offset is a
// regression; both fail closed and abort the whole commit. Re-committing the
// same offset (a redelivery at the recorded position) is a no-op.
func advanceCheckpoint(ctx context.Context, tx pgx.Tx, consumerGroup string, fencing int64, cp *CheckpointUpdate) error {
	if err := validateCheckpoint(*cp); err != nil {
		return err
	}
	var storedOffset, storedFencing int64
	err := tx.QueryRow(ctx, `
		SELECT next_offset, fencing_token FROM processor_checkpoints
		WHERE consumer_group = $1 AND topic = $2 AND partition = $3
		FOR UPDATE`, consumerGroup, cp.Topic, cp.Partition).Scan(&storedOffset, &storedFencing)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		_, err := tx.Exec(ctx, `
			INSERT INTO processor_checkpoints(consumer_group, topic, partition, next_offset, fencing_token, watermark)
			VALUES($1, $2, $3, $4, $5, $6)`,
			consumerGroup, cp.Topic, cp.Partition, cp.NextOffset, fencing, cp.Watermark)
		return err
	case err != nil:
		return err
	}
	if storedFencing > fencing {
		return fmt.Errorf("%w: checkpoint %s/%d fenced by token %d, writer holds %d",
			ErrStaleFencing, cp.Topic, cp.Partition, storedFencing, fencing)
	}
	if cp.NextOffset < storedOffset {
		return fmt.Errorf("%w: checkpoint %s/%d already at offset %d, writer proposed %d",
			ErrCheckpointRegression, cp.Topic, cp.Partition, storedOffset, cp.NextOffset)
	}
	if cp.NextOffset == storedOffset {
		return nil
	}
	_, err = tx.Exec(ctx, `
		UPDATE processor_checkpoints
		SET next_offset = $4, fencing_token = $5, watermark = COALESCE($6, watermark), updated_at = now()
		WHERE consumer_group = $1 AND topic = $2 AND partition = $3`,
		consumerGroup, cp.Topic, cp.Partition, cp.NextOffset, fencing, cp.Watermark)
	return err
}

// nextOutboxSequence assigns the next per-aggregate sequence. The
// UNIQUE(producer, aggregate_key, sequence) constraint is the hard guard:
// two racing transactions cannot both commit the same sequence.
func nextOutboxSequence(ctx context.Context, tx pgx.Tx, producer, aggregateKey string) (int64, error) {
	var sequence int64
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(sequence), 0) + 1 FROM processor_outbox
		WHERE producer = $1 AND aggregate_key = $2`, producer, aggregateKey).Scan(&sequence)
	return sequence, err
}

// LoadCheckpoints returns every stored resume position of a consumer group.
// This is the authoritative recovery state: after a crash or rebalance the
// new owner seeks each partition to NextOffset and relies on the inbox for
// exactly-once dedup of anything replayed from an earlier position.
func LoadCheckpoints(ctx context.Context, pool *pgxpool.Pool, consumerGroup string) (map[TopicPartition]Checkpoint, error) {
	rows, err := pool.Query(ctx, `
		SELECT topic, partition, next_offset, fencing_token, watermark, updated_at
		FROM processor_checkpoints
		WHERE consumer_group = $1`, consumerGroup)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	checkpoints := map[TopicPartition]Checkpoint{}
	for rows.Next() {
		var cp Checkpoint
		if err := rows.Scan(&cp.Topic, &cp.Partition, &cp.NextOffset, &cp.FencingToken, &cp.Watermark, &cp.UpdatedAt); err != nil {
			return nil, err
		}
		checkpoints[TopicPartition{Topic: cp.Topic, Partition: cp.Partition}] = cp
	}
	return checkpoints, rows.Err()
}
