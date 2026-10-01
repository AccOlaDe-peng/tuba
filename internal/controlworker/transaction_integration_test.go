package controlworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// txFixture wires a claimed job plus a dedicated consumer group so the test
// never touches tenant data. Cleanup removes inbox/checkpoint/outbox rows by
// the group name and the job via the organization fixture.
type txFixture struct {
	group string
	job   Job
	cfg   JobWorkerConfig
}

func newTxFixture(t *testing.T, pool *pgxpool.Pool, workerID string) txFixture {
	t.Helper()
	group := fmt.Sprintf("t02_it_%d", time.Now().UnixNano())
	jobID := insertIntegrationJob(t, pool, 3)
	cfg := integrationConfig(pool, workerID)
	job, err := claimJob(context.Background(), cfg, []string{"replay"})
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != jobID {
		t.Fatalf("claimed %s, want fixture job %s", job.ID, jobID)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, q := range []string{
			`DELETE FROM processor_inbox WHERE consumer_group=$1`,
			`DELETE FROM processor_checkpoints WHERE consumer_group=$1`,
			`DELETE FROM processor_outbox WHERE producer=$1`,
		} {
			if _, err := pool.Exec(ctx, q, group); err != nil {
				t.Logf("cleanup %q: %v", q, err)
			}
		}
	})
	return txFixture{group: group, job: job, cfg: cfg}
}

func (f txFixture) message(id string, offset int64) InboxMessage {
	sum := outboxPayloadHash(json.RawMessage(fmt.Sprintf(`{"id":%q}`, id)))
	return InboxMessage{
		ConsumerGroup: f.group,
		MessageID:     id,
		Topic:         "t02-it-topic",
		Partition:     0,
		Offset:        offset,
		PayloadHash:   sum,
	}
}

func (f txFixture) outcome(offset int64, payloads ...string) func(context.Context, pgx.Tx) (AttemptOutcome, error) {
	return func(context.Context, pgx.Tx) (AttemptOutcome, error) {
		out := AttemptOutcome{Checkpoint: &CheckpointUpdate{Topic: "t02-it-topic", Partition: 0, NextOffset: offset + 1}}
		for i, p := range payloads {
			out.Outbox = append(out.Outbox, OutboxMessage{
				Producer:     f.group,
				AggregateKey: "agg-1",
				Topic:        "t02-it-results",
				MessageKey:   []byte(fmt.Sprintf("key-%d", i)),
				Payload:      json.RawMessage(p),
			})
		}
		return out, nil
	}
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, group string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM processor_outbox WHERE producer=$1`, group).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestTxCommitsInboxCheckpointOutboxAtomicallyIntegration(t *testing.T) {
	pool := integrationPool(t)
	fx := newTxFixture(t, pool, "worker-a")
	ctx := context.Background()
	res, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 41),
		fx.outcome(41, `{"r":1}`, `{"r":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicate {
		t.Fatal("first delivery reported as duplicate")
	}
	if len(res.OutboxSequences) != 2 || res.OutboxSequences[0] != 1 || res.OutboxSequences[1] != 2 {
		t.Fatalf("sequences=%v, want [1 2]", res.OutboxSequences)
	}
	var inboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1 AND message_id='m1'`, fx.group).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != 1 {
		t.Fatalf("inbox rows=%d, want 1", inboxCount)
	}
	var nextOffset, fencing int64
	if err := pool.QueryRow(ctx, `SELECT next_offset, fencing_token FROM processor_checkpoints WHERE consumer_group=$1 AND topic='t02-it-topic' AND partition=0`, fx.group).Scan(&nextOffset, &fencing); err != nil {
		t.Fatal(err)
	}
	if nextOffset != 42 || fencing != fx.job.FencingToken {
		t.Fatalf("checkpoint offset=%d fencing=%d, want 42/%d", nextOffset, fencing, fx.job.FencingToken)
	}
	if n := countOutbox(t, pool, fx.group); n != 2 {
		t.Fatalf("outbox rows=%d, want 2", n)
	}
	// Payload hash persisted is the sha256 of the payload bytes.
	var storedHash string
	if err := pool.QueryRow(ctx, `SELECT payload_hash FROM processor_outbox WHERE producer=$1 AND sequence=1`, fx.group).Scan(&storedHash); err != nil {
		t.Fatal(err)
	}
	if want := outboxPayloadHash(json.RawMessage(`{"r":1}`)); storedHash != want {
		t.Fatalf("payload_hash=%s, want %s", storedHash, want)
	}
}

func TestTxCrashBeforeCommitLeavesNoTraceIntegration(t *testing.T) {
	pool := integrationPool(t)
	fx := newTxFixture(t, pool, "worker-a")
	ctx := context.Background()
	// The handler finishes its business write, then the "crash" happens before
	// commit: everything must roll back.
	_, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 41),
		func(ctx context.Context, tx pgx.Tx) (AttemptOutcome, error) {
			outcome, err := fx.outcome(41, `{"r":1}`)(ctx, tx)
			if err != nil {
				return outcome, err
			}
			return outcome, errors.New("simulated crash before commit")
		})
	if err == nil {
		t.Fatal("crash error swallowed")
	}
	var inboxCount, checkpointCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1`, fx.group).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_checkpoints WHERE consumer_group=$1`, fx.group).Scan(&checkpointCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != 0 || checkpointCount != 0 || countOutbox(t, pool, fx.group) != 0 {
		t.Fatalf("crash before commit left traces: inbox=%d checkpoint=%d outbox=%d", inboxCount, checkpointCount, countOutbox(t, pool, fx.group))
	}
	// Recovery reprocesses the same message from scratch and commits cleanly.
	res, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 41), fx.outcome(41, `{"r":1}`))
	if err != nil || res.Duplicate {
		t.Fatalf("recovery failed: duplicate=%v err=%v", res.Duplicate, err)
	}
	if n := countOutbox(t, pool, fx.group); n != 1 {
		t.Fatalf("outbox rows after recovery=%d, want 1", n)
	}
}

func TestTxRedeliveryAfterCommitEmitsNoDuplicateOutboxIntegration(t *testing.T) {
	pool := integrationPool(t)
	fx := newTxFixture(t, pool, "worker-a")
	ctx := context.Background()
	if _, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 41), fx.outcome(41, `{"r":1}`)); err != nil {
		t.Fatal(err)
	}
	// Crash after commit: redelivery at the same offset is deduplicated.
	res, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 41),
		func(context.Context, pgx.Tx) (AttemptOutcome, error) {
			t.Fatal("handler must not run for a duplicate")
			return AttemptOutcome{}, nil
		})
	if err != nil || !res.Duplicate {
		t.Fatalf("redelivery: duplicate=%v err=%v", res.Duplicate, err)
	}
	if n := countOutbox(t, pool, fx.group); n != 1 {
		t.Fatalf("outbox rows after same-offset redelivery=%d, want 1", n)
	}
	// Transport redelivery at a higher offset: still deduplicated, and the
	// checkpoint advances so recovery skips the message.
	res, err = ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 57), fx.outcome(57, `{"r":1}`))
	if err != nil || !res.Duplicate {
		t.Fatalf("cross-offset redelivery: duplicate=%v err=%v", res.Duplicate, err)
	}
	if n := countOutbox(t, pool, fx.group); n != 1 {
		t.Fatalf("outbox rows after cross-offset redelivery=%d, want 1", n)
	}
	var nextOffset int64
	if err := pool.QueryRow(ctx, `SELECT next_offset FROM processor_checkpoints WHERE consumer_group=$1 AND topic='t02-it-topic' AND partition=0`, fx.group).Scan(&nextOffset); err != nil {
		t.Fatal(err)
	}
	if nextOffset != 58 {
		t.Fatalf("checkpoint=%d, want 58", nextOffset)
	}
}

func TestTxRebalanceRejectsStaleHolderWritesIntegration(t *testing.T) {
	pool := integrationPool(t)
	fx := newTxFixture(t, pool, "worker-a")
	ctx := context.Background()
	// The lease expires, the reaper requeues, and worker-b claims the job with
	// a higher fencing token.
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET lease_until = now() - interval '1 second' WHERE id=$1`, fx.job.ID); err != nil {
		t.Fatal(err)
	}
	if err := reapExpiredJobs(ctx, pool, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE processing_jobs SET next_attempt_at = now() WHERE id=$1`, fx.job.ID); err != nil {
		t.Fatal(err)
	}
	freshCfg := integrationConfig(pool, "worker-b")
	fresh, err := claimJob(ctx, freshCfg, []string{"replay"})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.FencingToken <= fx.job.FencingToken {
		t.Fatalf("fencing token did not advance: stale=%d fresh=%d", fx.job.FencingToken, fresh.FencingToken)
	}
	// The old holder's transactional write is rejected and persists nothing.
	_, err = ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 41), fx.outcome(41, `{"r":1}`))
	if !errors.Is(err, ErrStaleFencing) {
		t.Fatalf("stale write: err=%v, want ErrStaleFencing", err)
	}
	var inboxCount, checkpointCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1`, fx.group).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_checkpoints WHERE consumer_group=$1`, fx.group).Scan(&checkpointCount); err != nil {
		t.Fatal(err)
	}
	if inboxCount != 0 || checkpointCount != 0 || countOutbox(t, pool, fx.group) != 0 {
		t.Fatalf("stale write left traces: inbox=%d checkpoint=%d outbox=%d", inboxCount, checkpointCount, countOutbox(t, pool, fx.group))
	}
	// The new owner proceeds normally.
	if _, err := ProcessInboxMessage(ctx, pool, "worker-b", fresh, fx.message("m1", 41), fx.outcome(41, `{"r":1}`)); err != nil {
		t.Fatalf("new owner rejected: %v", err)
	}
	if n := countOutbox(t, pool, fx.group); n != 1 {
		t.Fatalf("outbox rows=%d, want 1", n)
	}
	// Defense in depth: even with a valid job guard, a checkpoint fenced by a
	// newer token rejects the write.
	if _, err := pool.Exec(ctx, `UPDATE processor_checkpoints SET fencing_token=$2 WHERE consumer_group=$1`, fx.group, fresh.FencingToken+1); err != nil {
		t.Fatal(err)
	}
	_, err = ProcessInboxMessage(ctx, pool, "worker-b", fresh, fx.message("m2", 42), fx.outcome(42, `{"r":2}`))
	if !errors.Is(err, ErrStaleFencing) {
		t.Fatalf("checkpoint-fenced write: err=%v, want ErrStaleFencing", err)
	}
	var m2Inbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1 AND message_id='m2'`, fx.group).Scan(&m2Inbox); err != nil {
		t.Fatal(err)
	}
	if m2Inbox != 0 || countOutbox(t, pool, fx.group) != 1 {
		t.Fatalf("checkpoint-fenced write left traces: inbox m2=%d outbox=%d", m2Inbox, countOutbox(t, pool, fx.group))
	}
}

func TestTxCheckpointIsAuthoritativeForRecoveryIntegration(t *testing.T) {
	pool := integrationPool(t)
	fx := newTxFixture(t, pool, "worker-a")
	ctx := context.Background()
	for i, offset := range []int64{0, 1, 2} {
		id := fmt.Sprintf("m%d", i)
		if _, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message(id, offset), fx.outcome(offset, `{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	// A recovered worker loads PG checkpoints and resumes exactly there.
	checkpoints, err := LoadCheckpoints(ctx, pool, fx.group)
	if err != nil {
		t.Fatal(err)
	}
	cp, ok := checkpoints[TopicPartition{Topic: "t02-it-topic", Partition: 0}]
	if !ok || cp.NextOffset != 3 {
		t.Fatalf("checkpoint=%+v ok=%v, want next_offset 3", cp, ok)
	}
	// Replaying an older offset as a new message ID fails closed at the inbox
	// position constraint before any business effect.
	_, err = ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m-rewind", 1), fx.outcome(1, `{"ok":false}`))
	if err == nil {
		t.Fatal("replay of an old position accepted")
	}
	// A handler proposing a checkpoint behind the stored position is rejected
	// as a regression and the whole transaction (including its inbox row)
	// rolls back.
	_, err = ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m-rewind2", 5),
		func(context.Context, pgx.Tx) (AttemptOutcome, error) {
			return AttemptOutcome{Checkpoint: &CheckpointUpdate{Topic: "t02-it-topic", Partition: 0, NextOffset: 2}}, nil
		})
	if !errors.Is(err, ErrCheckpointRegression) {
		t.Fatalf("regression: err=%v, want ErrCheckpointRegression", err)
	}
	var rewindInbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1 AND message_id='m-rewind2'`, fx.group).Scan(&rewindInbox); err != nil {
		t.Fatal(err)
	}
	if rewindInbox != 0 {
		t.Fatal("regression left an inbox row behind")
	}
	// Re-committing the recorded position is a safe no-op: the handler
	// consumes offset 3 but proposes the already-stored next_offset.
	res, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m-at-edge", 3),
		func(context.Context, pgx.Tx) (AttemptOutcome, error) {
			return AttemptOutcome{Checkpoint: &CheckpointUpdate{Topic: "t02-it-topic", Partition: 0, NextOffset: 3}}, nil
		})
	if err != nil || res.Duplicate {
		t.Fatalf("edge re-commit: duplicate=%v err=%v", res.Duplicate, err)
	}
	checkpoints, err = LoadCheckpoints(ctx, pool, fx.group)
	if err != nil {
		t.Fatal(err)
	}
	if got := checkpoints[TopicPartition{Topic: "t02-it-topic", Partition: 0}].NextOffset; got != 3 {
		t.Fatalf("checkpoint after edge re-commit=%d, want 3", got)
	}
}

func TestTxBusinessStateSharesTheCommitIntegration(t *testing.T) {
	pool := integrationPool(t)
	fx := newTxFixture(t, pool, "worker-a")
	ctx := context.Background()
	// The handler writes business state (here: the job's own last_error, as a
	// stand-in for a business table) through the provided transaction handle;
	// it commits only together with inbox/checkpoint/outbox.
	_, err := ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m1", 9),
		func(ctx context.Context, tx pgx.Tx) (AttemptOutcome, error) {
			if _, err := tx.Exec(ctx, `UPDATE processing_jobs SET last_error='in-progress' WHERE id=$1`, fx.job.ID); err != nil {
				return AttemptOutcome{}, err
			}
			return fx.outcome(9, `{"r":1}`)(ctx, tx)
		})
	if err != nil {
		t.Fatal(err)
	}
	var marker string
	if err := pool.QueryRow(ctx, `SELECT COALESCE(last_error,'') FROM processing_jobs WHERE id=$1`, fx.job.ID).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != "in-progress" {
		t.Fatalf("business state marker=%q, want in-progress", marker)
	}
	// A handler failure after the business write rolls everything back.
	_, err = ProcessInboxMessage(ctx, pool, "worker-a", fx.job, fx.message("m2", 10),
		func(ctx context.Context, tx pgx.Tx) (AttemptOutcome, error) {
			if _, err := tx.Exec(ctx, `UPDATE processing_jobs SET last_error='should-rollback' WHERE id=$1`, fx.job.ID); err != nil {
				return AttemptOutcome{}, err
			}
			return AttemptOutcome{}, errors.New("handler failed after business write")
		})
	if err == nil {
		t.Fatal("handler error swallowed")
	}
	if err := pool.QueryRow(ctx, `SELECT COALESCE(last_error,'') FROM processing_jobs WHERE id=$1`, fx.job.ID).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != "in-progress" {
		t.Fatalf("business state after rollback=%q, want in-progress", marker)
	}
	var m2Inbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM processor_inbox WHERE consumer_group=$1 AND message_id='m2'`, fx.group).Scan(&m2Inbox); err != nil {
		t.Fatal(err)
	}
	if m2Inbox != 0 {
		t.Fatalf("failed handler left inbox row for m2")
	}
}
