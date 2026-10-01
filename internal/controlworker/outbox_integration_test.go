package controlworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/telemetry"
)

// outboxFixture provisions dedicated outbox rows under a unique producer
// prefix; processor_outbox has no organization scope, so cleanup is by prefix.
type outboxFixture struct {
	producer string
}

func newOutboxFixture(t *testing.T, pool *pgxpool.Pool) outboxFixture {
	t.Helper()
	f := outboxFixture{producer: fmt.Sprintf("outbox_it_%d", time.Now().UnixNano())}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DELETE FROM processor_outbox WHERE producer=$1`, f.producer); err != nil {
			t.Logf("cleanup outbox: %v", err)
		}
	})
	return f
}

func (f outboxFixture) insert(t *testing.T, pool *pgxpool.Pool, key string, sequence int64) int64 {
	t.Helper()
	payload := []byte(fmt.Sprintf(`{"fixture":%q,"key":%q,"sequence":%d}`, f.producer, key, sequence))
	hash := sha256.Sum256(payload)
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO processor_outbox(producer, aggregate_key, sequence, topic, message_key, payload, payload_hash)
		VALUES($1, $2, $3, 'outbox-it-topic', $4, $5::jsonb, $6) RETURNING id`,
		f.producer, key, sequence, []byte(key), payload, hex.EncodeToString(hash[:])).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

type outboxDelivery struct {
	worker   string
	key      string
	sequence int64
	id       int64
	at       time.Time
}

// recordingWriter serializes the WriteMessages contract of kafka.Writer.
type recordingWriter struct {
	mu          sync.Mutex
	worker      string
	deliveries  []outboxDelivery
	failKeys    map[string]bool // permanently failing aggregate keys
	crashOnce   bool
	crashed     bool
	crashCancel context.CancelFunc
	delay       time.Duration
}

func (w *recordingWriter) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	for _, message := range messages {
		if w.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.delay):
			}
		}
		headers := map[string]string{}
		for _, h := range message.Headers {
			headers[h.Key] = string(h.Value)
		}
		var id, sequence int64
		fmt.Sscan(headers["outbox-id"], &id)
		fmt.Sscan(headers["aggregate-sequence"], &sequence)
		w.mu.Lock()
		key := headers["aggregate-key"]
		fail := w.failKeys[key]
		crash := w.crashOnce && !w.crashed
		if crash {
			w.crashed = true
		}
		if !fail {
			w.deliveries = append(w.deliveries, outboxDelivery{worker: w.worker, key: key, sequence: sequence, id: id, at: time.Now()})
		}
		w.mu.Unlock()
		if crash && w.crashCancel != nil {
			// Broker acked, publisher process dies before marking sent.
			w.crashCancel()
		}
		if fail {
			return errors.New("broker rejected message")
		}
	}
	return nil
}

func (w *recordingWriter) snapshot() []outboxDelivery {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]outboxDelivery(nil), w.deliveries...)
}

func outboxITConfig(pool *pgxpool.Pool, workerID string, writer MessageWriter, metrics *telemetry.Registry) OutboxConfig {
	return OutboxConfig{
		Pool: pool, Writer: writer, WorkerID: workerID, Metrics: metrics,
		BatchSize: 50, PollInterval: 50 * time.Millisecond, LeaseDuration: 30 * time.Second,
		PublishTimeout: 5 * time.Second, MaxAttempts: 4, MaxConcurrentKeys: 4,
		StuckThreshold: time.Hour,
	}
}

func runOutboxPublisher(t *testing.T, ctx context.Context, cfg OutboxConfig) {
	t.Helper()
	go func() {
		if err := (OutboxPublisher{Config: cfg}).Run(ctx); err != nil {
			t.Errorf("publisher run: %v", err)
		}
	}()
}

func waitForCondition(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestOutboxSameKeyOrderingIntegration(t *testing.T) {
	pool := integrationPool(t)
	fixture := newOutboxFixture(t, pool)
	// Key "blocked": head record fails permanently; later sequences must never ship.
	fixture.insert(t, pool, "blocked", 1)
	fixture.insert(t, pool, "blocked", 2)
	fixture.insert(t, pool, "blocked", 3)
	// Key "ok": all sequences ship in order.
	for sequence := int64(1); sequence <= 4; sequence++ {
		fixture.insert(t, pool, "ok", sequence)
	}
	writer := &recordingWriter{worker: "w1", failKeys: map[string]bool{"blocked": true}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := outboxITConfig(pool, "w1", writer, telemetry.New())
	cfg.MaxAttempts = 2
	runOutboxPublisher(t, ctx, cfg)
	waitForCondition(t, 15*time.Second, "key ok fully delivered", func() bool {
		deliveries := writer.snapshot()
		count := 0
		for _, d := range deliveries {
			if d.key == "ok" {
				count++
			}
		}
		return count == 4
	})
	// Give the blocked key room to misbehave, then assert ordering.
	time.Sleep(300 * time.Millisecond)
	deliveries := writer.snapshot()
	byKey := map[string][]int64{}
	for _, d := range deliveries {
		byKey[d.key] = append(byKey[d.key], d.sequence)
	}
	if got := byKey["blocked"]; len(got) != 0 {
		t.Fatalf("sequences behind a failing head were delivered: %v", got)
	}
	if got := byKey["ok"]; !sort.SliceIsSorted(got, func(i, j int) bool { return got[i] < got[j] }) || len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("key ok delivered out of order: %v", got)
	}
	var blockedSent int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM processor_outbox WHERE producer=$1 AND aggregate_key='blocked' AND sent_at IS NOT NULL`, fixture.producer).Scan(&blockedSent); err != nil {
		t.Fatal(err)
	}
	if blockedSent != 0 {
		t.Fatalf("blocked key must stay unsent, sent=%d", blockedSent)
	}
}

func TestOutboxCrashRecoveryIntegration(t *testing.T) {
	pool := integrationPool(t)
	fixture := newOutboxFixture(t, pool)
	fixture.insert(t, pool, "crash", 1)
	ctx1, cancel1 := context.WithCancel(context.Background())
	writer := &recordingWriter{worker: "w1", crashOnce: true, crashCancel: cancel1}
	cfg := outboxITConfig(pool, "w1", writer, telemetry.New())
	runOutboxPublisher(t, ctx1, cfg)
	waitForCondition(t, 10*time.Second, "first delivery before crash", func() bool {
		return len(writer.snapshot()) == 1
	})
	// Publisher died after the broker ack but before marking sent. Force lease
	// expiry (the process is gone, so nothing renews) and let a fresh
	// publisher redeliver.
	if _, err := pool.Exec(context.Background(), `
		UPDATE processor_outbox SET lease_until = now() - interval '1 second' WHERE producer=$1`, fixture.producer); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	runOutboxPublisher(t, ctx2, outboxITConfig(pool, "w2", writer, telemetry.New()))
	waitForCondition(t, 10*time.Second, "redelivery and mark sent", func() bool {
		var sent bool
		if err := pool.QueryRow(context.Background(),
			`SELECT sent_at IS NOT NULL FROM processor_outbox WHERE producer=$1`, fixture.producer).Scan(&sent); err != nil {
			return false
		}
		return sent
	})
	if deliveries := len(writer.snapshot()); deliveries != 2 {
		t.Fatalf("at-least-once delivery: got %d deliveries, want exactly 2", deliveries)
	}
	var leaseOwner *string
	if err := pool.QueryRow(context.Background(),
		`SELECT lease_owner FROM processor_outbox WHERE producer=$1`, fixture.producer).Scan(&leaseOwner); err != nil {
		t.Fatal(err)
	}
	if leaseOwner != nil {
		t.Fatalf("sent row must not retain a lease, owner=%v", *leaseOwner)
	}
}

func TestOutboxBoundedRetryIntegration(t *testing.T) {
	pool := integrationPool(t)
	fixture := newOutboxFixture(t, pool)
	fixture.insert(t, pool, "poison", 1)
	metrics := telemetry.New()
	writer := &recordingWriter{worker: "w1", failKeys: map[string]bool{"poison": true}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := outboxITConfig(pool, "w1", writer, metrics)
	cfg.MaxAttempts = 2
	runOutboxPublisher(t, ctx, cfg)
	waitForCondition(t, 15*time.Second, "retry budget exhausted", func() bool {
		return metrics.Value("outbox_dead_total") >= 1
	})
	attemptsBefore := writerCalls(pool, t, fixture)
	var attempts int
	var next time.Time
	var lastError *string
	if err := pool.QueryRow(context.Background(), `
		SELECT attempts, next_attempt_at, last_error FROM processor_outbox WHERE producer=$1`, fixture.producer).
		Scan(&attempts, &next, &lastError); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want exactly MaxAttempts=2", attempts)
	}
	if delay := time.Until(next); delay > 300*time.Second {
		t.Fatalf("backoff not bounded: next attempt in %v", delay)
	}
	if lastError == nil || *lastError == "" {
		t.Fatal("last_error must record the failure cause")
	}
	// The exhausted row is dead: no publisher may claim it again, and the
	// backoff schedule must not cause further deliveries.
	time.Sleep(400 * time.Millisecond)
	if after := writerCalls(pool, t, fixture); after != attemptsBefore {
		t.Fatalf("dead row was delivered again: writer interactions %d -> %d", attemptsBefore, after)
	}
	if metrics.Value("outbox_delivery_retries_total") != 1 {
		t.Fatalf("retryable failures=%d, want 1 (second failure is terminal)", metrics.Value("outbox_delivery_retries_total"))
	}
}

// writerCalls approximates broker interactions through the dead gauge: an
// exhausted row must not be claimed again, so outbox_dead_total stays at 1.
func writerCalls(pool *pgxpool.Pool, t *testing.T, fixture outboxFixture) int {
	t.Helper()
	var attempts int
	if err := pool.QueryRow(context.Background(),
		`SELECT attempts FROM processor_outbox WHERE producer=$1`, fixture.producer).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	return attempts
}

func TestOutboxStuckAlertIntegration(t *testing.T) {
	pool := integrationPool(t)
	fixture := newOutboxFixture(t, pool)
	id := fixture.insert(t, pool, "stuck", 1)
	// Backdate so the row is older than the stuck threshold from the start.
	if _, err := pool.Exec(context.Background(),
		`UPDATE processor_outbox SET created_at = now() - interval '10 seconds' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	metrics := telemetry.New()
	writer := &recordingWriter{worker: "w1", failKeys: map[string]bool{"stuck": true}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := outboxITConfig(pool, "w1", writer, metrics)
	cfg.StuckThreshold = time.Second
	runOutboxPublisher(t, ctx, cfg)
	waitForCondition(t, 10*time.Second, "stuck alert", func() bool {
		return metrics.Value("outbox_stall_events_total") >= 1
	})
	if pending := metrics.Value("outbox_pending_records"); pending < 1 {
		t.Fatalf("pending gauge=%d, want >=1", pending)
	}
	if oldest := metrics.Value("outbox_oldest_unsent_seconds"); oldest < 9 {
		t.Fatalf("oldest unsent age gauge=%d, want >=9s", oldest)
	}
}

func TestOutboxConcurrentPublishersIntegration(t *testing.T) {
	pool := integrationPool(t)
	fixture := newOutboxFixture(t, pool)
	const keys = 5
	const perKey = 4
	for key := 0; key < keys; key++ {
		for sequence := int64(1); sequence <= perKey; sequence++ {
			fixture.insert(t, pool, fmt.Sprintf("k%d", key), sequence)
		}
	}
	writerA := &recordingWriter{worker: "wA", delay: 20 * time.Millisecond}
	writerB := &recordingWriter{worker: "wB", delay: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runOutboxPublisher(t, ctx, outboxITConfig(pool, "wA", writerA, telemetry.New()))
	runOutboxPublisher(t, ctx, outboxITConfig(pool, "wB", writerB, telemetry.New()))
	waitForCondition(t, 30*time.Second, "all rows sent", func() bool {
		var pending int
		if err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM processor_outbox WHERE producer=$1 AND sent_at IS NULL`, fixture.producer).Scan(&pending); err != nil {
			return false
		}
		return pending == 0
	})
	deliveries := append(writerA.snapshot(), writerB.snapshot()...)
	// Two writers record independently; merge by wall clock so the per-key
	// order check reflects real delivery time, not per-writer list order.
	sort.Slice(deliveries, func(i, j int) bool { return deliveries[i].at.Before(deliveries[j].at) })
	byID := map[int64]int{}
	byKey := map[string][]int64{}
	workers := map[string]bool{}
	for _, d := range deliveries {
		byID[d.id]++
		byKey[d.key] = append(byKey[d.key], d.sequence)
		workers[d.worker] = true
	}
	if len(deliveries) != keys*perKey {
		t.Fatalf("deliveries=%d, want exactly %d (no duplicate claims)", len(deliveries), keys*perKey)
	}
	for id, count := range byID {
		if count != 1 {
			t.Fatalf("outbox id %d delivered %d times under concurrent publishers", id, count)
		}
	}
	for key, sequences := range byKey {
		if !sort.SliceIsSorted(sequences, func(i, j int) bool { return sequences[i] < sequences[j] }) {
			t.Fatalf("key %s delivered out of order: %v", key, sequences)
		}
	}
	if !workers["wA"] || !workers["wB"] {
		t.Fatalf("expected both publishers to participate, got %v", workers)
	}
}
