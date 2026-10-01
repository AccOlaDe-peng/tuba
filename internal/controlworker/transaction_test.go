package controlworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func validInboxMessage() InboxMessage {
	sum := sha256.Sum256([]byte("payload"))
	return InboxMessage{
		ConsumerGroup: "g",
		MessageID:     "raw:" + strings.Repeat("a", 64),
		Topic:         "topic-a",
		Partition:     0,
		Offset:        41,
		PayloadHash:   hex.EncodeToString(sum[:]),
	}
}

func TestValidateInboxMessage(t *testing.T) {
	good := validInboxMessage()
	if err := validateInboxMessage(good); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	cases := map[string]InboxMessage{
		"empty group":      {ConsumerGroup: "", MessageID: "m", Topic: "t", PayloadHash: good.PayloadHash},
		"empty message id": {ConsumerGroup: "g", MessageID: "", Topic: "t", PayloadHash: good.PayloadHash},
		"empty topic":      {ConsumerGroup: "g", MessageID: "m", Topic: "", PayloadHash: good.PayloadHash},
		"bad hash":         {ConsumerGroup: "g", MessageID: "m", Topic: "t", PayloadHash: "zz"},
	}
	for name, msg := range cases {
		if err := validateInboxMessage(msg); err == nil {
			t.Fatalf("%s: accepted invalid inbox message", name)
		}
	}
	neg := good
	neg.Partition = -1
	if err := validateInboxMessage(neg); err == nil {
		t.Fatal("negative partition accepted")
	}
	neg = good
	neg.Offset = -1
	if err := validateInboxMessage(neg); err == nil {
		t.Fatal("negative offset accepted")
	}
}

func TestValidateCheckpoint(t *testing.T) {
	if err := validateCheckpoint(CheckpointUpdate{Topic: "t", Partition: 0, NextOffset: 7}); err != nil {
		t.Fatalf("valid checkpoint rejected: %v", err)
	}
	if err := validateCheckpoint(CheckpointUpdate{Topic: "", Partition: 0, NextOffset: 1}); err == nil {
		t.Fatal("empty topic accepted")
	}
	if err := validateCheckpoint(CheckpointUpdate{Topic: "t", Partition: -1, NextOffset: 1}); err == nil {
		t.Fatal("negative partition accepted")
	}
	if err := validateCheckpoint(CheckpointUpdate{Topic: "t", Partition: 0, NextOffset: -1}); err == nil {
		t.Fatal("negative offset accepted")
	}
}

func TestValidateOutboxMessage(t *testing.T) {
	good := OutboxMessage{
		Producer:     "p",
		AggregateKey: "k",
		Topic:        "t",
		MessageKey:   []byte("k"),
		Payload:      json.RawMessage(`{"a":1}`),
	}
	if err := validateOutboxMessage(good); err != nil {
		t.Fatalf("valid outbox message rejected: %v", err)
	}
	cases := map[string]func() OutboxMessage{
		"empty producer":  func() OutboxMessage { m := good; m.Producer = ""; return m },
		"empty aggregate": func() OutboxMessage { m := good; m.AggregateKey = ""; return m },
		"empty topic":     func() OutboxMessage { m := good; m.Topic = ""; return m },
		"empty key":       func() OutboxMessage { m := good; m.MessageKey = nil; return m },
		"invalid json":    func() OutboxMessage { m := good; m.Payload = json.RawMessage(`{`); return m },
	}
	for name, mutate := range cases {
		if err := validateOutboxMessage(mutate()); err == nil {
			t.Fatalf("%s: accepted invalid outbox message", name)
		}
	}
}

func TestOutboxPayloadHash(t *testing.T) {
	payload := json.RawMessage(`{"b":2,"a":1}`)
	want := sha256.Sum256(payload)
	if got := outboxPayloadHash(payload); got != hex.EncodeToString(want[:]) {
		t.Fatalf("hash=%s, want %s", got, hex.EncodeToString(want[:]))
	}
	if outboxPayloadHash(json.RawMessage(`{"a":1}`)) == outboxPayloadHash(json.RawMessage(`{"a":2}`)) {
		t.Fatal("distinct payloads produced identical hashes")
	}
}

func TestProcessInboxMessageRequiresWorkFunction(t *testing.T) {
	// Nil work must be rejected before any database interaction.
	if _, err := ProcessInboxMessage(t.Context(), nil, "w", Job{}, validInboxMessage(), nil); err == nil {
		t.Fatal("nil work function accepted")
	}
}

func TestProcessInboxMessageValidatesBeforeTouchingDatabase(t *testing.T) {
	msg := validInboxMessage()
	msg.PayloadHash = "not-hex"
	if _, err := ProcessInboxMessage(t.Context(), nil, "w", Job{}, msg, func(ctx context.Context, tx pgx.Tx) (AttemptOutcome, error) {
		return AttemptOutcome{}, nil
	}); err == nil {
		t.Fatal("invalid inbox message accepted")
	}
}

func TestWatermarkPointerRoundTripShape(t *testing.T) {
	// CheckpointUpdate carries an optional watermark; ensure a nil watermark
	// stays nil through the type (DB round trip is covered by integration).
	cp := CheckpointUpdate{Topic: "t", Partition: 0, NextOffset: 1}
	if cp.Watermark != nil {
		t.Fatal("watermark must default to nil")
	}
	now := time.Now().UTC()
	cp.Watermark = &now
	if cp.Watermark == nil || !cp.Watermark.Equal(now) {
		t.Fatal("watermark pointer not preserved")
	}
}
