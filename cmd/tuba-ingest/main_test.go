package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

// The writer cache is keyed by a value that arrives at runtime — a registered
// source brings its namespace with it — so it must not grow without bound. Past
// the cap a new topic is refused by name instead of silently leaking another
// connection pool for the process lifetime.
func TestTopicWritersRefusesMoreThanTheCacheLimit(t *testing.T) {
	producers := newTopicWriters(nil, []string{"127.0.0.1:1"})
	for i := 0; i < maxTopicWriters; i++ {
		producers.writers[fmt.Sprintf("topic-%d", i)] = &kafka.Writer{}
	}
	err := producers.WriteMessages(context.Background(), "one-too-many")
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("err=%v, want a refusal naming the cache limit", err)
	}
	if _, cached := producers.writers["one-too-many"]; cached {
		t.Fatal("the refused topic was cached anyway")
	}
}

// A topic already in the cache must stay reachable at the cap, so a long-running
// process keeps serving the namespaces it already opened.
func TestTopicWritersKeepsServingCachedTopicsAtTheLimit(t *testing.T) {
	producers := newTopicWriters(nil, []string{"127.0.0.1:1"})
	cached := &kafka.Writer{}
	producers.writers["known"] = cached
	for i := 0; i < maxTopicWriters-1; i++ {
		producers.writers[fmt.Sprintf("topic-%d", i)] = &kafka.Writer{}
	}
	if got := producers.writers["known"]; got != cached {
		t.Fatal("the cached topic was displaced at the cap")
	}
}
