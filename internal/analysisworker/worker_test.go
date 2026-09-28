package analysisworker

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysis"
)

type testConsumer struct {
	commits int
}

func (c *testConsumer) FetchMessage(context.Context) (kafka.Message, error) {
	return kafka.Message{}, nil
}

func (c *testConsumer) CommitMessages(context.Context, ...kafka.Message) error {
	c.commits++
	return nil
}

type testDeadLetter struct {
	messages []kafka.Message
}

func (d *testDeadLetter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	d.messages = append(d.messages, messages...)
	return nil
}

type testSink struct {
	calls int
	err   error
}

func (s *testSink) PutAnalysis(context.Context, analysis.Result) error {
	s.calls++
	return s.err
}

func TestInvalidTenantResultIsDeadLetteredAndCommitted(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	sink := &testSink{}
	worker := Worker{Organization: "tenant_a", Namespace: "tenant_a", Consumer: consumer, DeadLetter: deadLetter, Sink: sink}
	message := kafka.Message{Topic: "tuba.analysis.results.v1", Partition: 1, Offset: 42, Value: []byte(`{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_b","namespace":"tenant_b","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_b"}}}`)}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 0 || len(deadLetter.messages) != 1 || consumer.commits != 1 {
		t.Fatalf("sink=%d dead=%d commits=%d", sink.calls, len(deadLetter.messages), consumer.commits)
	}
}

func TestTransientSinkFailureDoesNotCommit(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	sink := &testSink{err: errors.New("elasticsearch unavailable")}
	worker := Worker{Organization: "tenant_a", Namespace: "tenant_a", Consumer: consumer, DeadLetter: deadLetter, Sink: sink}
	message := kafka.Message{Topic: "tuba.analysis.results.v1", Partition: 1, Offset: 42, Value: []byte(`{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_a"}}}`)}
	if err := worker.process(context.Background(), message); err == nil {
		t.Fatal("expected sink error")
	}
	if consumer.commits != 0 || len(deadLetter.messages) != 0 {
		t.Fatalf("dead=%d commits=%d", len(deadLetter.messages), consumer.commits)
	}
}
