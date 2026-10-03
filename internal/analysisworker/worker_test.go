package analysisworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysis"
	"tuba/product/internal/sink"
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
	err      error
}

func (d *testDeadLetter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	if d.err != nil {
		return d.err
	}
	d.messages = append(d.messages, messages...)
	return nil
}

type testSink struct {
	legacyCalls int
	objectCalls int
	legacyErr   error
	objectErr   error
	batchErr    error
	objectErrs  func(analysis.ObjectResult) error
	objects     []analysis.ObjectResult
}

func (s *testSink) PutAnalysis(context.Context, analysis.Result) error {
	s.legacyCalls++
	return s.legacyErr
}

func (s *testSink) PutAnalysisObject(_ context.Context, object analysis.ObjectResult) error {
	s.objectCalls++
	s.objects = append(s.objects, object)
	return s.objectErr
}

func (s *testSink) PutAnalysisObjectBatch(_ context.Context, objects []analysis.ObjectResult) ([]error, error) {
	results := make([]error, len(objects))
	for i, object := range objects {
		s.objectCalls++
		s.objects = append(s.objects, object)
		if s.objectErrs != nil {
			results[i] = s.objectErrs(object)
		} else {
			results[i] = s.objectErr
		}
	}
	return results, s.batchErr
}

const legacyMessage = `{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_a"},"detection":{"window":{"start":"2026-09-26T10:00:00Z"}}}}`

const legacyMessageNoWindow = `{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_a"}}}`

func v2Message(objectType string, revision int) string {
	return fmt.Sprintf(`{"contract_version":"2.0.0","object_type":%q,"object_id":"obj-1","revision":%d,"operation":"upsert","generation":"g1","organization_id":"tenant_a","namespace":"tenant_a","rule_id":"r1","rule_version":"2.1.0","run_id":"run-1","window_start":"2026-09-26T10:00:00Z","date_key":"2026-09-26","input_refs":[],"document":{"organization":{"id":"tenant_a"}}}`, objectType, revision)
}

func newWorker(consumer *testConsumer, deadLetter *testDeadLetter, s *testSink) Worker {
	return Worker{Organization: "tenant_a", Namespace: "tenant_a", Consumer: consumer, DeadLetter: deadLetter, Sink: s, MaxAttempts: 3, RetryBackoff: time.Millisecond}
}

func deadLetterEnvelope(t *testing.T, message kafka.Message) (code, stage string, retryable bool) {
	t.Helper()
	var envelope struct {
		Failure struct {
			Stage     string `json:"stage"`
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"failure"`
	}
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		t.Fatalf("dead letter payload is not the standard envelope: %v", err)
	}
	return envelope.Failure.Code, envelope.Failure.Stage, envelope.Failure.Retryable
}

func TestV2ObjectIsWrittenAndCommitted(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v2", Partition: 0, Offset: 7, Value: []byte(v2Message("feature", 3))}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if s.objectCalls != 1 || s.legacyCalls != 0 || consumer.commits != 1 || len(deadLetter.messages) != 0 {
		t.Fatalf("objects=%d legacy=%d commits=%d dead=%d", s.objectCalls, s.legacyCalls, consumer.commits, len(deadLetter.messages))
	}
	if s.objects[0].ObjectType != "feature" || s.objects[0].Revision != 3 {
		t.Fatalf("unexpected routed object %+v", s.objects[0])
	}
}

func TestStaleRevisionIsDroppedAndCommittedWithoutDLQ(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{objectErr: sink.StaleRevisionError{Index: "ueba-analysis-anomaly-tenant_a", ID: "obj-1", Existing: 5, Incoming: 2}}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v2", Value: []byte(v2Message("anomaly", 2))}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if consumer.commits != 1 || len(deadLetter.messages) != 0 {
		t.Fatalf("stale revision must be dropped and committed without DLQ: commits=%d dead=%d", consumer.commits, len(deadLetter.messages))
	}
}

func TestRevisionConflictIsDeadLetteredWithQueryableReason(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{objectErr: sink.PermanentIndexError{Code: "ANALYSIS_REVISION_CONFLICT", Message: "same revision, different content"}}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v2", Partition: 1, Offset: 42, Value: []byte(v2Message("anomaly", 2))}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(deadLetter.messages) != 1 || consumer.commits != 1 {
		t.Fatalf("dead=%d commits=%d", len(deadLetter.messages), consumer.commits)
	}
	code, stage, retryable := deadLetterEnvelope(t, deadLetter.messages[0])
	if code != "ANALYSIS_REVISION_CONFLICT" || stage != "analysis-sink-index" || retryable {
		t.Fatalf("dead letter reason not queryable: code=%q stage=%q retryable=%v", code, stage, retryable)
	}
}

func TestDeadLetterWriteFailureDoesNotCommit(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{err: errors.New("dlq broker unavailable")}
	s := &testSink{objectErr: sink.PermanentIndexError{Code: "ANALYSIS_REVISION_CONFLICT", Message: "conflict"}}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v2", Value: []byte(v2Message("anomaly", 2))}
	if err := worker.process(context.Background(), message); err == nil {
		t.Fatal("expected DLQ publish error")
	}
	if consumer.commits != 0 {
		t.Fatal("offset must not be committed when the DLQ write fails")
	}
}

func TestTransientObjectSinkFailureDoesNotCommit(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{objectErr: errors.New("elasticsearch unavailable")}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v2", Value: []byte(v2Message("baseline", 1))}
	if err := worker.process(context.Background(), message); err == nil {
		t.Fatal("expected sink error")
	}
	if consumer.commits != 0 || len(deadLetter.messages) != 0 {
		t.Fatalf("dead=%d commits=%d", len(deadLetter.messages), consumer.commits)
	}
}

func TestInvalidV2ContractIsDeadLetteredAndCommitted(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v2", Partition: 1, Offset: 42, Value: []byte(`{"contract_version":"2.0.0","object_type":"unknown"}`)}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if s.objectCalls != 0 || len(deadLetter.messages) != 1 || consumer.commits != 1 {
		t.Fatalf("objects=%d dead=%d commits=%d", s.objectCalls, len(deadLetter.messages), consumer.commits)
	}
	code, stage, _ := deadLetterEnvelope(t, deadLetter.messages[0])
	if code != "ANALYSIS_CONTRACT_INVALID" || stage != "analysis-sink-parse" {
		t.Fatalf("code=%q stage=%q", code, stage)
	}
}

func TestLegacyV1DualWritesAndMigrates(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v1", Partition: 1, Offset: 42, Value: []byte(legacyMessage)}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if s.legacyCalls != 1 || s.objectCalls != 1 || consumer.commits != 1 || len(deadLetter.messages) != 0 {
		t.Fatalf("legacy=%d objects=%d commits=%d dead=%d", s.legacyCalls, s.objectCalls, consumer.commits, len(deadLetter.messages))
	}
	object := s.objects[0]
	if object.ObjectType != analysis.ObjectTypeAnomaly || object.Revision != 1 || object.Generation != analysis.LegacyGeneration ||
		object.Operation != analysis.OperationUpsert || len(object.InputRefs) != 0 || object.DateKey != "2026-09-26" {
		t.Fatalf("unexpected migrated object %+v", object)
	}
}

func TestLegacyV1WithoutWindowGoesToMigrationQuarantine(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v1", Partition: 1, Offset: 42, Value: []byte(legacyMessageNoWindow)}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	// The legacy document is retained in the v1 index; the v2 mirror is
	// refused (no fabrication of the window time) and the message lands in
	// the migration quarantine with a queryable reason.
	if s.legacyCalls != 1 || s.objectCalls != 0 || consumer.commits != 1 || len(deadLetter.messages) != 1 {
		t.Fatalf("legacy=%d objects=%d commits=%d dead=%d", s.legacyCalls, s.objectCalls, consumer.commits, len(deadLetter.messages))
	}
	code, stage, _ := deadLetterEnvelope(t, deadLetter.messages[0])
	if code != "ANALYSIS_LEGACY_WINDOW_MISSING" || stage != "analysis-sink-migrate" {
		t.Fatalf("code=%q stage=%q", code, stage)
	}
}

func TestInvalidTenantResultIsDeadLetteredAndCommitted(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v1", Partition: 1, Offset: 42, Value: []byte(`{"contract_version":"1.0.0","result_type":"anomaly","result_id":"a1","organization_id":"tenant_b","namespace":"tenant_b","rule_id":"r1","rule_version":"1.0.0","run_id":"run-1","document":{"organization":{"id":"tenant_b"}}}`)}
	if err := worker.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if s.legacyCalls != 0 || len(deadLetter.messages) != 1 || consumer.commits != 1 {
		t.Fatalf("legacy=%d dead=%d commits=%d", s.legacyCalls, len(deadLetter.messages), consumer.commits)
	}
}

func TestTransientSinkFailureDoesNotCommit(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{legacyErr: errors.New("elasticsearch unavailable")}
	worker := newWorker(consumer, deadLetter, s)
	message := kafka.Message{Topic: "tuba.analysis.results.v1", Partition: 1, Offset: 42, Value: []byte(legacyMessage)}
	if err := worker.process(context.Background(), message); err == nil {
		t.Fatal("expected sink error")
	}
	if consumer.commits != 0 || len(deadLetter.messages) != 0 {
		t.Fatalf("dead=%d commits=%d", len(deadLetter.messages), consumer.commits)
	}
}

func TestUnknownContractVersionIsDeadLettered(t *testing.T) {
	consumer := &testConsumer{}
	deadLetter := &testDeadLetter{}
	s := &testSink{}
	worker := newWorker(consumer, deadLetter, s)
	for _, value := range []string{`{"contract_version":"9.9.9"}`, `not json`} {
		message := kafka.Message{Topic: "tuba.analysis.results.v1", Value: []byte(value)}
		if err := worker.process(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	if len(deadLetter.messages) != 2 || consumer.commits != 2 || s.legacyCalls != 0 || s.objectCalls != 0 {
		t.Fatalf("dead=%d commits=%d", len(deadLetter.messages), consumer.commits)
	}
}
