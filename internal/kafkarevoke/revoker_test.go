package kafkarevoke

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

type stubAdmin struct {
	writePresent bool
	deleteErr    error
	createErr    error
	describeErr  error
	deletes      int
	creates      int
	lastFilter   kafka.DeleteACLsFilter
}

func (s *stubAdmin) DeleteACLs(ctx context.Context, req *kafka.DeleteACLsRequest) (*kafka.DeleteACLsResponse, error) {
	s.deletes++
	if len(req.Filters) > 0 {
		s.lastFilter = req.Filters[0]
	}
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}
	s.writePresent = false
	return &kafka.DeleteACLsResponse{Results: make([]kafka.DeleteACLsResult, len(req.Filters))}, nil
}

func (s *stubAdmin) CreateACLs(ctx context.Context, req *kafka.CreateACLsRequest) (*kafka.CreateACLsResponse, error) {
	s.creates++
	if s.createErr != nil {
		return nil, s.createErr
	}
	s.writePresent = true
	return &kafka.CreateACLsResponse{Errors: make([]error, len(req.ACLs))}, nil
}

func (s *stubAdmin) DescribeACLs(ctx context.Context, req *kafka.DescribeACLsRequest) (*kafka.DescribeACLsResponse, error) {
	if s.describeErr != nil {
		return nil, s.describeErr
	}
	response := &kafka.DescribeACLsResponse{}
	if s.writePresent {
		response.Resources = []kafka.ACLResource{{
			ResourceType: kafka.ResourceTypeTopic, ResourceName: req.Filter.ResourceNameFilter, PatternType: kafka.PatternTypeLiteral,
			ACLs: []kafka.ACLDescription{{Principal: req.Filter.PrincipalFilter, Host: "*", Operation: kafka.ACLOperationTypeWrite, PermissionType: kafka.ACLPermissionTypeAllow}},
		}}
	}
	return response, nil
}

const (
	testPrincipal = "tuba-windows-139"
	testTopic     = "tuba.source.ctx_a6b2f9d8a890cf30255a27103c90af47.v1"
)

func TestRevokeDeletesAndVerifies(t *testing.T) {
	stub := &stubAdmin{writePresent: true}
	r := &Revoker{client: stub}
	if err := r.RevokeSourceWrite(context.Background(), testPrincipal, testTopic); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if stub.deletes != 1 || stub.writePresent {
		t.Fatalf("expected one delete converging to absent ACL, got deletes=%d present=%v", stub.deletes, stub.writePresent)
	}
}

func TestRevokeIsIdempotentWhenACLAbsent(t *testing.T) {
	stub := &stubAdmin{writePresent: false}
	r := &Revoker{client: stub}
	if err := r.RevokeSourceWrite(context.Background(), testPrincipal, testTopic); err != nil {
		t.Fatalf("revoke of absent ACL must succeed: %v", err)
	}
}

func TestRevokeFailsWhenACLSurvives(t *testing.T) {
	// A broker that reports the ACL still present after delete must be an error,
	// otherwise disable would silently leave write access in place.
	stub := &stubAdmin{writePresent: true}
	flake := &survivingDeleteAdmin{stubAdmin: stub}
	r := &Revoker{client: flake}
	if err := r.RevokeSourceWrite(context.Background(), testPrincipal, testTopic); err == nil {
		t.Fatal("expected error when the WRITE ACL survives deletion")
	}
}

type survivingDeleteAdmin struct{ *stubAdmin }

func (s *survivingDeleteAdmin) DeleteACLs(ctx context.Context, req *kafka.DeleteACLsRequest) (*kafka.DeleteACLsResponse, error) {
	s.deletes++
	return &kafka.DeleteACLsResponse{Results: make([]kafka.DeleteACLsResult, len(req.Filters))}, nil
}

func TestRevokePropagatesBrokerError(t *testing.T) {
	stub := &stubAdmin{deleteErr: errors.New("broker unavailable")}
	r := &Revoker{client: stub}
	if err := r.RevokeSourceWrite(context.Background(), testPrincipal, testTopic); err == nil {
		t.Fatal("expected broker error to propagate")
	}
}

func TestRestoreCreatesAndVerifies(t *testing.T) {
	stub := &stubAdmin{writePresent: false}
	r := &Revoker{client: stub}
	if err := r.RestoreSourceWrite(context.Background(), testPrincipal, testTopic); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if stub.creates != 1 || !stub.writePresent {
		t.Fatalf("expected one create converging to present ACL, got creates=%d present=%v", stub.creates, stub.writePresent)
	}
}

func TestRestoreIsIdempotentWhenACLExists(t *testing.T) {
	stub := &stubAdmin{writePresent: true}
	r := &Revoker{client: stub}
	if err := r.RestoreSourceWrite(context.Background(), testPrincipal, testTopic); err != nil {
		t.Fatalf("restore of existing ACL must succeed: %v", err)
	}
}

func TestRestoreFailsWhenACLNotVisible(t *testing.T) {
	stub := &stubAdmin{writePresent: false}
	stub.createErr = nil
	flake := &uncommittedCreateAdmin{stubAdmin: stub}
	r := &Revoker{client: flake}
	if err := r.RestoreSourceWrite(context.Background(), testPrincipal, testTopic); err == nil {
		t.Fatal("expected error when the new ACL is not visible afterwards")
	}
}

type uncommittedCreateAdmin struct{ *stubAdmin }

func (s *uncommittedCreateAdmin) CreateACLs(ctx context.Context, req *kafka.CreateACLsRequest) (*kafka.CreateACLsResponse, error) {
	s.creates++
	return &kafka.CreateACLsResponse{Errors: make([]error, len(req.ACLs))}, nil
}

func TestRevokeSendsTypedPrincipal(t *testing.T) {
	// Regression: an untyped principal ("tuba-windows-139") makes the broker
	// match nothing on delete/describe, so the revoke looked successful while
	// the WRITE ACL stayed in place (observed live on 248).
	stub := &stubAdmin{writePresent: true}
	r := &Revoker{client: stub}
	if err := r.RevokeSourceWrite(context.Background(), testPrincipal, testTopic); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if stub.lastFilter.PrincipalFilter != "User:"+testPrincipal {
		t.Fatalf("principal must be typed, got %q", stub.lastFilter.PrincipalFilter)
	}
}

func TestNonSourceTopicsAreRefused(t *testing.T) {
	r := &Revoker{client: &stubAdmin{}}
	for _, topic := range []string{"tuba.collector.zeek.raw.v1", "tuba.source.ctx_not_hex.v1", "tuba.source.ctx_a6b2f9d8a890cf30255a27103c90af4.v1", "tuba.source.ctx_a6b2f9d8a890cf30255a27103c90af47"} {
		if err := r.RevokeSourceWrite(context.Background(), testPrincipal, topic); err == nil {
			t.Fatalf("topic %q must be refused", topic)
		}
		if err := r.RestoreSourceWrite(context.Background(), testPrincipal, topic); err == nil {
			t.Fatalf("topic %q must be refused", topic)
		}
	}
}
