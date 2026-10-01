package control

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/auth"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), os.Getenv("TUBA_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &Store{Pool: pool, Issuer: "issuer"}
}

func testActiveReleaseID(ctx context.Context, t *testing.T, s *Store) string {
	t.Helper()
	var id string
	if err := s.Pool.QueryRow(ctx, `SELECT id FROM release_bundles WHERE state IN ('active','staged') ORDER BY created_at DESC LIMIT 1`).Scan(&id); err != nil {
		t.Skipf("no active/staged release available for binding test: %v", err)
	}
	return id
}

// fakeWriteRevoker records calls and can be told to fail, standing in for the
// broker Admin API so the disable/enable reconciliation can be tested end to
// end against a real database.
type fakeWriteRevoker struct {
	revoked  map[string]bool
	failWith error
	revokes  int
	restores int
}

func (f *fakeWriteRevoker) RevokeSourceWrite(ctx context.Context, principal, topic string) error {
	f.revokes++
	if f.failWith != nil {
		return f.failWith
	}
	f.revoked[principal+"/"+topic] = true
	return nil
}

func (f *fakeWriteRevoker) RestoreSourceWrite(ctx context.Context, principal, topic string) error {
	f.restores++
	if f.failWith != nil {
		return f.failWith
	}
	delete(f.revoked, principal+"/"+topic)
	return nil
}

// TestCollectorDisableRevokesSourceWriteIntegration drives the full
// disable/re-enable lifecycle with a fake revoker: registration, idempotent
// disable, loud failure with retry, and enable restoring the write ACL.
func TestCollectorDisableRevokesSourceWriteIntegration(t *testing.T) {
	if os.Getenv("TUBA_TEST_DATABASE_URL") == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s := openTestStore(t)
	principal := auth.Principal{Subject: "subject", Organization: "tenant_a"}

	enrollment, err := s.CreateCollectorEnrollment(ctx, principal, 30, "test-req-enroll")
	if err != nil {
		t.Fatalf("enrollment: %v", err)
	}
	collector, err := s.EnrollCollector(ctx, enrollment.Token, CollectorRegistration{InstallID: "col08-integration", Hostname: "test-host", OS: "linux", Architecture: "amd64", Version: "0.0.0-test"})
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	defer func() {
		_, _ = s.Pool.Exec(ctx, `DELETE FROM collector_source_kafka_bindings WHERE collector_id=$1`, collector.ID)
		_, _ = s.Pool.Exec(ctx, `DELETE FROM collector_agents WHERE id=$1`, collector.ID)
		_, _ = s.Pool.Exec(ctx, `DELETE FROM audit_events WHERE resource_id=$1 AND resource_type='collector'`, collector.ID)
	}()

	// A binding whose topic matches a real context of a real source is required;
	// build a throwaway source for it.
	source, err := s.RegisterSource(ctx, principal, SourceInput{VendorName: "test", VendorProduct: "test", VendorDataset: "col08.integration", ReleaseID: testActiveReleaseID(ctx, t, s), RateLimit: 10}, "test-req-source")
	if err != nil {
		t.Fatalf("register source: %v", err)
	}
	defer func() {
		_, _ = s.Pool.Exec(ctx, `DELETE FROM source_credentials WHERE source_instance_id=$1`, source.ID)
		_, _ = s.Pool.Exec(ctx, `DELETE FROM source_instances WHERE id=$1`, source.ID)
		_, _ = s.Pool.Exec(ctx, `DELETE FROM audit_events WHERE resource_id=$1 AND resource_type='source_instance'`, source.ID)
	}()
	topic := "tuba.source." + source.SourceContextID + ".v1"

	if _, err = s.RegisterSourceKafkaBinding(ctx, principal, collector.ID, source.ID, "tuba-test-principal", topic, "test-req-bind"); err != nil {
		t.Fatalf("register binding: %v", err)
	}
	// A topic that is not the source's own context topic must be refused.
	if _, err = s.RegisterSourceKafkaBinding(ctx, principal, collector.ID, source.ID, "tuba-test-principal", "tuba.source.ctx_00000000000000000000000000000000.v1", "test-req-bind-bad"); err == nil {
		t.Fatal("binding to a foreign context topic must be rejected")
	}

	revoker := &fakeWriteRevoker{revoked: map[string]bool{}}
	s.WriteRevoker = revoker

	// Disable revokes the write ACL and records it.
	if err = s.DisableCollector(ctx, principal, collector.ID, "test-req-disable"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if revoker.revokes != 1 || !revoker.revoked["tuba-test-principal/"+topic] {
		t.Fatalf("expected exactly one revoke, got %+v", revoker)
	}
	assertBindingRevoked(t, s, collector.ID, source.ID, true)

	// Re-disable is idempotent: no error, no second broker call.
	if err = s.DisableCollector(ctx, principal, collector.ID, "test-req-disable-2"); err != nil {
		t.Fatalf("re-disable must be a no-op: %v", err)
	}
	if revoker.revokes != 1 {
		t.Fatalf("idempotent re-disable must not call the broker again, revokes=%d", revoker.revokes)
	}

	// Enable restores the ACL before re-admitting the credential.
	if err = s.EnableCollector(ctx, principal, collector.ID, "test-req-enable"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if revoker.restores != 1 || revoker.revoked["tuba-test-principal/"+topic] {
		t.Fatalf("expected exactly one restore, got %+v", revoker)
	}
	assertBindingRevoked(t, s, collector.ID, source.ID, false)

	// Enable on a non-disabled collector is a conflict, not a no-op.
	if err = s.EnableCollector(ctx, principal, collector.ID, "test-req-enable-2"); err == nil || !strings.Contains(err.Error(), "not disabled") {
		t.Fatalf("expected not-disabled conflict, got %v", err)
	}

	// Failure path: the broker call fails; disable still commits the state but
	// the error is returned, recorded on the binding, and audited.
	revoker.failWith = errors.New("broker says no")
	err = s.DisableCollector(ctx, principal, collector.ID, "test-req-disable-fail")
	var revocationErr *SourceWriteRevocationError
	if !errors.As(err, &revocationErr) {
		t.Fatalf("expected SourceWriteRevocationError, got %v", err)
	}
	assertBindingError(t, s, collector.ID, source.ID, "broker says no")
	// Retry with a healthy broker converges without manual cleanup.
	revoker.failWith = nil
	if err = s.DisableCollector(ctx, principal, collector.ID, "test-req-disable-retry"); err != nil {
		t.Fatalf("retry after failure must succeed: %v", err)
	}
	assertBindingRevoked(t, s, collector.ID, source.ID, true)

	// Without a revoker the failure is loud, never silent.
	s.WriteRevoker = nil
	if _, err = s.RegisterSourceKafkaBinding(ctx, principal, collector.ID, source.ID, "tuba-test-principal", topic, "test-req-bind-2"); err != nil {
		t.Fatalf("re-register binding: %v", err)
	}
	// Reset the ledger so the binding is pending again.
	if _, err = s.Pool.Exec(ctx, `UPDATE collector_source_kafka_bindings SET write_revoked_at=NULL WHERE collector_id=$1`, collector.ID); err != nil {
		t.Fatal(err)
	}
	err = s.DisableCollector(ctx, principal, collector.ID, "test-req-disable-norevoker")
	if !errors.As(err, &revocationErr) || !strings.Contains(err.Error(), ErrRevokerNotConfigured.Error()) {
		t.Fatalf("expected loud revoker-not-configured error, got %v", err)
	}

	var auditCount int
	if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE resource_id=$1 AND action IN ('collector.source_write.revoke','collector.source_write.revoke_failed','collector.source_write.restore','collector.source_binding.register','collector.disable','collector.enable')`, collector.ID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount < 6 {
		t.Fatalf("expected revoke/failed/restore/register/disable/enable audit events, got %d", auditCount)
	}
}

func assertBindingRevoked(t *testing.T, s *Store, collectorID, sourceID string, revoked bool) {
	t.Helper()
	var revokedAt *time.Time
	var revokeErr *string
	if err := s.Pool.QueryRow(context.Background(), `SELECT write_revoked_at,write_revoke_error FROM collector_source_kafka_bindings WHERE collector_id=$1 AND source_instance_id=$2`, collectorID, sourceID).Scan(&revokedAt, &revokeErr); err != nil {
		t.Fatal(err)
	}
	if (revokedAt != nil) != revoked {
		t.Fatalf("write_revoked_at=%v, want revoked=%v", revokedAt, revoked)
	}
	if revoked && revokeErr != nil {
		t.Fatalf("successful revoke must clear the error, got %v", *revokeErr)
	}
}

func assertBindingError(t *testing.T, s *Store, collectorID, sourceID, want string) {
	t.Helper()
	var revokeErr *string
	if err := s.Pool.QueryRow(context.Background(), `SELECT write_revoke_error FROM collector_source_kafka_bindings WHERE collector_id=$1 AND source_instance_id=$2`, collectorID, sourceID).Scan(&revokeErr); err != nil {
		t.Fatal(err)
	}
	if revokeErr == nil || !strings.Contains(*revokeErr, want) {
		t.Fatalf("write_revoke_error=%v, want it to contain %q", revokeErr, want)
	}
}
