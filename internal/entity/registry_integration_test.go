package entity

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TUBA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// integrationOrg creates a dedicated organization and removes it plus all
// entity fixtures afterwards; tenant data is never touched.
func integrationOrg(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	slug := fmt.Sprintf("entity_it_%d", time.Now().UnixNano())
	var orgID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO organizations(slug, name, namespace) VALUES($1, $1, $1) RETURNING id::text`, slug).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		if _, err := pool.Exec(ctx, `DELETE FROM entities WHERE organization_id=$1`, orgID); err != nil {
			t.Logf("cleanup entities: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM identity_spaces WHERE organization_id=$1`, orgID); err != nil {
			t.Logf("cleanup spaces: %v", err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM organizations WHERE id=$1`, orgID); err != nil {
			t.Logf("cleanup org: %v", err)
		}
	})
	return orgID
}

func TestIntegrationSpaceRegistrationIdempotent(t *testing.T) {
	pool := integrationPool(t)
	orgID := integrationOrg(t, pool)
	r := NewRegistry(pool)
	ctx := context.Background()

	first, err := r.RegisterSpace(ctx, orgID, "CORP.Example", SpaceActiveDirectory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if first.SpaceID != second.SpaceID {
		t.Fatalf("idempotent registration produced different space ids: %q vs %q", first.SpaceID, second.SpaceID)
	}
	if first.Name != "corp.example" {
		t.Fatalf("space name not normalized: %q", first.Name)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM identity_spaces WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 space row, got %d", count)
	}
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceLocalAccounts); !errors.Is(err, ErrSpaceKindConflict) {
		t.Fatalf("kind conflict: got %v, want ErrSpaceKindConflict", err)
	}
	if _, err := r.RegisterSpace(ctx, orgID, "bad name!", SpaceCustom); err == nil {
		t.Fatal("invalid space name accepted")
	}
}

func TestIntegrationEntityIDStabilityAndIdempotentRegister(t *testing.T) {
	pool := integrationPool(t)
	orgID := integrationOrg(t, pool)
	r := NewRegistry(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}

	req := RegisterRequest{
		OrganizationID: orgID,
		Space:          "corp.example",
		EntityType:     TypeAccount,
		Identifiers: []Identifier{
			{KindSID, "S-1-5-21-100-200-300-1001"},
			{KindUsername, "JSmith"},
		},
		At: time.Now().UTC(),
	}
	first, err := r.Register(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Strength != StrengthStrong || first.CanonicalKey != "sid:s-1-5-21-100-200-300-1001" {
		t.Fatalf("strong identifier did not win: %+v", first)
	}
	// Re-register the same real identity, fields in different order/case.
	req.Identifiers = []Identifier{{KindUsername, "jsmith"}, {KindSID, "s-1-5-21-100-200-300-1001"}}
	second, err := r.Register(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.EntityID != first.EntityID {
		t.Fatalf("entity id not stable across registrations: %q vs %q", first.EntityID, second.EntityID)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("idempotent registration created %d rows", count)
	}
	// A different SID is a different real identity and must not collide.
	other := req
	other.Identifiers = []Identifier{{KindSID, "S-1-5-21-999-888-777-666"}}
	third, err := r.Register(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if third.EntityID == first.EntityID {
		t.Fatal("distinct strong identities collided on entity id")
	}
	// Cross-space same canonical key must not merge.
	if _, err := r.RegisterSpace(ctx, orgID, "other.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	req.Space = "other.example"
	fourth, err := r.Register(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if fourth.EntityID == first.EntityID {
		t.Fatal("same identity in another identity space merged")
	}
}

func TestIntegrationConcurrentRegistrationSingleRow(t *testing.T) {
	pool := integrationPool(t)
	orgID := integrationOrg(t, pool)
	r := NewRegistry(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	req := RegisterRequest{
		OrganizationID: orgID,
		Space:          "corp.example",
		EntityType:     TypeDevice,
		Identifiers:    []Identifier{{KindDeviceUUID, "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}},
		At:             time.Now().UTC(),
	}
	var wg sync.WaitGroup
	ids := make([]string, 8)
	errs := make([]error, 8)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e, err := r.Register(ctx, req)
			ids[i], errs[i] = e.EntityID, err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("registration %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("concurrent registration produced different ids: %q vs %q", ids[i], ids[0])
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent idempotent registration created %d rows", count)
	}
}

func TestIntegrationWeakTransferAndConflict(t *testing.T) {
	pool := integrationPool(t)
	orgID := integrationOrg(t, pool)
	r := NewRegistry(pool)
	ctx := context.Background()
	if _, err := r.RegisterSpace(ctx, orgID, "host-local", SpaceLocalAccounts); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC()

	first, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "host-local", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindHostname, "WEB01"}},
		At:          t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Strength != StrengthWeak || first.CanonicalKey != "hostname:web01" {
		t.Fatalf("weak registration: %+v", first)
	}
	// Idempotent while the occurrence is active.
	again, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "host-local", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindHostname, "web01"}},
		At:          t0.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.EntityID != first.EntityID || again.ValidTo != nil {
		t.Fatalf("active weak identity not idempotent: %+v", again)
	}
	// Transfer: hostname web01 changes hands.
	closed, err := r.TransferWeak(ctx, orgID, "host-local", TypeDevice, Identifier{KindHostname, "web01"}, t0.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if closed.ValidTo == nil || !closed.ValidTo.Equal(t0.Add(2*time.Hour)) {
		t.Fatalf("transfer did not close the occurrence: %+v", closed)
	}
	// Re-register after transfer: new occurrence, new entity id.
	third, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "host-local", EntityType: TypeDevice,
		Identifiers: []Identifier{{KindHostname, "Web01."}},
		At:          t0.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if third.EntityID == first.EntityID {
		t.Fatal("reused weak identity merged with previous owner")
	}
	if third.CanonicalKey != "hostname:web01#2" {
		t.Fatalf("new occurrence key = %q, want hostname:web01#2", third.CanonicalKey)
	}
	if !third.ValidFrom.Equal(t0.Add(3 * time.Hour)) {
		t.Fatalf("new occurrence valid_from = %v", third.ValidFrom)
	}
	// No active occurrence after both are closed.
	if _, err := r.TransferWeak(ctx, orgID, "host-local", TypeDevice, Identifier{KindHostname, "web01"}, t0.Add(4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.TransferWeak(ctx, orgID, "host-local", TypeDevice, Identifier{KindHostname, "web01"}, t0.Add(5*time.Hour)); !errors.Is(err, ErrNoActiveOccurrence) {
		t.Fatalf("transfer without active occurrence: %v", err)
	}
	// Strong identities are never transferable.
	if _, err := r.TransferWeak(ctx, orgID, "host-local", TypeAccount, Identifier{KindSID, "S-1-5-21-1-2-3-4"}, t0.Add(time.Hour)); !errors.Is(err, ErrNotWeakIdentity) {
		t.Fatalf("strong transfer: %v", err)
	}
	// Transfer before valid_from is rejected fail-closed.
	if _, err := r.TransferWeak(ctx, orgID, "host-local", TypeDevice, Identifier{KindHostname, "web02"}, t0); !errors.Is(err, ErrNoActiveOccurrence) {
		t.Fatalf("unknown weak transfer: %v", err)
	}
}

func TestIntegrationRegisterRequiresSpaceAndValidType(t *testing.T) {
	pool := integrationPool(t)
	orgID := integrationOrg(t, pool)
	r := NewRegistry(pool)
	ctx := context.Background()

	_, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "unregistered", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "S-1-5-21-1-2-3-4"}},
	})
	if !errors.Is(err, ErrSpaceNotRegistered) {
		t.Fatalf("unregistered space: %v", err)
	}
	if _, err := r.RegisterSpace(ctx, orgID, "corp.example", SpaceActiveDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "corp.example", EntityType: "user",
		Identifiers: []Identifier{{KindSID, "S-1-5-21-1-2-3-4"}},
	}); !errors.Is(err, ErrInvalidEntityType) {
		t.Fatalf("invalid entity type: %v", err)
	}
	if _, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
		Identifiers: []Identifier{{KindSID, "not-a-sid"}},
	}); !errors.Is(err, ErrInvalidIdentifier) {
		t.Fatalf("invalid identifier: %v", err)
	}
	if _, err := r.Register(ctx, RegisterRequest{
		OrganizationID: orgID, Space: "corp.example", EntityType: TypeAccount,
	}); !errors.Is(err, ErrNoIdentifier) {
		t.Fatalf("empty identifiers: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM entities WHERE organization_id=$1`, orgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("fail-closed rejections still wrote %d rows", count)
	}
}
