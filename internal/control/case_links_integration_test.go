package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/auth"
)

// The fixture runs against the existing tenant_a organization and the shared
// integration identity (issuer/subject), like the other control integration
// tests: audit_events is append-only, so a throwaway organization or identity
// could never be deleted once audit rows reference it. All mutable fixture
// rows (the case cascading to links/snapshots, entity, identity space,
// idempotency records) are deleted at the end; audit rows remain by design,
// as in every existing integration test.

func caseLinksFixture(t *testing.T) (context.Context, *pgxpool.Pool, *Store, auth.Principal, string, string) {
	t.Helper()
	url := os.Getenv("TUBA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `INSERT INTO identities(issuer,subject) VALUES('issuer','subject') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	var orgID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM organizations WHERE slug='tenant_a'`).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("r03it%d", time.Now().UnixNano())
	t.Cleanup(func() {
		clean := context.Background()
		// Deleting the fixture cases cascades to case_links and (via the
		// cascade exception of the immutability trigger) case_snapshots.
		if _, err := pool.Exec(clean, `DELETE FROM cases WHERE organization_id=$1 AND title LIKE 'R03 integration%'`, orgID); err != nil {
			t.Logf("cleanup cases: %v", err)
		}
		for _, q := range []string{
			`DELETE FROM entities WHERE organization_id=$1 AND canonical_key LIKE 'r03it.%'`,
			`DELETE FROM identity_spaces WHERE organization_id=$1 AND name LIKE 'r03it-%'`,
			`DELETE FROM idempotency_records WHERE organization_id=$1 AND idempotency_key LIKE 'r03it-%'`,
		} {
			if _, err := pool.Exec(clean, q, orgID); err != nil {
				t.Logf("cleanup %s: %v", q, err)
			}
		}
	})
	s := &Store{Pool: pool, Issuer: "issuer"}
	principal := auth.Principal{Subject: "subject", Organization: "tenant_a"}
	return ctx, pool, s, principal, orgID, suffix
}

func TestCaseLinksSnapshotsHoldIntegration(t *testing.T) {
	ctx, pool, s, principal, orgID, suffix := caseLinksFixture(t)

	// Idempotent creation: same key + same body replays the stored response.
	key := "r03it-" + suffix
	in := CreateCaseInput{Title: "R03 integration", Severity: "high", AnomalyIDs: []string{"anom-r03-1"}}
	a, status, err := s.CreateCase(ctx, principal, key, in)
	if err != nil || status != 201 {
		t.Fatalf("create: %d %v", status, err)
	}
	b, status, err := s.CreateCase(ctx, principal, key, in)
	if err != nil || status != 201 || b.ID != a.ID {
		t.Fatalf("idempotent create replay: %+v %d %v", b, status, err)
	}
	other, status, err := s.CreateCase(ctx, principal, key, CreateCaseInput{Title: "different", Severity: "low"})
	if status != 409 || err == nil {
		t.Fatalf("idempotency key reuse with different body: %+v %d %v", other, status, err)
	}

	// Entity link requires a resolvable entity in the same organization.
	spaceName := "r03it-" + suffix
	spaceID := "is:" + strings.Repeat("cd", 32)
	entityID := "ent:" + strings.Repeat("ef", 32)
	if _, err = pool.Exec(ctx, `INSERT INTO identity_spaces(organization_id,space_id,name,kind) VALUES($1,$2,$3,'custom')`, orgID, spaceID, spaceName); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO entities(organization_id,entity_id,entity_type,authority,canonical_key,identity_strength,document,valid_from) VALUES($1,$2,'account',$3,$4,'strong','{}',now())`, orgID, entityID, spaceName, "r03it.user-"+suffix); err != nil {
		t.Fatal(err)
	}
	linked, added, err := s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "entity", RefKind: "entity", TargetID: entityID, ExpectedVersion: a.Version}, "req-link-1")
	if err != nil || !added || linked.Version != a.Version+1 {
		t.Fatalf("entity link: added=%v %+v %v", added, linked, err)
	}
	// Idempotent replay: same link, current version -> added=false, version unchanged, single audit record.
	replay, added, err := s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "entity", RefKind: "entity", TargetID: entityID, ExpectedVersion: linked.Version}, "req-link-1-replay")
	if err != nil || added || replay.Version != linked.Version {
		t.Fatalf("link replay: added=%v version=%d %v", added, replay.Version, err)
	}
	var linkAudits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE organization_id=$1 AND action='case.link' AND resource_id=$2`, orgID, a.ID).Scan(&linkAudits); err != nil || linkAudits != 1 {
		t.Fatalf("link audit count: %d %v", linkAudits, err)
	}
	// Unresolvable entity reference is rejected fail-closed.
	missing := "ent:" + strings.Repeat("00", 32)
	if _, _, err = s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "entity", RefKind: "entity", TargetID: missing, ExpectedVersion: linked.Version}, "req-link-missing"); err == nil {
		t.Fatal("unresolvable entity link accepted")
	}
	// Malformed links are rejected before any state change.
	if _, _, err = s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "evidence", RefKind: "entity", TargetID: entityID, ExpectedVersion: linked.Version}, "req-link-bad"); err == nil {
		t.Fatal("evidence link with entity ref_kind accepted")
	}
	// Risk contribution and evidence links.
	rcID := "rc:" + strings.Repeat("ab", 32)
	c2, added, err := s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "risk_contribution", RefKind: "risk_contribution", TargetID: rcID, ExpectedVersion: linked.Version}, "req-link-rc")
	if err != nil || !added {
		t.Fatalf("risk contribution link: %v", err)
	}
	c3, added, err := s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "evidence", RefKind: "event_id", TargetID: "evt-r03-1", ExpectedVersion: c2.Version}, "req-link-ev1")
	if err != nil || !added {
		t.Fatalf("evidence link: %v", err)
	}
	c4, _, err := s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "evidence", RefKind: "raw_event_id", TargetID: "raw-r03-1", ExpectedVersion: c3.Version}, "req-link-ev2")
	if err != nil {
		t.Fatalf("raw evidence link: %v", err)
	}
	// Stale expected_version is rejected, never last-write-wins.
	if _, _, err = s.AddCaseLink(ctx, principal, a.ID, AddCaseLinkInput{LinkType: "evidence", RefKind: "event_id", TargetID: "evt-r03-2", ExpectedVersion: a.Version}, "req-link-stale"); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale version accepted: %v", err)
	}
	links, err := s.ListCaseLinks(ctx, principal, a.ID)
	if err != nil || len(links) != 4 {
		t.Fatalf("list links: %d %v", len(links), err)
	}

	// Forensic snapshot freezes the case header plus the full link set.
	snap, err := s.CreateCaseSnapshot(ctx, principal, a.ID, "pre-triage freeze", c4.Version, "req-snap-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var frozen snapshotBody
	if err = json.Unmarshal(snap.Snapshot, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen.Links) != 4 || frozen.Case.ID != a.ID || len(frozen.AnomalyIDs) != 1 {
		t.Fatalf("snapshot content: %+v", frozen)
	}
	// Snapshot creation is a guarded mutation: replaying with the old version conflicts.
	if _, err = s.CreateCaseSnapshot(ctx, principal, a.ID, "stale freeze", c4.Version, "req-snap-2"); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale snapshot version accepted: %v", err)
	}
	// The snapshot row is insert-only: UPDATE and DELETE fail closed.
	if _, err = pool.Exec(ctx, `UPDATE case_snapshots SET label='tampered' WHERE id=$1`, snap.ID); err == nil {
		t.Fatal("snapshot update accepted")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM case_snapshots WHERE id=$1`, snap.ID); err == nil {
		t.Fatal("snapshot delete accepted")
	}
	snaps, err := s.ListCaseSnapshots(ctx, principal, a.ID)
	if err != nil || len(snaps) != 1 || snaps[0].Label != "pre-triage freeze" {
		t.Fatalf("list snapshots: %+v %v", snaps, err)
	}

	// Legal hold under the version guard.
	afterSnap, err := s.GetCase(ctx, principal, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.SetCaseHold(ctx, principal, a.ID, SetCaseHoldInput{Hold: true, ExpectedVersion: afterSnap.Version}, "req-hold-noreason"); err == nil {
		t.Fatal("hold without reason accepted")
	}
	held, changed, err := s.SetCaseHold(ctx, principal, a.ID, SetCaseHoldInput{Hold: true, Reason: "litigation", ExpectedVersion: afterSnap.Version}, "req-hold-1")
	if err != nil || !changed || !held.Hold || held.HoldReason != "litigation" || held.HoldAt == nil {
		t.Fatalf("set hold: changed=%v %+v %v", changed, held, err)
	}
	// Idempotent replay of the current hold state: no version bump, one audit record.
	replayHold, changed, err := s.SetCaseHold(ctx, principal, a.ID, SetCaseHoldInput{Hold: true, Reason: "litigation", ExpectedVersion: held.Version}, "req-hold-1-replay")
	if err != nil || changed || replayHold.Version != held.Version {
		t.Fatalf("hold replay: changed=%v version=%d %v", changed, replayHold.Version, err)
	}
	if _, _, err = s.SetCaseHold(ctx, principal, a.ID, SetCaseHoldInput{Hold: false, Reason: "stale", ExpectedVersion: afterSnap.Version}, "req-hold-stale"); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale hold release accepted: %v", err)
	}
	released, changed, err := s.SetCaseHold(ctx, principal, a.ID, SetCaseHoldInput{Hold: false, Reason: "closed", ExpectedVersion: held.Version}, "req-hold-2")
	if err != nil || !changed || released.Hold || released.HoldAt != nil {
		t.Fatalf("release hold: changed=%v %+v %v", changed, released, err)
	}
	var holdAudits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE organization_id=$1 AND action IN ('case.hold','case.hold_release') AND resource_id=$2`, orgID, a.ID).Scan(&holdAudits); err != nil || holdAudits != 2 {
		t.Fatalf("hold audit count: %d %v", holdAudits, err)
	}
}
