package control

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
	"tuba/product/internal/auth"
)

func TestCaseIdempotencyAndCursorIntegration(t *testing.T) {
	url := os.Getenv("TUBA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TUBA_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	_, e = p.Exec(ctx, `INSERT INTO identities(issuer,subject) VALUES('issuer','subject') ON CONFLICT DO NOTHING; INSERT INTO memberships(organization_id,identity_id,role_id) SELECT o.id,i.id,r.id FROM organizations o,identities i,roles r WHERE o.slug='tenant_a' AND i.issuer='issuer' AND i.subject='subject' AND r.organization_id=o.id AND r.name='analyst' ON CONFLICT DO NOTHING`)
	if e != nil {
		t.Fatal(e)
	}
	s := &Store{Pool: p, Issuer: "issuer"}
	pr := auth.Principal{Subject: "subject", Organization: "tenant_a"}
	in := CreateCaseInput{Title: "test", Severity: "high", AnomalyIDs: []string{"anom-integration-1"}}
	key := fmt.Sprintf("integration-key-%d", time.Now().UnixNano())
	a, status, e := s.CreateCase(ctx, pr, key, in)
	if e != nil || status != 201 {
		t.Fatalf("%d %v", status, e)
	}
	b, status, e := s.CreateCase(ctx, pr, key, in)
	if e != nil || b.ID != a.ID || status != 201 {
		t.Fatal("idempotency failed")
	}
	items, _, e := s.ListCases(ctx, pr, 1, "")
	if e != nil || len(items) != 1 {
		t.Fatalf("list: %v %d", e, len(items))
	}
	if _, _, e = s.ListCases(ctx, auth.Principal{Subject: "subject", Organization: "other_tenant"}, 10, ""); e == nil {
		t.Fatal("cross-tenant access accepted")
	}
	updated, e := s.UpdateCase(ctx, pr, a.ID, CaseActionInput{Status: "in_progress", ExpectedVersion: a.Version, Reason: "triage"}, "req-1")
	if e != nil || updated.Version != a.Version+1 {
		t.Fatalf("update: %+v %v", updated, e)
	}
	if _, e = s.UpdateCase(ctx, pr, a.ID, CaseActionInput{Status: "closed", ExpectedVersion: a.Version}, "req-2"); e == nil {
		t.Fatal("stale version accepted")
	}
	if e = s.SetMember(ctx, pr, "new-user", "viewer", false, "req-3"); e != nil {
		t.Fatal(e)
	}
	updated2, e := s.UpdateCase(ctx, pr, a.ID, CaseActionInput{Assignee: "new-user", Verdict: "true_positive", ExpectedVersion: updated.Version, Reason: "confirmed"}, "req-feedback")
	if e != nil || updated2.Version != updated.Version+1 {
		t.Fatalf("feedback: %+v %v", updated2, e)
	}
	var feedbackCount int
	if e = p.QueryRow(ctx, `SELECT count(*) FROM analysis_feedback WHERE source_case_id=$1 AND feedback_type='true_positive'`, a.ID).Scan(&feedbackCount); e != nil || feedbackCount != 1 {
		t.Fatalf("analysis feedback: %d %v", feedbackCount, e)
	}
	feedback, e := s.CreateAnalysisFeedback(ctx, pr, AnalysisFeedbackInput{
		FeedbackType: "false_negative",
		RuleID:       "auth.failure-then-success",
		EntityID:     "missing.user",
		EventIDs:     []string{"event-1", "event-2"},
		Reason:       "expected detection was absent",
	}, "req-fn")
	if e != nil || feedback.ID == 0 || len(feedback.EventIDs) != 2 {
		t.Fatalf("manual feedback: %+v %v", feedback, e)
	}
	audit, next, e := s.ListAudit(ctx, pr, 2, "")
	if e != nil || len(audit) != 2 || next == "" {
		t.Fatalf("audit page: %d %q %v", len(audit), next, e)
	}
	older, _, e := s.ListAudit(ctx, pr, 100, next)
	if e != nil || len(older) < 1 {
		t.Fatalf("audit cursor: %v %d", e, len(older))
	}
	activity, activityNext, e := s.ListCaseActivity(ctx, pr, a.ID, 1, "")
	if e != nil || len(activity) != 1 || activityNext == "" {
		t.Fatalf("case activity: %v %d %q", e, len(activity), activityNext)
	}
	if activity[0].Action != "case.update" || activity[0].Actor != "subject" {
		t.Fatalf("unexpected activity: %+v", activity[0])
	}
	overview, e := s.Overview(ctx, pr)
	if e != nil || overview.ActiveCases < 1 {
		t.Fatalf("overview: %+v %v", overview, e)
	}
	member, e := s.Authorize(ctx, auth.Principal{Subject: "new-user", Organization: "tenant_a"})
	if e != nil || !member.Can("case:read") {
		t.Fatal("grant ineffective")
	}
	if e = s.SetMember(ctx, pr, "new-user", "viewer", true, "req-4"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Authorize(ctx, auth.Principal{Subject: "new-user", Organization: "tenant_a"}); e == nil {
		t.Fatal("revocation ineffective")
	}
}
