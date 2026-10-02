package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/auth"
)

// R04 integration fixture, same conventions as the R03 fixture: the
// existing tenant_a organization and shared integration identity are reused
// (audit_events is append-only). All mutable fixture rows are deleted at
// the end; audit rows remain by design.
func r04Fixture(t *testing.T) (context.Context, *pgxpool.Pool, *Store, auth.Principal, string, string) {
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
	suffix := fmt.Sprintf("r04it%d", time.Now().UnixNano())
	t.Cleanup(func() {
		clean := context.Background()
		for _, q := range []string{
			`DELETE FROM analysis_feedback WHERE organization_id=$1 AND (idempotency_key LIKE 'r04it-%' OR anomaly_id LIKE 'r04it-%' OR rule_id LIKE 'r04it-%')`,
			`DELETE FROM finding_labels WHERE organization_id=$1 AND anomaly_id LIKE 'r04it-%'`,
			`DELETE FROM cases WHERE organization_id=$1 AND title LIKE 'r04it-%'`,
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

func TestFeedbackAuditEvaluationIntegration(t *testing.T) {
	ctx, pool, s, principal, orgID, suffix := r04Fixture(t)

	anomaly := "r04it-anom-" + suffix
	rule := "r04it-rule-" + suffix

	// Idempotent submission: same key replays the stored row, exactly one
	// feedback row and one audit event.
	key := "r04it-key-" + suffix
	in := AnalysisFeedbackInput{FeedbackType: "true_positive", AnomalyID: anomaly, RuleID: rule, EntityID: "ent:" + strings.Repeat("ab", 32), Reason: "confirmed by triage", IdempotencyKey: key}
	first, err := s.CreateAnalysisFeedback(ctx, principal, in, "req-r04-1")
	if err != nil {
		t.Fatalf("feedback: %v", err)
	}
	replay, err := s.CreateAnalysisFeedback(ctx, principal, in, "req-r04-1-replay")
	if err != nil {
		t.Fatalf("feedback replay: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("idempotent replay inserted a new row: %d vs %d", replay.ID, first.ID)
	}
	var feedbackRows, feedbackAudits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM analysis_feedback WHERE organization_id=$1 AND idempotency_key=$2`, orgID, key).Scan(&feedbackRows); err != nil || feedbackRows != 1 {
		t.Fatalf("feedback rows: %d %v", feedbackRows, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE organization_id=$1 AND action='analysis.feedback' AND resource_id=$2`, orgID, anomaly).Scan(&feedbackAudits); err != nil || feedbackAudits != 1 {
		t.Fatalf("feedback audit count: %d %v", feedbackAudits, err)
	}

	// The feedback-derived finding annotation is decoupled metadata.
	label, err := s.GetFindingLabel(ctx, principal, anomaly)
	if err != nil || label.Label != "confirmed" {
		t.Fatalf("finding label: %+v %v", label, err)
	}
	// A later correction relabels the same finding (metadata only).
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "false_positive", AnomalyID: anomaly, RuleID: rule, Reason: "reclassified", IdempotencyKey: key + "-2"}, "req-r04-2"); err != nil {
		t.Fatalf("correction feedback: %v", err)
	}
	label, err = s.GetFindingLabel(ctx, principal, anomaly)
	if err != nil || label.Label != "false_positive" {
		t.Fatalf("relabeled finding: %+v %v", label, err)
	}

	// Red line: feedback never touches production model artifacts online.
	var modelRowsBefore, sampleRowsBefore, modelRowsAfter, sampleRowsAfter int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM baseline_models`).Scan(&modelRowsBefore); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM feature_samples`).Scan(&sampleRowsBefore); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "inconclusive", AnomalyID: anomaly, RuleID: rule, Reason: "unsure", IdempotencyKey: key + "-3"}, "req-r04-3"); err != nil {
		t.Fatalf("inconclusive feedback: %v", err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM baseline_models`).Scan(&modelRowsAfter); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM feature_samples`).Scan(&sampleRowsAfter); err != nil {
		t.Fatal(err)
	}
	if modelRowsAfter != modelRowsBefore || sampleRowsAfter != sampleRowsBefore {
		t.Fatalf("feedback mutated model artifacts: models %d->%d samples %d->%d", modelRowsBefore, modelRowsAfter, sampleRowsBefore, sampleRowsAfter)
	}

	// Case-targeted feedback: the target case must resolve in the tenant.
	c, status, err := s.CreateCase(ctx, principal, "r04it-case-"+suffix, CreateCaseInput{Title: "r04it-case " + suffix, Severity: "medium", AnomalyIDs: []string{anomaly}})
	if err != nil || status != 201 {
		t.Fatalf("case: %d %v", status, err)
	}
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "true_positive", TargetCaseID: c.ID, Reason: "case verdict"}, "req-r04-4"); err != nil {
		t.Fatalf("case-targeted feedback: %v", err)
	}
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "true_positive", TargetCaseID: "00000000-0000-0000-0000-000000000000", Reason: "no such case"}, "req-r04-5"); err == nil {
		t.Fatal("feedback targeting a missing case accepted")
	}
	// Fail-closed input validation.
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "bogus", AnomalyID: anomaly}, "req-r04-6"); err == nil {
		t.Fatal("unknown feedback type accepted")
	}
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "true_positive"}, "req-r04-7"); err == nil {
		t.Fatal("feedback without resolvable target accepted")
	}
	if _, err = s.CreateAnalysisFeedback(ctx, principal, AnalysisFeedbackInput{FeedbackType: "false_negative", RuleID: rule, EntityID: "ent:" + strings.Repeat("cd", 32), Reason: "missed detection", IdempotencyKey: key + "-fn"}, "req-r04-8"); err != nil {
		t.Fatalf("false-negative feedback: %v", err)
	}

	// Offline evaluation dataset: feedback joined with the decoupled label
	// and the attached case verdict, filterable per rule.
	if _, err = s.UpdateCase(ctx, principal, c.ID, CaseActionInput{Status: "closed", Verdict: "true_positive", ExpectedVersion: c.Version, Reason: "done"}, "req-r04-9"); err != nil {
		t.Fatalf("case verdict: %v", err)
	}
	rows, err := s.ListFeedbackEvaluation(ctx, principal, FeedbackEvaluationFilter{RuleID: rule})
	if err != nil {
		t.Fatalf("evaluation rows: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("evaluation rows for rule: %d", len(rows))
	}
	var sawLabel, sawVerdict bool
	for _, r := range rows {
		if r.AnomalyID == anomaly && r.FindingLabel == "inconclusive" {
			sawLabel = true
		}
	}
	caseRows, err := s.ListFeedbackEvaluation(ctx, principal, FeedbackEvaluationFilter{FeedbackType: "true_positive"})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range caseRows {
		if r.TargetCaseID != nil && *r.TargetCaseID == c.ID && r.CaseVerdict == "true_positive" {
			sawVerdict = true
		}
	}
	if !sawLabel || !sawVerdict {
		t.Fatalf("evaluation join incomplete: label=%v verdict=%v", sawLabel, sawVerdict)
	}

	// Per-rule metrics: precision = tp / (tp + fp) over the exported dataset.
	metrics, err := s.SummarizeFeedbackByRule(ctx, principal)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, m := range metrics {
		if m.RuleID == rule {
			found = true
			if m.TruePositive != 1 || m.FalsePositive != 1 || m.FalseNegative != 1 || m.Inconclusive != 1 {
				t.Fatalf("rule metrics: %+v", m)
			}
			if m.Precision < 0.49 || m.Precision > 0.51 {
				t.Fatalf("rule precision: %+v", m)
			}
		}
	}
	if !found {
		t.Fatal("rule missing from feedback metrics")
	}
}

func TestAutoCaseDedupeIntegration(t *testing.T) {
	ctx, pool, s, principal, orgID, suffix := r04Fixture(t)

	entity := "ent:" + strings.Repeat("ef", 32)
	policy := "r04it-policy-" + suffix
	bucket := time.Now().UTC().Truncate(time.Hour)
	in := AutoCaseFindingInput{
		EntityID:    entity,
		PolicyID:    policy,
		BucketStart: bucket,
		AnomalyID:   "r04it-anom-" + suffix + "-1",
		Title:       "r04it-auto " + suffix,
		Severity:    "high",
	}

	// Default-off: disabled path fails closed and creates nothing.
	if _, _, err := s.AutoCaseForFinding(ctx, principal, false, in, "req-auto-off"); !errors.Is(err, ErrAutoCaseDisabled) {
		t.Fatalf("disabled auto case: %v", err)
	}
	var caseCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM cases WHERE organization_id=$1 AND title LIKE 'r04it-auto%'`, orgID).Scan(&caseCount); err != nil || caseCount != 0 {
		t.Fatalf("disabled path created a case: %d", caseCount)
	}

	// Enabled: first finding for the key creates exactly one case.
	c1, created, err := s.AutoCaseForFinding(ctx, principal, true, in, "req-auto-1")
	if err != nil || !created {
		t.Fatalf("auto create: created=%v %v", created, err)
	}

	// Same key, second finding: no new case; the anomaly link accumulates.
	in2 := in
	in2.AnomalyID = "r04it-anom-" + suffix + "-2"
	c2, created, err := s.AutoCaseForFinding(ctx, principal, true, in2, "req-auto-2")
	if err != nil || created || c2.ID != c1.ID {
		t.Fatalf("same-key finding created a new case: created=%v ids %s vs %s %v", created, c2.ID, c1.ID, err)
	}
	// Replaying the same anomaly does not inflate the link count.
	if _, created, err = s.AutoCaseForFinding(ctx, principal, true, in2, "req-auto-2-replay"); err != nil || created {
		t.Fatalf("anomaly replay: created=%v %v", created, err)
	}
	var linked int
	if err = pool.QueryRow(ctx, `SELECT linked_anomalies FROM auto_case_dedupe WHERE organization_id=$1 AND entity_id=$2 AND policy_id=$3 AND time_bucket=$4`, orgID, entity, policy, bucket).Scan(&linked); err != nil || linked != 2 {
		t.Fatalf("linked anomalies: %d %v", linked, err)
	}
	detail, err := s.GetCase(ctx, principal, c1.ID)
	if err != nil || len(detail.AnomalyIDs) != 2 {
		t.Fatalf("case anomaly accumulation: %+v %v", detail.AnomalyIDs, err)
	}

	// Different bucket and different entity each produce their own case.
	in3 := in
	in3.BucketStart = bucket.Add(time.Hour)
	c3, created, err := s.AutoCaseForFinding(ctx, principal, true, in3, "req-auto-3")
	if err != nil || !created || c3.ID == c1.ID {
		t.Fatalf("different bucket did not create its own case: created=%v %v", created, err)
	}
	in4 := in
	in4.EntityID = "ent:" + strings.Repeat("01", 32)
	c4, created, err := s.AutoCaseForFinding(ctx, principal, true, in4, "req-auto-4")
	if err != nil || !created || c4.ID == c1.ID {
		t.Fatalf("different entity did not create its own case: created=%v %v", created, err)
	}

	// Audit: one auto case.create for the first key, auto_link per newly
	// linked anomaly (replay adds none).
	var createAudits, linkAudits int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE organization_id=$1 AND action='case.create' AND resource_id=$2`, orgID, c1.ID).Scan(&createAudits); err != nil || createAudits != 1 {
		t.Fatalf("auto create audits: %d %v", createAudits, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE organization_id=$1 AND action='case.auto_link' AND resource_id=$2`, orgID, c1.ID).Scan(&linkAudits); err != nil || linkAudits != 2 {
		t.Fatalf("auto link audits: %d %v", linkAudits, err)
	}
}
