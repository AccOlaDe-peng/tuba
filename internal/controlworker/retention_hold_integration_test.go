package controlworker

import (
	"context"
	"testing"
	"time"

	"tuba/product/internal/telemetry"
)

// TestRetentionHoldGuardIntegration locks the R03 protection semantics: a
// processing job referenced as job_id evidence of a case under legal hold is
// never swept, while an equivalent unreferenced job is; releasing the hold
// makes the evidence job eligible again.
func TestRetentionHoldGuardIntegration(t *testing.T) {
	ctx, pool, orgID, suffix := retentionFixture(t)

	// Identity + case with hold=true, linked to an old terminal job as
	// job_id evidence. Inserted directly so the guard is exercised at the
	// database level, independent of the API layer.
	subject := "retention-hold-it-" + suffix
	var identityID string
	if err := pool.QueryRow(ctx, `INSERT INTO identities(issuer,subject) VALUES('retention-hold-it',$1) RETURNING id::text`, subject).Scan(&identityID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean := context.Background()
		if _, err := pool.Exec(clean, `DELETE FROM cases WHERE organization_id=$1`, orgID); err != nil {
			t.Logf("cleanup cases: %v", err)
		}
		if _, err := pool.Exec(clean, `DELETE FROM identities WHERE issuer='retention-hold-it' AND subject=$1`, subject); err != nil {
			t.Logf("cleanup identity: %v", err)
		}
	})
	old := time.Now().Add(-10 * 24 * time.Hour)
	evidenceJob := insertRetentionJob(t, ctx, pool, orgID, "succeeded", &old)
	unprotectedJob := insertRetentionJob(t, ctx, pool, orgID, "succeeded", &old)

	var caseID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO cases(organization_id,title,severity,created_by,hold,hold_reason,hold_set_by,hold_at)
		VALUES($1,'hold guard it','high',$2,true,'litigation',$2,now()) RETURNING id::text`, orgID, identityID).Scan(&caseID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO case_links(case_id,link_type,ref_kind,target_id,linked_by)
		VALUES($1,'evidence','job_id',$2,$3)`, caseID, evidenceJob, identityID); err != nil {
		t.Fatal(err)
	}

	cleaner := RetentionCleaner{Config: RetentionConfig{Pool: pool, Metrics: telemetry.New(), BatchSize: 100}}
	if err := cleaner.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := countWhere(t, ctx, pool, `SELECT count(*) FROM processing_jobs WHERE id=$1`, evidenceJob); got != 1 {
		t.Fatal("job referenced as evidence of a held case must be protected from retention")
	}
	if got := countWhere(t, ctx, pool, `SELECT count(*) FROM processing_jobs WHERE id=$1`, unprotectedJob); got != 0 {
		t.Fatal("unprotected old terminal job must be swept")
	}

	// Releasing the hold makes the evidence job eligible again.
	if _, err := pool.Exec(ctx, `UPDATE cases SET hold=false, hold_reason='', hold_set_by=NULL, hold_at=NULL WHERE id=$1`, caseID); err != nil {
		t.Fatal(err)
	}
	if err := cleaner.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	if got := countWhere(t, ctx, pool, `SELECT count(*) FROM processing_jobs WHERE id=$1`, evidenceJob); got != 0 {
		t.Fatal("evidence job must become eligible once the hold is released")
	}
}
