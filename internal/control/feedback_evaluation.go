package control

import (
	"context"
	"errors"
	"regexp"
	"time"

	"encoding/json"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

// R04: analyst feedback enters the audit chain and the offline evaluation
// dataset only — it never modifies production models or baselines online
// (any model effect flows exclusively through the offline F04 retraining
// pipeline). The finding status annotation written here is metadata
// decoupled from model artifacts. Auto case creation is default-off; when
// enabled it follows the tenant/entity/policy/time_bucket dedupe contract:
// one case per key, anomaly links accumulate on the same case.

var (
	// ErrAutoCaseDisabled fails closed when the auto case creation path is
	// invoked while the feature flag is off (the default).
	ErrAutoCaseDisabled = errors.New("auto case creation disabled")
	// ErrAutoCaseConflict is internal: a concurrent creator won the dedupe
	// key race; the call is retried against the now-existing case.
	errAutoCaseConflict = errors.New("auto case dedupe conflict")
)

var autoCaseEntityPattern = regexp.MustCompile(`^ent:[a-f0-9]{64}$`)

// feedbackLabel maps a feedback type to the decoupled finding status
// annotation. false_negative has no finding to annotate and maps to "".
func feedbackLabel(feedbackType string) string {
	switch feedbackType {
	case "true_positive":
		return "confirmed"
	case "false_positive":
		return "false_positive"
	case "inconclusive":
		return "inconclusive"
	}
	return ""
}

type FindingLabel struct {
	AnomalyID string    `json:"anomaly_id"`
	Label     string    `json:"label"`
	Reason    string    `json:"reason"`
	UpdatedBy string    `json:"updated_by,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// annotateFinding upserts the finding status annotation derived from
// feedback. This table is the only finding-state effect of feedback: model
// artifacts (feature samples, baselines) are never touched here.
func annotateFinding(ctx context.Context, tx pgx.Tx, org, actor, anomalyID, feedbackType, reason string) error {
	label := feedbackLabel(feedbackType)
	if label == "" || anomalyID == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO finding_labels(organization_id,anomaly_id,label,reason,updated_by,updated_at)
		VALUES($1,$2,$3,$4,$5,now())
		ON CONFLICT(organization_id,anomaly_id)
		DO UPDATE SET label=excluded.label,reason=excluded.reason,updated_by=excluded.updated_by,updated_at=now()`,
		org, anomalyID, label, reason, actor)
	return err
}

// GetFindingLabel returns the current feedback-derived annotation of a
// finding, or pgx.ErrNoRows when the finding has no annotation.
func (s *Store) GetFindingLabel(ctx context.Context, p auth.Principal, anomalyID string) (FindingLabel, error) {
	var out FindingLabel
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return out, err
	}
	err = s.Pool.QueryRow(ctx, `
		SELECT fl.anomaly_id,fl.label,fl.reason,coalesce(i.subject,''),fl.updated_at
		FROM finding_labels fl LEFT JOIN identities i ON i.id=fl.updated_by
		WHERE fl.organization_id=$1 AND fl.anomaly_id=$2`, org, anomalyID).
		Scan(&out.AnomalyID, &out.Label, &out.Reason, &out.UpdatedBy, &out.UpdatedAt)
	return out, err
}

// FeedbackEvaluationRow is one row of the offline evaluation dataset
// (analysis_feedback_evaluation view): feedback joined with the decoupled
// finding label and the verdict of the attached case.
type FeedbackEvaluationRow struct {
	FeedbackID   int64      `json:"feedback_id"`
	FeedbackType string     `json:"feedback_type"`
	AnomalyID    string     `json:"anomaly_id"`
	RuleID       string     `json:"rule_id,omitempty"`
	EntityID     string     `json:"entity_id,omitempty"`
	EventIDs     []string   `json:"event_ids"`
	Reason       string     `json:"reason"`
	Actor        string     `json:"actor,omitempty"`
	SourceCaseID *string    `json:"source_case_id,omitempty"`
	TargetCaseID *string    `json:"target_case_id,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	FindingLabel string     `json:"finding_label,omitempty"`
	LabeledAt    *time.Time `json:"labeled_at,omitempty"`
	CaseVerdict  string     `json:"case_verdict,omitempty"`
}

type FeedbackEvaluationFilter struct {
	RuleID       string
	EntityID     string
	FeedbackType string
	Since        time.Time
	Limit        int
}

// ListFeedbackEvaluation exports the offline evaluation dataset for rule
// quality assessment (precision / false-positive-rate computation offline).
func (s *Store) ListFeedbackEvaluation(ctx context.Context, p auth.Principal, f FeedbackEvaluationFilter) ([]FeedbackEvaluationRow, error) {
	if f.Limit < 1 || f.Limit > 1000 {
		f.Limit = 1000
	}
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT e.feedback_id,e.feedback_type,e.anomaly_id,coalesce(e.rule_id,''),coalesce(e.entity_id,''),
		       e.event_ids,e.reason,coalesce(i.subject,''),e.source_case_id,e.target_case_id,e.created_at,
		       coalesce(e.finding_label,''),e.labeled_at,coalesce(e.case_verdict,'')
		FROM analysis_feedback_evaluation e
		LEFT JOIN identities i ON i.id=e.actor_identity_id
		WHERE e.organization_id=$1
		  AND ($2='' OR e.rule_id=$2)
		  AND ($3='' OR e.entity_id=$3)
		  AND ($4='' OR e.feedback_type=$4)
		  AND ($5::timestamptz IS NULL OR e.created_at>=$5)
		ORDER BY e.created_at,e.feedback_id
		LIMIT $6`, org, f.RuleID, f.EntityID, f.FeedbackType, nullableTime(f.Since), f.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FeedbackEvaluationRow{}
	for rows.Next() {
		var r FeedbackEvaluationRow
		if err := rows.Scan(&r.FeedbackID, &r.FeedbackType, &r.AnomalyID, &r.RuleID, &r.EntityID,
			&r.EventIDs, &r.Reason, &r.Actor, &r.SourceCaseID, &r.TargetCaseID, &r.CreatedAt,
			&r.FindingLabel, &r.LabeledAt, &r.CaseVerdict); err != nil {
			return nil, err
		}
		if r.EventIDs == nil {
			r.EventIDs = []string{}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// FeedbackRuleMetrics aggregates the evaluation dataset per rule: the
// exportable shape for assessing rule precision and false-positive rate.
type FeedbackRuleMetrics struct {
	RuleID        string  `json:"rule_id"`
	TruePositive  int     `json:"true_positive"`
	FalsePositive int     `json:"false_positive"`
	FalseNegative int     `json:"false_negative"`
	Inconclusive  int     `json:"inconclusive"`
	Precision     float64 `json:"precision"`
}

func (s *Store) SummarizeFeedbackByRule(ctx context.Context, p auth.Principal) ([]FeedbackRuleMetrics, error) {
	org, _, err := s.ids(ctx, p)
	if err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT coalesce(rule_id,''),
		       count(*) FILTER (WHERE feedback_type='true_positive'),
		       count(*) FILTER (WHERE feedback_type='false_positive'),
		       count(*) FILTER (WHERE feedback_type='false_negative'),
		       count(*) FILTER (WHERE feedback_type='inconclusive')
		FROM analysis_feedback
		WHERE organization_id=$1
		GROUP BY coalesce(rule_id,'')
		ORDER BY coalesce(rule_id,'')`, org)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FeedbackRuleMetrics{}
	for rows.Next() {
		var m FeedbackRuleMetrics
		if err := rows.Scan(&m.RuleID, &m.TruePositive, &m.FalsePositive, &m.FalseNegative, &m.Inconclusive); err != nil {
			return nil, err
		}
		if d := m.TruePositive + m.FalsePositive; d > 0 {
			m.Precision = float64(m.TruePositive) / float64(d)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// AutoCaseFindingInput is one finding presented to the auto case creation
// path. BucketStart is the (UTC, hour-aligned) time bucket of the dedupe
// key tenant/entity/policy/time_bucket.
type AutoCaseFindingInput struct {
	EntityID    string    `json:"entity_id"`
	PolicyID    string    `json:"policy_id"`
	BucketStart time.Time `json:"bucket_start"`
	AnomalyID   string    `json:"anomaly_id"`
	Title       string    `json:"title"`
	Severity    string    `json:"severity"`
}

func validateAutoCaseFinding(in AutoCaseFindingInput) error {
	if !autoCaseEntityPattern.MatchString(in.EntityID) {
		return errors.New("invalid auto case entity")
	}
	if len(in.PolicyID) < 1 || len(in.PolicyID) > 256 {
		return errors.New("invalid auto case policy")
	}
	if in.BucketStart.IsZero() || in.BucketStart.Location() != time.UTC ||
		!in.BucketStart.Equal(in.BucketStart.Truncate(time.Hour)) {
		return errors.New("invalid auto case time bucket")
	}
	if len(in.AnomalyID) < 1 || len(in.AnomalyID) > 256 {
		return errors.New("invalid auto case anomaly")
	}
	if len(in.Title) > 300 {
		return errors.New("invalid auto case title")
	}
	if !map[string]bool{"low": true, "medium": true, "high": true, "critical": true}[in.Severity] {
		return errors.New("invalid auto case severity")
	}
	return nil
}

// AutoCaseForFinding implements the auto case creation dedupe contract.
// The feature is default-off: with enabled=false the call fails closed.
// When enabled, the first finding for a (tenant, entity, policy,
// time_bucket) key creates one case; later findings with the same key are
// linked to that same case (case_anomalies accumulation), never a new case.
func (s *Store) AutoCaseForFinding(ctx context.Context, p auth.Principal, enabled bool, in AutoCaseFindingInput, requestID string) (Case, bool, error) {
	var out Case
	if !enabled {
		return out, false, ErrAutoCaseDisabled
	}
	if err := validateAutoCaseFinding(in); err != nil {
		return out, false, err
	}
	for attempt := 0; attempt < 2; attempt++ {
		c, created, err := s.autoCaseAttempt(ctx, p, in, requestID)
		if errors.Is(err, errAutoCaseConflict) {
			continue
		}
		return c, created, err
	}
	return out, false, errAutoCaseConflict
}

func (s *Store) autoCaseAttempt(ctx context.Context, p auth.Principal, in AutoCaseFindingInput, requestID string) (Case, bool, error) {
	var out Case
	org, user, err := s.ids(ctx, p)
	if err != nil {
		return out, false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback(ctx)
	var caseID string
	err = tx.QueryRow(ctx, `
		SELECT case_id::text FROM auto_case_dedupe
		WHERE organization_id=$1 AND entity_id=$2 AND policy_id=$3 AND time_bucket=$4
		FOR UPDATE`, org, in.EntityID, in.PolicyID, in.BucketStart).Scan(&caseID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, false, err
	}
	created := false
	if errors.Is(err, pgx.ErrNoRows) {
		title := in.Title
		if title == "" {
			title = "auto: " + in.PolicyID + " " + in.EntityID[:15]
		}
		if err = tx.QueryRow(ctx, `
			INSERT INTO cases(organization_id,title,description,severity,created_by)
			VALUES($1,$2,$3,$4,$5) RETURNING id::text`,
			org, title, "auto-created case (dedupe tenant/entity/policy/time_bucket)", in.Severity, user).Scan(&caseID); err != nil {
			return out, false, err
		}
		var inserted bool
		err = tx.QueryRow(ctx, `
			INSERT INTO auto_case_dedupe(organization_id,entity_id,policy_id,time_bucket,case_id,linked_anomalies)
			VALUES($1,$2,$3,$4,$5,0)
			ON CONFLICT DO NOTHING RETURNING true`,
			org, in.EntityID, in.PolicyID, in.BucketStart, caseID).Scan(&inserted)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// A concurrent transaction created the case for this key;
				// roll back ours and retry against the existing case.
				return out, false, errAutoCaseConflict
			}
			return out, false, err
		}
		created = true
		meta, _ := json.Marshal(map[string]string{
			"auto":      "true",
			"entity_id": in.EntityID,
			"policy_id": in.PolicyID,
			"bucket":    in.BucketStart.Format(time.RFC3339),
		})
		if _, err = tx.Exec(ctx, `
			INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata)
			VALUES($1,$2,'case.create','case',$3,$4,$5)`, org, user, caseID, requestID, meta); err != nil {
			return out, false, err
		}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO case_anomalies(case_id,anomaly_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, caseID, in.AnomalyID)
	if err != nil {
		return out, false, err
	}
	if tag.RowsAffected() == 1 {
		if _, err = tx.Exec(ctx, `
			UPDATE auto_case_dedupe SET linked_anomalies=linked_anomalies+1,updated_at=now()
			WHERE organization_id=$1 AND entity_id=$2 AND policy_id=$3 AND time_bucket=$4`,
			org, in.EntityID, in.PolicyID, in.BucketStart); err != nil {
			return out, false, err
		}
		meta, _ := json.Marshal(map[string]string{
			"anomaly_id": in.AnomalyID,
			"entity_id":  in.EntityID,
			"policy_id":  in.PolicyID,
			"bucket":     in.BucketStart.Format(time.RFC3339),
		})
		if _, err = tx.Exec(ctx, `
			INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata)
			VALUES($1,$2,'case.auto_link','case',$3,$4,$5)`, org, user, caseID, requestID, meta); err != nil {
			return out, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return out, false, err
	}
	out, err = s.GetCase(ctx, p, caseID)
	return out, created, err
}
