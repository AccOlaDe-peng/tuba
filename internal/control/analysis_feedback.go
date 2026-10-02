package control

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

type AnalysisFeedbackInput struct {
	FeedbackType   string   `json:"feedback_type"`
	AnomalyID      string   `json:"anomaly_id"`
	RuleID         string   `json:"rule_id"`
	EntityID       string   `json:"entity_id"`
	EventIDs       []string `json:"event_ids"`
	Reason         string   `json:"reason"`
	IdempotencyKey string   `json:"idempotency_key"`
	TargetCaseID   string   `json:"target_case_id"`
}

type AnalysisFeedback struct {
	ID           int64     `json:"id"`
	FeedbackType string    `json:"feedback_type"`
	AnomalyID    string    `json:"anomaly_id,omitempty"`
	RuleID       string    `json:"rule_id,omitempty"`
	EntityID     string    `json:"entity_id,omitempty"`
	EventIDs     []string  `json:"event_ids"`
	Reason       string    `json:"reason"`
	CreatedAt    time.Time `json:"created_at"`
}

// CreateAnalysisFeedback persists analyst feedback into the append-only
// feedback log plus one audit event; the only finding-state effect is the
// decoupled status annotation (finding_labels) — production models and
// baselines are never touched here. Submissions carrying an idempotency
// key are replay-safe: the same (tenant, key) returns the stored row
// instead of inserting a duplicate. The feedback target must be
// resolvable: a finding business key (shape contract; finding bodies live
// in Elasticsearch) and/or a case id that must exist in this organization.
func (s *Store) CreateAnalysisFeedback(
	ctx context.Context,
	principal auth.Principal,
	input AnalysisFeedbackInput,
	requestID string,
) (AnalysisFeedback, error) {
	var output AnalysisFeedback
	validTypes := map[string]bool{
		"true_positive":  true,
		"false_positive": true,
		"false_negative": true,
		"inconclusive":   true,
	}
	if !validTypes[input.FeedbackType] || len(input.AnomalyID) > 256 || len(input.RuleID) > 256 ||
		len(input.EntityID) > 256 || len(input.Reason) > 2000 || len(input.EventIDs) > 100 {
		return output, errors.New("invalid analysis feedback")
	}
	if input.IdempotencyKey != "" && (len(input.IdempotencyKey) < 8 || len(input.IdempotencyKey) > 128) {
		return output, errors.New("invalid idempotency key")
	}
	if input.FeedbackType != "false_negative" && input.AnomalyID == "" && input.TargetCaseID == "" {
		return output, errors.New("anomaly_id or target_case_id is required for this feedback type")
	}
	if input.FeedbackType == "false_negative" && (input.RuleID == "" || input.EntityID == "" || input.Reason == "") {
		return output, errors.New("false-negative feedback requires rule_id, entity_id and reason")
	}
	for _, eventID := range input.EventIDs {
		if eventID == "" || len(eventID) > 256 {
			return output, errors.New("invalid event id")
		}
	}
	org, actor, err := s.ids(ctx, principal)
	if err != nil {
		return output, err
	}
	eventIDs := input.EventIDs
	if eventIDs == nil {
		eventIDs = []string{}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return output, err
	}
	defer tx.Rollback(ctx)
	var targetCase any
	if input.TargetCaseID != "" {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cases WHERE organization_id=$1 AND id=$2)`, org, input.TargetCaseID).Scan(&exists); err != nil {
			return output, err
		}
		if !exists {
			return output, errors.New("feedback target case not found")
		}
		targetCase = input.TargetCaseID
	}
	var idempotencyKey any
	if input.IdempotencyKey != "" {
		idempotencyKey = input.IdempotencyKey
	}
	inserted := true
	err = tx.QueryRow(ctx, `
		INSERT INTO analysis_feedback(
			organization_id,anomaly_id,feedback_type,reason,actor_identity_id,
			rule_id,entity_id,event_ids,idempotency_key,target_case_id
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT(organization_id,idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id,feedback_type,anomaly_id,rule_id,entity_id,event_ids,reason,created_at`,
		org,
		input.AnomalyID,
		input.FeedbackType,
		input.Reason,
		actor,
		input.RuleID,
		input.EntityID,
		eventIDs,
		idempotencyKey,
		targetCase,
	).Scan(
		&output.ID,
		&output.FeedbackType,
		&output.AnomalyID,
		&output.RuleID,
		&output.EntityID,
		&output.EventIDs,
		&output.Reason,
		&output.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Idempotent replay: return the row stored by the first submission.
			inserted = false
			err = tx.QueryRow(ctx, `
				SELECT id,feedback_type,anomaly_id,rule_id,entity_id,event_ids,reason,created_at
				FROM analysis_feedback WHERE organization_id=$1 AND idempotency_key=$2`,
				org, input.IdempotencyKey,
			).Scan(
				&output.ID,
				&output.FeedbackType,
				&output.AnomalyID,
				&output.RuleID,
				&output.EntityID,
				&output.EventIDs,
				&output.Reason,
				&output.CreatedAt,
			)
			if err != nil {
				return output, err
			}
		} else {
			return output, err
		}
	}
	if output.EventIDs == nil {
		output.EventIDs = []string{}
	}
	if inserted {
		if err = annotateFinding(ctx, tx, org, actor, output.AnomalyID, output.FeedbackType, output.Reason); err != nil {
			return output, err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO audit_events(
				organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata
			) VALUES($1,$2,'analysis.feedback','anomaly',$3,$4,jsonb_build_object('type',$5::text,'label',$6::text))`,
			org,
			actor,
			input.AnomalyID,
			requestID,
			input.FeedbackType,
			feedbackLabel(input.FeedbackType),
		)
		if err != nil {
			return output, err
		}
	}
	return output, tx.Commit(ctx)
}
