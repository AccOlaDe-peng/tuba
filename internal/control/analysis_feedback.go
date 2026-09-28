package control

import (
	"context"
	"errors"
	"time"

	"tuba/product/internal/auth"
)

type AnalysisFeedbackInput struct {
	FeedbackType string   `json:"feedback_type"`
	AnomalyID    string   `json:"anomaly_id"`
	RuleID       string   `json:"rule_id"`
	EntityID     string   `json:"entity_id"`
	EventIDs     []string `json:"event_ids"`
	Reason       string   `json:"reason"`
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
	if input.FeedbackType != "false_negative" && input.AnomalyID == "" {
		return output, errors.New("anomaly_id is required for this feedback type")
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
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return output, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `
		INSERT INTO analysis_feedback(
			organization_id,anomaly_id,feedback_type,reason,actor_identity_id,
			rule_id,entity_id,event_ids
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id,feedback_type,anomaly_id,rule_id,entity_id,event_ids,reason,created_at`,
		org,
		input.AnomalyID,
		input.FeedbackType,
		input.Reason,
		actor,
		input.RuleID,
		input.EntityID,
		input.EventIDs,
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
	if output.EventIDs == nil {
		output.EventIDs = []string{}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO audit_events(
			organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata
		) VALUES($1,$2,'analysis.feedback','anomaly',$3,$4,jsonb_build_object('type',$5::text))`,
		org,
		actor,
		input.AnomalyID,
		requestID,
		input.FeedbackType,
	)
	if err != nil {
		return output, err
	}
	return output, tx.Commit(ctx)
}
