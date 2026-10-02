package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

func (s Server) createAnalysisFeedback(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input control.AnalysisFeedbackInput
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid analysis feedback", http.StatusBadRequest)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		http.Error(w, "trailing data", http.StatusBadRequest)
		return
	}
	feedback, err := s.Control.CreateAnalysisFeedback(r.Context(), principal, input, requestID(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusCreated, feedback)
}

func (s Server) listFeedbackEvaluation(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	filter := control.FeedbackEvaluationFilter{
		RuleID:       q.Get("rule_id"),
		EntityID:     q.Get("entity_id"),
		FeedbackType: q.Get("feedback_type"),
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, "invalid since", http.StatusBadRequest)
			return
		}
		filter.Since = t.UTC()
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		filter.Limit = n
	}
	rows, err := s.Control.ListFeedbackEvaluation(r.Context(), principal, filter)
	if err != nil {
		http.Error(w, "evaluation query failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": rows})
}

func (s Server) feedbackRuleMetrics(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	metrics, err := s.Control.SummarizeFeedbackByRule(r.Context(), principal)
	if err != nil {
		http.Error(w, "evaluation query failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": metrics})
}

// autoCaseForFinding is the machine-facing auto case creation entry point.
// It fails closed unless TUBA_AUTO_CASE_ENABLED=true was set at startup
// (the feature is default-off per the design baseline).
func (s Server) autoCaseForFinding(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input control.AutoCaseFindingInput
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid auto case finding", http.StatusBadRequest)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		http.Error(w, "trailing data", http.StatusBadRequest)
		return
	}
	c, created, err := s.Control.AutoCaseForFinding(r.Context(), principal, s.AutoCaseEnabled, input, requestID(r))
	if err != nil {
		if errors.Is(err, control.ErrAutoCaseDisabled) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"case": c, "created": created})
}
