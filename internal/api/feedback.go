package api

import (
	"encoding/json"
	"io"
	"net/http"

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
