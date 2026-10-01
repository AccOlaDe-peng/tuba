package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

func (s Server) listCollectors(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	items, err := s.Control.ListCollectors(r.Context(), p)
	if err != nil {
		http.Error(w, "collector registry unavailable", 503)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (s Server) createCollectorEnrollment(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	var body struct {
		ExpiresInMinutes int `json:"expires_in_minutes"`
	}
	if !decodeCollectorJSON(w, r, bodyLimitEnrollment, &body) {
		return
	}
	if body.ExpiresInMinutes == 0 {
		body.ExpiresInMinutes = 30
	}
	item, err := s.Control.CreateCollectorEnrollment(r.Context(), p, body.ExpiresInMinutes, requestID(r))
	if err != nil {
		if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "expiration") {
			http.Error(w, err.Error(), 400)
		} else {
			http.Error(w, "could not create enrollment token", 503)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 201, item)
}

func (s Server) enrollCollector(w http.ResponseWriter, r *http.Request) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	var body struct {
		EnrollmentToken string `json:"enrollment_token"`
		control.CollectorRegistration
	}
	if d.Decode(&body) != nil {
		http.Error(w, "invalid collector enrollment", 400)
		return
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		http.Error(w, "trailing request data", 400)
		return
	}
	registered, err := s.Control.EnrollCollector(r.Context(), body.EnrollmentToken, body.CollectorRegistration)
	if errors.Is(err, control.ErrCollectorUnauthorized) {
		http.Error(w, "enrollment token is invalid, expired, or already used", 401)
		return
	}
	if err != nil {
		if errors.Is(err, control.ErrInvalidCollectorRegistration) {
			http.Error(w, "invalid collector registration", 400)
		} else {
			http.Error(w, "collector registry unavailable", 503)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 201, registered)
}

func (s Server) collectorHeartbeat(w http.ResponseWriter, r *http.Request) {
	credential, ok := collectorCredential(w, r)
	if !ok {
		return
	}
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	var body control.CollectorHeartbeat
	if !decodeCollectorJSON(w, r, 64<<10, &body) {
		return
	}
	err := s.Control.ReportCollectorHeartbeat(r.Context(), credential, body)
	if errors.Is(err, control.ErrCollectorUnauthorized) {
		http.Error(w, "collector credential is invalid or disabled", 401)
		return
	}
	if err != nil {
		if errors.Is(err, control.ErrInvalidCollectorHeartbeat) {
			http.Error(w, "invalid collector heartbeat", 400)
		} else {
			http.Error(w, "collector registry unavailable", 503)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s Server) collectorConfig(w http.ResponseWriter, r *http.Request) {
	credential, ok := collectorCredential(w, r)
	if !ok {
		return
	}
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	if _, err := s.Control.AuthenticateCollector(r.Context(), credential); errors.Is(err, control.ErrCollectorUnauthorized) {
		http.Error(w, "collector credential is invalid or disabled", 401)
		return
	} else if err != nil {
		http.Error(w, "collector registry unavailable", 503)
		return
	}
	item, err := s.Control.GetCollectorConfig(r.Context(), credential)
	if errors.Is(err, control.ErrCollectorConfigNotFound) {
		w.Header().Set("Cache-Control", "private, no-store")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		http.Error(w, "collector config unavailable", 503)
		return
	}
	etag := "\"" + strconv.FormatInt(item.Version, 10) + "\""
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-store")
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	writeJSON(w, 200, item)
}

func (s Server) putCollectorConfig(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "config exceeds 64 KiB", 413)
		return
	}
	item, err := s.Control.SaveCollectorConfig(r.Context(), p, r.PathValue("id"), raw, requestID(r))
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "collector not found", 404)
		} else if errors.Is(err, control.ErrInvalidCollectorConfig) {
			http.Error(w, "invalid collector configuration; inline secrets and executable directives are forbidden", 400)
		} else {
			http.Error(w, "collector configuration unavailable", 503)
		}
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	writeJSON(w, 201, item)
}

func (s Server) disableCollector(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	err := s.Control.DisableCollector(r.Context(), p, r.PathValue("id"), requestID(r))
	if err != nil {
		var revocation *control.SourceWriteRevocationError
		if errors.As(err, &revocation) {
			http.Error(w, revocation.Error(), 502)
		} else if strings.Contains(err.Error(), "not found") {
			http.Error(w, "collector not found", 404)
		} else {
			http.Error(w, "collector registry unavailable", 503)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s Server) enableCollector(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	err := s.Control.EnableCollector(r.Context(), p, r.PathValue("id"), requestID(r))
	if err != nil {
		var revocation *control.SourceWriteRevocationError
		if errors.As(err, &revocation) {
			http.Error(w, revocation.Error(), 502)
		} else if strings.Contains(err.Error(), "not disabled") {
			http.Error(w, "collector is not disabled", 409)
		} else if strings.Contains(err.Error(), "not found") {
			http.Error(w, "collector not found", 404)
		} else {
			http.Error(w, "collector registry unavailable", 503)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s Server) putCollectorSourceBinding(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	var body struct {
		SourceID       string `json:"source_id"`
		KafkaPrincipal string `json:"kafka_principal"`
		KafkaTopic     string `json:"kafka_topic"`
	}
	if !decodeCollectorJSON(w, r, 4<<10, &body) {
		return
	}
	binding, err := s.Control.RegisterSourceKafkaBinding(r.Context(), p, r.PathValue("id"), body.SourceID, body.KafkaPrincipal, body.KafkaTopic, requestID(r))
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, err.Error(), 404)
		} else if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "must be") {
			http.Error(w, err.Error(), 400)
		} else {
			http.Error(w, "collector registry unavailable", 503)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 201, binding)
}

const bodyLimitEnrollment = 4 << 10

func decodeCollectorJSON(w http.ResponseWriter, r *http.Request, limit int64, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		http.Error(w, "invalid JSON request", 400)
		return false
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		http.Error(w, "trailing request data", 400)
		return false
	}
	return true
}
func collectorCredential(w http.ResponseWriter, r *http.Request) (string, bool) {
	key, err := auth.Bearer(r)
	if err != nil || !strings.HasPrefix(key, "tuba_col_") {
		http.Error(w, "collector bearer credential required", 401)
		return "", false
	}
	return key, true
}
