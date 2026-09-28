package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

func (s Server) listSources(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	items, err := s.Control.ListSources(r.Context(), principal)
	if err != nil {
		http.Error(w, "source registry unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s Server) registerSource(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input control.SourceInput
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid source registration", http.StatusBadRequest)
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		http.Error(w, "trailing request data", http.StatusBadRequest)
		return
	}
	registered, err := s.Control.RegisterSource(r.Context(), principal, input, requestID(r))
	if err != nil {
		message := err.Error()
		if strings.HasPrefix(message, "invalid source") || strings.HasPrefix(message, "release must") {
			http.Error(w, message, http.StatusBadRequest)
		} else if strings.Contains(message, "active tenant membership") {
			http.Error(w, "membership inactive", http.StatusForbidden)
		} else {
			http.Error(w, "source registry unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, registered)
}

func (s Server) revokeSource(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	err := s.Control.RevokeSource(r.Context(), principal, r.PathValue("id"), requestID(r))
	if err != nil {
		message := err.Error()
		if strings.Contains(message, "not found") || strings.Contains(message, "invalid source ID") {
			http.Error(w, "source not found", http.StatusNotFound)
		} else if strings.Contains(message, "active tenant membership") {
			http.Error(w, "membership inactive", http.StatusForbidden)
		} else {
			http.Error(w, "source registry unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s Server) rotateSource(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	source, err := s.Control.RotateSourceCredential(r.Context(), principal, r.PathValue("id"), requestID(r))
	if err != nil {
		message := err.Error()
		if strings.Contains(message, "not found") || strings.Contains(message, "invalid source ID") {
			http.Error(w, "source not found or disabled", http.StatusNotFound)
		} else if strings.Contains(message, "active tenant membership") {
			http.Error(w, "membership inactive", http.StatusForbidden)
		} else {
			http.Error(w, "source registry unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, source)
}
