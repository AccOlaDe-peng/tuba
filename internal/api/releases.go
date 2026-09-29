package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

func (s Server) listReleases(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	items, e := s.Control.ListReleases(r.Context())
	if e != nil {
		http.Error(w, "release registry unavailable", 503)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s Server) getRelease(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	x, e := s.Control.GetRelease(r.Context(), r.PathValue("id"))
	if errors.Is(e, pgx.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if e != nil {
		http.Error(w, "release registry unavailable", 503)
		return
	}
	writeJSON(w, 200, x)
}
func (s Server) createRelease(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	key := r.Header.Get("Idempotency-Key")
	if len(key) < 16 || len(key) > 128 {
		http.Error(w, "valid Idempotency-Key required", 400)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	var m control.ReleaseManifest
	if e := d.Decode(&m); e != nil {
		http.Error(w, "invalid release manifest", 400)
		return
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		http.Error(w, "trailing request data", 400)
		return
	}
	x, e := s.Control.CreateRelease(r.Context(), p, m, key, requestID(r))
	if e != nil {
		if errors.Is(e, control.ErrReleaseConflict) {
			http.Error(w, e.Error(), 409)
		} else {
			http.Error(w, e.Error(), 400)
		}
		return
	}
	writeJSON(w, 201, x)
}
func (s Server) transitionRelease(action string) func(http.ResponseWriter, *http.Request, auth.Principal) {
	return func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		x, e := s.Control.TransitionRelease(r.Context(), p, r.PathValue("id"), action, requestID(r), s.ReleaseRoot)
		if errors.Is(e, pgx.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if e != nil {
			if action != "validate" && e.Error() == "release state changed concurrently" || e.Error() == "release is not draft" || e.Error() == "release is not validated" || e.Error() == "release is not staged" {
				http.Error(w, e.Error(), 409)
			} else {
				http.Error(w, e.Error(), 400)
			}
			return
		}
		writeJSON(w, 200, x)
	}
}

func (s Server) releaseAudit(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	items, e := s.Control.ListReleaseAudit(r.Context(), r.PathValue("id"))
	if e != nil {
		http.Error(w, "release audit unavailable", 503)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s Server) setReleasePublisher(revoke bool) func(http.ResponseWriter, *http.Request, auth.Principal) {
	return func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		if e := s.Control.SetReleasePublisher(r.Context(), p, r.PathValue("subject"), revoke, requestID(r)); e != nil {
			http.Error(w, "publisher grant update failed", 400)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
