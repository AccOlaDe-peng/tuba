package api

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
	"tuba/product/internal/es"
	"tuba/product/internal/telemetry"
)

type Authorizer interface {
	Authorize(context.Context, auth.Principal) (auth.Principal, error)
}

type Server struct {
	Verifier        *auth.Verifier
	Authorizer      Authorizer
	ES              *es.Client
	Control         *control.Store
	StartedAt       time.Time
	RequestTimeout  time.Duration
	Metrics         *telemetry.Registry
	KafkaConfigured bool
	ReleaseRoot     string
}

func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if s.Control == nil || s.ES == nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if _, err := s.Control.Ping(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		status, err := s.ES.Do(ctx, http.MethodGet, "/", nil, nil)
		if err != nil || status < http.StatusOK || status >= http.StatusMultipleChoices {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Handler())
	}
	mux.HandleFunc("GET /api/v1/me", s.protected("", s.me))
	mux.HandleFunc("GET /api/v1/events", s.protected("event:read", s.events))
	mux.HandleFunc("GET /api/v1/overview", s.protected("anomaly:read", s.overview))
	mux.HandleFunc("GET /api/v1/anomalies", s.protected("anomaly:read", s.list("anomalies")))
	mux.HandleFunc("GET /api/v1/anomalies/{id}", s.protected("anomaly:read", s.anomalyDetail))
	mux.HandleFunc("GET /api/v1/anomalies/{id}/evidence", s.protected("anomaly:read", s.anomalyEvidence))
	mux.HandleFunc("GET /api/v1/cases", s.protected("case:read", s.listCases))
	mux.HandleFunc("POST /api/v1/cases", s.protected("case:write", s.createCase))
	mux.HandleFunc("GET /api/v1/cases/{id}", s.protected("case:read", s.caseDetail))
	mux.HandleFunc("POST /api/v1/cases/{id}/actions", s.protected("case:write", s.caseAction))
	mux.HandleFunc("GET /api/v1/cases/{id}/activity", s.protected("case:read", s.caseActivity))
	mux.HandleFunc("GET /api/v1/operations/status", s.protected("operations:read", s.operations))
	mux.HandleFunc("GET /api/v1/releases", s.protected("release:read", s.listReleases))
	mux.HandleFunc("POST /api/v1/releases", s.protected("release:manage", s.createRelease))
	mux.HandleFunc("GET /api/v1/releases/{id}", s.protected("release:read", s.getRelease))
	mux.HandleFunc("POST /api/v1/releases/{id}/validate", s.protected("release:manage", s.transitionRelease("validate")))
	mux.HandleFunc("POST /api/v1/releases/{id}/stage", s.protected("release:manage", s.transitionRelease("stage")))
	mux.HandleFunc("POST /api/v1/releases/{id}/activate", s.protected("release:manage", s.transitionRelease("activate")))
	mux.HandleFunc("GET /api/v1/releases/{id}/audit", s.protected("release:read", s.releaseAudit))
	mux.HandleFunc("PUT /api/v1/platform/release-publishers/{subject}", s.protected("release:manage", s.setReleasePublisher(false)))
	mux.HandleFunc("DELETE /api/v1/platform/release-publishers/{subject}", s.protected("release:manage", s.setReleasePublisher(true)))
	mux.HandleFunc("POST /api/v1/analysis/feedback", s.protected("analysis:feedback", s.createAnalysisFeedback))
	mux.HandleFunc("GET /api/v1/sources", s.protected("source:manage", s.listSources))
	mux.HandleFunc("POST /api/v1/sources", s.protected("source:manage", s.registerSource))
	mux.HandleFunc("POST /api/v1/sources/{id}/rotate", s.protected("source:manage", s.rotateSource))
	mux.HandleFunc("DELETE /api/v1/sources/{id}", s.protected("source:manage", s.revokeSource))
	mux.HandleFunc("GET /api/v1/collectors", s.protected("source:manage", s.listCollectors))
	mux.HandleFunc("POST /api/v1/collectors/enrollments", s.protected("source:manage", s.createCollectorEnrollment))
	mux.HandleFunc("PUT /api/v1/collectors/{id}/config", s.protected("source:manage", s.putCollectorConfig))
	mux.HandleFunc("DELETE /api/v1/collectors/{id}", s.protected("source:manage", s.disableCollector))
	mux.HandleFunc("POST /api/v1/collectors/{id}/enable", s.protected("source:manage", s.enableCollector))
	mux.HandleFunc("PUT /api/v1/collectors/{id}/source-binding", s.protected("source:manage", s.putCollectorSourceBinding))
	mux.HandleFunc("POST /api/v1/collector/enroll", s.enrollCollector)
	mux.HandleFunc("POST /api/v1/collector/heartbeat", s.collectorHeartbeat)
	mux.HandleFunc("GET /api/v1/collector/config", s.collectorConfig)
	mux.HandleFunc("GET /api/v1/audit", s.protected("user:manage", s.listAudit))
	mux.HandleFunc("GET /api/v1/members", s.protected("user:manage", s.listMembers))
	mux.HandleFunc("PUT /api/v1/members/{subject}/roles/{role}", s.protected("user:manage", s.grantMember))
	mux.HandleFunc("DELETE /api/v1/members/{subject}/roles/{role}", s.protected("user:manage", s.revokeMember))
	return telemetry.RequestTimeout(telemetry.Middleware(mux, s.Metrics), s.RequestTimeout)
}

func (s Server) protected(permission string, next func(http.ResponseWriter, *http.Request, auth.Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, err := auth.Bearer(r)
		if err != nil {
			http.Error(w, "bearer token required", 401)
			return
		}
		principal, err := s.Verifier.Verify(r.Context(), token)
		if err != nil {
			http.Error(w, "invalid access token", 401)
			return
		}
		if s.Authorizer != nil {
			principal, err = s.Authorizer.Authorize(r.Context(), principal)
			if err != nil {
				http.Error(w, "membership inactive", 403)
				return
			}
		}
		if permission != "" && !principal.Can(permission) {
			http.Error(w, "permission denied", 403)
			return
		}
		next(w, r, principal)
	}
}

func (s Server) me(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	writeJSON(w, 200, map[string]any{"subject": p.Subject, "organization_id": p.Organization, "namespace": p.Namespace, "roles": p.Roles, "permissions": p.Permissions()})
}

func (s Server) list(kind string) func(http.ResponseWriter, *http.Request, auth.Principal) {
	return func(w http.ResponseWriter, r *http.Request, p auth.Principal) {
		limit := 50
		if raw := r.URL.Query().Get("limit"); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 || value > 100 {
				http.Error(w, "limit must be 1..100", 400)
				return
			}
			limit = value
		}
		tie := "anomaly.id"
		if kind == "cases" {
			tie = "case.id"
		}
		filters := []any{map[string]any{"term": map[string]any{"organization.id": p.Organization}}}
		if kind == "anomalies" {
			severity := r.URL.Query().Get("severity")
			if severity != "" && !map[string]bool{"low": true, "medium": true, "high": true, "critical": true}[severity] {
				http.Error(w, "invalid severity", 400)
				return
			}
			if severity != "" {
				filters = append(filters, map[string]any{"term": map[string]any{"anomaly.severity": severity}})
			}
			status := r.URL.Query().Get("status")
			if status != "" && !map[string]bool{"open": true, "investigating": true, "closed": true, "false_positive": true}[status] {
				http.Error(w, "invalid status", 400)
				return
			}
			if status != "" {
				filters = append(filters, map[string]any{"term": map[string]any{"anomaly.status": status}})
			}
			entity := strings.TrimSpace(r.URL.Query().Get("entity"))
			if len(entity) > 128 {
				http.Error(w, "entity filter is too long", 400)
				return
			}
			if entity != "" {
				filters = append(filters, map[string]any{"term": map[string]any{"entity.id": entity}})
			}
			to := time.Now().UTC()
			from := to.Add(-24 * time.Hour)
			var err error
			if v := r.URL.Query().Get("from"); v != "" {
				from, err = time.Parse(time.RFC3339, v)
				if err != nil {
					http.Error(w, "invalid from", 400)
					return
				}
			}
			if v := r.URL.Query().Get("to"); v != "" {
				to, err = time.Parse(time.RFC3339, v)
				if err != nil {
					http.Error(w, "invalid to", 400)
					return
				}
			}
			if !from.Before(to) || to.Sub(from) > 31*24*time.Hour {
				http.Error(w, "time range must be within 31 days", 400)
				return
			}
			filters = append(filters, map[string]any{"range": map[string]any{"@timestamp": map[string]any{"gte": from.Format(time.RFC3339), "lte": to.Format(time.RFC3339)}}})
		}
		query := map[string]any{"size": limit, "track_total_hits": true, "sort": []any{map[string]any{"@timestamp": "desc"}, map[string]any{tie: "asc"}}, "query": map[string]any{"bool": map[string]any{"filter": filters}}}
		if c := r.URL.Query().Get("cursor"); c != "" {
			b, e := base64.RawURLEncoding.DecodeString(c)
			if e != nil {
				http.Error(w, "invalid cursor", 400)
				return
			}
			var sort []any
			if json.Unmarshal(b, &sort) != nil || len(sort) != 2 {
				http.Error(w, "invalid cursor", 400)
				return
			}
			query["search_after"] = sort
		}
		var result es.SearchResult
		status, err := s.ES.Do(r.Context(), "POST", "/ueba-"+kind+"-"+p.Namespace+"/_search", query, &result)
		if kind == "anomalies" && err == nil && status == http.StatusNotFound {
			writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "next_cursor": "", "total": 0})
			return
		}
		if err != nil || status != 200 {
			http.Error(w, "data store unavailable", 503)
			return
		}
		items := make([]map[string]any, 0, len(result.Hits.Hits))
		for _, hit := range result.Hits.Hits {
			var source map[string]any
			if json.Unmarshal(hit.Source, &source) != nil {
				http.Error(w, "invalid stored document", 503)
				return
			}
			if kind == "anomalies" {
				source = anomalySummary(source, hit.ID)
			} else {
				source["id"] = hit.ID
			}
			items = append(items, source)
		}
		next := ""
		if len(result.Hits.Hits) == limit {
			if b, e := json.Marshal(result.Hits.Hits[len(result.Hits.Hits)-1].Sort); e == nil {
				next = base64.RawURLEncoding.EncodeToString(b)
			}
		}
		writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next, "total": result.Hits.Total.Value})
	}
}

func (s Server) listCases(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		s.list("cases")(w, r, p)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 100 {
			http.Error(w, "limit must be 1..100", 400)
			return
		}
		limit = n
	}
	queryText := strings.TrimSpace(r.URL.Query().Get("q"))
	if len(queryText) > 200 {
		http.Error(w, "query is too long", 400)
		return
	}
	items, next, err := s.Control.ListCasesFiltered(
		r.Context(),
		p,
		limit,
		r.URL.Query().Get("cursor"),
		r.URL.Query().Get("status"),
		r.URL.Query().Get("severity"),
		queryText,
	)
	if err != nil {
		http.Error(w, "invalid cursor or data store unavailable", 400)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next})
}
func (s Server) createCase(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	var in control.CreateCaseInput
	if d.Decode(&in) != nil {
		http.Error(w, "invalid case", 400)
		return
	}
	created, status, err := s.Control.CreateCase(r.Context(), p, r.Header.Get("Idempotency-Key"), in)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, status, created)
}

func (s Server) getCase(ctx context.Context, id string, p auth.Principal) (es.Hit, int, error) {
	var hit es.Hit
	status, err := s.ES.Do(ctx, "GET", "/ueba-cases-"+p.Namespace+"/_doc/"+url.PathEscape(id), nil, &hit)
	if err != nil {
		return hit, 503, err
	}
	if status == 404 {
		return hit, 404, nil
	}
	if status != 200 {
		return hit, 503, fmt.Errorf("Elasticsearch HTTP %d", status)
	}
	var source struct {
		Organization struct {
			ID string `json:"id"`
		} `json:"organization"`
	}
	if json.Unmarshal(hit.Source, &source) != nil || source.Organization.ID != p.Organization {
		return hit, 404, nil
	}
	return hit, 200, nil
}

func validCaseID(id string) bool { return id != "" && len(id) <= 256 && !strings.Contains(id, "/") }

func (s Server) caseDetail(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id := r.PathValue("id")
	if !validCaseID(id) {
		http.Error(w, "invalid case ID", 400)
		return
	}
	if s.Control != nil {
		c, err := s.Control.GetCase(r.Context(), p, id)
		if err != nil {
			http.Error(w, "Not Found", 404)
			return
		}
		writeJSON(w, 200, c)
		return
	}
	hit, status, _ := s.getCase(r.Context(), id, p)
	if status != 200 {
		http.Error(w, http.StatusText(status), status)
		return
	}
	var source map[string]any
	if json.Unmarshal(hit.Source, &source) != nil {
		http.Error(w, "invalid stored document", 503)
		return
	}
	source["id"] = id
	writeJSON(w, 200, source)
}

type CaseAction struct {
	Status, Assignee, Verdict, Reason string
	ExpectedVersion                   int64 `json:"expected_version"`
}

func (s Server) caseAction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id := r.PathValue("id")
	if !validCaseID(id) {
		http.Error(w, "invalid case ID", 400)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var action CaseAction
	if err := decoder.Decode(&action); err != nil {
		http.Error(w, "invalid action", 400)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		http.Error(w, "trailing data", 400)
		return
	}
	if len(action.Assignee) > 128 || len(action.Reason) > 2000 {
		http.Error(w, "action field too long", 400)
		return
	}
	if s.Control != nil {
		c, err := s.Control.UpdateCase(r.Context(), p, id, control.CaseActionInput{Status: action.Status, Assignee: action.Assignee, Verdict: action.Verdict, ExpectedVersion: action.ExpectedVersion, Reason: action.Reason}, requestID(r))
		if err != nil {
			code := 400
			if strings.Contains(err.Error(), "conflict") {
				code = 409
			}
			http.Error(w, err.Error(), code)
			return
		}
		writeJSON(w, 200, c)
		return
	}
	hit, status, _ := s.getCase(r.Context(), id, p)
	if status != 200 {
		http.Error(w, http.StatusText(status), status)
		return
	}
	var document map[string]any
	if json.Unmarshal(hit.Source, &document) != nil {
		http.Error(w, "invalid stored document", 503)
		return
	}
	if err := applyAction(document, p.Subject, action); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	path := fmt.Sprintf("/ueba-cases-%s/_doc/%s?if_seq_no=%d&if_primary_term=%d&refresh=wait_for", p.Namespace, url.PathEscape(id), hit.SeqNo, hit.PrimaryTerm)
	status, err := s.ES.Do(r.Context(), "PUT", path, document, nil)
	if err != nil {
		http.Error(w, "data store unavailable", 503)
		return
	}
	if status == 409 {
		http.Error(w, "case was changed by another operator", 409)
		return
	}
	if status != 200 && status != 201 {
		http.Error(w, "data store unavailable", 503)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "case": document["case"], "version": document["version"]})
}

func (s Server) caseActivity(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", http.StatusServiceUnavailable)
		return
	}
	id := r.PathValue("id")
	if !validCaseID(id) {
		http.Error(w, "invalid case ID", http.StatusBadRequest)
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			http.Error(w, "limit must be 1..100", http.StatusBadRequest)
			return
		}
		limit = value
	}
	items, next, err := s.Control.ListCaseActivity(r.Context(), p, id, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, "case not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

var transitions = map[string]map[string]bool{"open": {"in_progress": true, "closed": true}, "in_progress": {"open": true, "closed": true}, "closed": {"open": true}}
var verdicts = map[string]bool{"true_positive": true, "benign_positive": true, "false_positive": true, "inconclusive": true}

func applyAction(document map[string]any, actor string, action CaseAction) error {
	if action.Status == "" && action.Assignee == "" && action.Verdict == "" {
		return errors.New("an action is required")
	}
	caseObject, ok := document["case"].(map[string]any)
	if !ok {
		return errors.New("invalid case document")
	}
	oldStatus, _ := caseObject["status"].(string)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	actions := []string{}
	if action.Status != "" {
		if !transitions[oldStatus][action.Status] {
			return errors.New("invalid case transition")
		}
		caseObject["status"] = action.Status
		if action.Status == "closed" {
			caseObject["closed_at"] = now
		} else {
			delete(caseObject, "closed_at")
		}
		name := "change_status"
		if action.Status == "closed" {
			name = "close"
		} else if oldStatus == "closed" {
			name = "reopen"
		}
		actions = append(actions, name)
	}
	if action.Assignee != "" {
		caseObject["assignee"] = action.Assignee
		actions = append(actions, "assign")
	}
	if action.Verdict != "" {
		if !verdicts[action.Verdict] {
			return errors.New("invalid verdict")
		}
		caseObject["feedback"] = map[string]any{"verdict": action.Verdict, "reason": action.Reason, "analyst": actor, "recorded_at": now}
		actions = append(actions, "record_verdict")
	}
	caseObject["updated_at"] = now
	version, _ := document["version"].(float64)
	document["version"] = int(version) + 1
	timeline, _ := document["timeline"].([]any)
	for _, name := range actions {
		timeline = append(timeline, map[string]any{"at": now, "action": name, "actor": actor, "from_status": oldStatus, "to_status": caseObject["status"], "reason": action.Reason})
	}
	document["timeline"] = timeline
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func requestID(r *http.Request) string {
	if v := r.Header.Get("X-Request-ID"); v != "" && len(v) <= 128 {
		return v
	}
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
func (s Server) listMembers(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	x, e := s.Control.ListMembers(r.Context(), p)
	if e != nil {
		http.Error(w, "data store unavailable", 503)
		return
	}
	writeJSON(w, 200, map[string]any{"items": x})
}
func (s Server) grantMember(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	s.changeMember(w, r, p, false)
}
func (s Server) revokeMember(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	s.changeMember(w, r, p, true)
}
func (s Server) changeMember(w http.ResponseWriter, r *http.Request, p auth.Principal, revoke bool) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	if e := s.Control.SetMember(r.Context(), p, r.PathValue("subject"), r.PathValue("role"), revoke, requestID(r)); e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	w.WriteHeader(204)
}

func (s Server) listAudit(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if s.Control == nil {
		http.Error(w, "control store unavailable", 503)
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 || n > 100 {
			http.Error(w, "invalid limit", 400)
			return
		}
		limit = n
	}
	items, next, e := s.Control.ListAudit(r.Context(), p, limit, r.URL.Query().Get("cursor"))
	if e != nil {
		http.Error(w, "invalid cursor or data store unavailable", 400)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items, "next_cursor": next})
}
