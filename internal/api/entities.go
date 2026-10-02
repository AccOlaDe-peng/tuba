package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"tuba/product/internal/auth"
	"tuba/product/internal/entity"
)

// Entity profile read endpoints (W03 backend). Master data, attribution
// history, temporal relations, feature samples and baseline status are read
// from PostgreSQL via entity.Queries (tenant-scoped by the principal's
// organization slug); the current risk projection is read back from the
// ueba-analysis-entity_risk-<namespace> state index written by the F07/R02
// sink. Every endpoint fails closed: without the entity store or the ES
// client the answer is 503, never a guess.

var entityIDPattern = regexp.MustCompile(`^ent:[a-f0-9]{64}$`)

func (s Server) requireEntities(w http.ResponseWriter) bool {
	if s.Entities == nil {
		http.Error(w, "entity store unavailable", http.StatusServiceUnavailable)
		return false
	}
	return true
}

func entityPathID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !entityIDPattern.MatchString(id) {
		http.Error(w, "invalid entity ID", http.StatusBadRequest)
		return "", false
	}
	return id, true
}

func queryLimit(w http.ResponseWriter, r *http.Request, fallback, max int) (int, bool) {
	limit := fallback
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > max {
			http.Error(w, "limit must be 1.."+strconv.Itoa(max), http.StatusBadRequest)
			return 0, false
		}
		limit = value
	}
	return limit, true
}

func (s Server) listEntities(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !s.requireEntities(w) {
		return
	}
	limit, ok := queryLimit(w, r, 50, 100)
	if !ok {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if len(query) > 200 {
		http.Error(w, "query is too long", http.StatusBadRequest)
		return
	}
	entityType := strings.TrimSpace(r.URL.Query().Get("type"))
	items, next, err := s.Entities.SearchEntities(r.Context(), p.Organization, query, entityType, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		if errors.Is(err, entity.ErrInvalidCursor) || errors.Is(err, entity.ErrInvalidEntityType) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s Server) entityDetail(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !s.requireEntities(w) {
		return
	}
	id, ok := entityPathID(w, r)
	if !ok {
		return
	}
	detail, found, err := s.Entities.GetEntity(r.Context(), p.Organization, id)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	if !found {
		http.Error(w, "entity not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s Server) entityAttributions(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !s.requireEntities(w) {
		return
	}
	id, ok := entityPathID(w, r)
	if !ok {
		return
	}
	limit, ok := queryLimit(w, r, 50, 100)
	if !ok {
		return
	}
	items, next, err := s.Entities.ListAttributions(r.Context(), p.Organization, id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		if errors.Is(err, entity.ErrInvalidCursor) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s Server) entityRelations(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !s.requireEntities(w) {
		return
	}
	id, ok := entityPathID(w, r)
	if !ok {
		return
	}
	history := true
	if raw := r.URL.Query().Get("history"); raw != "" {
		switch raw {
		case "true":
		case "false":
			history = false
		default:
			http.Error(w, "history must be true or false", http.StatusBadRequest)
			return
		}
	}
	items, err := s.Entities.ListRelations(r.Context(), p.Organization, id, history)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	if items == nil {
		items = []entity.Relation{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s Server) entityFeatures(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !s.requireEntities(w) {
		return
	}
	id, ok := entityPathID(w, r)
	if !ok {
		return
	}
	limit, ok := queryLimit(w, r, 20, 50)
	if !ok {
		return
	}
	items, err := s.Entities.ListFeatureSamples(r.Context(), p.Organization, id, limit)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s Server) entityBaseline(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !s.requireEntities(w) {
		return
	}
	id, ok := entityPathID(w, r)
	if !ok {
		return
	}
	items, err := s.Entities.ListBaselines(r.Context(), p.Organization, id)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

// entityRisk reads back the entity's current R02 risk projection from the
// strict state index. An entity without any finding contributions has no
// projection document; that is reported as projection:null, not as an error.
func (s Server) entityRisk(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id, ok := entityPathID(w, r)
	if !ok {
		return
	}
	var source struct {
		Organization struct {
			ID string `json:"id"`
		} `json:"organization"`
		Object struct {
			Revision  int64  `json:"revision"`
			Operation string `json:"operation"`
		} `json:"object"`
		Document json.RawMessage `json:"document"`
	}
	var hit struct {
		Found  bool            `json:"found"`
		Source json.RawMessage `json:"_source"`
	}
	status, err := s.ES.Do(r.Context(), http.MethodGet, "/ueba-analysis-entity_risk-"+p.Namespace+"/_doc/"+id, nil, &hit)
	if err != nil {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	if status == http.StatusNotFound || (status == http.StatusOK && !hit.Found) {
		writeJSON(w, http.StatusOK, map[string]any{"entity_id": id, "projection": nil})
		return
	}
	if status != http.StatusOK {
		http.Error(w, "data store unavailable", http.StatusServiceUnavailable)
		return
	}
	if json.Unmarshal(hit.Source, &source) != nil || source.Organization.ID != p.Organization {
		http.Error(w, "invalid stored document", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entity_id":  id,
		"projection": source.Document,
		"revision":   source.Object.Revision,
		"operation":  source.Object.Operation,
	})
}
