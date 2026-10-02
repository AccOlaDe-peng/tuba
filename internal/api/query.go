package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/spl"
)

// Q02 SPL subset query endpoint. Users submit SPL text only; it is parsed
// into a logical plan, validated against the Q01 catalog field whitelist and
// compiled server-side into a bounded Elasticsearch query. Raw ES DSL in any
// form is rejected fail-closed: the request schema is closed
// (DisallowUnknownFields) and the SPL lexer rejects `{`, `}`, `[`, `]`,
// regex, wildcards and subqueries.

type queryRequest struct {
	Query  string `json:"query"`
	From   string `json:"from"`
	To     string `json:"to"`
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`
}

// datasetCaps adapts a catalog DatasetDecl to the spl.FieldCaps whitelist.
type datasetCaps struct{ fields map[string]FieldDecl }

func (c datasetCaps) Lookup(name string) (string, bool, bool, bool) {
	f, ok := c.fields[name]
	if !ok {
		return "", false, false, false
	}
	return f.Type, f.Searchable, f.Aggregable, true
}

func capsFor(dataset DatasetDecl) datasetCaps {
	fields := make(map[string]FieldDecl, len(dataset.Fields))
	for _, f := range dataset.Fields {
		fields[f.Name] = f
	}
	return datasetCaps{fields: fields}
}

func (s Server) query(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var in queryRequest
	if err := decoder.Decode(&in); err != nil {
		http.Error(w, "invalid request: only query/from/to/limit/cursor are accepted; raw ES DSL is not accepted", http.StatusBadRequest)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		http.Error(w, "trailing data", http.StatusBadRequest)
		return
	}
	in.Query = strings.TrimSpace(in.Query)
	plan, err := spl.Parse(in.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dataset, ok := findDataset(plan.Dataset)
	if !ok {
		http.Error(w, "spl_field: unknown dataset "+plan.Dataset, http.StatusBadRequest)
		return
	}
	if err := plan.Validate(capsFor(dataset)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Time range: default 24h, absolute API cap 31 days (design baseline §7).
	now := time.Now().UTC()
	to := now
	if in.To != "" {
		parsed, err := time.Parse(time.RFC3339, in.To)
		if err != nil {
			http.Error(w, "invalid to", http.StatusBadRequest)
			return
		}
		to = parsed.UTC()
	}
	from := to.Add(-24 * time.Hour)
	if in.From != "" {
		parsed, err := time.Parse(time.RFC3339, in.From)
		if err != nil {
			http.Error(w, "invalid from", http.StatusBadRequest)
			return
		}
		from = parsed.UTC()
	}
	if !from.Before(to) || to.Sub(from) > spl.MaxTimeRangeDays*24*time.Hour {
		http.Error(w, "time range must be within 31 days", http.StatusBadRequest)
		return
	}

	// Pagination limit: default 100, single page max 1000.
	limit := spl.DefaultLimit
	if plan.Head > 0 {
		limit = plan.Head
	}
	if in.Limit > 0 {
		if in.Limit > spl.MaxLimit {
			http.Error(w, "limit must be 1..1000", http.StatusBadRequest)
			return
		}
		limit = in.Limit
	}
	var cursor []any
	if in.Cursor != "" {
		if plan.Mode != spl.ModeEvents {
			http.Error(w, "cursor is only valid for event queries", http.StatusBadRequest)
			return
		}
		if len(in.Cursor) > 2048 {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		decoded, err := base64.RawURLEncoding.DecodeString(in.Cursor)
		if err != nil || json.Unmarshal(decoded, &cursor) != nil || len(cursor) < 1 {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
	}

	// Four enforced scoping elements: tenant (term), namespace (index path),
	// dataset (index + domain term for uim domains), generation (term).
	env := spl.Env{
		Organization: principal.Organization,
		Index:        strings.ReplaceAll(dataset.IndexPattern, "<namespace>", principal.Namespace),
		From:         from,
		To:           to,
	}
	if dataset.Kind == "uim-domain" {
		env.Domain = dataset.Name
		env.Generation = dataset.ActiveGeneration
	}
	body, err := plan.Compile(env, limit, cursor)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Synchronous queries are bounded by the 30s design-baseline timeout.
	ctx, cancel := context.WithTimeout(r.Context(), spl.QueryTimeout)
	defer cancel()
	var result map[string]any
	status, err := s.ES.Do(ctx, http.MethodPost, "/"+env.Index+"/_search", body, &result)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			http.Error(w, "query_timeout: synchronous queries are limited to 30s", http.StatusGatewayTimeout)
			return
		}
		http.Error(w, "event store unavailable", http.StatusServiceUnavailable)
		return
	}
	if status != http.StatusOK {
		http.Error(w, "event store unavailable", http.StatusServiceUnavailable)
		return
	}
	s.writeQueryResult(w, plan, dataset, principal.Organization, result, limit)
}

func (s Server) writeQueryResult(w http.ResponseWriter, plan *spl.Plan, dataset DatasetDecl, organization string, result map[string]any, limit int) {
	hits, _ := result["hits"].(map[string]any)
	switch plan.Mode {
	case spl.ModeEvents:
		items := make([]map[string]any, 0)
		hitList, _ := hits["hits"].([]any)
		for _, raw := range hitList {
			hit, _ := raw.(map[string]any)
			source, _ := hit["_source"].(map[string]any)
			id, _ := hit["_id"].(string)
			if dataset.Kind == "uim-domain" {
				if item, ok := publicEvent(source, organization, id); ok {
					items = append(items, item)
				}
			} else if item, ok := sourceOK(source); ok {
				items = append(items, item)
			}
		}
		total := int64(0)
		if t, ok := hits["total"].(map[string]any); ok {
			if v, ok := t["value"].(float64); ok {
				total = int64(v)
			}
		}
		next := ""
		if len(hitList) == limit && len(hitList) > 0 {
			last, _ := hitList[len(hitList)-1].(map[string]any)
			if sort, ok := last["sort"].([]any); ok {
				if encoded, err := json.Marshal(sort); err == nil {
					next = base64.RawURLEncoding.EncodeToString(encoded)
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"mode": "events", "items": items, "next_cursor": next, "total": total})
	default:
		writeJSON(w, http.StatusOK, map[string]any{
			"mode":         plan.Mode,
			"aggregations": result["aggregations"],
		})
	}
}

// sourceOK passes through non-UIM datasets (raw/quarantine) minus the
// sensitive original payload, for datasets publicEvent does not cover.
func sourceOK(source map[string]any) (map[string]any, bool) {
	if source == nil {
		return nil, false
	}
	if event, ok := source["event"].(map[string]any); ok {
		delete(event, "original")
	}
	return source, true
}
