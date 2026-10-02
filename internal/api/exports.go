package api

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
	"tuba/product/internal/controlworker"
	"tuba/product/internal/spl"
)

// Q03 asynchronous export endpoints. Creation freezes the request snapshot
// and the caller's permission snapshot; the control worker pins the
// retention boundary at execution; download re-authorizes the live principal
// (role revocation or a different subject is denied), enforces the 15-minute
// single-use link, and everything is audited.

type exportRequest struct {
	Query  string `json:"query"`
	Format string `json:"format"`
	From   string `json:"from"`
	To     string `json:"to"`
	Limit  int    `json:"limit"`
}

func (s Server) createExport(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if !s.requireControl(w) {
		return
	}
	if s.ExportDir == "" {
		// Fail closed: without controlled export storage no export is accepted.
		http.Error(w, "export storage not configured", http.StatusServiceUnavailable)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var in exportRequest
	if err := decoder.Decode(&in); err != nil {
		http.Error(w, "invalid request: only query/format/from/to/limit are accepted", http.StatusBadRequest)
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
	format := in.Format
	if format == "" {
		format = control.ExportFormatNDJSON
	}
	if format != control.ExportFormatNDJSON && format != control.ExportFormatCSV {
		http.Error(w, "format must be ndjson or csv", http.StatusBadRequest)
		return
	}
	if plan.Mode != spl.ModeEvents && format == control.ExportFormatCSV {
		http.Error(w, "csv export supports event queries only", http.StatusBadRequest)
		return
	}

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
	// Early retention check (the authoritative boundary is pinned again at
	// execution): never accept a range that already starts outside retention.
	if from.Before(now.Add(-control.ExportDatasetRetention)) {
		http.Error(w, "retention_exceeded: export range starts before the dataset retention boundary", http.StatusBadRequest)
		return
	}

	limit := control.ExportMaxRows
	if in.Limit > 0 {
		if in.Limit > control.ExportMaxRows {
			http.Error(w, "limit must be 1..100000", http.StatusBadRequest)
			return
		}
		limit = in.Limit
	}

	includeSensitive := principal.Can("sensitive:read")
	includeRaw := principal.Can("raw:read")
	if dataset.Kind == "raw" && !includeRaw {
		// The raw dataset's content is the original payload; exporting it
		// without raw:read would be an empty shell, so refuse fail-closed.
		http.Error(w, "permission denied: raw:read is required to export the raw dataset", http.StatusForbidden)
		return
	}

	export, status, err := s.Control.CreateExport(r.Context(), principal, control.CreateExportInput{
		Dataset:          dataset.Name,
		Format:           format,
		Query:            in.Query,
		Generation:       dataset.ActiveGeneration,
		From:             from,
		To:               to,
		RowLimit:         limit,
		ByteLimit:        control.ExportMaxBytes,
		IncludeSensitive: includeSensitive,
		IncludeRaw:       includeRaw,
	}, requestID(r))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, status, export)
}

func (s Server) listExports(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if !s.requireControl(w) {
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
	items, next, err := s.Control.ListExports(r.Context(), principal, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		http.Error(w, "invalid cursor or data store unavailable", http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}

func (s Server) getExport(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if !s.requireControl(w) {
		return
	}
	export, err := s.Control.GetExport(r.Context(), principal, r.PathValue("id"))
	if err != nil {
		http.Error(w, "export not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, export)
}

func (s Server) downloadExport(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	if !s.requireControl(w) {
		return
	}
	if s.ExportDir == "" {
		http.Error(w, "export storage not configured", http.StatusServiceUnavailable)
		return
	}
	// Download re-authorization happens inside the store against the live
	// principal; a bearer link alone is never sufficient.
	export, status, err := s.Control.AuthorizeExportDownload(r.Context(), principal, r.PathValue("id"), requestID(r))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	path, err := controlworker.ExportFilePath(s.ExportDir, export.ID, export.Format)
	if err != nil {
		http.Error(w, "invalid export", http.StatusInternalServerError)
		return
	}
	file, err := os.Open(path)
	if err != nil {
		http.Error(w, "export file unavailable", http.StatusInternalServerError)
		return
	}
	defer file.Close()
	contentType := "application/x-ndjson"
	if export.Format == control.ExportFormatCSV {
		contentType = "text/csv; charset=utf-8"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", "attachment; filename=\"export-"+export.ID+"."+export.Format+"\"")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.Copy(w, file)
}
