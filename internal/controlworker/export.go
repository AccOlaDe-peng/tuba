package controlworker

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"log/slog"
	"os"
	"path/filepath"
	
	"time"

	"tuba/product/internal/catalog"
	"tuba/product/internal/control"
	"tuba/product/internal/es"
	"tuba/product/internal/spl"
)

// Q03 export executor. Selection rationale: exports are long-running (up to
// 100k rows / 1 GiB) and must survive API restarts with exactly-once state
// semantics, so they ride the existing T01/T02 processing_jobs framework
// (lease, fencing token, bounded attempts, retention cleaner) instead of an
// in-process API goroutine, which would lose work on restart and could not
// re-authorize completion. job_type 'export' was reserved in migration
// 00007. Files are written to a controlled local directory (TUBA_EXPORT_DIR)
// shared with the API on the single-node deployment; PostgreSQL bytea was
// rejected (1 GiB rows in the control plane) and object storage is not part
// of the current single-node facility (design baseline §8 lists S3 only as a
// backup target).

type ExportExecutorConfig struct {
	Store      *control.Store
	ES         *es.Client
	ExportRoot string
	WorkerID   string
	// DatasetRetention is the approved queryable window of every dataset
	// (A03: ES keeps 7 UTC day partitions). The execution-time retention
	// boundary is pinned as now-DatasetRetention; requests whose fixed range
	// starts before it fail with retention_exceeded, never silent truncation.
	DatasetRetention time.Duration
	PageSize         int
	Now              func() time.Time
}

type ExportExecutor struct{ Config ExportExecutorConfig }

func (e ExportExecutor) now() time.Time {
	if e.Config.Now != nil {
		return e.Config.Now().UTC()
	}
	return time.Now().UTC()
}

func (e ExportExecutor) datasetRetention() time.Duration {
	if e.Config.DatasetRetention > 0 {
		return e.Config.DatasetRetention
	}
	return control.ExportDatasetRetention
}

// Handler returns the processing-job handler for job_type "export".
func (e ExportExecutor) Handler() JobHandler {
	return func(ctx context.Context, job Job) error {
		return e.execute(ctx, job)
	}
}

func (e ExportExecutor) execute(ctx context.Context, job Job) error {
	c := e.Config
	if c.Store == nil || c.ES == nil || c.ExportRoot == "" {
		return errors.New("export executor is not fully configured")
	}
	export, err := c.Store.LoadExportForJob(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("export row for job: %w", err)
	}
	fail := func(reason string) error {
		if err := c.Store.FailExport(context.Background(), export.ID, reason, c.WorkerID); err != nil {
			slog.Error("export failure persistence failed", "export_id", export.ID, "error", err)
		}
		return errors.New(reason)
	}

	// Pin the retention boundary at execution time (implementation boundary
	// requirement: 导出任务必须在执行前固定边界). The range itself was frozen
	// at creation; if it starts before the currently approved retention, the
	// export fails with retention_exceeded instead of silently truncating.
	retentionFrom := e.now().Add(-e.datasetRetention())
	if export.From.Before(retentionFrom) {
		return fail(fmt.Sprintf("retention_exceeded: export starts %s before the dataset retention boundary %s",
			export.From.Format(time.RFC3339), retentionFrom.Format(time.RFC3339)))
	}

	// Completion re-authorization: the creator must still hold exactly the
	// permissions the snapshot was taken with (完成时重新授权).
	fingerprint, err := c.Store.FingerprintForIdentity(ctx, export.CreatedBy)
	if err != nil {
		return fail("export creator re-authorization failed: " + err.Error())
	}
	if fingerprint != export.Fingerprint {
		return fail("export creator permissions changed since creation")
	}
	if err := c.Store.MarkExportRunning(ctx, export.ID, retentionFrom); err != nil {
		return fmt.Errorf("mark export running: %w", err)
	}

	result, err := e.run(ctx, export)
	if err != nil {
		if ctx.Err() != nil {
			return err // lease lost or shutting down; the job is requeued/finished by the framework
		}
		return fail("export execution failed: " + err.Error())
	}
	if err := c.Store.CompleteExport(ctx, export.ID, result, c.WorkerID); err != nil {
		return fmt.Errorf("complete export: %w", err)
	}
	return nil
}

// datasetCaps adapts the catalog to the spl.FieldCaps whitelist.
type exportCaps struct{ fields map[string]catalog.FieldDecl }

func (c exportCaps) Lookup(name string) (string, bool, bool, bool) {
	f, ok := c.fields[name]
	if !ok {
		return "", false, false, false
	}
	return f.Type, f.Searchable, f.Aggregable, true
}

func (e ExportExecutor) run(ctx context.Context, export control.Export) (control.ExportResult, error) {
	var result control.ExportResult
	plan, err := spl.Parse(export.Query)
	if err != nil {
		return result, err
	}
	dataset, ok := catalog.Find(export.Dataset)
	if !ok {
		return result, fmt.Errorf("unknown dataset %q", export.Dataset)
	}
	caps := exportCaps{fields: map[string]catalog.FieldDecl{}}
	for _, f := range dataset.Fields {
		caps.fields[f.Name] = f
	}
	if err := plan.Validate(caps); err != nil {
		return result, err
	}
	if plan.Mode != spl.ModeEvents && export.Format == control.ExportFormatCSV {
		return result, errors.New("csv export supports event queries only")
	}

	var orgSlug string
	if err := e.Config.Store.Pool.QueryRow(ctx, `SELECT slug FROM organizations WHERE id=$1`, export.OrganizationID).Scan(&orgSlug); err != nil {
		return result, fmt.Errorf("resolve organization: %w", err)
	}
	env := spl.Env{
		Organization: orgSlug,
		Index:        replaceNamespace(dataset.IndexPattern, export.Namespace),
		From:         export.From,
		To:           export.To,
	}
	if dataset.Kind == "uim-domain" {
		env.Domain = dataset.Name
		env.Generation = dataset.ActiveGeneration
	}

	file, err := os.OpenFile(e.tmpPath(export.ID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, fmt.Errorf("create export file: %w", err)
	}
	hash := sha256.New()
	writer := &boundedWriter{w: io.MultiWriter(file, hash), limit: export.ByteLimit}
	policy := catalog.MaskingPolicy{IncludeSensitive: export.IncludeSensitive, IncludeRaw: export.IncludeRaw}

	switch plan.Mode {
	case spl.ModeEvents:
		err = e.exportEvents(ctx, plan, env, dataset, policy, export, writer)
	default:
		err = e.exportAggregation(ctx, plan, env, export, writer)
	}
	closeErr := file.Close()
	if err != nil {
		os.Remove(e.tmpPath(export.ID))
		return result, err
	}
	if closeErr != nil {
		os.Remove(e.tmpPath(export.ID))
		return result, closeErr
	}
	if writer.overflow {
		result.LimitReached = "bytes"
	} else if writer.limitRows {
		result.LimitReached = "rows"
	}
	result.RowCount = writer.rows
	result.ByteCount = writer.written
	result.FileSHA256 = hex.EncodeToString(hash.Sum(nil))
	if err := os.Rename(e.tmpPath(export.ID), e.finalPath(export.ID, export.Format)); err != nil {
		os.Remove(e.tmpPath(export.ID))
		return result, fmt.Errorf("publish export file: %w", err)
	}
	return result, nil
}

func (e ExportExecutor) exportEvents(ctx context.Context, plan *spl.Plan, env spl.Env, dataset catalog.DatasetDecl, policy catalog.MaskingPolicy, export control.Export, out *boundedWriter) error {
	pageSize := e.Config.PageSize
	if pageSize < 1 || pageSize > spl.MaxLimit {
		pageSize = spl.MaxLimit
	}
	var csvWriter *csv.Writer
	var columns []string
	if export.Format == control.ExportFormatCSV {
		columns = catalog.ExportColumns(dataset, policy)
		csvWriter = csv.NewWriter(out)
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
	}
	var cursor []any
	for {
		remaining := export.RowLimit - int(out.rows)
		if remaining <= 0 {
			out.limitRows = true
			break
		}
		size := pageSize
		if remaining < size {
			size = remaining
		}
		body, err := plan.Compile(env, size, cursor)
		if err != nil {
			return err
		}
		reqCtx, cancel := context.WithTimeout(ctx, spl.QueryTimeout)
		var response map[string]any
		status, err := e.Config.ES.Do(reqCtx, http.MethodPost, "/"+env.Index+"/_search", body, &response)
		cancel()
		if err != nil {
			return fmt.Errorf("event store unavailable: %w", err)
		}
		if status != http.StatusOK {
			return fmt.Errorf("event store returned status %d", status)
		}
		hits, _ := response["hits"].(map[string]any)
		hitList, _ := hits["hits"].([]any)
		if len(hitList) == 0 {
			break
		}
		for _, raw := range hitList {
			if int(out.rows) >= export.RowLimit {
				out.limitRows = true
				break
			}
			hit, _ := raw.(map[string]any)
			source, _ := hit["_source"].(map[string]any)
			masked := catalog.MaskEvent(source, dataset, policy)
			if csvWriter != nil {
				record := make([]string, len(columns))
				for i, column := range columns {
					record[i] = exportCell(masked, column)
				}
				if err := csvWriter.Write(record); err != nil {
					return err
				}
				csvWriter.Flush()
				if err := csvWriter.Error(); err != nil {
					return err
				}
			} else {
				line, err := json.Marshal(masked)
				if err != nil {
					return err
				}
				if _, err := out.Write(append(line, '\n')); err != nil {
					return err
				}
			}
			out.rows++
			if out.overflow {
				break
			}
		}
		if out.overflow {
			break
		}
		last, _ := hitList[len(hitList)-1].(map[string]any)
		cursor, _ = last["sort"].([]any)
		if len(hitList) < size || cursor == nil {
			break
		}
	}
	if int(out.rows) >= export.RowLimit {
		out.limitRows = true
	}
	return nil
}

func (e ExportExecutor) exportAggregation(ctx context.Context, plan *spl.Plan, env spl.Env, export control.Export, out *boundedWriter) error {
	body, err := plan.Compile(env, 0, nil)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, spl.QueryTimeout)
	var response map[string]any
	status, err := e.Config.ES.Do(reqCtx, http.MethodPost, "/"+env.Index+"/_search", body, &response)
	cancel()
	if err != nil {
		return fmt.Errorf("event store unavailable: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("event store returned status %d", status)
	}
	line, err := json.Marshal(map[string]any{"mode": plan.Mode, "aggregations": response["aggregations"]})
	if err != nil {
		return err
	}
	if _, err := out.Write(append(line, '\n')); err != nil {
		return err
	}
	out.rows = 1
	return nil
}

func exportCell(masked map[string]any, column string) string {
	value, ok := catalog.LookupPath(masked, column)
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case float64, bool:
		return fmt.Sprint(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(encoded)
	}
}

func (e ExportExecutor) tmpPath(id string) string {
	return filepath.Join(e.Config.ExportRoot, id+".tmp")
}

func (e ExportExecutor) finalPath(id, format string) string {
	return filepath.Join(e.Config.ExportRoot, id+"."+format)
}

// ExportFilePath resolves the on-disk file of a completed export. The id
// must be a canonical UUID so a malformed id can never escape the root.
func ExportFilePath(root, id, format string) (string, error) {
	if root == "" {
		return "", errors.New("export root is not configured")
	}
	if !isCanonicalUUID(id) {
		return "", fmt.Errorf("export id %q is not a canonical UUID; refusing to derive a path", id)
	}
	if format != control.ExportFormatNDJSON && format != control.ExportFormatCSV {
		return "", fmt.Errorf("unknown export format %q", format)
	}
	return filepath.Join(root, id+"."+format), nil
}

func replaceNamespace(pattern, namespace string) string {
	out := ""
	for {
		i := indexOf(pattern, "<namespace>")
		if i < 0 {
			return out + pattern
		}
		out += pattern[:i] + namespace
		pattern = pattern[i+len("<namespace>"):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// boundedWriter counts rows/bytes and stops accepting writes once the byte
// limit would be exceeded; the caller then stops with limit_reached=bytes.
type boundedWriter struct {
	w         io.Writer
	limit     int64
	written   int64
	rows      int64
	overflow  bool
	limitRows bool
}

func (b *boundedWriter) Write(p []byte) (int, error) {
	if b.written+int64(len(p)) > b.limit {
		b.overflow = true
		return 0, nil
	}
	n, err := b.w.Write(p)
	b.written += int64(n)
	return n, err
}

// ExportExpirer periodically expires lapsed download links and removes their
// files. Expiry is audited by Store.ExpireExports (export.expire).
type ExportExpirerConfig struct {
	Store        *control.Store
	ExportRoot   string
	PollInterval time.Duration
}

type ExportExpirer struct{ Config ExportExpirerConfig }

func (x ExportExpirer) Run(ctx context.Context) error {
	if x.Config.Store == nil {
		return errors.New("export expirer requires the control store")
	}
	interval := x.Config.PollInterval
	if interval <= 0 {
		interval = time.Minute
	}
	for ctx.Err() == nil {
		if err := x.Sweep(ctx); err != nil && ctx.Err() == nil {
			slog.Error("export expiry sweep failed", "error", err)
		}
		if !wait(ctx, interval) {
			return nil
		}
	}
	return nil
}

func (x ExportExpirer) Sweep(ctx context.Context) error {
	expired, err := x.Config.Store.ExpireExports(ctx, 500)
	if err != nil {
		return err
	}
	for _, item := range expired {
		for _, format := range []string{control.ExportFormatNDJSON, control.ExportFormatCSV} {
			path, err := ExportFilePath(x.Config.ExportRoot, item.ID, format)
			if err != nil {
				slog.Error("export file path derivation failed; keeping file", "export_id", item.ID, "error", err)
				continue
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				slog.Error("expired export file removal failed", "export_id", item.ID, "error", err)
			}
		}
	}
	return nil
}

