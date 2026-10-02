package controlworker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"tuba/product/internal/catalog"
	"tuba/product/internal/control"
	"tuba/product/internal/es"
	"tuba/product/internal/spl"
)

func fakeES(t *testing.T, response string, pages []string) *es.Client {
	t.Helper()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if len(pages) > 0 {
			if calls < len(pages) {
				fmt.Fprint(w, pages[calls])
			} else {
				fmt.Fprint(w, `{"hits":{"hits":[]}}`)
			}
			calls++
			return
		}
		fmt.Fprint(w, response)
	}))
	t.Cleanup(server.Close)
	client, err := es.New(server.URL, "test-key")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

const exportHitsPage = `{"hits":{"total":{"value":2,"relation":"eq"},"hits":[` +
	`{"_id":"1","sort":["2026-10-12T00:00:00Z","evt-1"],"_source":{"@timestamp":"2026-10-12T00:00:00Z","organization":{"id":"tenant_a"},"event":{"id":"evt-1","outcome":"failure","original":"raw-secret"},"user":{"name":"alice"},"source":{"ip":"10.0.0.1"}}},` +
	`{"_id":"2","sort":["2026-10-12T00:00:01Z","evt-2"],"_source":{"@timestamp":"2026-10-12T00:00:01Z","organization":{"id":"tenant_a"},"event":{"id":"evt-2","outcome":"success","original":"raw-secret-2"},"user":{"name":"bob"},"source":{"ip":"10.0.0.2"}}}` +
	`]}}`

func exportEventsFixture(t *testing.T, format string, rowLimit int, byteLimit int64, policy catalog.MaskingPolicy, esResponse string) (*boundedWriter, *bytes.Buffer) {
	t.Helper()
	plan, err := spl.Parse("search authentication")
	if err != nil {
		t.Fatal(err)
	}
	dataset, _ := catalog.Find("authentication")
	caps := exportCaps{fields: map[string]catalog.FieldDecl{}}
	for _, f := range dataset.Fields {
		caps.fields[f.Name] = f
	}
	if err := plan.Validate(caps); err != nil {
		t.Fatal(err)
	}
	executor := ExportExecutor{Config: ExportExecutorConfig{ES: fakeES(t, esResponse, nil)}}
	export := control.Export{Format: format, RowLimit: rowLimit, ByteLimit: byteLimit}
	buf := &bytes.Buffer{}
	out := &boundedWriter{w: buf, limit: byteLimit}
	env := spl.Env{Organization: "tenant_a", Index: "idx", Domain: "authentication", Generation: "g1"}
	if err := executor.exportEvents(context.Background(), plan, env, dataset, policy, export, out); err != nil {
		t.Fatal(err)
	}
	return out, buf
}

func TestExportEventsNDJSONMasking(t *testing.T) {
	out, buf := exportEventsFixture(t, control.ExportFormatNDJSON, 10, 1<<20, catalog.MaskingPolicy{}, exportHitsPage)
	if out.rows != 2 || out.overflow || out.limitRows {
		t.Fatalf("unexpected writer state: %+v", out)
	}
	content := buf.String()
	if strings.Contains(content, "alice") || strings.Contains(content, "bob") || strings.Contains(content, "raw-secret") {
		t.Fatalf("unmasked content leaked: %s", content)
	}
	if !strings.Contains(content, catalog.RedactedValue) {
		t.Fatalf("redaction marker missing: %s", content)
	}
	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%d", len(lines))
	}
	for _, line := range lines {
		var doc map[string]any
		if json.Unmarshal([]byte(line), &doc) != nil {
			t.Fatalf("invalid ndjson line: %s", line)
		}
	}
}

func TestExportEventsWithFullPermissions(t *testing.T) {
	_, buf := exportEventsFixture(t, control.ExportFormatNDJSON, 10, 1<<20,
		catalog.MaskingPolicy{IncludeSensitive: true, IncludeRaw: true}, exportHitsPage)
	content := buf.String()
	for _, want := range []string{"alice", "bob", "raw-secret"} {
		if !strings.Contains(content, want) {
			t.Fatalf("expected %s in full-permission export", want)
		}
	}
}

func TestExportEventsRowLimit(t *testing.T) {
	out, buf := exportEventsFixture(t, control.ExportFormatNDJSON, 1, 1<<20, catalog.MaskingPolicy{}, exportHitsPage)
	if out.rows != 1 || !out.limitRows {
		t.Fatalf("row limit not enforced: %+v", out)
	}
	if strings.Contains(buf.String(), "evt-2") {
		t.Fatal("second row present past the row limit")
	}
}

func TestExportEventsByteLimit(t *testing.T) {
	// Byte limit below one line: nothing is written, overflow is flagged and
	// the file never silently contains a truncated line.
	out, buf := exportEventsFixture(t, control.ExportFormatNDJSON, 10, 10, catalog.MaskingPolicy{}, exportHitsPage)
	if !out.overflow {
		t.Fatal("byte overflow not flagged")
	}
	if buf.Len() > 10 {
		t.Fatalf("byte limit exceeded: %d", buf.Len())
	}
}

func TestExportEventsCSV(t *testing.T) {
	out, buf := exportEventsFixture(t, control.ExportFormatCSV, 10, 1<<20, catalog.MaskingPolicy{}, exportHitsPage)
	if out.rows != 2 {
		t.Fatalf("rows=%d", out.rows)
	}
	content := buf.String()
	header := strings.SplitN(content, "\n", 2)[0]
	if !strings.Contains(header, "user.name") || !strings.Contains(header, "@timestamp") {
		t.Fatalf("csv header: %s", header)
	}
	if strings.Contains(content, "alice") || strings.Contains(content, "raw-secret") {
		t.Fatalf("unmasked content in csv: %s", content)
	}
	if !strings.Contains(content, catalog.RedactedValue) {
		t.Fatalf("redaction marker missing in csv: %s", content)
	}
}

func TestExportAggregation(t *testing.T) {
	plan, err := spl.Parse("search authentication | stats count BY event.outcome")
	if err != nil {
		t.Fatal(err)
	}
	dataset, _ := catalog.Find("authentication")
	caps := exportCaps{fields: map[string]catalog.FieldDecl{}}
	for _, f := range dataset.Fields {
		caps.fields[f.Name] = f
	}
	if err := plan.Validate(caps); err != nil {
		t.Fatal(err)
	}
	executor := ExportExecutor{Config: ExportExecutorConfig{ES: fakeES(t, `{"aggregations":{"by":{"buckets":[{"key":"failure","doc_count":172}]}}}`, nil)}}
	export := control.Export{Format: control.ExportFormatNDJSON, RowLimit: 100, ByteLimit: 1 << 20}
	buf := &bytes.Buffer{}
	out := &boundedWriter{w: buf, limit: 1 << 20}
	env := spl.Env{Organization: "tenant_a", Index: "idx", Domain: "authentication", Generation: "g1"}
	if err := executor.exportAggregation(context.Background(), plan, env, export, out); err != nil {
		t.Fatal(err)
	}
	if out.rows != 1 || !strings.Contains(buf.String(), `"doc_count":172`) {
		t.Fatalf("aggregation export: %+v %s", out, buf.String())
	}
}

func TestExportFilePath(t *testing.T) {
	path, err := ExportFilePath("/root", "3f6b6d2e-1234-4abc-8def-0123456789ab", "ndjson")
	if err != nil || !strings.HasSuffix(path, "3f6b6d2e-1234-4abc-8def-0123456789ab.ndjson") {
		t.Fatalf("%v %v", path, err)
	}
	for _, bad := range []string{"../etc", "not-a-uuid", ""} {
		if _, err := ExportFilePath("/root", bad, "ndjson"); err == nil {
			t.Fatalf("bad id %q accepted", bad)
		}
	}
	if _, err := ExportFilePath("", "3f6b6d2e-1234-4abc-8def-0123456789ab", "ndjson"); err == nil {
		t.Fatal("empty root accepted")
	}
	if _, err := ExportFilePath("/root", "3f6b6d2e-1234-4abc-8def-0123456789ab", "xml"); err == nil {
		t.Fatal("bad format accepted")
	}
}
