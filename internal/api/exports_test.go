package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"tuba/product/internal/auth"
	"tuba/product/internal/control"
)

// Q03 export endpoints fail closed when their dependencies are missing:
// no control store, or no controlled export directory, never degrades to an
// uncontrolled path.
func TestCreateExportFailsClosedWithoutDependencies(t *testing.T) {
	analyst := auth.Principal{Subject: "u1", Organization: "tenant_a", Namespace: "tenant_a", Roles: []string{"analyst"}}
	body := bytes.NewBufferString(`{"query":"search authentication"}`)

	recorder := httptest.NewRecorder()
	Server{}.createExport(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/exports", body), analyst)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("no control store: %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	Server{Control: &control.Store{}}.createExport(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/exports", body), analyst)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("no export dir: %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	Server{Control: &control.Store{}}.downloadExport(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/exports/x/download", nil), analyst)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("download without export dir: %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	Server{}.listExports(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/exports", nil), analyst)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("list without control store: %d", recorder.Code)
	}
}
