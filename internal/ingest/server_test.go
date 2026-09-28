package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReadinessTracksDependencyRecovery(t *testing.T) {
	dependencyErr := errors.New("dependency unavailable")
	dependencyReady := false
	server := Server{
		RequestTimeout: time.Second,
		ReadyCheck: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				t.Error("readiness dependency check has no deadline")
			}
			if !dependencyReady {
				return dependencyErr
			}
			return nil
		},
	}
	check := func(want int) {
		t.Helper()
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
		if response.Code != want {
			t.Fatalf("readiness status=%d, want %d", response.Code, want)
		}
	}

	check(http.StatusServiceUnavailable)
	dependencyReady = true
	check(http.StatusOK)
	dependencyReady = false
	check(http.StatusServiceUnavailable)
}

func TestReadinessWithoutDependencyCheckIsUnavailable(t *testing.T) {
	response := httptest.NewRecorder()
	Server{}.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness status=%d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}
