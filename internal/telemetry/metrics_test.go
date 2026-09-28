package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestListenAddressDefaultsToLoopbackAndRequiresExplicitOptIn(t *testing.T) {
	t.Setenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", "")
	t.Setenv("TEST_METRICS_LISTEN", "")
	address, err := ListenAddress("TEST_METRICS_LISTEN", "127.0.0.1:19000")
	if err != nil || address != "127.0.0.1:19000" {
		t.Fatalf("default metrics listener=%q err=%v", address, err)
	}
	t.Setenv("TEST_METRICS_LISTEN", ":19000")
	if _, err := ListenAddress("TEST_METRICS_LISTEN", "127.0.0.1:19000"); err == nil {
		t.Fatal("wildcard metrics listener accepted without opt-in")
	}
	t.Setenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", "true")
	if address, err := ListenAddress("TEST_METRICS_LISTEN", "127.0.0.1:19000"); err != nil || address != ":19000" {
		t.Fatalf("explicit wildcard metrics listener=%q err=%v", address, err)
	}
	t.Setenv("TEST_METRICS_LISTEN", "metrics.internal:19000")
	if _, err := ListenAddress("TEST_METRICS_LISTEN", "127.0.0.1:19000"); err == nil {
		t.Fatal("DNS metrics listener accepted despite opt-in")
	}
}

func TestRuntimeHandlerExposesHealthAndMetrics(t *testing.T) {
	registry := New()
	registry.Add("tuba_test_count-total", 3)

	notReady := httptest.NewRecorder()
	registry.RuntimeHandler().ServeHTTP(notReady, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if notReady.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d", notReady.Code)
	}

	registry.SetReady(true)
	ready := httptest.NewRecorder()
	registry.RuntimeHandler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status=%d", ready.Code)
	}

	metrics := httptest.NewRecorder()
	registry.RuntimeHandler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), "tuba_test_count_total 3") {
		t.Fatalf("metric missing: %s", metrics.Body.String())
	}
}

func TestRequestTimeoutPassesDeadlineAndCancelsHandlerContext(t *testing.T) {
	finished := make(chan struct{})
	handler := RequestTimeout(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("request deadline missing")
		}
		<-r.Context().Done()
		if r.Context().Err() != context.DeadlineExceeded {
			t.Errorf("context error=%v", r.Context().Err())
		}
		close(finished)
	}), 10*time.Millisecond)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled at its deadline")
	}
}
