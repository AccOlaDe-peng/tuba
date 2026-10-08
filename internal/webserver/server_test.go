package webserver

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func webRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("<html>shell</html>"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "assets"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "assets", "app.js"), []byte("asset"), 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestHandlerRoutesAPIAndIngestAndServesRuntimeConfig(t *testing.T) {
	type observed struct {
		method, path, auth, body string
	}
	apiRequests := make(chan observed, 1)
	ingestRequests := make(chan observed, 1)
	upstream := func(ch chan observed) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health/ready" {
				w.WriteHeader(http.StatusOK)
				return
			}
			body, _ := io.ReadAll(r.Body)
			ch <- observed{method: r.Method, path: r.URL.RequestURI(), auth: r.Header.Get("Authorization"), body: string(body)}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte("accepted"))
		}))
	}
	api := upstream(apiRequests)
	defer api.Close()
	ingest := upstream(ingestRequests)
	defer ingest.Close()
	app, err := New(Config{Root: webRoot(t), APIURL: api.URL, IngestURL: ingest.URL})
	if err != nil {
		t.Fatal(err)
	}
	handler := app.Handler()

	apiRequest := httptest.NewRequest(http.MethodGet, "/api/v1/me?expand=roles", nil)
	apiRequest.Header.Set("Authorization", "Bearer api-canary")
	apiResponse := httptest.NewRecorder()
	handler.ServeHTTP(apiResponse, apiRequest)
	if apiResponse.Code != http.StatusAccepted {
		t.Fatalf("API proxy status=%d body=%s", apiResponse.Code, apiResponse.Body.String())
	}
	if got := <-apiRequests; got != (observed{method: http.MethodGet, path: "/api/v1/me?expand=roles", auth: "Bearer api-canary"}) {
		t.Fatalf("API upstream got %+v", got)
	}

	body := `{"event":"sample"}`
	ingestRequest := httptest.NewRequest(http.MethodPost, "/api/v1/ingest/events", strings.NewReader(body))
	ingestRequest.Header.Set("Authorization", "Bearer ingest-canary")
	ingestResponse := httptest.NewRecorder()
	handler.ServeHTTP(ingestResponse, ingestRequest)
	if ingestResponse.Code != http.StatusAccepted {
		t.Fatalf("ingest proxy status=%d body=%s", ingestResponse.Code, ingestResponse.Body.String())
	}
	if got := <-ingestRequests; got != (observed{method: http.MethodPost, path: "/api/v1/ingest/events", auth: "Bearer ingest-canary", body: body}) {
		t.Fatalf("ingest upstream got %+v", got)
	}

	configResponse := httptest.NewRecorder()
	handler.ServeHTTP(configResponse, httptest.NewRequest(http.MethodGet, "/config.js", nil))
	if configResponse.Code != http.StatusOK || configResponse.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("runtime config response status=%d headers=%v", configResponse.Code, configResponse.Header())
	}
	if !strings.Contains(configResponse.Body.String(), `"basePath":"/"`) || strings.Contains(configResponse.Body.String(), "oidc") {
		t.Fatalf("unexpected runtime config: %s", configResponse.Body.String())
	}

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status=%d body=%s", ready.Code, ready.Body.String())
	}
}

func TestHandlerBlocksInternalIngestAndServesSPAFallback(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	app, err := New(Config{Root: webRoot(t), APIURL: upstream.URL, IngestURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	handler := app.Handler()

	blocked := httptest.NewRecorder()
	handler.ServeHTTP(blocked, httptest.NewRequest(http.MethodPost, "/api/v1/internal/ingest/beat-events", nil))
	if blocked.Code != http.StatusNotFound {
		t.Fatalf("internal ingest route status=%d", blocked.Code)
	}

	spa := httptest.NewRecorder()
	handler.ServeHTTP(spa, httptest.NewRequest(http.MethodGet, "/overview", nil))
	if spa.Code != http.StatusOK || spa.Body.String() != "<html>shell</html>" {
		t.Fatalf("SPA fallback status=%d body=%s", spa.Code, spa.Body.String())
	}
	asset := httptest.NewRecorder()
	handler.ServeHTTP(asset, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	if asset.Code != http.StatusOK || asset.Body.String() != "asset" {
		t.Fatalf("static asset status=%d body=%s", asset.Code, asset.Body.String())
	}
	missingAsset := httptest.NewRecorder()
	handler.ServeHTTP(missingAsset, httptest.NewRequest(http.MethodGet, "/assets/missing.js", nil))
	if missingAsset.Code != http.StatusNotFound {
		t.Fatalf("missing asset status=%d", missingAsset.Code)
	}
	if got := spa.Header().Get("Content-Security-Policy"); !strings.Contains(got, "connect-src 'self';") {
		t.Fatalf("CSP must restrict authentication to this origin: %s", got)
	}
}

func TestReadinessFailsWhenEitherUpstreamIsUnavailable(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ingest.Close()
	app, err := New(Config{Root: webRoot(t), APIURL: api.URL, IngestURL: ingest.URL})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRuntimeConfigIsJSONSafe(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()
	app, err := New(Config{Root: webRoot(t), APIURL: upstream.URL, IngestURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	app.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/config.js", nil))
	if strings.Contains(strings.ToLower(response.Body.String()), "</script>") {
		t.Fatalf("runtime JS contains an unescaped script terminator: %s", response.Body.String())
	}
	var config map[string]string
	encoded := strings.TrimPrefix(strings.TrimSpace(response.Body.String()), "window.TUBA_CONFIG = ")
	encoded = strings.TrimSuffix(encoded, ";")
	if err := json.Unmarshal([]byte(encoded), &config); err != nil {
		t.Fatalf("runtime configuration is not JSON: %v", err)
	}
	if config["basePath"] != "/" {
		t.Fatalf("runtime config client id=%q", config["basePath"])
	}
}
