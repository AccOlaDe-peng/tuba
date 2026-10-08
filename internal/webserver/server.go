package webserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	Root       string
	APIURL     string
	IngestURL  string
	ProbeLimit time.Duration
}

type Server struct {
	root       string
	apiURL     *url.URL
	ingestURL  *url.URL
	apiProxy   *httputil.ReverseProxy
	ingest     *httputil.ReverseProxy
	probe      *http.Client
	probeLimit time.Duration
}

func New(c Config) (*Server, error) {
	root, err := filepath.Abs(c.Root)
	if err != nil {
		return nil, fmt.Errorf("resolve web root: %w", err)
	}
	info, err := os.Stat(filepath.Join(root, "index.html"))
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("web root must contain a regular index.html")
	}
	apiURL, err := parseURL(c.APIURL, "API_UPSTREAM", true, false)
	if err != nil {
		return nil, err
	}
	ingestURL, err := parseURL(c.IngestURL, "INGEST_UPSTREAM", true, false)
	if err != nil {
		return nil, err
	}
	probeLimit := c.ProbeLimit
	if probeLimit <= 0 {
		probeLimit = 2 * time.Second
	}
	return &Server{
		root:   root,
		apiURL: apiURL, ingestURL: ingestURL,
		apiProxy: httputil.NewSingleHostReverseProxy(apiURL),
		ingest:   httputil.NewSingleHostReverseProxy(ingestURL),
		probe:    &http.Client{Timeout: probeLimit}, probeLimit: probeLimit,
	}, nil
}

func parseURL(value, name string, allowHTTP, allowPath bool) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(value))
	if err != nil || u == nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (!allowPath && u.Path != "") {
		return nil, fmt.Errorf("%s must be an HTTP(S) URL without query, userinfo, or fragment", name)
	}
	if u.Scheme != "https" && (!allowHTTP || u.Scheme != "http") {
		return nil, fmt.Errorf("%s must use HTTPS", name)
	}
	return u, nil
}

func DefaultRoot() string {
	executable, err := os.Executable()
	if err == nil {
		return filepath.Clean(filepath.Join(filepath.Dir(executable), "..", "web"))
	}
	return filepath.Join("web", "dist")
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.HandleFunc("GET /config.js", s.runtimeConfig)
	internalNotFound := func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}
	mux.HandleFunc("/api/v1/internal", internalNotFound)
	mux.HandleFunc("/api/v1/internal/", internalNotFound)
	mux.HandleFunc("/api/v1/ingest", s.proxyIngest)
	mux.HandleFunc("/api/v1/ingest/", s.proxyIngest)
	mux.HandleFunc("/api/", s.proxyAPI)
	mux.HandleFunc("/", s.static)
	return securityHeaders(mux)
}

func (s *Server) runtimeConfig(w http.ResponseWriter, _ *http.Request) {
	values, err := json.Marshal(map[string]string{"basePath": "/"})
	if err != nil {
		http.Error(w, "runtime configuration unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = fmt.Fprintf(w, "window.TUBA_CONFIG = %s;\n", values)
}

func (s *Server) proxyAPI(w http.ResponseWriter, r *http.Request) {
	s.apiProxy.ServeHTTP(w, r)
}

func (s *Server) proxyIngest(w http.ResponseWriter, r *http.Request) {
	s.ingest.ServeHTTP(w, r)
}

func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	relative := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if relative == "." || relative == "" {
		relative = "index.html"
	}
	file := filepath.Join(s.root, filepath.FromSlash(relative))
	info, err := os.Stat(file)
	if err != nil || !info.Mode().IsRegular() {
		if path.Ext(relative) != "" {
			http.NotFound(w, r)
			return
		}
		file = filepath.Join(s.root, "index.html")
	}
	http.ServeFile(w, r, file)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	for _, target := range []*url.URL{s.apiURL, s.ingestURL} {
		probeURL := *target
		probeURL.Path = "/health/ready"
		ctx, cancel := context.WithTimeout(r.Context(), s.probeLimit)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
		if err == nil {
			response, probeErr := s.probe.Do(request)
			if probeErr == nil {
				_ = response.Body.Close()
				if response.StatusCode >= 200 && response.StatusCode < 300 {
					cancel()
					continue
				}
				err = fmt.Errorf("upstream readiness returned %s", response.Status)
			} else {
				err = probeErr
			}
		}
		cancel()
		http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
		next.ServeHTTP(w, r)
	})
}
