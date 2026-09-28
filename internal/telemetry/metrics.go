package telemetry

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tuba/product/internal/config"
)

type Registry struct {
	mu    sync.Mutex
	c     map[string]*atomic.Uint64
	ready atomic.Bool
}

func New() *Registry {
	return &Registry{c: map[string]*atomic.Uint64{}}
}

// ListenAddress resolves a per-service listener with a safe local fallback.
func ListenAddress(serviceEnv, fallback string) (string, error) {
	address := os.Getenv(serviceEnv)
	if address == "" {
		address = fallback
	}
	if err := config.ValidateListenerAddress(serviceEnv, address, os.Getenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN")); err != nil {
		return "", fmt.Errorf("%s: %w", serviceEnv, err)
	}
	return address, nil
}

func (r *Registry) Inc(name string) { r.Add(name, 1) }
func (r *Registry) Add(name string, n uint64) {
	r.mu.Lock()
	v := r.c[name]
	if v == nil {
		v = &atomic.Uint64{}
		r.c[name] = v
	}
	r.mu.Unlock()
	v.Add(n)
}

func (r *Registry) SetReady(ready bool) {
	r.ready.Store(ready)
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.mu.Lock()
		names := make([]string, 0, len(r.c))
		for n := range r.c {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(w, "%s %d\n", strings.ReplaceAll(n, "-", "_"), r.c[n].Load())
		}
		r.mu.Unlock()
	})
}

func (r *Registry) RuntimeHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !r.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	mux.Handle("GET /metrics", r.Handler())
	return mux
}

func Serve(ctx context.Context, addr string, registry *Registry) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           registry.RuntimeHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	select {
	case err := <-serveResult:
		registry.SetReady(false)
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	case <-ctx.Done():
		registry.SetReady(false)
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			<-serveResult
			return err
		}
		err := <-serveResult
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(body)
}

func Middleware(next http.Handler, registry *Registry) http.Handler {
	if registry == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registry.Inc("tuba_api_http_requests_total")
		recorder := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		if recorder.status >= http.StatusInternalServerError {
			registry.Inc("tuba_api_http_errors_total")
		}
	})
}

// RequestTimeout bounds downstream database, Kafka, and Elasticsearch work
// performed with the request context. The HTTP server should set a slightly
// longer WriteTimeout so the handler can return its timeout response.
func RequestTimeout(next http.Handler, timeout time.Duration) http.Handler {
	if timeout <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
