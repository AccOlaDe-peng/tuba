package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"tuba/product/internal/config"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/webserver"
)

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func main() {
	listen := envOr("WEB_LISTEN", "127.0.0.1:8088")
	if err := config.ValidateListenerAddress("WEB_LISTEN", listen, os.Getenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN")); err != nil {
		log.Fatalf("WEB_LISTEN: %v", err)
	}
	issuer := os.Getenv("OIDC_ISSUER")
	if issuer == "" {
		log.Fatal("OIDC_ISSUER is required")
	}
	app, err := webserver.New(webserver.Config{
		Root: envOr("WEB_ROOT", webserver.DefaultRoot()), Issuer: issuer,
		ClientID:  envOr("OIDC_CLIENT_ID", "tuba-web"),
		APIURL:    envOr("API_UPSTREAM", "http://127.0.0.1:8788"),
		IngestURL: envOr("INGEST_UPSTREAM", "http://127.0.0.1:8080"),
	})
	if err != nil {
		log.Fatalf("web configuration: %v", err)
	}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	server := &http.Server{
		Addr: listen, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 40 * time.Second, IdleTimeout: 60 * time.Second,
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("web listener: %v", err)
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	log.Printf("web gateway listening on %s", listen)
	select {
	case err := <-serveResult:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("web server: %v", err)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			<-serveResult
			log.Fatalf("web server shutdown: %v", err)
		}
		if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("web server shutdown: %v", err)
		}
	}
}
