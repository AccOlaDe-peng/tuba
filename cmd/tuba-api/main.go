package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"tuba/product/internal/api"
	"tuba/product/internal/auth"
	"tuba/product/internal/config"
	"tuba/product/internal/control"
	"tuba/product/internal/es"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/telemetry"
)

func main() {
	requestTimeout, err := config.HTTPRequestTimeout()
	if err != nil {
		log.Fatal(err)
	}
	listen, err := config.APIListenAddress()
	if err != nil {
		log.Fatal(err)
	}
	verifier, err := auth.NewVerifier(os.Getenv("OIDC_ISSUER"), os.Getenv("OIDC_AUDIENCE"), os.Getenv("OIDC_JWKS_URL"))
	if err != nil {
		log.Fatal(err)
	}
	store, err := control.Open(context.Background(), os.Getenv("DATABASE_URL"), verifier.Issuer)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	client, err := es.New(os.Getenv("ES_URL"), os.Getenv("ES_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: listen, Handler: api.Server{Verifier: verifier, Authorizer: store, ES: client, Control: store, StartedAt: time.Now().UTC(), RequestTimeout: requestTimeout, Metrics: telemetry.New(), KafkaConfigured: os.Getenv("KAFKA_BROKERS") != ""}.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: requestTimeout, WriteTimeout: requestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	log.Printf("TUBA API listening on %s", listen)
	if err := lifecycle.ServeHTTP(ctx, server, 10*time.Second); err != nil {
		log.Fatal(err)
	}
}
