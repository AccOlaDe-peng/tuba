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
	"tuba/product/internal/kafkarevoke"
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
	revoker, err := kafkaWriteRevoker()
	if err != nil {
		log.Fatal(err)
	}
	store.WriteRevoker = revoker
	client, err := es.New(os.Getenv("ES_URL"), os.Getenv("ES_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: listen, Handler: api.Server{Verifier: verifier, Authorizer: store, ES: client, Control: store, StartedAt: time.Now().UTC(), RequestTimeout: requestTimeout, Metrics: telemetry.New(), KafkaConfigured: os.Getenv("KAFKA_BROKERS") != "", ReleaseRoot: os.Getenv("TUBA_RELEASE_ROOT")}.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: requestTimeout, WriteTimeout: requestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	log.Printf("TUBA API listening on %s", listen)
	if err := lifecycle.ServeHTTP(ctx, server, 10*time.Second); err != nil {
		log.Fatal(err)
	}
}

// kafkaWriteRevoker wires the broker-side enforcement of collector disable.
// The admin credential lives in a root-managed Java-properties file (the same
// shape kafka-acls.sh --command-config uses) rather than the Launcher manifest:
// the supervisor resolves service environment once at its own start, so a
// manifest env change would need a manifest-wide restart, while a file is
// picked up by an ordinary api process restart. When the file is absent the
// API still runs, but disabling a collector that has source bindings fails
// loudly instead of silently leaving Kafka write access in place.
func kafkaWriteRevoker() (control.SourceWriteRevoker, error) {
	path := os.Getenv("TUBA_KAFKA_ADMIN_PROPERTIES")
	if path == "" {
		path = defaultKafkaAdminProperties
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			log.Printf("Kafka admin properties %s not present; collector disable cannot revoke source write ACLs", path)
			return nil, nil
		}
		return nil, err
	}
	return kafkarevoke.FromPropertiesFile(path)
}
