package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
	"tuba/product/internal/ingest"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/pgutil"
	"tuba/product/internal/telemetry"
)

func main() {
	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	if c.DatabaseURL == "" {
		log.Fatal("configure DATABASE_URL for registered source credentials")
	}
	kc := kafkautil.Config{Protocol: c.KafkaProtocol, Mechanism: c.KafkaSASLMechanism, Username: c.KafkaUsername, Password: c.KafkaPassword, CAFile: c.KafkaCAFile, CertFile: c.KafkaCertFile, KeyFile: c.KafkaKeyFile, ServerName: c.KafkaServerName}
	transport, err := kc.Transport()
	if err != nil {
		log.Fatal(err)
	}
	dialer, err := kc.Dialer()
	if err != nil {
		log.Fatal(err)
	}
	metrics := telemetry.New()
	rawWriter := &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Topic: c.RawTopic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, BatchTimeout: 10 * time.Millisecond}
	defer rawWriter.Close()
	var rawReceipts ingest.RawReceiptStore
	var sourceResolver ingest.SourceResolver
	var topicResolver ingest.TopicSourceResolver
	var sourceLimiters ingest.SourceLimiters
	var pool *pgxpool.Pool
	if c.DatabaseURL != "" {
		createdPool, poolErr := pgutil.NewPool(context.Background(), c.DatabaseURL)
		if poolErr == nil {
			pool = createdPool
			rawReceipts = ingest.PostgresRawReceipts{Pool: pool}
			resolver := ingest.PostgresSourceResolver{Pool: pool}
			sourceResolver = resolver
			topicResolver = resolver
			defer pool.Close()
		} else {
			log.Printf("raw receipt store unavailable; raw ingestion will return 503")
		}
	}
	if sourceResolver == nil || rawReceipts == nil {
		log.Fatal("database-backed source registry and raw receipt store are required")
	}
	readyCheck := func(ctx context.Context) error {
		if pool == nil {
			return http.ErrServerClosed
		}
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		conn, err := dialer.DialContext(ctx, "tcp", c.Brokers[0])
		if err != nil {
			return err
		}
		return conn.Close()
	}
	server := &http.Server{Addr: c.Listen, Handler: ingest.Server{RawProducer: rawWriter, RawTopic: c.RawTopic, SourceResolver: sourceResolver, TopicResolver: topicResolver, AdapterToken: os.Getenv("SOURCE_ADAPTER_TOKEN"), RawReceipts: rawReceipts, Limiter: ingest.NewLimiter(c.IngestRate, c.IngestBurst), SourceLimiters: &sourceLimiters, Metrics: metrics, ReadyCheck: readyCheck, RequestTimeout: c.HTTPRequestTimeout}.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: c.HTTPRequestTimeout, WriteTimeout: c.HTTPRequestTimeout + 5*time.Second, IdleTimeout: 60 * time.Second}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	log.Printf("ingest listening on %s", c.Listen)
	if err := lifecycle.ServeHTTP(ctx, server, 10*time.Second); err != nil {
		log.Fatal(err)
	}
}
