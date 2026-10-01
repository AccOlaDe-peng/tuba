package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/sink"
	"tuba/product/internal/standardindexer"
	"tuba/product/internal/telemetry"
)

var domains = []string{"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"}

func main() {
	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	if c.ESURL == "" || c.ESAPIKey == "" {
		log.Fatal("ES_URL and ES_API_KEY are required")
	}
	kc := kafkautil.Config{Protocol: c.KafkaProtocol, Mechanism: c.KafkaSASLMechanism, Username: c.KafkaUsername, Password: c.KafkaPassword, CAFile: c.KafkaCAFile, CertFile: c.KafkaCertFile, KeyFile: c.KafkaKeyFile, ServerName: c.KafkaServerName}
	dialer, err := kc.Dialer()
	if err != nil {
		log.Fatal(err)
	}
	transport, err := kc.Transport()
	if err != nil {
		log.Fatal(err)
	}
	dlq := &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Topic: c.DeadLetterTopic, RequiredAcks: kafka.RequireAll, Async: false}
	defer dlq.Close()
	indexSink := sink.New(c.ESURL, c.ESAPIKey, c.Namespace)
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("STANDARD_INDEXER_METRICS_LISTEN", "127.0.0.1:19097")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := telemetry.Serve(ctx, metricsListen, metrics); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	var workers sync.WaitGroup
	errCh := make(chan error, len(domains))
	for _, domain := range domains {
		domain := domain
		reader := kafka.NewReader(kafka.ReaderConfig{Dialer: dialer, Brokers: c.Brokers, Topic: fmt.Sprintf("%s.%s.v1", c.EventsTopicPrefix, domain), GroupID: c.ConsumerGroup("tuba-standard-indexer-" + domain), CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6})
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer reader.Close()
			if err := (standardindexer.Worker{Domain: domain, Organization: c.Organization, Consumer: reader, DeadLetter: dlq, Sink: indexSink, BatchSize: c.IndexBatchSize, MaxBatchBytes: c.IndexBatchBytes, BatchWait: c.IndexBatchWait, MaxAttempts: c.IndexMaxAttempts, RetryBackoff: c.IndexRetryBackoff, Metrics: metrics}).Run(ctx); err != nil {
				errCh <- fmt.Errorf("%s indexer: %w", domain, err)
				stop()
			}
		}()
	}
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			log.Print(err)
		}
	}
	workers.Wait()
	if len(errCh) > 0 {
		log.Fatal(<-errCh)
	}
}
