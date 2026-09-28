package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/normalizer"
	"tuba/product/internal/telemetry"
)

var domains = []string{"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"}

func main() {
	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
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
	reader := kafka.NewReader(kafka.ReaderConfig{Dialer: dialer, Brokers: c.Brokers, Topic: c.RawTopic, GroupID: c.ConsumerGroup("tuba-normalizer"), CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, IsolationLevel: kafka.ReadCommitted})
	defer reader.Close()
	writers := make(map[string]normalizer.Writer, len(domains))
	for _, domain := range domains {
		writers[domain] = &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Topic: c.EventsTopicPrefix + "." + domain + ".v1", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 10 * time.Millisecond, Async: false}
	}
	quarantine := &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Topic: c.QuarantineTopic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 10 * time.Millisecond, Async: false}
	defer quarantine.Close()
	for _, output := range writers {
		if closer, ok := output.(*kafka.Writer); ok {
			defer closer.Close()
		}
	}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("NORMALIZER_METRICS_LISTEN", "127.0.0.1:19096")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := telemetry.Serve(ctx, metricsListen, metrics); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	log.Printf("normalizer consuming %s", c.RawTopic)
	worker := normalizer.Worker{Organization: c.Organization, Namespace: c.Namespace, Consumer: reader, DomainWriters: writers, QuarantineWriter: quarantine}
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
