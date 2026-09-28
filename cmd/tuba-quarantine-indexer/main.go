package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/quarantineindexer"
	"tuba/product/internal/sink"
	"tuba/product/internal/telemetry"
)

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
	reader := kafka.NewReader(kafka.ReaderConfig{Dialer: dialer, Brokers: c.Brokers, Topic: c.QuarantineTopic, GroupID: c.ConsumerGroup("tuba-quarantine-indexer"), CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6})
	defer reader.Close()
	dlq := &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Topic: c.DeadLetterTopic, RequiredAcks: kafka.RequireAll, Async: false}
	defer dlq.Close()
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("QUARANTINE_INDEXER_METRICS_LISTEN", "127.0.0.1:19098")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := telemetry.Serve(ctx, metricsListen, metrics); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	log.Printf("quarantine indexer consuming %s", c.QuarantineTopic)
	worker := quarantineindexer.Worker{Organization: c.Organization, Namespace: c.Namespace, Consumer: reader, DeadLetter: dlq, Sink: sink.New(c.ESURL, c.ESAPIKey, c.Namespace)}
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
