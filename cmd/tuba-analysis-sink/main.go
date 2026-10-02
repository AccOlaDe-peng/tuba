package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysisworker"
	"tuba/product/internal/config"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
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
	// During the v1→v2 migration window the sink consumes both result topics
	// with the same consumer group: v1 messages keep their legacy anomaly
	// write and are mirrored through the v2 object machinery; v2 messages are
	// multi-object writes with external revision semantics (F07).
	topics := []string{c.AnalysisTopic}
	if c.AnalysisV2Topic != "" && c.AnalysisV2Topic != c.AnalysisTopic {
		topics = append(topics, c.AnalysisV2Topic)
	}
	group := "tuba-analysis-sink-" + c.Namespace
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
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("ANALYSIS_SINK_METRICS_LISTEN", "127.0.0.1:19094")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := telemetry.Serve(ctx, metricsListen, metrics); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	es := sink.New(c.ESURL, c.ESAPIKey, c.Namespace)
	errs := make(chan error, len(topics))
	for _, topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{Dialer: dialer, Brokers: c.Brokers, Topic: topic, GroupID: group, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6})
		defer reader.Close()
		worker := analysisworker.Worker{Organization: c.Organization, Namespace: c.Namespace, Consumer: reader, DeadLetter: dlq, Sink: es, Metrics: metrics}
		log.Printf("analysis sink consuming %s (group %s)", topic, group)
		go func() { errs <- worker.Run(ctx) }()
	}
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	if err := <-errs; err != nil {
		log.Fatal(err)
	}
}
