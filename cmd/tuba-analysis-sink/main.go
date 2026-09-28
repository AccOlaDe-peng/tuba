package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/analysisworker"
	"tuba/product/internal/config"
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
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers, Topic: c.AnalysisTopic, GroupID: "tuba-analysis-sink-" + c.Namespace, CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6})
	defer reader.Close()
	dlq := &kafka.Writer{Addr: kafka.TCP(c.Brokers...), Topic: c.DeadLetterTopic, RequiredAcks: kafka.RequireAll, Async: false}
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
	log.Printf("analysis sink consuming %s", c.AnalysisTopic)
	worker := analysisworker.Worker{Organization: c.Organization, Namespace: c.Namespace, Consumer: reader, DeadLetter: dlq, Sink: sink.New(c.ESURL, c.ESAPIKey, c.Namespace), Metrics: metrics}
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
