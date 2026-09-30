package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/sourceadapter"
	"tuba/product/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	configPath := os.Getenv("SOURCE_ADAPTER_CONFIG")
	if configPath == "" {
		return errors.New("SOURCE_ADAPTER_CONFIG is required")
	}
	file, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("open source adapter config: %w", err)
	}
	defer file.Close()
	configBytes, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return fmt.Errorf("read source adapter config: %w", err)
	}
	if len(configBytes) > 1<<20 {
		return errors.New("source adapter config exceeds 1 MiB")
	}
	var cfg sourceadapter.Config
	decoder := json.NewDecoder(bytes.NewReader(configBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return fmt.Errorf("decode source adapter config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("source adapter config must contain exactly one JSON object")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate source adapter config: %w", err)
	}
	adapterToken := os.Getenv("SOURCE_ADAPTER_TOKEN")
	if len(adapterToken) < 32 {
		return errors.New("SOURCE_ADAPTER_TOKEN must contain at least 32 characters")
	}

	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		if brokers[i] == "" {
			return errors.New("KAFKA_BROKERS must contain one or more broker addresses")
		}
	}
	kc := kafkautil.Config{
		Protocol: os.Getenv("KAFKA_SECURITY_PROTOCOL"), Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD"),
		CAFile: os.Getenv("KAFKA_TLS_CA_FILE"), CertFile: os.Getenv("KAFKA_TLS_CERT_FILE"),
		KeyFile: os.Getenv("KAFKA_TLS_KEY_FILE"), ServerName: os.Getenv("KAFKA_TLS_SERVER_NAME"),
	}
	dialer, err := kc.Dialer()
	if err != nil {
		return fmt.Errorf("configure Kafka reader: %w", err)
	}
	transport, err := kc.Transport()
	if err != nil {
		return fmt.Errorf("configure Kafka writer: %w", err)
	}
	dlqTopic := os.Getenv("KAFKA_SOURCE_ADAPTER_DLQ_TOPIC")
	if dlqTopic == "" {
		dlqTopic = "tuba.source-adapter.dlq.v1"
	}
	dlq := &kafka.Writer{Transport: transport, Addr: kafka.TCP(brokers...), Topic: dlqTopic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false}
	defer dlq.Close()
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("SOURCE_ADAPTER_METRICS_LISTEN", "127.0.0.1:9185")
	if err != nil {
		return err
	}
	metricsErr := make(chan error, 1)
	go func() { metricsErr <- telemetry.Serve(ctx, metricsListen, metrics) }()
	log.Printf("source adapter metrics listening at %s", metricsListen)
	errCh := make(chan error, len(cfg.Bindings))
	readers := make([]*kafka.Reader, 0, len(cfg.Bindings))
	for _, binding := range cfg.Bindings {
		groupSuffix := os.Getenv("SOURCE_ADAPTER_CONSUMER_GROUP_SUFFIX")
		if !validGroupSuffix(groupSuffix) {
			return errors.New("SOURCE_ADAPTER_CONSUMER_GROUP_SUFFIX must contain only letters, numbers, underscore or hyphen")
		}
		newReader := func() *kafka.Reader {
			return kafka.NewReader(kafka.ReaderConfig{
				Dialer: dialer, Brokers: brokers, Topic: binding.Topic,
				GroupID:        sourceadapter.GroupID(binding.Topic, groupSuffix),
				CommitInterval: 0, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 2 << 20,
			})
		}
		reader := newReader()
		readers = append(readers, reader)
		adapter := sourceadapter.Adapter{
			Binding: binding, IngestURL: cfg.IngestURL, AdapterToken: adapterToken,
			Consumer: reader, DeadLetter: dlq, HTTPClient: client, Metrics: metrics,
			Probe: sourceadapter.TopicProber{
				Topic:          binding.Topic,
				ReadPartitions: kafkaPartitionsFunc(dialer, brokers),
			},
			NewConsumer: func(context.Context) (sourceadapter.Consumer, error) {
				fresh := newReader()
				log.Printf("source adapter topic=%s re-creating Kafka reader after topic deletion", binding.Topic)
				return fresh, nil
			},
		}
		go func(topic string, worker sourceadapter.Adapter) {
			log.Printf("source adapter consuming topic=%s", topic)
			errCh <- worker.Run(ctx)
		}(binding.Topic, adapter)
	}
	metrics.SetReady(true)
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()
	select {
	case <-ctx.Done():
		metrics.SetReady(false)
		return nil
	case err := <-metricsErr:
		metrics.SetReady(false)
		cancel()
		if err != nil {
			return fmt.Errorf("serve source adapter metrics: %w", err)
		}
		return nil
	case err := <-errCh:
		metrics.SetReady(false)
		cancel()
		if err != nil {
			return err
		}
		return nil
	}
}

// kafkaPartitionsFunc reads partition metadata for a topic, trying each broker
// until one answers. A definitive UnknownTopicOrPartition stops the loop early
// because that answer, not reachability, is what the prober acts on.
func kafkaPartitionsFunc(dialer *kafka.Dialer, brokers []string) func(context.Context, string) ([]kafka.Partition, error) {
	return func(ctx context.Context, topic string) ([]kafka.Partition, error) {
		var lastErr error
		for _, broker := range brokers {
			conn, err := dialer.DialContext(ctx, "tcp", broker)
			if err != nil {
				lastErr = err
				continue
			}
			partitions, err := conn.ReadPartitions(topic)
			_ = conn.Close()
			if err == nil {
				return partitions, nil
			}
			lastErr = err
			if errors.Is(err, kafka.UnknownTopicOrPartition) {
				return nil, err
			}
		}
		return nil, lastErr
	}
}

func validGroupSuffix(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
