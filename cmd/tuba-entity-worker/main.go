package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
	"tuba/product/internal/entity"
	"tuba/product/internal/entityworker"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/lifecycle"
	"tuba/product/internal/pgutil"
	"tuba/product/internal/telemetry"
)

var domains = []string{"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"}

func main() {
	c, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	if c.DatabaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	accountSpace, deviceSpace := os.Getenv("ENTITY_ACCOUNT_SPACE"), os.Getenv("ENTITY_DEVICE_SPACE")
	if accountSpace == "" || deviceSpace == "" {
		log.Fatal("ENTITY_ACCOUNT_SPACE and ENTITY_DEVICE_SPACE are required")
	}
	// Boot-time space registration is opt-in: when the kind env var is set
	// the worker ensures the space exists (idempotent, kind conflict is
	// fatal). When unset, spaces are assumed pre-registered by the operator.
	spaceKind := func(env string) (entity.SpaceKind, bool) {
		kind := entity.SpaceKind(os.Getenv(env))
		if kind == "" {
			return "", false
		}
		switch kind {
		case entity.SpaceActiveDirectory, entity.SpaceLocalAccounts, entity.SpaceCloudDirectory, entity.SpaceCustom:
			return kind, true
		}
		log.Fatalf("%s=%q is not a valid identity space kind", env, kind)
		return "", false
	}
	accountSpaceKind, accountKindSet := spaceKind("ENTITY_ACCOUNT_SPACE_KIND")
	deviceSpaceKind, deviceKindSet := spaceKind("ENTITY_DEVICE_SPACE_KIND")
	kc := kafkautil.Config{Protocol: c.KafkaProtocol, Mechanism: c.KafkaSASLMechanism, Username: c.KafkaUsername, Password: c.KafkaPassword, CAFile: c.KafkaCAFile, CertFile: c.KafkaCertFile, KeyFile: c.KafkaKeyFile, ServerName: c.KafkaServerName}
	dialer, err := kc.Dialer()
	if err != nil {
		log.Fatal(err)
	}
	transport, err := kc.Transport()
	if err != nil {
		log.Fatal(err)
	}
	pool, err := pgutil.NewPool(context.Background(), c.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = pool.Ping(pingCtx)
	pingCancel()
	if err != nil {
		log.Fatal(err)
	}
	writer := &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Topic: c.AttributedTopic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchTimeout: 10 * time.Millisecond, Async: false}
	defer writer.Close()

	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("ENTITY_WORKER_METRICS_LISTEN", "127.0.0.1:19110")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := telemetry.Serve(runCtx, metricsListen, metrics); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	var processor entityworker.Processor = entityworker.AttributionProcessor{Attributor: entity.NewAttributor(pool), AccountSpace: accountSpace, DeviceSpace: deviceSpace}
	if os.Getenv("ENTITY_REGISTER_ON_SIGHT") != "false" {
		registry := entity.NewRegistry(pool)
		bootCtx, bootCancel := context.WithTimeout(context.Background(), 15*time.Second)
		if accountKindSet {
			if _, err := registry.RegisterSpace(bootCtx, c.Organization, accountSpace, accountSpaceKind); err != nil {
				bootCancel()
				log.Fatalf("register account identity space: %v", err)
			}
		}
		if deviceKindSet {
			if _, err := registry.RegisterSpace(bootCtx, c.Organization, deviceSpace, deviceSpaceKind); err != nil {
				bootCancel()
				log.Fatalf("register device identity space: %v", err)
			}
		}
		bootCancel()
		processor = entityworker.NewRegisteringProcessor(registry, entity.NewAttributor(pool), accountSpace, deviceSpace, metrics)
		log.Printf("register-on-sight enabled (account space %s, device space %s)", accountSpace, deviceSpace)
	}
	log.Printf("entity worker consuming %s.<domain>.v1, publishing %s (account space %s, device space %s)", c.EventsTopicPrefix, c.AttributedTopic, accountSpace, deviceSpace)
	failures := make(chan error, len(domains))
	var wg sync.WaitGroup
	for _, domain := range domains {
		reader := kafka.NewReader(kafka.ReaderConfig{Dialer: dialer, Brokers: c.Brokers, Topic: c.EventsTopicPrefix + "." + domain + ".v1", GroupID: c.ConsumerGroup("tuba-entity-worker-" + domain), CommitInterval: 0, MinBytes: 1, MaxBytes: 10e6, IsolationLevel: kafka.ReadCommitted})
		defer reader.Close()
		worker := entityworker.Worker{Organization: c.Organization, Domain: domain, Consumer: reader, Processor: processor, Output: writer}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.Run(runCtx); err != nil {
				failures <- err
				cancel()
			}
		}()
	}
	metrics.SetReady(true)
	defer metrics.SetReady(false)
	wg.Wait()
	select {
	case err := <-failures:
		log.Fatal(err)
	default:
	}
}
