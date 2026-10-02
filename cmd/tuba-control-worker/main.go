package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
	"tuba/product/internal/control"
	"tuba/product/internal/es"
	"tuba/product/internal/controlworker"
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
		log.Fatal("DATABASE_URL is required")
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
	writer := &kafka.Writer{Transport: transport, Addr: kafka.TCP(c.Brokers...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, Async: false, BatchTimeout: 10 * time.Millisecond}
	defer writer.Close()
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "tuba-control-worker"
	}
	workerID := fmt.Sprintf("%s-%d", host, os.Getpid())
	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := telemetry.New()
	metricsListen, err := telemetry.ListenAddress("CONTROL_WORKER_METRICS_LISTEN", "127.0.0.1:19099")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := telemetry.Serve(runCtx, metricsListen, metrics); err != nil {
			log.Fatalf("metrics server: %v", err)
		}
	}()
	log.Printf("control worker running job lease maintenance, PostgreSQL outbox publisher, and retention cleaner")
	store := &control.Store{Pool: pool}
	handlers := map[string]controlworker.JobHandler{}
	exportDir := os.Getenv("TUBA_EXPORT_DIR")
	if exportDir != "" {
		// Q03 export executor: fail closed at startup when the controlled
		// export directory is configured but the event store is not, so an
		// accepted export can never be stranded without a runner.
		esClient, err := es.New(c.ESURL, c.ESAPIKey)
		if err != nil {
			log.Fatalf("TUBA_EXPORT_DIR is set but the event store is not configured: %v", err)
		}
		handlers["export"] = controlworker.ExportExecutor{Config: controlworker.ExportExecutorConfig{
			Store: store, ES: esClient, ExportRoot: exportDir, WorkerID: workerID,
		}}.Handler()
		log.Printf("export executor registered (root %s)", exportDir)
	}
	runners := []func(context.Context) error{
		func(ctx context.Context) error {
			return (controlworker.OutboxPublisher{Config: controlworker.OutboxConfig{
				Pool: pool, Writer: writer, WorkerID: workerID, Metrics: metrics, BatchSize: 10,
				PollInterval: 500 * time.Millisecond, LeaseDuration: 2 * time.Minute,
				PublishTimeout: 10 * time.Second, MaxAttempts: 8, MaxConcurrentKeys: 4,
				StuckThreshold: 5 * time.Minute,
			}}).Run(ctx)
		},
		func(ctx context.Context) error {
			return (controlworker.JobWorker{Config: controlworker.JobWorkerConfig{
				Pool: pool, WorkerID: workerID, Handlers: handlers,
				PollInterval: time.Second, LeaseDuration: 30 * time.Second,
			}}).Run(ctx)
		},
		func(ctx context.Context) error {
			return (controlworker.RetentionCleaner{Config: controlworker.RetentionConfig{
				Pool: pool, Metrics: metrics,
				InboxRetention:        envDuration("CONTROL_WORKER_INBOX_RETENTION", 48*time.Hour),
				OutboxRetention:       envDuration("CONTROL_WORKER_OUTBOX_RETENTION", 48*time.Hour),
				SucceededJobRetention: envDuration("CONTROL_WORKER_SUCCEEDED_JOB_RETENTION", 7*24*time.Hour),
				FailedJobRetention:    envDuration("CONTROL_WORKER_FAILED_JOB_RETENTION", 30*24*time.Hour),
				PollInterval:          envDuration("CONTROL_WORKER_RETENTION_POLL_INTERVAL", time.Hour),
				TempRoot:              os.Getenv("CONTROL_WORKER_JOB_TEMP_ROOT"),
			}}).Run(ctx)
		},
	}
	if exportDir != "" {
		runners = append(runners, func(ctx context.Context) error {
			return (controlworker.ExportExpirer{Config: controlworker.ExportExpirerConfig{
				Store:        store,
				ExportRoot:   exportDir,
				PollInterval: envDuration("CONTROL_WORKER_EXPORT_SWEEP_INTERVAL", time.Minute),
			}}).Run(ctx)
		})
	}
	errCh := make(chan error, len(runners))
	for _, run := range runners {
		go func() { errCh <- run(runCtx) }()
	}
	probeDependencies := func() error {
		probeCtx, probeCancel := context.WithTimeout(runCtx, 10*time.Second)
		defer probeCancel()
		return controlworker.ProbeDependencies(probeCtx, pool, dialer, c.Brokers)
	}
	wasReady, hasProbeResult := false, false
	probe := func() {
		if err := probeDependencies(); err != nil {
			metrics.SetReady(false)
			if !hasProbeResult || wasReady {
				log.Printf("control worker dependencies unavailable: %v", err)
			}
			hasProbeResult, wasReady = true, false
			return
		}
		metrics.SetReady(true)
		if !hasProbeResult || !wasReady {
			log.Print("control worker dependencies ready")
		}
		hasProbeResult, wasReady = true, true
	}
	probe()
	probeTicker := time.NewTicker(5 * time.Second)
	defer probeTicker.Stop()
	var firstErr error
	gotWorkerResult := false
	for !gotWorkerResult {
		select {
		case firstErr = <-errCh:
			gotWorkerResult = true
		case <-runCtx.Done():
			firstErr = <-errCh
			gotWorkerResult = true
		case <-probeTicker.C:
			probe()
		}
	}
	metrics.SetReady(false)
	cancel()
	for i := 1; i < len(runners); i++ {
		if err := <-errCh; err != nil {
			log.Printf("control worker stopped (%d): %v", i+1, err)
		}
	}
	if firstErr != nil {
		log.Printf("control worker stopped: %v", firstErr)
	}
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		log.Printf("invalid %s %q; using %s", name, value, fallback)
		return fallback
	}
	return parsed
}
