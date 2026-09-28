package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/config"
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
	log.Printf("control worker running job lease maintenance and PostgreSQL outbox publisher")
	errCh := make(chan error, 2)
	go func() {
		errCh <- (controlworker.OutboxPublisher{Config: controlworker.OutboxConfig{
			Pool: pool, Writer: writer, WorkerID: workerID, BatchSize: 10,
			PollInterval: 500 * time.Millisecond, LeaseDuration: 2 * time.Minute,
			PublishTimeout: 10 * time.Second,
		}}).Run(runCtx)
	}()
	go func() {
		errCh <- (controlworker.JobWorker{Config: controlworker.JobWorkerConfig{
			Pool: pool, WorkerID: workerID, Handlers: map[string]controlworker.JobHandler{},
			PollInterval: time.Second, LeaseDuration: 30 * time.Second,
		}}).Run(runCtx)
	}()
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
	secondErr := <-errCh
	if firstErr != nil {
		log.Printf("control worker stopped: %v", firstErr)
	}
	if secondErr != nil {
		log.Printf("control worker stopped: %v", secondErr)
	}
}
