package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/kafkaadmin"
	"tuba/product/internal/kafkautil"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "tuba-topic-admin:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("tuba-topic-admin", flag.ContinueOnError)
	contractPath := flags.String("contract", "", "topic contract JSON file (defaults to the package or current workspace)")
	profile := flags.String("profile", "zeek_validation_single_node", "bounded runtime profile from the topic contract")
	namespace := flags.String("namespace", "", "TUBA topic namespace")
	apply := flags.Bool("apply", false, "create missing topics and verify existing topic configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *namespace == "" {
		return errors.New("--namespace is required")
	}
	resolvedContractPath, err := findContractPath(*contractPath)
	if err != nil {
		return err
	}
	contractJSON, err := os.ReadFile(resolvedContractPath)
	if err != nil {
		return fmt.Errorf("read topic contract: %w", err)
	}
	plan, err := kafkaadmin.BuildPlan(contractJSON, *profile, *namespace)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	if !*apply {
		return nil
	}
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		if brokers[i] == "" {
			return errors.New("KAFKA_BROKERS must contain broker addresses")
		}
	}
	transport, err := (kafkautil.Config{
		Protocol: os.Getenv("KAFKA_SECURITY_PROTOCOL"), Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD"),
		CAFile: os.Getenv("KAFKA_TLS_CA_FILE"), CertFile: os.Getenv("KAFKA_TLS_CERT_FILE"),
		KeyFile: os.Getenv("KAFKA_TLS_KEY_FILE"), ServerName: os.Getenv("KAFKA_TLS_SERVER_NAME"),
	}).Transport()
	if err != nil {
		return fmt.Errorf("configure Kafka admin transport: %w", err)
	}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second, Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := kafkaadmin.Apply(ctx, client, plan); err != nil {
		return err
	}
	fmt.Printf("created or verified %d Kafka topics in namespace %s using %s\n", len(plan.Topics), plan.Namespace, plan.Profile)
	return nil
}

func findContractPath(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	candidates := []string{"contracts/events/topics.v1.json"}
	if executable, err := os.Executable(); err == nil {
		candidates = append([]string{
			filepath.Clean(filepath.Join(filepath.Dir(executable), "..", "contracts", "events", "topics.v1.json")),
		}, candidates...)
	}
	if workingDir, err := os.Getwd(); err == nil {
		for dir := workingDir; ; dir = filepath.Dir(dir) {
			candidates = append(candidates, filepath.Join(dir, "contracts", "events", "topics.v1.json"))
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
		}
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("topic contract not found; pass --contract (checked %s)", strings.Join(candidates, ", "))
}
