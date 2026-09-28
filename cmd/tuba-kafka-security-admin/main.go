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
		fmt.Fprintln(os.Stderr, "tuba-kafka-security-admin:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("tuba-kafka-security-admin", flag.ContinueOnError)
	contractPath := flags.String("contract", "", "topic contract JSON (defaults to package or current workspace)")
	profile := flags.String("profile", "zeek_validation_single_node", "bounded validation profile")
	namespace := flags.String("namespace", "", "TUBA topic namespace")
	groupSuffix := flags.String("group-suffix", "", "optional consumer group isolation suffix")
	apply := flags.Bool("apply", false, "upsert service SCRAM credentials and exact literal ACLs")
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
	plan, err := kafkaadmin.BuildSecurityPlan(contractJSON, *profile, *namespace, *groupSuffix)
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
	if len(brokers) != 1 || strings.TrimSpace(brokers[0]) == "" {
		return errors.New("single-node security apply requires exactly one KAFKA_BROKERS address")
	}
	brokers[0] = strings.TrimSpace(brokers[0])
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
	client := &kafka.Client{Addr: kafka.TCP(brokers[0]), Timeout: 10 * time.Second, Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := verifyAuthorizer(ctx, client, plan); err != nil {
		return err
	}
	if err := applySCRAM(ctx, client, plan); err != nil {
		return err
	}
	if err := applyACLs(ctx, client, plan); err != nil {
		return err
	}
	fmt.Printf("Kafka service security reconciled: principals=%d literal_acls=%d namespace=%s\n", len(plan.Principals), len(plan.ACLs), plan.Namespace)
	return nil
}

func findContractPath(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	candidates := []string{"contracts/events/topics.v1.json"}
	if executable, err := os.Executable(); err == nil {
		candidates = append([]string{filepath.Clean(filepath.Join(filepath.Dir(executable), "..", "contracts", "events", "topics.v1.json"))}, candidates...)
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
	for _, path := range candidates {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return path, nil
		}
	}
	return "", fmt.Errorf("topic contract not found; pass --contract (checked %s)", strings.Join(candidates, ", "))
}

func verifyAuthorizer(ctx context.Context, client *kafka.Client, plan kafkaadmin.SecurityPlan) error {
	response, err := client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{Filter: kafka.ACLFilter{
		ResourceTypeFilter: kafka.ResourceTypeTopic, Operation: kafka.ACLOperationTypeAny,
		ResourcePatternTypeFilter: kafka.PatternTypeAny, PermissionType: kafka.ACLPermissionTypeAny,
	}})
	if err != nil || response.Error != nil {
		return errors.New("Kafka ACL authorizer unavailable; no SCRAM credentials or ACLs were changed")
	}
	return nil
}

func applySCRAM(ctx context.Context, client *kafka.Client, plan kafkaadmin.SecurityPlan) error {
	upsertions := make([]kafka.UserScramCredentialsUpsertion, 0, len(plan.Principals))
	for _, principal := range plan.Principals {
		usernameEnv := kafkaadmin.SecurityUsernameEnv(principal.Role)
		configured := os.Getenv(usernameEnv)
		if configured == "" {
			return fmt.Errorf("missing required environment variable %s", usernameEnv)
		}
		if configured != principal.Username {
			return fmt.Errorf("%s does not match the username fixed by the selected profile", usernameEnv)
		}
		password := os.Getenv(kafkaadmin.SecurityPasswordEnv(principal.Role))
		if password == "" {
			return fmt.Errorf("missing required secret environment variable %s", kafkaadmin.SecurityPasswordEnv(principal.Role))
		}
		salt, saltedPassword, err := kafkaadmin.GenerateSaltedPassword(password)
		if err != nil {
			return fmt.Errorf("invalid service credential for role %s: %w", principal.Role, err)
		}
		upsertions = append(upsertions, kafka.UserScramCredentialsUpsertion{Name: principal.Username,
			Mechanism: kafka.ScramMechanismSha512, Iterations: 4096, Salt: salt, SaltedPassword: saltedPassword})
	}
	response, err := client.AlterUserScramCredentials(ctx, &kafka.AlterUserScramCredentialsRequest{Addr: client.Addr, Upsertions: upsertions})
	if err != nil {
		return errors.New("upsert service SCRAM credentials failed")
	}
	if len(response.Results) != len(upsertions) {
		return errors.New("Kafka returned an incomplete SCRAM credential result")
	}
	for _, result := range response.Results {
		if result.Error != nil {
			return fmt.Errorf("upsert SCRAM credentials for service user %s failed", result.User)
		}
	}
	return nil
}

func applyACLs(ctx context.Context, client *kafka.Client, plan kafkaadmin.SecurityPlan) error {
	entries := make([]kafka.ACLEntry, 0, len(plan.ACLs))
	for _, item := range plan.ACLs {
		resourceType := kafka.ResourceTypeTopic
		if item.ResourceType == "group" {
			resourceType = kafka.ResourceTypeGroup
		}
		operation, ok := kafkaOperation(item.Operation)
		if !ok {
			return fmt.Errorf("unsupported ACL operation %q in security plan", item.Operation)
		}
		entries = append(entries, kafka.ACLEntry{ResourceType: resourceType, ResourceName: item.ResourceName,
			ResourcePatternType: kafka.PatternTypeLiteral, Principal: item.Principal, Host: "*",
			Operation: operation, PermissionType: kafka.ACLPermissionTypeAllow})
	}
	response, err := client.CreateACLs(ctx, &kafka.CreateACLsRequest{ACLs: entries})
	if err != nil || len(response.Errors) != len(entries) {
		return errors.New("create Kafka service ACLs failed; reconcile may be partial")
	}
	for _, item := range response.Errors {
		if item != nil {
			return errors.New("create Kafka service ACLs failed; reconcile may be partial")
		}
	}
	for _, entry := range entries {
		response, err := client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{Filter: kafka.ACLFilter{
			ResourceTypeFilter: entry.ResourceType, ResourceNameFilter: entry.ResourceName,
			ResourcePatternTypeFilter: kafka.PatternTypeLiteral, PrincipalFilter: entry.Principal,
			HostFilter: "*", Operation: entry.Operation, PermissionType: kafka.ACLPermissionTypeAllow,
		}})
		if err != nil || response.Error != nil || !aclPresent(response.Resources, entry) {
			return fmt.Errorf("could not verify literal %s ACL for %s on %s", entry.Operation, entry.Principal, entry.ResourceName)
		}
	}
	return nil
}

func kafkaOperation(value string) (kafka.ACLOperationType, bool) {
	switch value {
	case "READ":
		return kafka.ACLOperationTypeRead, true
	case "WRITE":
		return kafka.ACLOperationTypeWrite, true
	case "DESCRIBE":
		return kafka.ACLOperationTypeDescribe, true
	default:
		return kafka.ACLOperationTypeUnknown, false
	}
}

func aclPresent(resources []kafka.ACLResource, expected kafka.ACLEntry) bool {
	for _, resource := range resources {
		if resource.ResourceType != expected.ResourceType || resource.ResourceName != expected.ResourceName || resource.PatternType != expected.ResourcePatternType {
			continue
		}
		for _, actual := range resource.ACLs {
			if actual.Principal == expected.Principal && actual.Host == expected.Host && actual.Operation == expected.Operation && actual.PermissionType == expected.PermissionType {
				return true
			}
		}
	}
	return false
}
