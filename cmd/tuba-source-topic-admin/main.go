package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"tuba/product/internal/kafkautil"
	"tuba/product/internal/sourceadapter"
)

const (
	retentionMS     = "86400000"
	maxMessageBytes = "2097152"
)

var contextIDPattern = regexp.MustCompile(`^ctx_[a-f0-9]{32}$`)
var principalPattern = regexp.MustCompile(`^User:[A-Za-z0-9._-]{1,128}$`)

type sourcePlan struct {
	SourceContextID  string `json:"source_context_id"`
	SourceInstanceID string `json:"source_instance_id"`
	Organization     string `json:"organization_id"`
	Namespace        string `json:"namespace"`
	Dataset          string `json:"dataset"`
	Topic            string `json:"topic"`
	GroupID          string `json:"adapter_group_id"`
	SourcePrincipal  string `json:"source_principal"`
	AdapterPrincipal string `json:"adapter_principal"`
	RetentionHours   int    `json:"retention_hours"`
	MaxMessageBytes  int    `json:"max_message_bytes"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	contextID := flag.String("context", "", "registered immutable source context ID")
	sourcePrincipal := flag.String("source-principal", "", "Kafka principal for one Beat source, e.g. User:zeek-conn")
	adapterPrincipal := flag.String("adapter-principal", "", "Kafka principal for the source adapter")
	apply := flag.Bool("apply", false, "create the topic and exact ACLs; default prints the plan only")
	flag.Parse()
	if !contextIDPattern.MatchString(*contextID) || !principalPattern.MatchString(*sourcePrincipal) || !principalPattern.MatchString(*adapterPrincipal) || *sourcePrincipal == *adapterPrincipal {
		return errors.New("provide a canonical context ID and two distinct User:<name> Kafka principals")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required to verify the registered source context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return errors.New("source registry connection unavailable")
	}
	defer pool.Close()
	plan, err := loadPlan(ctx, pool, *contextID, *sourcePrincipal, *adapterPrincipal)
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
	kc := kafkautil.Config{
		Protocol: os.Getenv("KAFKA_SECURITY_PROTOCOL"), Mechanism: os.Getenv("KAFKA_SASL_MECHANISM"),
		Username: os.Getenv("KAFKA_SASL_USERNAME"), Password: os.Getenv("KAFKA_SASL_PASSWORD"),
		CAFile: os.Getenv("KAFKA_TLS_CA_FILE"), CertFile: os.Getenv("KAFKA_TLS_CERT_FILE"),
		KeyFile: os.Getenv("KAFKA_TLS_KEY_FILE"), ServerName: os.Getenv("KAFKA_TLS_SERVER_NAME"),
	}
	transport, err := kc.Transport()
	if err != nil {
		return fmt.Errorf("configure Kafka admin transport: %w", err)
	}
	defer transport.CloseIdleConnections()
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second, Transport: transport}
	if err := verifyAuthorizer(ctx, client, plan.Topic); err != nil {
		return err
	}
	if err := ensureTopic(ctx, client, plan.Topic); err != nil {
		return err
	}
	if err := ensureACLs(ctx, client, plan); err != nil {
		return err
	}
	fmt.Printf("source topic and exact ACLs ready: %s\n", plan.Topic)
	return nil
}

func loadPlan(ctx context.Context, pool *pgxpool.Pool, contextID, sourcePrincipal, adapterPrincipal string) (sourcePlan, error) {
	plan := sourcePlan{
		SourceContextID: contextID, Topic: "tuba.source." + contextID + ".v1",
		SourcePrincipal: sourcePrincipal, AdapterPrincipal: adapterPrincipal,
		RetentionHours: 24, MaxMessageBytes: 2097152,
	}
	plan.GroupID = sourceadapter.GroupID(plan.Topic)
	err := pool.QueryRow(ctx, `
		SELECT si.id,sc.organization_slug,sc.namespace,sc.vendor_dataset
		FROM source_contexts sc JOIN source_instances si ON si.id=sc.source_instance_id
		WHERE sc.id=$1 AND si.enabled=true`, contextID).Scan(
		&plan.SourceInstanceID, &plan.Organization, &plan.Namespace, &plan.Dataset)
	if errors.Is(err, pgx.ErrNoRows) {
		return sourcePlan{}, errors.New("source context is not registered or its source is disabled")
	}
	if err != nil {
		return sourcePlan{}, errors.New("source registry query unavailable")
	}
	return plan, nil
}

func verifyAuthorizer(ctx context.Context, client *kafka.Client, topic string) error {
	response, err := client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{Filter: kafka.ACLFilter{
		ResourceTypeFilter: kafka.ResourceTypeTopic, ResourceNameFilter: topic,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		Operation:                 kafka.ACLOperationTypeAny, PermissionType: kafka.ACLPermissionTypeAny,
	}})
	if err != nil || response.Error != nil {
		return errors.New("Kafka ACL authorizer is unavailable; no topic or ACL was created")
	}
	return nil
}

func ensureTopic(ctx context.Context, client *kafka.Client, topic string) error {
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
		ConfigEntries: []kafka.ConfigEntry{
			{ConfigName: "retention.ms", ConfigValue: retentionMS},
			{ConfigName: "max.message.bytes", ConfigValue: maxMessageBytes},
		},
	}}})
	if err != nil {
		return fmt.Errorf("create source topic: %w", err)
	}
	if topicErr := response.Errors[topic]; topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create source topic: %w", topicErr)
	}
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil || len(metadata.Topics) != 1 || metadata.Topics[0].Error != nil || len(metadata.Topics[0].Partitions) != 1 || len(metadata.Topics[0].Partitions[0].Replicas) != 1 {
		return errors.New("source topic does not have exactly one partition and one replica")
	}
	configs, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{Resources: []kafka.DescribeConfigRequestResource{{
		ResourceType: kafka.ResourceTypeTopic, ResourceName: topic,
		ConfigNames: []string{"retention.ms", "max.message.bytes"},
	}}})
	if err != nil || len(configs.Resources) != 1 || configs.Resources[0].Error != nil {
		return errors.New("source topic configuration could not be verified")
	}
	found := make(map[string]string)
	for _, entry := range configs.Resources[0].ConfigEntries {
		found[entry.ConfigName] = entry.ConfigValue
	}
	if found["retention.ms"] != retentionMS || found["max.message.bytes"] != maxMessageBytes {
		return errors.New("existing source topic retention or message size differs from the contract")
	}
	return nil
}

func ensureACLs(ctx context.Context, client *kafka.Client, plan sourcePlan) error {
	makeACL := func(resource kafka.ResourceType, name, principal string, operation kafka.ACLOperationType) kafka.ACLEntry {
		return kafka.ACLEntry{ResourceType: resource, ResourceName: name, ResourcePatternType: kafka.PatternTypeLiteral,
			Principal: principal, Host: "*", Operation: operation, PermissionType: kafka.ACLPermissionTypeAllow}
	}
	entries := []kafka.ACLEntry{
		makeACL(kafka.ResourceTypeTopic, plan.Topic, plan.SourcePrincipal, kafka.ACLOperationTypeWrite),
		makeACL(kafka.ResourceTypeTopic, plan.Topic, plan.SourcePrincipal, kafka.ACLOperationTypeDescribe),
		makeACL(kafka.ResourceTypeTopic, plan.Topic, plan.AdapterPrincipal, kafka.ACLOperationTypeRead),
		makeACL(kafka.ResourceTypeTopic, plan.Topic, plan.AdapterPrincipal, kafka.ACLOperationTypeDescribe),
		makeACL(kafka.ResourceTypeGroup, plan.GroupID, plan.AdapterPrincipal, kafka.ACLOperationTypeRead),
	}
	response, err := client.CreateACLs(ctx, &kafka.CreateACLsRequest{ACLs: entries})
	if err != nil || len(response.Errors) != len(entries) {
		return errors.New("create source ACLs failed; inspect partial changes before retry")
	}
	for _, entryErr := range response.Errors {
		if entryErr != nil {
			return fmt.Errorf("create source ACLs failed; inspect partial changes before retry: %w", entryErr)
		}
	}
	for _, entry := range entries {
		response, err := client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{Filter: kafka.ACLFilter{
			ResourceTypeFilter: entry.ResourceType, ResourceNameFilter: entry.ResourceName,
			ResourcePatternTypeFilter: entry.ResourcePatternType, PrincipalFilter: entry.Principal,
			HostFilter: entry.Host, Operation: entry.Operation, PermissionType: entry.PermissionType,
		}})
		if err != nil || response.Error != nil || !containsACL(response.Resources, entry) {
			return fmt.Errorf("source ACL could not be verified (%s %s %v); inspect partial changes before retry", entry.ResourceName, entry.Principal, entry.Operation)
		}
	}
	return nil
}

func containsACL(resources []kafka.ACLResource, expected kafka.ACLEntry) bool {
	for _, resource := range resources {
		if resource.ResourceType != expected.ResourceType || resource.ResourceName != expected.ResourceName || resource.PatternType != expected.ResourcePatternType {
			continue
		}
		for _, acl := range resource.ACLs {
			if acl.Principal == expected.Principal && acl.Host == expected.Host && acl.Operation == expected.Operation && acl.PermissionType == expected.PermissionType {
				return true
			}
		}
	}
	return false
}
