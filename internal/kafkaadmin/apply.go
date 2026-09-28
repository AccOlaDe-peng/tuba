package kafkaadmin

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Apply creates missing topics and verifies existing ones without mutating
// incompatible configurations. ACL provisioning is intentionally a separate
// operation because it requires explicit principal credentials and policies.
func Apply(ctx context.Context, client *kafka.Client, plan Plan) error {
	configs := make([]kafka.TopicConfig, 0, len(plan.Topics))
	for _, topic := range plan.Topics {
		configs = append(configs, kafka.TopicConfig{
			Topic: topic.Name, NumPartitions: topic.Partitions, ReplicationFactor: topic.ReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{ConfigName: "retention.ms", ConfigValue: fmt.Sprint(topic.RetentionMS)},
				{ConfigName: "max.message.bytes", ConfigValue: fmt.Sprint(topic.MaxMessageBytes)},
				{ConfigName: "min.insync.replicas", ConfigValue: fmt.Sprint(topic.MinInsyncReplicas)},
			},
		})
	}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		return fmt.Errorf("create Kafka topics: %w", err)
	}
	for _, topic := range plan.Topics {
		if topicErr := created.Errors[topic.Name]; topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
			return fmt.Errorf("create Kafka topic %s: %w", topic.Name, topicErr)
		}
	}

	names := make([]string, 0, len(plan.Topics))
	resources := make([]kafka.DescribeConfigRequestResource, 0, len(plan.Topics))
	for _, topic := range plan.Topics {
		names = append(names, topic.Name)
		resources = append(resources, kafka.DescribeConfigRequestResource{
			ResourceType: kafka.ResourceTypeTopic, ResourceName: topic.Name,
			ConfigNames: []string{"retention.ms", "max.message.bytes", "min.insync.replicas"},
		})
	}
	metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: names})
	if err != nil {
		return fmt.Errorf("verify Kafka topic metadata: %w", err)
	}
	metadataByName := make(map[string]kafka.Topic, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return fmt.Errorf("describe Kafka topic %s: %w", topic.Name, topic.Error)
		}
		metadataByName[topic.Name] = topic
	}
	for _, expected := range plan.Topics {
		actual, ok := metadataByName[expected.Name]
		if !ok || len(actual.Partitions) != expected.Partitions {
			return fmt.Errorf("Kafka topic %s does not have %d partitions", expected.Name, expected.Partitions)
		}
		for _, partition := range actual.Partitions {
			if len(partition.Replicas) != expected.ReplicationFactor {
				return fmt.Errorf("Kafka topic %s partition %d does not have %d replicas", expected.Name, partition.ID, expected.ReplicationFactor)
			}
		}
	}

	configResponse, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{Resources: resources})
	if err != nil {
		return fmt.Errorf("verify Kafka topic configuration: %w", err)
	}
	configsByName := make(map[string]map[string]string, len(configResponse.Resources))
	for _, resource := range configResponse.Resources {
		if resource.Error != nil {
			return fmt.Errorf("describe Kafka topic configuration %s: %w", resource.ResourceName, resource.Error)
		}
		values := make(map[string]string, len(resource.ConfigEntries))
		for _, entry := range resource.ConfigEntries {
			values[entry.ConfigName] = entry.ConfigValue
		}
		configsByName[resource.ResourceName] = values
	}
	for _, expected := range plan.Topics {
		actual := configsByName[expected.Name]
		if actual["retention.ms"] != fmt.Sprint(expected.RetentionMS) || actual["max.message.bytes"] != fmt.Sprint(expected.MaxMessageBytes) || actual["min.insync.replicas"] != fmt.Sprint(expected.MinInsyncReplicas) {
			return fmt.Errorf("Kafka topic %s configuration differs from the %s profile; no existing values were changed", expected.Name, plan.Profile)
		}
	}
	return nil
}
