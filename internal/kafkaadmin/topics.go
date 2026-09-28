package kafkaadmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

var namespacePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
var durationPattern = regexp.MustCompile(`^([1-9][0-9]*)(s|m|h|d)$`)

type Contract struct {
	Defaults struct {
		Partitions        int `json:"partitions"`
		ReplicationFactor int `json:"replication_factor"`
	} `json:"defaults"`
	RuntimeProfiles []RuntimeProfile `json:"runtime_profiles"`
	Topics          []TopicTemplate  `json:"topics"`
}

type RuntimeProfile struct {
	Name              string            `json:"profile"`
	Scope             string            `json:"scope"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	MinInsyncReplicas int               `json:"min_insync_replicas"`
	Retention         string            `json:"retention"`
	RetentionStatus   string            `json:"retention_status"`
	MaxMessageBytes   int               `json:"max_message_bytes"`
	ServicePrincipals map[string]string `json:"service_principals"`
}

type TopicTemplate struct {
	Name            string              `json:"name"`
	PhysicalName    string              `json:"physical_name"`
	Domains         []string            `json:"domains"`
	MaxMessageBytes int                 `json:"max_message_bytes"`
	ACL             map[string][]string `json:"acl"`
	Consumers       []struct {
		ConsumerGroup string `json:"consumer_group"`
	} `json:"consumers"`
}

type TopicPlan struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	MinInsyncReplicas int    `json:"min_insync_replicas"`
	RetentionMS       int64  `json:"retention_ms"`
	MaxMessageBytes   int    `json:"max_message_bytes"`
}

type Plan struct {
	Profile   string      `json:"profile"`
	Namespace string      `json:"namespace"`
	Topics    []TopicPlan `json:"topics"`
}

func BuildPlan(contractJSON []byte, profileName, namespace string) (Plan, error) {
	if !namespacePattern.MatchString(namespace) {
		return Plan{}, errors.New("namespace must be a lowercase slug containing only letters, digits, underscores, or hyphens")
	}
	var contract Contract
	if err := json.Unmarshal(contractJSON, &contract); err != nil {
		return Plan{}, fmt.Errorf("decode topic contract: %w", err)
	}
	var profile *RuntimeProfile
	for i := range contract.RuntimeProfiles {
		if contract.RuntimeProfiles[i].Name == profileName {
			profile = &contract.RuntimeProfiles[i]
			break
		}
	}
	if profile == nil {
		return Plan{}, fmt.Errorf("runtime profile %q is not declared in the topic contract", profileName)
	}
	if !strings.Contains(profile.Scope, "development validation only") || !strings.Contains(profile.RetentionStatus, "bounded_validation_profile") {
		return Plan{}, fmt.Errorf("refusing to create topics from non-validation profile %q", profileName)
	}
	if profile.Partitions != 1 || profile.ReplicationFactor != 1 || profile.MinInsyncReplicas != 1 || profile.MaxMessageBytes <= 0 {
		return Plan{}, errors.New("validation profile must define one partition, one replica, one in-sync replica, and a positive message limit")
	}
	retention, err := parseRetention(profile.Retention)
	if err != nil {
		return Plan{}, fmt.Errorf("validation profile retention: %w", err)
	}

	plans := make([]TopicPlan, 0, len(contract.Topics))
	seen := make(map[string]struct{})
	for _, topic := range contract.Topics {
		if strings.Contains(topic.PhysicalName, "{source_context_id}") {
			// Source topics are created only by tuba-source-topic-admin after the
			// immutable context and its exact ACL principals have been verified.
			continue
		}
		if topic.Name == "" || topic.PhysicalName == "" || topic.MaxMessageBytes <= 0 {
			return Plan{}, errors.New("topic contract has an incomplete topic template")
		}
		names := []string{topic.PhysicalName}
		if strings.Contains(topic.PhysicalName, "{domain}") {
			if len(topic.Domains) == 0 {
				return Plan{}, fmt.Errorf("topic template %q has a domain placeholder but no domains", topic.Name)
			}
			names = make([]string, 0, len(topic.Domains))
			for _, domain := range topic.Domains {
				if !namespacePattern.MatchString(domain) {
					return Plan{}, fmt.Errorf("topic template %q has invalid domain %q", topic.Name, domain)
				}
				names = append(names, strings.ReplaceAll(topic.PhysicalName, "{domain}", domain))
			}
		}
		for _, name := range names {
			name = strings.ReplaceAll(name, "{namespace}", namespace)
			if strings.Contains(name, "{") || strings.Contains(name, "}") {
				return Plan{}, fmt.Errorf("topic template %q contains an unsupported placeholder", topic.Name)
			}
			if len(name) > 249 || strings.ContainsAny(name, " /\\") {
				return Plan{}, fmt.Errorf("resolved Kafka topic name %q is invalid", name)
			}
			if _, exists := seen[name]; exists {
				return Plan{}, fmt.Errorf("topic contract resolves to duplicate physical name %q", name)
			}
			seen[name] = struct{}{}
			maxBytes := topic.MaxMessageBytes
			if maxBytes > profile.MaxMessageBytes {
				maxBytes = profile.MaxMessageBytes
			}
			plans = append(plans, TopicPlan{
				Name: name, Partitions: profile.Partitions, ReplicationFactor: profile.ReplicationFactor,
				MinInsyncReplicas: profile.MinInsyncReplicas,
				RetentionMS:       retention.Milliseconds(), MaxMessageBytes: maxBytes,
			})
		}
	}
	if len(plans) == 0 {
		return Plan{}, errors.New("topic contract resolved to no physical topics")
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Name < plans[j].Name })
	return Plan{Profile: profile.Name, Namespace: namespace, Topics: plans}, nil
}

func parseRetention(value string) (time.Duration, error) {
	match := durationPattern.FindStringSubmatch(value)
	if match == nil {
		return 0, fmt.Errorf("unsupported duration %q", value)
	}
	var unit time.Duration
	switch match[2] {
	case "s":
		unit = time.Second
	case "m":
		unit = time.Minute
	case "h":
		unit = time.Hour
	case "d":
		unit = 24 * time.Hour
	}
	var amount int64
	if _, err := fmt.Sscan(match[1], &amount); err != nil || amount <= 0 || amount > int64((1<<63-1)/unit) {
		return 0, fmt.Errorf("duration %q is out of range", value)
	}
	return time.Duration(amount) * unit, nil
}
