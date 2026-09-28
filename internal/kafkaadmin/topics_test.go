package kafkaadmin

import (
	"os"
	"strings"
	"testing"
)

func TestBuildPlanUsesBoundedValidationContract(t *testing.T) {
	contract, err := os.ReadFile("../../contracts/events/topics.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(contract, "zeek_validation_single_node", "test_validation")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Namespace != "test_validation" || len(plan.Topics) != 15 {
		t.Fatalf("unexpected plan identity or topic count: namespace=%q topics=%d", plan.Namespace, len(plan.Topics))
	}
	for _, topic := range plan.Topics {
		if !strings.Contains(topic.Name, "test_validation") {
			t.Errorf("topic was not expanded with namespace: %s", topic.Name)
		}
		if strings.Contains(topic.Name, "ctx_") {
			t.Errorf("source context topic should be left to source-topic-admin: %s", topic.Name)
		}
		if topic.Partitions != 1 || topic.ReplicationFactor != 1 || topic.MinInsyncReplicas != 1 || topic.RetentionMS != 86_400_000 || topic.MaxMessageBytes > 2_097_152 {
			t.Errorf("topic does not match bounded one-node profile: %+v", topic)
		}
		if strings.Contains(topic.Name, "analysis.results") && topic.MaxMessageBytes != 1_048_576 {
			t.Errorf("analysis result topic exceeded its contract message limit: %+v", topic)
		}
	}
}

func TestBuildPlanRejectsUnsafeNamespace(t *testing.T) {
	contract, err := os.ReadFile("../../contracts/events/topics.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildPlan(contract, "zeek_validation_single_node", "../other"); err == nil {
		t.Fatal("unsafe namespace was accepted")
	}
}

func TestBuildPlanRejectsUnknownProfile(t *testing.T) {
	contract, err := os.ReadFile("../../contracts/events/topics.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildPlan(contract, "production", "tenant_a"); err == nil {
		t.Fatal("undeclared production profile was accepted")
	}
}
