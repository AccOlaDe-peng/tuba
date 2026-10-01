package kafkarevoke

import (
	"os"
	"path/filepath"
	"testing"
)

func writeProps(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "admin.properties")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFromPropertiesFileParsesAdminConfig(t *testing.T) {
	r, err := FromPropertiesFile(writeProps(t, "bootstrap.servers=10.6.68.248:29292\nsecurity.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"tuba-kafka-admin\" password=\"s3cret\";\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if r == nil || r.client == nil {
		t.Fatal("expected a revoker with a client")
	}
}

func TestFromPropertiesFileRejectsMultipleBrokers(t *testing.T) {
	if _, err := FromPropertiesFile(writeProps(t, "bootstrap.servers=a:1,b:2\nsasl.jaas.config=x username=\"u\" password=\"p\";\n")); err == nil {
		t.Fatal("multiple brokers must be rejected")
	}
}

func TestFromPropertiesFileRejectsMissingCredentials(t *testing.T) {
	if _, err := FromPropertiesFile(writeProps(t, "bootstrap.servers=a:1\nsecurity.protocol=SASL_PLAINTEXT\n")); err == nil {
		t.Fatal("missing sasl.jaas.config must be rejected")
	}
}

func TestFromPropertiesFileRejectsMalformedLine(t *testing.T) {
	if _, err := FromPropertiesFile(writeProps(t, "bootstrap.servers=a:1\nno-equals-sign\n")); err == nil {
		t.Fatal("malformed line must be rejected")
	}
}
