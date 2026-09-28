package config

import "testing"

func TestLoadSupportsIsolatedEventTopicsAndConsumerGroups(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("TUBA_ORGANIZATION_ID", "tenant_a")
	t.Setenv("TUBA_NAMESPACE", "tenant_a")
	t.Setenv("HTTP_LISTEN", "")
	t.Setenv("KAFKA_EVENTS_TOPIC_PREFIX", "tuba.collector.validation.events")
	t.Setenv("KAFKA_CONSUMER_GROUP_SUFFIX", "validation")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.EventsTopicPrefix != "tuba.collector.validation.events" || c.ConsumerGroup("tuba-normalizer") != "tuba-normalizer-tenant_a-validation" {
		t.Fatalf("isolation config not applied: %+v", c)
	}
	if c.Listen != "127.0.0.1:8080" {
		t.Fatalf("default ingest listener should be loopback, got %q", c.Listen)
	}
}

func TestPrivateListenAddressesRequireLoopback(t *testing.T) {
	t.Setenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", "")
	t.Setenv("HTTP_LISTEN", "")
	httpListen, err := HTTPListenAddress()
	if err != nil || httpListen != "127.0.0.1:8080" {
		t.Fatalf("HTTP listen=%q err=%v", httpListen, err)
	}
	t.Setenv("API_LISTEN", "")
	apiListen, err := APIListenAddress()
	if err != nil || apiListen != "127.0.0.1:8788" {
		t.Fatalf("API listen=%q err=%v", apiListen, err)
	}
	t.Setenv("HTTP_LISTEN", "0.0.0.0:8080")
	if _, err := HTTPListenAddress(); err == nil {
		t.Fatal("wildcard HTTP listener accepted")
	}
	t.Setenv("API_LISTEN", "192.0.2.10:8788")
	if _, err := APIListenAddress(); err == nil {
		t.Fatal("non-loopback API listener accepted")
	}
	t.Setenv("HTTP_LISTEN", "127.0.0.1:0")
	if _, err := HTTPListenAddress(); err == nil {
		t.Fatal("ephemeral HTTP listener port accepted")
	}
	t.Setenv("API_LISTEN", "[::1]:8788")
	if listen, err := APIListenAddress(); err != nil || listen != "[::1]:8788" {
		t.Fatalf("IPv6 loopback listen=%q err=%v", listen, err)
	}
	t.Setenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", "true")
	t.Setenv("HTTP_LISTEN", ":8080")
	if listen, err := HTTPListenAddress(); err != nil || listen != ":8080" {
		t.Fatalf("explicit wildcard HTTP listen=%q err=%v", listen, err)
	}
	t.Setenv("API_LISTEN", "192.0.2.10:8788")
	if listen, err := APIListenAddress(); err != nil || listen != "192.0.2.10:8788" {
		t.Fatalf("explicit Pod-IP API listen=%q err=%v", listen, err)
	}
	for _, invalid := range []string{"1", "TRUE", "sometimes"} {
		t.Setenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", invalid)
		if _, err := APIListenAddress(); err == nil {
			t.Fatalf("invalid non-loopback opt-in value %q accepted", invalid)
		}
	}
	t.Setenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", "true")
	t.Setenv("API_LISTEN", "api.internal:8788")
	if _, err := APIListenAddress(); err == nil {
		t.Fatal("DNS hostname listener accepted even with non-loopback opt-in")
	}
}

func TestLoadRejectsUnsafeEventTopicPrefix(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("TUBA_ORGANIZATION_ID", "tenant_a")
	t.Setenv("TUBA_NAMESPACE", "tenant_a")
	t.Setenv("KAFKA_EVENTS_TOPIC_PREFIX", "../tuba.events")
	if _, err := Load(); err == nil {
		t.Fatal("unsafe topic prefix accepted")
	}
}

func TestLoadRejectsInvalidRuntimeBounds(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092")
	t.Setenv("TUBA_ORGANIZATION_ID", "tenant_a")
	t.Setenv("TUBA_NAMESPACE", "tenant_a")
	t.Setenv("INDEX_BATCH_SIZE", "50000")
	if _, err := Load(); err == nil {
		t.Fatal("out-of-range INDEX_BATCH_SIZE accepted")
	}
	t.Setenv("INDEX_BATCH_SIZE", "500")
	t.Setenv("INDEX_BATCH_WAIT", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("invalid INDEX_BATCH_WAIT silently accepted")
	}
	t.Setenv("INDEX_BATCH_WAIT", "1s")
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:9092,")
	if _, err := Load(); err == nil {
		t.Fatal("empty Kafka broker address accepted")
	}
}

func TestHTTPRequestTimeoutIsValidated(t *testing.T) {
	t.Setenv("HTTP_REQUEST_TIMEOUT", "30s")
	timeout, err := HTTPRequestTimeout()
	if err != nil || timeout.String() != "30s" {
		t.Fatalf("timeout=%s err=%v", timeout, err)
	}
	t.Setenv("HTTP_REQUEST_TIMEOUT", "0s")
	if _, err := HTTPRequestTimeout(); err == nil {
		t.Fatal("zero HTTP_REQUEST_TIMEOUT accepted")
	}
}
