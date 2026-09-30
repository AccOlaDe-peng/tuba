package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Brokers                   []string
	RawTopic, QuarantineTopic string
	// RawTopicPattern is the ingest's produce topic, with {namespace} standing in
	// for each source's namespace. It is separate from RawTopic because the
	// consumers of that topic need the concrete name, and one process cannot
	// treat the same variable as both a pattern and a literal.
	RawTopicPattern                                                            string
	EventsTopicPrefix                                                          string
	DeadLetterTopic                                                            string
	AnalysisTopic                                                              string
	Organization                                                               string
	Namespace                                                                  string
	ESURL                                                                      string
	ESAPIKey                                                                   string
	DatabaseURL                                                                string
	Listen                                                                     string
	KafkaProtocol, KafkaSASLMechanism, KafkaUsername, KafkaPassword            string
	KafkaCAFile, KafkaCertFile, KafkaKeyFile, KafkaServerName                  string
	ConsumerGroupSuffix                                                        string
	IngestRate, IngestBurst, IndexBatchSize, IndexBatchBytes, IndexMaxAttempts int
	IndexBatchWait, IndexRetryBackoff                                          time.Duration
	HTTPRequestTimeout                                                         time.Duration
}

func Load() (Config, error) {
	httpListen, err := HTTPListenAddress()
	if err != nil {
		return Config{}, err
	}
	ingestRate, err := intValue("INGEST_RATE_PER_SECOND", 1000, 1, 1_000_000)
	if err != nil {
		return Config{}, err
	}
	ingestBurst, err := intValue("INGEST_RATE_BURST", 2000, 1, 2_000_000)
	if err != nil {
		return Config{}, err
	}
	indexBatchSize, err := intValue("INDEX_BATCH_SIZE", 500, 1, 10_000)
	if err != nil {
		return Config{}, err
	}
	indexBatchBytes, err := intValue("INDEX_BATCH_BYTES", 16<<20, 2<<20, 64<<20)
	if err != nil {
		return Config{}, err
	}
	indexMaxAttempts, err := intValue("INDEX_MAX_ATTEMPTS", 5, 1, 100)
	if err != nil {
		return Config{}, err
	}
	indexBatchWait, err := durationValue("INDEX_BATCH_WAIT", "1s", time.Millisecond, time.Minute)
	if err != nil {
		return Config{}, err
	}
	indexRetryBackoff, err := durationValue("INDEX_RETRY_BACKOFF", "200ms", time.Millisecond, time.Minute)
	if err != nil {
		return Config{}, err
	}
	httpRequestTimeout, err := HTTPRequestTimeout()
	if err != nil {
		return Config{}, err
	}
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
		if brokers[i] == "" {
			return Config{}, errors.New("KAFKA_BROKERS must contain non-empty broker addresses")
		}
	}
	c := Config{
		Brokers:  brokers,
		RawTopic: value("KAFKA_RAW_TOPIC", "tuba.raw.events.v1"),
		// Deliberately no default: a pattern default that differs from the fixed
		// topic would send the ingest somewhere the consumers never read. The
		// ingest resolves an unset pattern to RawTopic so the two agree.
		RawTopicPattern:   os.Getenv("KAFKA_RAW_TOPIC_PATTERN"),
		QuarantineTopic:   value("KAFKA_QUARANTINE_TOPIC", "tuba.quarantine.v1"),
		EventsTopicPrefix: value("KAFKA_EVENTS_TOPIC_PREFIX", "tuba.events"),
		DeadLetterTopic:   value("KAFKA_DLQ_TOPIC", "tuba.indexing.dlq.v1"),
		AnalysisTopic:     value("KAFKA_ANALYSIS_RESULTS_TOPIC", "tuba.analysis.results.v1"),
		Organization:      os.Getenv("TUBA_ORGANIZATION_ID"),
		Namespace:         os.Getenv("TUBA_NAMESPACE"),
		ESURL:             os.Getenv("ES_URL"),
		ESAPIKey:          os.Getenv("ES_API_KEY"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		Listen:            httpListen,
		KafkaProtocol:     value("KAFKA_SECURITY_PROTOCOL", "plaintext"), KafkaSASLMechanism: value("KAFKA_SASL_MECHANISM", "none"), KafkaUsername: os.Getenv("KAFKA_SASL_USERNAME"), KafkaPassword: os.Getenv("KAFKA_SASL_PASSWORD"), KafkaCAFile: os.Getenv("KAFKA_TLS_CA_FILE"), KafkaCertFile: os.Getenv("KAFKA_TLS_CERT_FILE"), KafkaKeyFile: os.Getenv("KAFKA_TLS_KEY_FILE"), KafkaServerName: os.Getenv("KAFKA_TLS_SERVER_NAME"),
		ConsumerGroupSuffix: os.Getenv("KAFKA_CONSUMER_GROUP_SUFFIX"),
		IngestRate:          ingestRate, IngestBurst: ingestBurst, IndexBatchSize: indexBatchSize, IndexBatchBytes: indexBatchBytes, IndexMaxAttempts: indexMaxAttempts, IndexBatchWait: indexBatchWait, IndexRetryBackoff: indexRetryBackoff,
		HTTPRequestTimeout: httpRequestTimeout,
	}
	if len(c.Brokers) == 0 || strings.TrimSpace(c.Brokers[0]) == "" || c.Organization == "" || c.Namespace == "" {
		return c, errors.New("KAFKA_BROKERS, TUBA_ORGANIZATION_ID and TUBA_NAMESPACE are required")
	}
	if !safeID(c.Organization) || !safeID(c.Namespace) {
		return c, errors.New("organization and namespace must contain only letters, numbers, underscore or hyphen")
	}
	if !safeTopicPrefix(c.EventsTopicPrefix) {
		return c, errors.New("KAFKA_EVENTS_TOPIC_PREFIX contains unsupported characters")
	}
	if c.ConsumerGroupSuffix != "" && !safeID(c.ConsumerGroupSuffix) {
		return c, errors.New("KAFKA_CONSUMER_GROUP_SUFFIX must contain only letters, numbers, underscore or hyphen")
	}
	return c, nil
}

func HTTPRequestTimeout() (time.Duration, error) {
	return durationValue("HTTP_REQUEST_TIMEOUT", "30s", time.Second, 5*time.Minute)
}

func HTTPListenAddress() (string, error) {
	return privateListenAddress("HTTP_LISTEN", "127.0.0.1:8080")
}

func APIListenAddress() (string, error) {
	return privateListenAddress("API_LISTEN", "127.0.0.1:8788")
}

func privateListenAddress(variable, fallback string) (string, error) {
	address := value(variable, fallback)
	allowNonLoopback := os.Getenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN")
	if err := ValidateListenerAddress(variable, address, allowNonLoopback); err != nil {
		return "", err
	}
	return address, nil
}

// ValidateListenerAddress checks the listener value and its explicit opt-in.
// An empty opt-in has the same safe behavior as false.
func ValidateListenerAddress(variable, address, allowNonLoopbackValue string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%s must be a host:port loopback address", variable)
	}
	allowNonLoopback := false
	switch allowNonLoopbackValue {
	case "", "false":
	case "true":
		allowNonLoopback = true
	default:
		return errors.New("TUBA_ALLOW_NON_LOOPBACK_LISTEN must be exactly true or false")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		if host != "" || !allowNonLoopback {
			return fmt.Errorf("%s must bind to a loopback IP address unless TUBA_ALLOW_NON_LOOPBACK_LISTEN=true", variable)
		}
	} else if !ip.IsLoopback() && !allowNonLoopback {
		return fmt.Errorf("%s must bind to a loopback IP address unless TUBA_ALLOW_NON_LOOPBACK_LISTEN=true", variable)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("%s must use a TCP port between 1 and 65535", variable)
	}
	return nil
}

func (c Config) ConsumerGroup(base string) string {
	group := base + "-" + c.Namespace
	if c.ConsumerGroupSuffix != "" {
		group += "-" + c.ConsumerGroupSuffix
	}
	return group
}

func value(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func safeID(s string) bool {
	if len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return len(s) > 0
}

func safeTopicPrefix(s string) bool {
	if s == "" || strings.HasPrefix(s, ".") || strings.HasPrefix(s, "-") || strings.HasPrefix(s, "_") {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

func intValue(k string, fallback, min, max int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < min || n > max {
		return 0, errors.New(k + " must be an integer between " + strconv.Itoa(min) + " and " + strconv.Itoa(max))
	}
	return n, nil
}
func durationValue(k, fallback string, min, max time.Duration) (time.Duration, error) {
	v := value(k, fallback)
	d, err := time.ParseDuration(v)
	if err != nil || d < min || d > max {
		return 0, errors.New(k + " must be a duration between " + min.String() + " and " + max.String())
	}
	return d, nil
}
