package kafkautil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
	"strings"
	"time"
)

type Config struct{ Protocol, Mechanism, Username, Password, CAFile, CertFile, KeyFile, ServerName string }

func (c Config) security() (*tls.Config, sasl.Mechanism, error) {
	var t *tls.Config
	if strings.Contains(strings.ToLower(c.Protocol), "tls") {
		t = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName}
		if c.CAFile != "" {
			b, e := os.ReadFile(c.CAFile)
			if e != nil {
				return nil, nil, e
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(b) {
				return nil, nil, errors.New("KAFKA_TLS_CA_FILE has no valid certificate")
			}
			t.RootCAs = pool
		}
		if c.CertFile != "" || c.KeyFile != "" {
			cert, e := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
			if e != nil {
				return nil, nil, e
			}
			t.Certificates = []tls.Certificate{cert}
		}
	}
	var m sasl.Mechanism
	var e error
	switch strings.ToLower(c.Mechanism) {
	case "", "none":
	case "plain":
		m = plain.Mechanism{Username: c.Username, Password: c.Password}
	case "scram-sha256", "scram-sha-256":
		m, e = scram.Mechanism(scram.SHA256, c.Username, c.Password)
	case "scram-sha512", "scram-sha-512":
		m, e = scram.Mechanism(scram.SHA512, c.Username, c.Password)
	default:
		return nil, nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q", c.Mechanism)
	}
	return t, m, e
}
func (c Config) Dialer() (*kafka.Dialer, error) {
	t, m, e := c.security()
	if e != nil {
		return nil, e
	}
	return &kafka.Dialer{ClientID: "tuba", Timeout: 10 * time.Second, DualStack: true, TLS: t, SASLMechanism: m}, nil
}
func (c Config) Transport() (*kafka.Transport, error) {
	t, m, e := c.security()
	if e != nil {
		return nil, e
	}
	return &kafka.Transport{ClientID: "tuba", DialTimeout: 10 * time.Second, TLS: t, SASL: m}, nil
}
