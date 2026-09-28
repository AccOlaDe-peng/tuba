package kafkautil

import "testing"

func TestSecurityAcceptsKafkaMechanismNames(t *testing.T) {
	for _, mechanism := range []string{"scram-sha256", "scram-sha-256", "scram-sha512", "scram-sha-512"} {
		t.Run(mechanism, func(t *testing.T) {
			_, sasl, err := (Config{Protocol: "sasl_plaintext", Mechanism: mechanism, Username: "test-user", Password: "test-password"}).security()
			if err != nil {
				t.Fatalf("security() error = %v", err)
			}
			if sasl == nil {
				t.Fatal("security() returned no SASL mechanism")
			}
		})
	}
}
