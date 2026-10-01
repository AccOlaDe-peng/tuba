// Package kafkarevoke implements the broker-side half of collector
// disable/enable: removing and re-adding the bound source principal's WRITE
// ACL on its source topic. The SCRAM credential and the DESCRIBE ACL are left
// in place so that re-enabling is an exact inverse and never has to re-issue
// secrets.
package kafkarevoke

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/kafkautil"
)

type aclAdmin interface {
	CreateACLs(ctx context.Context, req *kafka.CreateACLsRequest) (*kafka.CreateACLsResponse, error)
	DeleteACLs(ctx context.Context, req *kafka.DeleteACLsRequest) (*kafka.DeleteACLsResponse, error)
	DescribeACLs(ctx context.Context, req *kafka.DescribeACLsRequest) (*kafka.DescribeACLsResponse, error)
}

type Revoker struct {
	client aclAdmin
}

// sourceTopic mirrors the database CHECK constraint: a revoker must never be
// able to touch ACLs on topics that are not per-source context topics.
var sourceTopic = regexp.MustCompile(`^tuba\.source\.ctx_[a-f0-9]{32}\.v[0-9]+$`)

func New(client *kafka.Client) *Revoker {
	return &Revoker{client: client}
}

var jaasCredential = regexp.MustCompile(`username="([^"]+)"\s+password="([^"]+)"`)

// FromPropertiesFile builds a revoker from a Java-properties admin client file
// (the same shape operators use for kafka-acls.sh --command-config). Keeping
// the admin credential in a root-managed file instead of the service
// environment lets the API pick it up on a plain process restart and keeps the
// secret out of the Launcher manifest. The file must grant exactly one broker.
func FromPropertiesFile(path string) (*Revoker, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	props := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s: malformed line %q", path, line)
		}
		props[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	brokers := strings.Split(props["bootstrap.servers"], ",")
	if len(brokers) != 1 || strings.TrimSpace(brokers[0]) == "" {
		return nil, fmt.Errorf("%s: exactly one bootstrap.servers address is required", path)
	}
	credentials := jaasCredential.FindStringSubmatch(props["sasl.jaas.config"])
	if credentials == nil {
		return nil, fmt.Errorf("%s: sasl.jaas.config must carry username/password", path)
	}
	transport, err := (kafkautil.Config{
		Protocol: props["security.protocol"], Mechanism: props["sasl.mechanism"],
		Username: credentials[1], Password: credentials[2],
	}).Transport()
	if err != nil {
		return nil, fmt.Errorf("configure Kafka admin transport: %w", err)
	}
	return New(&kafka.Client{Addr: kafka.TCP(strings.TrimSpace(brokers[0])), Timeout: 10 * time.Second, Transport: transport}), nil
}

func (r *Revoker) RevokeSourceWrite(ctx context.Context, principal, topic string) error {
	if err := validateTarget(principal, topic); err != nil {
		return err
	}
	principal = kafkaPrincipal(principal)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := r.client.DeleteACLs(ctx, &kafka.DeleteACLsRequest{Filters: []kafka.DeleteACLsFilter{{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topic,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		Operation:                 kafka.ACLOperationTypeWrite,
		PermissionType:            kafka.ACLPermissionTypeAllow,
	}}})
	if err != nil {
		return fmt.Errorf("delete WRITE ACL for %s on %s: %w", principal, topic, err)
	}
	if len(response.Results) != 1 {
		return fmt.Errorf("delete WRITE ACL for %s on %s: incomplete broker result", principal, topic)
	}
	if response.Results[0].Error != nil {
		return fmt.Errorf("delete WRITE ACL for %s on %s: %w", principal, topic, response.Results[0].Error)
	}
	for _, match := range response.Results[0].MatchingACLs {
		if match.Error != nil {
			return fmt.Errorf("delete WRITE ACL for %s on %s: %w", principal, topic, match.Error)
		}
	}
	present, err := r.writeACLPresent(ctx, principal, topic)
	if err != nil {
		return err
	}
	if present {
		return fmt.Errorf("WRITE ACL for %s on %s still present after delete", principal, topic)
	}
	return nil
}

func (r *Revoker) RestoreSourceWrite(ctx context.Context, principal, topic string) error {
	if err := validateTarget(principal, topic); err != nil {
		return err
	}
	principal = kafkaPrincipal(principal)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := r.client.CreateACLs(ctx, &kafka.CreateACLsRequest{ACLs: []kafka.ACLEntry{{
		ResourceType:        kafka.ResourceTypeTopic,
		ResourceName:        topic,
		ResourcePatternType: kafka.PatternTypeLiteral,
		Principal:           principal,
		Host:                "*",
		Operation:           kafka.ACLOperationTypeWrite,
		PermissionType:      kafka.ACLPermissionTypeAllow,
	}}})
	if err != nil {
		return fmt.Errorf("create WRITE ACL for %s on %s: %w", principal, topic, err)
	}
	if len(response.Errors) != 1 {
		return fmt.Errorf("create WRITE ACL for %s on %s: incomplete broker result", principal, topic)
	}
	if response.Errors[0] != nil {
		return fmt.Errorf("create WRITE ACL for %s on %s: %w", principal, topic, response.Errors[0])
	}
	present, err := r.writeACLPresent(ctx, principal, topic)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("WRITE ACL for %s on %s not visible after create", principal, topic)
	}
	return nil
}

// writeACLPresent is the idempotency check: deleting an absent ACL or creating
// an existing one must both converge on the verified end state.
func (r *Revoker) writeACLPresent(ctx context.Context, principal, topic string) (bool, error) {
	response, err := r.client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{Filter: kafka.ACLFilter{
		ResourceTypeFilter:        kafka.ResourceTypeTopic,
		ResourceNameFilter:        topic,
		ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
		PrincipalFilter:           principal,
		HostFilter:                "*",
		Operation:                 kafka.ACLOperationTypeWrite,
		PermissionType:            kafka.ACLPermissionTypeAllow,
	}})
	if err != nil {
		return false, fmt.Errorf("verify WRITE ACL for %s on %s: %w", principal, topic, err)
	}
	if response.Error != nil {
		return false, fmt.Errorf("verify WRITE ACL for %s on %s: %w", principal, topic, response.Error)
	}
	for _, resource := range response.Resources {
		if resource.ResourceType != kafka.ResourceTypeTopic || resource.ResourceName != topic || resource.PatternType != kafka.PatternTypeLiteral {
			continue
		}
		for _, acl := range resource.ACLs {
			if acl.Principal == principal && acl.Host == "*" && acl.Operation == kafka.ACLOperationTypeWrite && acl.PermissionType == kafka.ACLPermissionTypeAllow {
				return true, nil
			}
		}
	}
	return false, nil
}

func validateTarget(principal, topic string) error {
	if strings.TrimSpace(principal) == "" || len(principal) > 255 {
		return errors.New("invalid Kafka principal")
	}
	if !sourceTopic.MatchString(topic) {
		return fmt.Errorf("refusing to touch ACLs on non-source topic %q", topic)
	}
	return nil
}

// kafkaPrincipal converts a stored SCRAM username to the typed principal form
// the Admin API requires ("User:<name>"). Filters with an untyped principal
// silently match nothing on delete/describe, which once made a revocation look
// successful while the ACL stayed in place; always go through this.
func kafkaPrincipal(principal string) string {
	if strings.Contains(principal, ":") {
		return principal
	}
	return "User:" + principal
}
