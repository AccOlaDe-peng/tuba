package kafkaadmin

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type SecurityPrincipal struct {
	Role     string `json:"role"`
	Username string `json:"username"`
}

type SecurityACL struct {
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	Principal    string `json:"principal"`
	Operation    string `json:"operation"`
}

type SecurityPlan struct {
	Profile    string              `json:"profile"`
	Namespace  string              `json:"namespace"`
	Principals []SecurityPrincipal `json:"principals"`
	ACLs       []SecurityACL       `json:"acls"`
}

// BuildSecurityPlan derives additive literal ACLs for the service principals
// declared by a bounded validation profile. Source-context topics are excluded:
// they require a database-backed registration and tuba-source-topic-admin.
func BuildSecurityPlan(contractJSON []byte, profileName, namespace, groupSuffix string) (SecurityPlan, error) {
	if groupSuffix != "" && !namespacePattern.MatchString(groupSuffix) {
		return SecurityPlan{}, errors.New("consumer group suffix must be a lowercase slug")
	}
	var contract Contract
	if err := json.Unmarshal(contractJSON, &contract); err != nil {
		return SecurityPlan{}, fmt.Errorf("decode topic contract: %w", err)
	}
	var profile *RuntimeProfile
	for i := range contract.RuntimeProfiles {
		if contract.RuntimeProfiles[i].Name == profileName {
			profile = &contract.RuntimeProfiles[i]
			break
		}
	}
	if profile == nil || len(profile.ServicePrincipals) == 0 {
		return SecurityPlan{}, fmt.Errorf("profile %q has no declared service principals", profileName)
	}
	// Reuse topic-plan validation so security cannot be applied to an unbounded
	// or production retention profile.
	if _, err := BuildPlan(contractJSON, profileName, namespace); err != nil {
		return SecurityPlan{}, err
	}

	plan := SecurityPlan{Profile: profileName, Namespace: namespace}
	roleUser := make(map[string]string, len(profile.ServicePrincipals))
	roles := make([]string, 0, len(profile.ServicePrincipals))
	for role, user := range profile.ServicePrincipals {
		if role == "" || user == "" || strings.ContainsAny(user, ":* ") {
			return SecurityPlan{}, fmt.Errorf("profile contains invalid service principal for role %q", role)
		}
		roleUser[role] = user
		roles = append(roles, role)
		plan.Principals = append(plan.Principals, SecurityPrincipal{Role: role, Username: user})
	}

	addACL := func(resourceType, resourceName, role, operation string) {
		user, ok := roleUser[role]
		if !ok || resourceName == "" {
			return
		}
		plan.ACLs = append(plan.ACLs, SecurityACL{ResourceType: resourceType, ResourceName: resourceName,
			Principal: "User:" + user, Operation: operation})
	}
	for _, topic := range contract.Topics {
		if strings.Contains(topic.PhysicalName, "{source_context_id}") {
			continue
		}
		topicNames := []string{topic.PhysicalName}
		if strings.Contains(topic.PhysicalName, "{domain}") {
			if len(topic.Domains) == 0 {
				return SecurityPlan{}, fmt.Errorf("topic %q has no domain expansion", topic.Name)
			}
			topicNames = make([]string, 0, len(topic.Domains))
			for _, domain := range topic.Domains {
				topicNames = append(topicNames, strings.ReplaceAll(topic.PhysicalName, "{domain}", domain))
			}
		}
		for _, name := range topicNames {
			name = strings.ReplaceAll(name, "{namespace}", namespace)
			if strings.ContainsAny(name, "{}") {
				return SecurityPlan{}, fmt.Errorf("topic %q has unsupported physical-name placeholder", topic.Name)
			}
			for contractRole, operations := range topic.ACL {
				role := strings.TrimPrefix(contractRole, "tuba-")
				for _, operation := range operations {
					if operation != "READ" && operation != "WRITE" && operation != "DESCRIBE" {
						return SecurityPlan{}, fmt.Errorf("topic %q declares unsupported ACL operation %q", topic.Name, operation)
					}
					addACL("topic", name, role, operation)
				}
			}
			for _, consumer := range topic.Consumers {
				groupTemplates := []string{consumer.ConsumerGroup}
				if strings.Contains(consumer.ConsumerGroup, "<domain>") {
					groupTemplates = make([]string, 0, len(topic.Domains))
					for _, domain := range topic.Domains {
						groupTemplates = append(groupTemplates, strings.ReplaceAll(consumer.ConsumerGroup, "<domain>", domain))
					}
				}
				for _, groupTemplate := range groupTemplates {
					group := expandGroup(groupTemplate, namespace, groupSuffix)
					if group == "" {
						continue
					}
					for _, role := range roles {
						contractRole := "tuba-" + role
						if strings.HasPrefix(groupTemplate, contractRole+"-") && containsString(topic.ACL[contractRole], "READ") {
							addACL("group", group, role, "READ")
						}
					}
				}
			}
		}
	}
	sortSecurityPlan(&plan)
	if len(plan.ACLs) == 0 {
		return SecurityPlan{}, errors.New("security plan resolved to no ACLs")
	}
	return plan, nil
}

func expandGroup(value, namespace, suffix string) string {
	if strings.Contains(value, "<first-16-hex-of-sha256-topic>") {
		return "" // Per-source groups are derived only after immutable context registration.
	}
	value = strings.ReplaceAll(value, "<namespace>", namespace)
	value = strings.ReplaceAll(value, "[-<consumer_group_suffix>]", suffixPart(suffix))
	if strings.ContainsAny(value, "<>[]") {
		return ""
	}
	return value
}

func suffixPart(suffix string) string {
	if suffix == "" {
		return ""
	}
	return "-" + suffix
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func sortSecurityPlan(plan *SecurityPlan) {
	sort.Slice(plan.Principals, func(i, j int) bool { return plan.Principals[i].Role < plan.Principals[j].Role })
	sort.Slice(plan.ACLs, func(i, j int) bool {
		a, b := plan.ACLs[i], plan.ACLs[j]
		if a.ResourceType != b.ResourceType {
			return a.ResourceType < b.ResourceType
		}
		if a.ResourceName != b.ResourceName {
			return a.ResourceName < b.ResourceName
		}
		if a.Principal != b.Principal {
			return a.Principal < b.Principal
		}
		return a.Operation < b.Operation
	})
	unique := plan.ACLs[:0]
	for _, acl := range plan.ACLs {
		if len(unique) == 0 || unique[len(unique)-1] != acl {
			unique = append(unique, acl)
		}
	}
	plan.ACLs = unique
}

// GenerateSaltedPassword returns the SCRAM-SHA-512 salted password and a
// cryptographically random salt suitable for Kafka AlterUserScramCredentials.
func GenerateSaltedPassword(password string) (salt, saltedPassword []byte, err error) {
	if len(password) < 32 {
		return nil, nil, errors.New("service password must contain at least 32 bytes")
	}
	salt = make([]byte, 32)
	if _, err = rand.Read(salt); err != nil {
		return nil, nil, errors.New("generate SCRAM salt")
	}
	const iterations = 4096
	return salt, pbkdf2SHA512([]byte(password), salt, iterations), nil
}

func pbkdf2SHA512(password, salt []byte, iterations int) []byte {
	key := []byte(password)
	block := make([]byte, 0, sha512.Size)
	mac := hmac.New(sha512.New, key)
	mac.Write(salt)
	mac.Write([]byte{0, 0, 0, 1})
	u := mac.Sum(nil)
	block = append(block, u...)
	for i := 1; i < iterations; i++ {
		mac = hmac.New(sha512.New, key)
		mac.Write(u)
		u = mac.Sum(nil)
		for j := range block {
			block[j] ^= u[j]
		}
	}
	return block
}

func SecurityPasswordEnv(role string) string {
	return "TUBA_" + strings.ToUpper(strings.ReplaceAll(role, "-", "_")) + "_KAFKA_PASSWORD"
}

func SecurityUsernameEnv(role string) string {
	return "TUBA_" + strings.ToUpper(strings.ReplaceAll(role, "-", "_")) + "_KAFKA_USER"
}
