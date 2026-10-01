package sourceadapter

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// FilterPolicy is a versioned admission filter evaluated by the source adapter
// before an event is delivered to ingest. Only shadow mode exists: matching
// events are counted per rule with their reason code, and delivery continues
// unchanged. Counting at the adapter reduces nothing upstream — the source
// still ships every byte to the ingress Kafka topic — so these counts must
// never be reported as source-side traffic reduction.
//
// Enforcement is deliberately not implemented. Publishing an enforced rule
// requires representative shadow counts first (the COL-06 gate), so Validate
// rejects any mode other than shadow; there is no code path that can drop a
// filtered event. Rollback is the versioned config file: every change carries
// a new version, and reverting redeploys the previous one.
type FilterPolicy struct {
	PolicyID string       `json:"policy_id"`
	Version  string       `json:"version"`
	Mode     string       `json:"mode"`
	Rules    []FilterRule `json:"rules"`
	// Protected lists scenario-protection conditions. An event matching any of
	// them is exempt from filtering and counted separately, so a rule that
	// would swallow a protected scenario (for example a Windows 1102 log-clear)
	// is visible instead of silently matching.
	Protected []FilterRule `json:"protected,omitempty"`
}

// FilterRule matches when every equality clause holds. Clauses are ANDed, so
// map iteration order does not affect the result.
type FilterRule struct {
	ID     string            `json:"id"`
	Reason string            `json:"reason"`
	Equals map[string]string `json:"equals"`
}

// FilterDecision reports how a policy classified one event. RuleID/Reason are
// set only when a filtering rule matched.
type FilterDecision struct {
	Protected bool
	RuleID    string
	Reason    string
}

var (
	// Identifiers surface as Prometheus label values and the telemetry
	// registry renders "-" as "_", so they are restricted to characters that
	// survive that rendering unchanged.
	filterPolicyIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_]{0,62}$`)
	filterReasonPattern   = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,63}$`)
	filterVersionPattern  = regexp.MustCompile(`^[A-Za-z0-9._]{1,64}$`)
)

func (p *FilterPolicy) Validate() error {
	if !filterPolicyIDPattern.MatchString(p.PolicyID) {
		return fmt.Errorf("filter policy_id %q must match %s", p.PolicyID, filterPolicyIDPattern)
	}
	if !filterVersionPattern.MatchString(p.Version) {
		return fmt.Errorf("filter policy version %q must match %s", p.Version, filterVersionPattern)
	}
	if p.Mode != "shadow" {
		return fmt.Errorf("filter mode %q is not publishable: only shadow counting exists, enforcement requires representative shadow counts first", p.Mode)
	}
	if err := validateFilterRules("rules", p.Rules); err != nil {
		return err
	}
	return validateFilterRules("protected", p.Protected)
}

func validateFilterRules(section string, rules []FilterRule) error {
	if len(rules) > 64 {
		return fmt.Errorf("filter %s may contain at most 64 rules", section)
	}
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		if !filterPolicyIDPattern.MatchString(rule.ID) {
			return fmt.Errorf("filter %s rule id %q must match %s", section, rule.ID, filterPolicyIDPattern)
		}
		if _, ok := seen[rule.ID]; ok {
			return fmt.Errorf("filter %s rule id %q is duplicated", section, rule.ID)
		}
		seen[rule.ID] = struct{}{}
		if !filterReasonPattern.MatchString(rule.Reason) {
			return fmt.Errorf("filter %s rule %q reason %q must match %s", section, rule.ID, rule.Reason, filterReasonPattern)
		}
		if len(rule.Equals) == 0 {
			return fmt.Errorf("filter %s rule %q needs at least one equality clause", section, rule.ID)
		}
		if len(rule.Equals) > 32 {
			return fmt.Errorf("filter %s rule %q may contain at most 32 equality clauses", section, rule.ID)
		}
		for path, value := range rule.Equals {
			if strings.TrimSpace(path) == "" || len(path) > 256 {
				return fmt.Errorf("filter %s rule %q has an invalid field path", section, rule.ID)
			}
			if len(value) > 512 {
				return fmt.Errorf("filter %s rule %q equality value exceeds 512 bytes", section, rule.ID)
			}
		}
	}
	return nil
}

// Evaluate classifies one event payload. Protected conditions win over
// filtering rules; among filtering rules the first match in config order wins
// so a decision always names exactly one rule and reason code.
func (p *FilterPolicy) Evaluate(payload []byte) (FilterDecision, error) {
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		return FilterDecision{}, fmt.Errorf("filter evaluation needs a JSON object event: %w", err)
	}
	for _, rule := range p.Protected {
		if filterRuleMatches(rule, event) {
			return FilterDecision{Protected: true, RuleID: rule.ID, Reason: rule.Reason}, nil
		}
	}
	for _, rule := range p.Rules {
		if filterRuleMatches(rule, event) {
			return FilterDecision{RuleID: rule.ID, Reason: rule.Reason}, nil
		}
	}
	return FilterDecision{}, nil
}

func filterRuleMatches(rule FilterRule, event map[string]any) bool {
	for path, want := range rule.Equals {
		got, ok := filterFieldValue(event, path).(string)
		if !ok || got != want {
			return false
		}
	}
	return true
}

// filterFieldValue resolves a dotted path against an event. Beat payloads mix
// nested objects (event.dataset) with literal dotted keys (id.orig_h), so at
// each level the longest literal key prefix wins before descending.
func filterFieldValue(event map[string]any, path string) any {
	parts := strings.Split(path, ".")
	value := any(event)
	for len(parts) > 0 {
		m, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		next, found := any(nil), false
		for take := len(parts); take > 0 && !found; take-- {
			next, found = m[strings.Join(parts[:take], ".")]
			if found {
				parts = parts[take:]
			}
		}
		if !found {
			return nil
		}
		value = next
	}
	return value
}

// Shadow-match and protection counters carry the policy identity and rule as
// Prometheus labels. The validation patterns above restrict every label value
// to characters that need no escaping, so the registry's flat name→line
// rendering stays valid exposition.
func (p *FilterPolicy) shadowMatchMetric(decision FilterDecision) string {
	return fmt.Sprintf(`tuba_source_adapter_filter_shadow_matches_total{policy_id=%q,version=%q,rule_id=%q,reason=%q}`,
		p.PolicyID, p.Version, decision.RuleID, decision.Reason)
}

func (p *FilterPolicy) protectedMetric(decision FilterDecision) string {
	return fmt.Sprintf(`tuba_source_adapter_filter_protected_total{policy_id=%q,version=%q,rule_id=%q,reason=%q}`,
		p.PolicyID, p.Version, decision.RuleID, decision.Reason)
}
