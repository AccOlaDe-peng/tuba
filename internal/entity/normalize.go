// Package entity implements Account/Device identity space registration,
// strong/weak identifier priority, normalization and stable entity.id
// generation as defined by contracts/ids.md and docs/DESIGN-BASELINE.md §5.
package entity

import (
	"errors"
	"fmt"
	"net/netip"
	"regexp"
	"strings"
)

// Kind classifies an identifier observed for an entity.
type Kind string

const (
	KindSID        Kind = "sid"
	KindGUID       Kind = "guid"
	KindDeviceUUID Kind = "device_uuid"
	KindAgentID    Kind = "agent_id"
	KindNTName     Kind = "ntname"
	KindUPN        Kind = "upn"
	KindEmail      Kind = "email"
	KindUsername   Kind = "username"
	KindHostname   Kind = "hostname"
	KindIP         Kind = "ip"
)

// Strength is the identity_strength of the resolved canonical identifier.
type Strength string

const (
	StrengthStrong Strength = "strong"
	StrengthWeak   Strength = "weak"
)

// priority ranks identifier kinds; higher wins during canonical selection.
// Strong identifiers (SID, directory object GUID, device UUID, stable agent
// ID) always outrank weak ones (username, hostname, UPN, email, IP), which
// may be reused by a different real identity over time.
var priority = map[Kind]int{
	KindSID:        100,
	KindGUID:       95,
	KindDeviceUUID: 90,
	KindAgentID:    85,
	KindNTName:     50,
	KindUPN:        45,
	KindEmail:      40,
	KindUsername:   30,
	KindHostname:   30,
	KindIP:         10,
}

// Identifier is a single observed identifier, Value as reported by the source.
type Identifier struct {
	Kind  Kind   `json:"kind"`
	Value string `json:"value"`
}

// Normalized is an identifier after validation and canonicalization.
type Normalized struct {
	Kind      Kind     `json:"kind"`
	Value     string   `json:"value"` // original, preserved as reported
	Canonical string   `json:"canonical"`
	Strength  Strength `json:"strength"`
}

var (
	ErrInvalidIdentifier = errors.New("identifier failed normalization")
	ErrNoIdentifier      = errors.New("at least one identifier is required")
	ErrAmbiguousInput    = errors.New("conflicting identifiers of the same kind")
)

var (
	sidPattern       = regexp.MustCompile(`^[Ss]-\d+(-\d+)+$`)
	guidPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	agentPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{7,127}$`)
	// A single trailing "$" is allowed: AD machine accounts (WIN-139$) are
	// legitimate usernames observed in security events.
	usernamePattern  = regexp.MustCompile(`^[a-z0-9._-]{1,104}\$?$`)
	spaceNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
)

// NormalizeSpaceName canonicalizes an identity space name. Fail-closed:
// anything outside the restricted alphabet is rejected.
func NormalizeSpaceName(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if !spaceNamePattern.MatchString(name) {
		return "", fmt.Errorf("%w: identity space name %q", ErrInvalidIdentifier, name)
	}
	return name, nil
}

// Normalize validates one identifier and returns its canonical form. The
// original value is preserved alongside. Unknown kinds and malformed values
// are rejected fail-closed instead of being guessed at.
func Normalize(id Identifier) (Normalized, error) {
	out := Normalized{Kind: id.Kind, Value: id.Value}
	value := strings.TrimSpace(id.Value)
	if value == "" {
		return out, fmt.Errorf("%w: empty %s", ErrInvalidIdentifier, id.Kind)
	}
	var canonical string
	var err error
	switch id.Kind {
	case KindSID:
		canonical, err = normalizeSID(value)
	case KindGUID, KindDeviceUUID:
		canonical, err = normalizeGUID(value)
	case KindAgentID:
		canonical, err = normalizeAgentID(value)
	case KindNTName:
		canonical, err = normalizeNTName(value)
	case KindUPN, KindEmail:
		canonical, err = normalizeUPN(value)
	case KindUsername:
		canonical, err = normalizeUsername(value)
	case KindHostname:
		canonical, err = normalizeHostname(value)
	case KindIP:
		canonical, err = normalizeIP(value)
	default:
		return out, fmt.Errorf("%w: unknown kind %q", ErrInvalidIdentifier, id.Kind)
	}
	if err != nil {
		return out, err
	}
	out.Canonical = canonical
	out.Strength = strengthOf(id.Kind)
	return out, nil
}

func strengthOf(kind Kind) Strength {
	if priority[kind] >= priority[KindAgentID] {
		return StrengthStrong
	}
	return StrengthWeak
}

// Strong reports whether the kind is a strong identifier.
func (k Kind) Strong() bool {
	_, ok := priority[k]
	return ok && strengthOf(k) == StrengthStrong
}

// normalizeSID lowercases and validates a Windows security identifier.
func normalizeSID(value string) (string, error) {
	value = strings.ToLower(value)
	if !sidPattern.MatchString(value) || len(value) > 256 {
		return "", fmt.Errorf("%w: sid %q", ErrInvalidIdentifier, value)
	}
	return value, nil
}

// normalizeGUID strips optional braces, lowercases and validates the
// canonical 8-4-4-4-12 form.
func normalizeGUID(value string) (string, error) {
	value = strings.ToLower(strings.Trim(strings.TrimSpace(value), "{}"))
	if !guidPattern.MatchString(value) {
		return "", fmt.Errorf("%w: guid %q", ErrInvalidIdentifier, value)
	}
	return value, nil
}

func normalizeAgentID(value string) (string, error) {
	value = strings.ToLower(value)
	if !agentPattern.MatchString(value) {
		return "", fmt.Errorf("%w: agent id %q", ErrInvalidIdentifier, value)
	}
	return value, nil
}

// normalizeNTName splits a Windows DOMAIN\user into lowercase namespace and
// name, joined back with a single backslash.
func normalizeNTName(value string) (string, error) {
	parts := strings.Split(value, `\`)
	if len(parts) != 2 {
		return "", fmt.Errorf("%w: ntname %q must be DOMAIN\\user", ErrInvalidIdentifier, value)
	}
	domain, err := normalizeHostnameLabel(parts[0])
	if err != nil {
		return "", fmt.Errorf("%w: ntname domain %q", ErrInvalidIdentifier, parts[0])
	}
	user, err := normalizeUsername(parts[1])
	if err != nil {
		return "", err
	}
	return domain + `\` + user, nil
}

// normalizeUPN lowercases the domain part; AD account names are
// case-insensitive, so the local part is lowercased as well while the
// original is preserved on Normalized.Value.
func normalizeUPN(value string) (string, error) {
	parts := strings.Split(value, "@")
	if len(parts) != 2 {
		return "", fmt.Errorf("%w: upn %q must be user@domain", ErrInvalidIdentifier, value)
	}
	user, err := normalizeUsername(parts[0])
	if err != nil {
		return "", err
	}
	domain, err := normalizeHostname(parts[1])
	if err != nil {
		return "", fmt.Errorf("%w: upn domain %q", ErrInvalidIdentifier, parts[1])
	}
	return user + "@" + domain, nil
}

func normalizeUsername(value string) (string, error) {
	value = strings.ToLower(value)
	if !usernamePattern.MatchString(value) || strings.ContainsAny(value, `\@`) {
		return "", fmt.Errorf("%w: username %q", ErrInvalidIdentifier, value)
	}
	return value, nil
}

// normalizeHostname lowercases and strips trailing dots; it never appends a
// domain suffix.
func normalizeHostname(value string) (string, error) {
	value = strings.ToLower(strings.TrimRight(value, "."))
	if value == "" || len(value) > 253 {
		return "", fmt.Errorf("%w: hostname %q", ErrInvalidIdentifier, value)
	}
	for _, label := range strings.Split(value, ".") {
		if _, err := normalizeHostnameLabel(label); err != nil {
			return "", fmt.Errorf("%w: hostname %q", ErrInvalidIdentifier, value)
		}
	}
	return value, nil
}

func normalizeHostnameLabel(label string) (string, error) {
	label = strings.ToLower(label)
	if label == "" || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
		return "", fmt.Errorf("%w: label %q", ErrInvalidIdentifier, label)
	}
	for _, r := range label {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return "", fmt.Errorf("%w: label %q", ErrInvalidIdentifier, label)
		}
	}
	return label, nil
}

// normalizeIP renders an IP literal in its standard text form.
func normalizeIP(value string) (string, error) {
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return "", fmt.Errorf("%w: ip %q", ErrInvalidIdentifier, value)
	}
	return addr.String(), nil
}

// CanonicalKey derives the storage/canonical key for an identifier:
// "<kind>:<normalized>".
func (n Normalized) CanonicalKey() string {
	return string(n.Kind) + ":" + n.Canonical
}

// SelectCanonical adjudicates a set of observed identifiers down to the one
// canonical identifier. Strong identifiers always outrank weak ones; between
// kinds the fixed priority table decides. Two distinct values of the same
// kind are contradictory input and rejected fail-closed.
func SelectCanonical(ids []Identifier) (Normalized, []Normalized, error) {
	if len(ids) == 0 {
		return Normalized{}, nil, ErrNoIdentifier
	}
	normalized := make([]Normalized, 0, len(ids))
	byKind := map[Kind]string{}
	for _, id := range ids {
		n, err := Normalize(id)
		if err != nil {
			return Normalized{}, nil, err
		}
		if existing, ok := byKind[n.Kind]; ok && existing != n.Canonical {
			return Normalized{}, nil, fmt.Errorf("%w: kind %s has both %q and %q", ErrAmbiguousInput, n.Kind, existing, n.Canonical)
		}
		if _, ok := byKind[n.Kind]; !ok {
			byKind[n.Kind] = n.Canonical
			normalized = append(normalized, n)
		}
	}
	best := normalized[0]
	for _, n := range normalized[1:] {
		if priority[n.Kind] > priority[best.Kind] ||
			(priority[n.Kind] == priority[best.Kind] && n.Kind < best.Kind) {
			best = n
		}
	}
	return best, normalized, nil
}
