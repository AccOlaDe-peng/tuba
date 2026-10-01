package entity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Rule snapshot kinds. The snapshot content is the exact rule payload that
// produced a class of results; the version is immutable once registered.
const (
	// RuleKindRelationMapping versions the rules that derive entity
	// relations (which relation types exist, their cardinality, how events
	// assert them).
	RuleKindRelationMapping = "relation_mapping"
)

// RelationMappingVersionV1 is the initial relation mapping rule version,
// folded into every relation row's resolution snapshot.
const RelationMappingVersionV1 = "1.0.0"

var (
	ruleKindPattern    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	ruleVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

	ErrInvalidRuleKind       = errors.New("rule kind must be a lowercase identifier")
	ErrInvalidRuleVersion    = errors.New("rule version must be semantic major.minor.patch")
	ErrEmptyRuleContent      = errors.New("rule snapshot content must be a non-empty JSON object")
	ErrRuleSnapshotConflict  = errors.New("rule version already registered with different content")
	ErrRuleSnapshotNotStored = errors.New("rule snapshot is not registered")
)

// RuleSnapshot is one immutable registered rule version.
type RuleSnapshot struct {
	OrganizationID string          `json:"organization_id"`
	RuleKind       string          `json:"rule_kind"`
	RuleVersion    string          `json:"rule_version"`
	Content        json.RawMessage `json:"content"`
	ContentSHA256  string          `json:"content_sha256"`
	CreatedAt      time.Time       `json:"created_at"`
}

// RuleSnapshots persists immutable rule versions in PostgreSQL.
type RuleSnapshots struct {
	Pool *pgxpool.Pool
}

func NewRuleSnapshots(pool *pgxpool.Pool) *RuleSnapshots {
	return &RuleSnapshots{Pool: pool}
}

// ValidateRuleRef checks kind and version formats fail-closed.
func ValidateRuleRef(kind, version string) error {
	if !ruleKindPattern.MatchString(kind) {
		return fmt.Errorf("%w: %q", ErrInvalidRuleKind, kind)
	}
	if !ruleVersionPattern.MatchString(version) {
		return fmt.Errorf("%w: %q", ErrInvalidRuleVersion, version)
	}
	return nil
}

// canonicalRuleContent compacts the JSON payload so the stored hash does not
// depend on caller formatting.
func canonicalRuleContent(content json.RawMessage) ([]byte, string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, content); err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrEmptyRuleContent, err)
	}
	canonical := buf.Bytes()
	if !bytes.HasPrefix(canonical, []byte("{")) {
		return nil, "", fmt.Errorf("%w: must be a JSON object", ErrEmptyRuleContent)
	}
	sum := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(sum[:]), nil
}

// Register stores a rule version idempotently: the same (kind, version,
// content) always succeeds and returns the stored snapshot. The same version
// with different content is rejected fail-closed — upgrading rules requires a
// new version, never an in-place rewrite.
func (rs *RuleSnapshots) Register(ctx context.Context, organizationID, kind, version string, content json.RawMessage) (RuleSnapshot, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return RuleSnapshot{}, fmt.Errorf("invalid organization id: %w", err)
	}
	if err := ValidateRuleRef(kind, version); err != nil {
		return RuleSnapshot{}, err
	}
	canonical, hash, err := canonicalRuleContent(content)
	if err != nil {
		return RuleSnapshot{}, err
	}
	snap := RuleSnapshot{
		OrganizationID: orgID.String(), RuleKind: kind, RuleVersion: version,
		Content: json.RawMessage(canonical), ContentSHA256: hash,
	}
	var storedHash string
	err = rs.Pool.QueryRow(ctx, `
		INSERT INTO rule_snapshots(organization_id, rule_kind, rule_version, content, content_sha256)
		VALUES($1, $2, $3, $4, $5)
		ON CONFLICT (organization_id, rule_kind, rule_version) DO NOTHING
		RETURNING content_sha256, created_at`,
		orgID, kind, version, string(canonical), hash).Scan(&storedHash, &snap.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		err = rs.Pool.QueryRow(ctx, `
			SELECT content_sha256, created_at FROM rule_snapshots
			WHERE organization_id = $1 AND rule_kind = $2 AND rule_version = $3`,
			orgID, kind, version).Scan(&storedHash, &snap.CreatedAt)
	}
	if err != nil {
		return RuleSnapshot{}, err
	}
	if storedHash != hash {
		return RuleSnapshot{}, fmt.Errorf("%w: %s %s", ErrRuleSnapshotConflict, kind, version)
	}
	return snap, nil
}

// Load returns the registered snapshot; an unregistered version is a hard
// error so results never reference rules that cannot be explained later.
func (rs *RuleSnapshots) Load(ctx context.Context, organizationID, kind, version string) (RuleSnapshot, error) {
	orgID, err := uuid.Parse(strings.TrimSpace(organizationID))
	if err != nil {
		return RuleSnapshot{}, fmt.Errorf("invalid organization id: %w", err)
	}
	if err := ValidateRuleRef(kind, version); err != nil {
		return RuleSnapshot{}, err
	}
	var snap RuleSnapshot
	var content string
	err = rs.Pool.QueryRow(ctx, `
		SELECT content::text, content_sha256, created_at FROM rule_snapshots
		WHERE organization_id = $1 AND rule_kind = $2 AND rule_version = $3`,
		orgID, kind, version).Scan(&content, &snap.ContentSHA256, &snap.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RuleSnapshot{}, fmt.Errorf("%w: %s %s", ErrRuleSnapshotNotStored, kind, version)
	}
	if err != nil {
		return RuleSnapshot{}, err
	}
	snap.OrganizationID = orgID.String()
	snap.RuleKind = kind
	snap.RuleVersion = version
	snap.Content = json.RawMessage(content)
	return snap, nil
}
