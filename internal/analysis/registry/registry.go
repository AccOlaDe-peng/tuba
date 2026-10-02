// Package registry is the unified, fail-closed registration point for the
// three analysis module kinds (feature, baseline, detection). Every module
// must register a descriptor carrying its semantic version, its explicit
// versioned dependencies on other modules, its input contract, and its
// quality gate (declared test-coverage floor plus test evidence) before it
// may run. Persistence of immutable descriptor versions reuses the E03
// rule_snapshots facility (internal/entity.RuleSnapshots) — descriptors are
// stored as rule snapshots of kind analysis_feature / analysis_baseline /
// analysis_detection, never in a parallel store.
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"tuba/product/internal/entity"
)

// Kind identifies one of the three analysis modules.
type Kind string

const (
	KindFeature   Kind = "feature"
	KindBaseline  Kind = "baseline"
	KindDetection Kind = "detection"
)

// Rule snapshot kinds under which descriptor versions are persisted through
// the E03 rule_snapshots facility.
const (
	RuleKindFeature   = "analysis_feature"
	RuleKindBaseline  = "analysis_baseline"
	RuleKindDetection = "analysis_detection"
)

var (
	idPattern      = regexp.MustCompile(`^[a-z][a-z0-9]*([.-][a-z0-9]+)*$`)
	versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

	ErrInvalidKind        = errors.New("module kind must be feature, baseline, or detection")
	ErrInvalidID          = errors.New("module id must be a lowercase dotted identifier")
	ErrInvalidVersion     = errors.New("module version must be semantic major.minor.patch")
	ErrInvalidConstraint  = errors.New("dependency constraint must be an exact version or >=version")
	ErrInvalidInput       = errors.New("input contract requires a schema id and a semantic version")
	ErrInvalidQuality     = errors.New("quality gate requires coverage within [1, 100] and non-empty test evidence")
	ErrUnknownDependency  = errors.New("dependency module is not registered")
	ErrUnsatisfiedVersion = errors.New("registered dependency version does not satisfy the constraint")
	ErrDescriptorConflict = errors.New("module version already registered with different content")
)

// Dependency pins a required module by id with an explicit version
// constraint: an exact "major.minor.patch" or a floor ">=major.minor.patch".
type Dependency struct {
	ID         string `json:"id"`
	Constraint string `json:"constraint"`
}

// InputContract declares what the module consumes.
type InputContract struct {
	Schema  string `json:"schema"`
	Version string `json:"version"`
}

// Quality is the registration-time quality gate: the module declares the
// minimum test coverage it maintains and where the proof lives.
type Quality struct {
	MinCoveragePercent int    `json:"min_coverage_percent"`
	TestEvidence       string `json:"test_evidence"`
}

// Descriptor is the complete registration record of one module version.
type Descriptor struct {
	Kind         Kind           `json:"kind"`
	ID           string         `json:"id"`
	Version      string         `json:"version"`
	Dependencies []Dependency   `json:"dependencies,omitempty"`
	Input        InputContract  `json:"input"`
	Quality      Quality        `json:"quality"`
}

// Registry holds the registered module descriptors in memory; immutable
// persistence is delegated to entity.RuleSnapshots.
type Registry struct {
	modules map[Kind]map[string]map[string]Descriptor // kind -> id -> version
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{modules: map[Kind]map[string]map[string]Descriptor{}}
}

func validVersion(version string) bool { return versionPattern.MatchString(version) }

// parseVersion decomposes a validated semantic version into comparable parts.
func parseVersion(version string) ([3]int, error) {
	var parts [3]int
	if !validVersion(version) {
		return parts, fmt.Errorf("%w: %q", ErrInvalidVersion, version)
	}
	for i, piece := range strings.Split(version, ".") {
		n, err := strconv.Atoi(piece)
		if err != nil {
			return parts, fmt.Errorf("%w: %q", ErrInvalidVersion, version)
		}
		parts[i] = n
	}
	return parts, nil
}

func compareVersions(a, b [3]int) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// validateConstraint checks the constraint shape: exact version or >=floor.
func validateConstraint(constraint string) error {
	floor := strings.TrimPrefix(constraint, ">=")
	if !validVersion(floor) {
		return fmt.Errorf("%w: %q", ErrInvalidConstraint, constraint)
	}
	return nil
}

// satisfies reports whether version meets the constraint (exact or >=floor).
func satisfies(version, constraint string) (bool, error) {
	if err := validateConstraint(constraint); err != nil {
		return false, err
	}
	floor := strings.TrimPrefix(constraint, ">=")
	have, err := parseVersion(version)
	if err != nil {
		return false, err
	}
	want, err := parseVersion(floor)
	if err != nil {
		return false, err
	}
	if constraint == floor {
		return have == want, nil
	}
	return compareVersions(have, want) >= 0, nil
}

// validate enforces the quality gate and metadata requirements fail-closed;
// dependency resolution against already-registered modules happens in
// Register.
func validate(d Descriptor) error {
	switch d.Kind {
	case KindFeature, KindBaseline, KindDetection:
	default:
		return fmt.Errorf("%w: %q", ErrInvalidKind, d.Kind)
	}
	if !idPattern.MatchString(d.ID) {
		return fmt.Errorf("%w: %q", ErrInvalidID, d.ID)
	}
	if !validVersion(d.Version) {
		return fmt.Errorf("%w: %q", ErrInvalidVersion, d.Version)
	}
	for _, dep := range d.Dependencies {
		if !idPattern.MatchString(dep.ID) {
			return fmt.Errorf("%w: dependency %q", ErrInvalidID, dep.ID)
		}
		if dep.ID == d.ID {
			return fmt.Errorf("%w: self dependency %q", ErrUnknownDependency, dep.ID)
		}
		if err := validateConstraint(dep.Constraint); err != nil {
			return err
		}
	}
	if !idPattern.MatchString(d.Input.Schema) || !validVersion(d.Input.Version) {
		return fmt.Errorf("%w: %s@%s", ErrInvalidInput, d.Input.Schema, d.Input.Version)
	}
	if d.Quality.MinCoveragePercent < 1 || d.Quality.MinCoveragePercent > 100 || strings.TrimSpace(d.Quality.TestEvidence) == "" {
		return fmt.Errorf("%w: coverage=%d evidence=%q", ErrInvalidQuality, d.Quality.MinCoveragePercent, d.Quality.TestEvidence)
	}
	return nil
}

// latestVersion returns the highest registered version of a module id.
func (r *Registry) latestVersion(kind Kind, id string) (string, bool) {
	versions := r.modules[kind][id]
	if len(versions) == 0 {
		return "", false
	}
	latest := ""
	var latestParts [3]int
	for version := range versions {
		parts, err := parseVersion(version)
		if err != nil {
			continue
		}
		if latest == "" || compareVersions(parts, latestParts) > 0 {
			latest, latestParts = version, parts
		}
	}
	return latest, true
}

// dependencyTarget finds the registered module (any kind) with the given id.
func (r *Registry) dependencyTarget(id string) (string, bool) {
	latest := ""
	var latestParts [3]int
	found := false
	for _, kind := range []Kind{KindFeature, KindBaseline, KindDetection} {
		version, ok := r.latestVersion(kind, id)
		if !ok {
			continue
		}
		parts, err := parseVersion(version)
		if err != nil {
			continue
		}
		if !found || compareVersions(parts, latestParts) > 0 {
			latest, latestParts, found = version, parts, true
		}
	}
	return latest, found
}

// canonical renders a descriptor deterministically for equality checks and
// snapshot content.
func canonical(d Descriptor) (json.RawMessage, error) {
	normalized := d
	if normalized.Dependencies == nil {
		normalized.Dependencies = []Dependency{}
	}
	sorted := append([]Dependency(nil), normalized.Dependencies...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ID != sorted[j].ID {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Constraint < sorted[j].Constraint
	})
	normalized.Dependencies = sorted
	raw, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	return json.RawMessage(buf.Bytes()), nil
}

// Register validates a descriptor (metadata, quality gate, and dependency
// version constraints against already-registered modules) and records it.
// Re-registering an identical descriptor is idempotent; the same version
// with different content is rejected fail-closed — upgrading a module
// requires a new version, mirroring the rule_snapshots immutability rule.
func (r *Registry) Register(d Descriptor) error {
	if err := validate(d); err != nil {
		return err
	}
	for _, dep := range d.Dependencies {
		latest, found := r.dependencyTarget(dep.ID)
		if !found {
			return fmt.Errorf("%w: %s required by %s", ErrUnknownDependency, dep.ID, d.ID)
		}
		ok, err := satisfies(latest, dep.Constraint)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: %s has %s, needs %s", ErrUnsatisfiedVersion, dep.ID, latest, dep.Constraint)
		}
	}
	incoming, err := canonical(d)
	if err != nil {
		return err
	}
	byID := r.modules[d.Kind]
	if byID == nil {
		byID = map[string]map[string]Descriptor{}
		r.modules[d.Kind] = byID
	}
	byVersion := byID[d.ID]
	if byVersion == nil {
		byVersion = map[string]Descriptor{}
		byID[d.ID] = byVersion
	}
	if existing, ok := byVersion[d.Version]; ok {
		stored, err := canonical(existing)
		if err != nil {
			return err
		}
		if !bytes.Equal(stored, incoming) {
			return fmt.Errorf("%w: %s %s %s", ErrDescriptorConflict, d.Kind, d.ID, d.Version)
		}
		return nil
	}
	byVersion[d.Version] = d
	return nil
}

// Lookup returns the registered descriptor for an exact module version.
func (r *Registry) Lookup(kind Kind, id, version string) (Descriptor, bool) {
	d, ok := r.modules[kind][id][version]
	return d, ok
}

// Resolve returns the descriptor of the latest registered version of a
// module — the deterministic resolution target for unversioned references.
func (r *Registry) Resolve(kind Kind, id string) (Descriptor, bool) {
	version, ok := r.latestVersion(kind, id)
	if !ok {
		return Descriptor{}, false
	}
	return r.modules[kind][id][version], true
}

// RuleKindFor maps a module kind onto its E03 rule_snapshots kind.
func RuleKindFor(kind Kind) (string, error) {
	switch kind {
	case KindFeature:
		return RuleKindFeature, nil
	case KindBaseline:
		return RuleKindBaseline, nil
	case KindDetection:
		return RuleKindDetection, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidKind, kind)
	}
}

// SnapshotContent renders a descriptor as the canonical JSON object stored
// in rule_snapshots.
func SnapshotContent(d Descriptor) (json.RawMessage, error) {
	if err := validate(d); err != nil {
		return nil, err
	}
	return canonical(d)
}

// RegisterSnapshot persists a descriptor version immutably through the E03
// rule_snapshots facility: replaying identical content returns the stored
// snapshot, and the same version with different content is rejected by
// entity.RuleSnapshots — history stays explainable, upgrades need a new
// version.
func RegisterSnapshot(ctx context.Context, snapshots *entity.RuleSnapshots, organizationID string, d Descriptor) (entity.RuleSnapshot, error) {
	kind, err := RuleKindFor(d.Kind)
	if err != nil {
		return entity.RuleSnapshot{}, err
	}
	content, err := SnapshotContent(d)
	if err != nil {
		return entity.RuleSnapshot{}, err
	}
	return snapshots.Register(ctx, organizationID, kind, d.Version, content)
}
