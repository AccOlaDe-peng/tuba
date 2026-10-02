package registry

import (
	"encoding/json"
	"errors"
	"testing"
)

func featureDescriptor() Descriptor {
	return Descriptor{
		Kind:    KindFeature,
		ID:      "authentication.failure-burst",
		Version: "1.0.0",
		Input:   InputContract{Schema: "attributed-event", Version: "1.0.0"},
		Quality: Quality{MinCoveragePercent: 80, TestEvidence: "go test ./internal/analysis/feature"},
	}
}

func baselineDescriptor() Descriptor {
	return Descriptor{
		Kind:         KindBaseline,
		ID:           "authentication.failure-rate-baseline",
		Version:      "1.0.0",
		Dependencies: []Dependency{{ID: "authentication.failure-burst", Constraint: ">=1.0.0"}},
		Input:        InputContract{Schema: "attributed-event", Version: "1.0.0"},
		Quality:      Quality{MinCoveragePercent: 75, TestEvidence: "go test ./internal/analysis/baseline"},
	}
}

func detectionDescriptor() Descriptor {
	return Descriptor{
		Kind:    KindDetection,
		ID:      "auth.failure-then-success",
		Version: "1.0.0",
		Dependencies: []Dependency{
			{ID: "authentication.failure-burst", Constraint: ">=1.0.0"},
			{ID: "authentication.failure-rate-baseline", Constraint: "1.0.0"},
		},
		Input:   InputContract{Schema: "attributed-event", Version: "1.0.0"},
		Quality: Quality{MinCoveragePercent: 90, TestEvidence: "go test ./internal/analysis/detection"},
	}
}

func TestRegisterFeatureBaselineDetectionChain(t *testing.T) {
	r := New()
	if err := r.Register(featureDescriptor()); err != nil {
		t.Fatalf("feature register: %v", err)
	}
	if err := r.Register(baselineDescriptor()); err != nil {
		t.Fatalf("baseline register: %v", err)
	}
	if err := r.Register(detectionDescriptor()); err != nil {
		t.Fatalf("detection register: %v", err)
	}
	d, ok := r.Resolve(KindDetection, "auth.failure-then-success")
	if !ok || d.Version != "1.0.0" {
		t.Fatalf("resolve detection: %+v ok=%v", d, ok)
	}
	if _, ok := r.Lookup(KindFeature, "authentication.failure-burst", "1.0.0"); !ok {
		t.Fatal("lookup feature")
	}
}

func TestRegisterIdempotentAndConflictOnDifferentContent(t *testing.T) {
	r := New()
	if err := r.Register(featureDescriptor()); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := r.Register(featureDescriptor()); err != nil {
		t.Fatalf("idempotent replay must succeed: %v", err)
	}
	changed := featureDescriptor()
	changed.Quality.MinCoveragePercent = 10
	if err := r.Register(changed); !errors.Is(err, ErrDescriptorConflict) {
		t.Fatalf("same version different content must be rejected, got %v", err)
	}
	// The stored descriptor must be unchanged.
	d, _ := r.Lookup(KindFeature, "authentication.failure-burst", "1.0.0")
	if d.Quality.MinCoveragePercent != 80 {
		t.Fatalf("stored descriptor was rewritten: %+v", d.Quality)
	}
	// A new version of the same module is allowed.
	newer := featureDescriptor()
	newer.Version = "1.1.0"
	if err := r.Register(newer); err != nil {
		t.Fatalf("new version register: %v", err)
	}
	if d, _ := r.Resolve(KindFeature, "authentication.failure-burst"); d.Version != "1.1.0" {
		t.Fatalf("resolve must pick latest version, got %s", d.Version)
	}
}

func TestQualityGateRejectsNoncompliantDescriptors(t *testing.T) {
	cases := map[string]func(*Descriptor){
		"bad kind":            func(d *Descriptor) { d.Kind = "model" },
		"bad id":              func(d *Descriptor) { d.ID = "Auth.Failure" },
		"bad version":         func(d *Descriptor) { d.Version = "1.0" },
		"empty schema":        func(d *Descriptor) { d.Input.Schema = "" },
		"bad input version":   func(d *Descriptor) { d.Input.Version = "v1" },
		"zero coverage":       func(d *Descriptor) { d.Quality.MinCoveragePercent = 0 },
		"coverage over 100":   func(d *Descriptor) { d.Quality.MinCoveragePercent = 101 },
		"empty test evidence": func(d *Descriptor) { d.Quality.TestEvidence = "  " },
		"bad constraint":      func(d *Descriptor) { d.Dependencies = []Dependency{{ID: "x.y", Constraint: ">1.0"}} },
		"bad dependency id":   func(d *Descriptor) { d.Dependencies = []Dependency{{ID: "X", Constraint: "1.0.0"}} },
	}
	want := map[string]error{
		"bad kind":            ErrInvalidKind,
		"bad id":              ErrInvalidID,
		"bad version":         ErrInvalidVersion,
		"empty schema":        ErrInvalidInput,
		"bad input version":   ErrInvalidInput,
		"zero coverage":       ErrInvalidQuality,
		"coverage over 100":   ErrInvalidQuality,
		"empty test evidence": ErrInvalidQuality,
		"bad constraint":      ErrInvalidConstraint,
		"bad dependency id":   ErrInvalidID,
	}
	for name, mutate := range cases {
		d := featureDescriptor()
		mutate(&d)
		err := New().Register(d)
		if !errors.Is(err, want[name]) {
			t.Fatalf("%s: got %v, want %v", name, err, want[name])
		}
	}
}

func TestDependencyVersionConstraints(t *testing.T) {
	r := New()
	// Unknown dependency is rejected before the feature exists.
	if err := r.Register(baselineDescriptor()); !errors.Is(err, ErrUnknownDependency) {
		t.Fatalf("unknown dependency: got %v", err)
	}
	if err := r.Register(featureDescriptor()); err != nil {
		t.Fatalf("feature register: %v", err)
	}
	// Exact constraint not satisfiable by 1.0.0.
	exact := baselineDescriptor()
	exact.Dependencies = []Dependency{{ID: "authentication.failure-burst", Constraint: "2.0.0"}}
	if err := r.Register(exact); !errors.Is(err, ErrUnsatisfiedVersion) {
		t.Fatalf("unsatisfied exact constraint: got %v", err)
	}
	// Floor above the registered version is rejected.
	high := baselineDescriptor()
	high.Dependencies = []Dependency{{ID: "authentication.failure-burst", Constraint: ">=1.1.0"}}
	if err := r.Register(high); !errors.Is(err, ErrUnsatisfiedVersion) {
		t.Fatalf("unsatisfied floor constraint: got %v", err)
	}
	// Registering a newer feature version satisfies the floor.
	newer := featureDescriptor()
	newer.Version = "1.2.0"
	if err := r.Register(newer); err != nil {
		t.Fatalf("newer feature register: %v", err)
	}
	if err := r.Register(high); err != nil {
		t.Fatalf("floor satisfied by newer version: %v", err)
	}
	// Self dependency is rejected.
	self := featureDescriptor()
	self.Version = "9.9.9"
	self.Dependencies = []Dependency{{ID: "authentication.failure-burst", Constraint: "1.0.0"}}
	if err := r.Register(self); !errors.Is(err, ErrUnknownDependency) {
		t.Fatalf("self dependency: got %v", err)
	}
}

func TestSnapshotContentAndRuleKinds(t *testing.T) {
	d := detectionDescriptor()
	content, err := SnapshotContent(d)
	if err != nil {
		t.Fatalf("snapshot content: %v", err)
	}
	var decoded Descriptor
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("snapshot content is not a descriptor: %v", err)
	}
	if decoded.Kind != KindDetection || decoded.ID != d.ID || decoded.Version != d.Version {
		t.Fatalf("round trip mismatch: %+v", decoded)
	}
	// Canonical form is dependency-order independent.
	reordered := d
	reordered.Dependencies = []Dependency{d.Dependencies[1], d.Dependencies[0]}
	again, err := SnapshotContent(reordered)
	if err != nil {
		t.Fatalf("snapshot content reordered: %v", err)
	}
	if string(content) != string(again) {
		t.Fatalf("canonical content must not depend on dependency order:\n%s\n%s", content, again)
	}
	for kind, want := range map[Kind]string{
		KindFeature:   RuleKindFeature,
		KindBaseline:  RuleKindBaseline,
		KindDetection: RuleKindDetection,
	} {
		got, err := RuleKindFor(kind)
		if err != nil || got != want {
			t.Fatalf("rule kind for %s: %q %v", kind, got, err)
		}
	}
	if _, err := RuleKindFor("model"); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("invalid kind: got %v", err)
	}
	// Snapshot content of an invalid descriptor is refused fail-closed.
	bad := d
	bad.Quality.MinCoveragePercent = 0
	if _, err := SnapshotContent(bad); !errors.Is(err, ErrInvalidQuality) {
		t.Fatalf("invalid descriptor snapshot: got %v", err)
	}
}
