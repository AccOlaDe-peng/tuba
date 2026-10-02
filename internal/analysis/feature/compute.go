package feature

import (
	"errors"
	"fmt"
	"sort"
)

// F03 concrete feature computers. AuthComputer below is the Go reference
// implementation of the first-phase authentication feature set; the
// authoritative production implementation is the Python analysis worker
// (python/tuba_analysis/features.py — compute_window_features), per the
// design baseline division of labor (formal scheduling entry is the Python
// analysis worker; the Go side is diagnostic/reference until F08 unifies
// scheduling). Both sides implement the identical definitions and are pinned
// to the same golden test vectors: same window, same contributions, same
// values. This file has no output path of its own — nothing here is wired to
// a worker, so there is no double-emission risk.

// AuthFeatureVersionV1 is the immutable feature version stamp carried by
// every feature record of the first-phase authentication set. Any change to
// the definitions below requires a new version.
const AuthFeatureVersionV1 = "1.0.0"

// Feature names of the first-phase authentication set (design baseline §6).
const (
	// FeatureAuthAttemptCount counts all authentication contributions in
	// the window, regardless of outcome.
	FeatureAuthAttemptCount = "auth.attempt.count"
	// FeatureAuthFailureCount counts contributions with outcome "failure".
	FeatureAuthFailureCount = "auth.failure.count"
	// FeatureAuthFailureRate is failures / attempts (0 when the window has
	// no attempts; such windows are normally never computed).
	FeatureAuthFailureRate = "auth.failure.rate"
	// FeatureAuthSourceDeviceCount / FeatureAuthSourceIPCount count
	// distinct non-empty source endpoint attributes in the window.
	FeatureAuthSourceDeviceCount = "auth.source_device.count"
	FeatureAuthSourceIPCount     = "auth.source_ip.count"
	// FeatureAuthFailureThenSuccessCount counts success events inside the
	// window that follow at least one failure since the previous success
	// (contributions ordered by (event time, event id)).
	FeatureAuthFailureThenSuccessCount = "auth.failure_then_success.count"
)

var (
	ErrEmptyFeatureVersion = errors.New("feature version is required")
	ErrInvalidRevision     = errors.New("feature record revision must be >= 1")
)

// AuthComputer computes the first-phase authentication feature set for one
// bounded window. It is deterministic and order-independent: contributions
// are re-sorted by (event time, event id) before the sequence feature is
// evaluated.
type AuthComputer struct{}

// Compute implements Computer.
func (AuthComputer) Compute(window Window, contributions []Contribution) (Values, error) {
	if err := window.Validate(); err != nil {
		return nil, err
	}
	ordered := make([]Contribution, len(contributions))
	copy(ordered, contributions)
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].At.Equal(ordered[j].At) {
			return ordered[i].At.Before(ordered[j].At)
		}
		return ordered[i].EventID < ordered[j].EventID
	})

	failures := 0
	failureThenSuccess := 0
	failuresSinceSuccess := 0
	devices := map[string]struct{}{}
	ips := map[string]struct{}{}
	for _, c := range ordered {
		switch c.Outcome {
		case "failure":
			failures++
			failuresSinceSuccess++
		case "success":
			if failuresSinceSuccess >= 1 {
				failureThenSuccess++
			}
			failuresSinceSuccess = 0
		}
		if c.SourceDevice != "" {
			devices[c.SourceDevice] = struct{}{}
		}
		if c.SourceIP != "" {
			ips[c.SourceIP] = struct{}{}
		}
	}
	attempts := len(ordered)
	rate := 0.0
	if attempts > 0 {
		rate = float64(failures) / float64(attempts)
	}
	return Values{
		FeatureAuthAttemptCount:            float64(attempts),
		FeatureAuthFailureCount:            float64(failures),
		FeatureAuthFailureRate:             rate,
		FeatureAuthSourceDeviceCount:       float64(len(devices)),
		FeatureAuthSourceIPCount:           float64(len(ips)),
		FeatureAuthFailureThenSuccessCount: float64(failureThenSuccess),
	}, nil
}

// Record is the versioned feature output for one entity and one window: the
// computed values plus the window, the immutable feature version stamp, a
// monotonically increasing revision (same business key corrected by late
// data recomputes the same window with a higher revision, per the design
// baseline revision semantics), and the sorted contributing event ids as
// input references for F04 baseline sampling and F05 detection input.
type Record struct {
	EntityID       string   `json:"entity_id"`
	Window         Window   `json:"window"`
	FeatureVersion string   `json:"feature_version"`
	Revision       uint64   `json:"revision"`
	Values         Values   `json:"values"`
	Inputs         []string `json:"inputs"`
}

// NewRecord builds a validated feature record fail-closed.
func NewRecord(entityID string, window Window, featureVersion string, revision uint64, values Values, contributions []Contribution) (Record, error) {
	if entityID == "" {
		return Record{}, ErrEmptyEntity
	}
	if err := window.Validate(); err != nil {
		return Record{}, err
	}
	if featureVersion == "" {
		return Record{}, ErrEmptyFeatureVersion
	}
	if revision < 1 {
		return Record{}, fmt.Errorf("%w: %d", ErrInvalidRevision, revision)
	}
	inputs := make([]string, 0, len(contributions))
	for _, c := range contributions {
		if c.EventID == "" {
			return Record{}, ErrEmptyEvent
		}
		inputs = append(inputs, c.EventID)
	}
	sort.Strings(inputs)
	return Record{
		EntityID:       entityID,
		Window:         window,
		FeatureVersion: featureVersion,
		Revision:       revision,
		Values:         values,
		Inputs:         inputs,
	}, nil
}
