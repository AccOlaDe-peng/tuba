// Package feature is the bottom layer of the analysis module DAG
// (feature <- baseline <- detection). It defines the versioned input
// contract every feature computation consumes: bounded event-time windows
// over per-entity contributions. It must not import baseline, detection, or
// registry.
package feature

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidWindow = errors.New("feature window must be positive with non-negative allowed lateness")
	ErrEmptyEntity   = errors.New("contribution entity id is required")
	ErrEmptyEvent    = errors.New("contribution event id is required")
	ErrEmptyTime     = errors.New("contribution event time is required")
)

// Contribution is one entity's attributed stake in a single event, keyed by
// entity.id as produced on the attributed-events stream. AttributionID is the
// stable attribution id from E02/E04; the F02 window engine deduplicates
// repeat deliveries by it.
type Contribution struct {
	EntityID      string    `json:"entity_id"`
	Role          string    `json:"role"`
	EventID       string    `json:"event_id"`
	AttributionID string    `json:"attribution_id,omitempty"`
	At            time.Time `json:"at"`
	// Outcome is the UIM event.outcome (for example "success" / "failure");
	// empty means the event carries no outcome. F03 auth features consume it.
	Outcome string `json:"outcome,omitempty"`
	// SourceDevice / SourceIP are the observed source endpoint attributes
	// (host.id / source.ip), consumed by the distinct-count auth features.
	SourceDevice string `json:"source_device,omitempty"`
	SourceIP     string `json:"source_ip,omitempty"`
}

// Validate rejects malformed contributions fail-closed.
func (c Contribution) Validate() error {
	if c.EntityID == "" {
		return ErrEmptyEntity
	}
	if c.EventID == "" {
		return ErrEmptyEvent
	}
	if c.At.IsZero() {
		return ErrEmptyTime
	}
	return nil
}

// Window is a bounded event-time interval [Start, End) plus the allowed
// lateness that bounds how far behind the watermark input may arrive.
type Window struct {
	Start           time.Time     `json:"start"`
	End             time.Time     `json:"end"`
	AllowedLateness time.Duration `json:"allowed_lateness"`
}

// Validate rejects non-positive windows fail-closed.
func (w Window) Validate() error {
	if !w.Start.Before(w.End) || w.AllowedLateness < 0 {
		return fmt.Errorf("%w: [%s, %s) lateness=%s", ErrInvalidWindow, w.Start, w.End, w.AllowedLateness)
	}
	return nil
}

// Values are the computed feature outputs for one window, keyed by a stable
// feature-defined name (for example "auth.failure.count").
type Values map[string]float64

// Computer produces the feature values of one bounded window from the
// entity-keyed contributions inside it. Implementations must be
// deterministic: the same window and contributions always yield the same
// values, regardless of input arrival order.
type Computer interface {
	Compute(window Window, contributions []Contribution) (Values, error)
}
