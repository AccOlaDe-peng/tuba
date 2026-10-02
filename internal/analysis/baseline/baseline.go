// Package baseline is the middle layer of the analysis module DAG
// (feature <- baseline <- detection). It defines the baseline model
// contract: immutable, versioned models trained from feature values with an
// explicit cold-start state. Training jobs and evaluation are F04 scope;
// this package carries only the framework types. It may import feature but
// must not import detection or registry.
package baseline

import (
	"errors"
	"fmt"
	"time"

	"tuba/product/internal/analysis/feature"
)

// Status is the lifecycle state of a baseline model version.
type Status string

const (
	// StatusColdStart means the model has declared inputs but not enough
	// samples to emit scores; consumers must treat it as "no baseline".
	StatusColdStart Status = "cold_start"
	// StatusTraining means a training run is in progress.
	StatusTraining Status = "training"
	// StatusReady means the model version is immutable and scorable.
	StatusReady Status = "ready"
	// StatusRetired means the version is preserved for audit but must not
	// score new data.
	StatusRetired Status = "retired"
)

var (
	ErrInvalidStatus     = errors.New("baseline status is not a known lifecycle state")
	ErrEmptyModelRef     = errors.New("baseline model id, version, feature id, and feature version are required")
	ErrReadyRequiresData = errors.New("ready baseline requires samples and a training timestamp")
)

// Model is one versioned baseline over one feature version.
type Model struct {
	ID             string    `json:"id"`
	Version        string    `json:"version"`
	FeatureID      string    `json:"feature_id"`
	FeatureVersion string    `json:"feature_version"`
	Status         Status    `json:"status"`
	Samples        int       `json:"samples"`
	TrainedAt      time.Time `json:"trained_at,omitempty"`
}

// Validate enforces the lifecycle invariants fail-closed.
func (m Model) Validate() error {
	if m.ID == "" || m.Version == "" || m.FeatureID == "" || m.FeatureVersion == "" {
		return ErrEmptyModelRef
	}
	switch m.Status {
	case StatusColdStart, StatusTraining:
		return nil
	case StatusReady:
		if m.Samples < 1 || m.TrainedAt.IsZero() {
			return fmt.Errorf("%w: samples=%d trained_at=%s", ErrReadyRequiresData, m.Samples, m.TrainedAt)
		}
		return nil
	case StatusRetired:
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidStatus, m.Status)
	}
}

// Trainer produces one immutable model version from feature windows. The
// minimum-sample and deadline policies are F04 scope.
type Trainer interface {
	Train(window feature.Window, series []feature.Values) (Model, error)
}

// Scorer compares live feature values against a ready baseline model.
type Scorer interface {
	Score(model Model, values feature.Values) (float64, error)
}
