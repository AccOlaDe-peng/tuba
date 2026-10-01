package component

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// RolloutTarget is one independently supervised installation of a component
// (typically one host or one component instance under one root).
type RolloutTarget struct {
	Name      string // human/ops identifier used in reports
	Root      string // supervisor root on that target
	Keyring   Keyring
	Component string
	Instances []string
	Processes ProcessManager
	Health    HealthChecker
}

// RolloutReport summarizes a finished (or aborted) gray rollout.
type RolloutReport struct {
	// Confirmed targets run the new version.
	Confirmed []string
	// Reverted targets were upgraded, then rolled back after a later batch
	// failed.
	Reverted []string
	// AutoRolledBack targets failed their own observation window and rolled
	// themselves back (the orchestrator's built-in behavior).
	AutoRolledBack []string
	// FailedTarget is the first target whose upgrade failed and aborted the
	// rollout.
	FailedTarget string
	Err          error
}

// RunRollout upgrades targets in batches of batchSize. Each target goes
// through the full orchestrated upgrade (stop → apply → start → observe →
// confirm). The first failure aborts the rollout: no further batch starts,
// and every already-confirmed target is reverted to its previous version with
// the same health observation. The failed target has already rolled itself
// back via the orchestrator.
func RunRollout(ctx context.Context, packageDir string, targets []RolloutTarget, batchSize int, observeWindow, startGrace, pollInterval time.Duration) (*RolloutReport, error) {
	if batchSize < 1 {
		return nil, errors.New("batch size must be at least 1")
	}
	if len(targets) == 0 {
		return nil, errors.New("rollout needs at least one target")
	}
	report := &RolloutReport{}
	for batchStart := 0; batchStart < len(targets); batchStart += batchSize {
		batch := targets[batchStart:min(batchStart+batchSize, len(targets))]
		for _, target := range batch {
			err := target.upgrade(ctx, packageDir, observeWindow, startGrace, pollInterval)
			if err == nil {
				report.Confirmed = append(report.Confirmed, target.Name)
				continue
			}
			var rolledBack *RolledBackError
			if errors.As(err, &rolledBack) {
				report.AutoRolledBack = append(report.AutoRolledBack, target.Name)
			}
			report.FailedTarget = target.Name
			report.Err = err
			// Abort: revert everything confirmed so far, never start the
			// remaining batches.
			revertErrors := revertAll(ctx, targets[:batchStart+indexOf(batch, target)], report, observeWindow, startGrace, pollInterval)
			if len(revertErrors) > 0 {
				report.Err = fmt.Errorf("%w; additionally failed to revert: %s", report.Err, strings.Join(revertErrors, "; "))
			}
			return report, report.Err
		}
	}
	return report, nil
}

func indexOf(batch []RolloutTarget, target RolloutTarget) int {
	for i, candidate := range batch {
		if candidate.Name == target.Name {
			return i
		}
	}
	return 0
}

// revertAll rolls already-confirmed targets back to their previous version,
// collecting per-target failures instead of stopping at the first one: the
// goal is to return the fleet to the old version, and every target deserves
// the attempt.
func revertAll(ctx context.Context, targets []RolloutTarget, report *RolloutReport, observeWindow, startGrace, pollInterval time.Duration) []string {
	var failures []string
	for _, target := range targets {
		if !contains(report.Confirmed, target.Name) {
			continue
		}
		if err := target.revert(ctx, observeWindow, startGrace, pollInterval); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", target.Name, err))
			continue
		}
		report.Reverted = append(report.Reverted, target.Name)
	}
	return failures
}

func (t *RolloutTarget) orchestrator(observeWindow, startGrace, pollInterval time.Duration) *Orchestrator {
	return &Orchestrator{
		Upgrader:      &Upgrader{Root: t.Root, Keyring: t.Keyring},
		Processes:     t.Processes,
		Health:        t.Health,
		ObserveWindow: observeWindow,
		StartGrace:    startGrace,
		PollInterval:  pollInterval,
	}
}

func (t *RolloutTarget) upgrade(ctx context.Context, packageDir string, observeWindow, startGrace, pollInterval time.Duration) error {
	return t.orchestrator(observeWindow, startGrace, pollInterval).Upgrade(ctx, packageDir, t.Component, t.Instances)
}

func (t *RolloutTarget) revert(ctx context.Context, observeWindow, startGrace, pollInterval time.Duration) error {
	return t.orchestrator(observeWindow, startGrace, pollInterval).Revert(ctx, t.Component, t.Instances)
}

func contains(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}
