package control

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type AnalysisRunStatus struct {
	RunID           string     `json:"run_id"`
	WorkerInstance  string     `json:"worker_instance"`
	Status          string     `json:"status"`
	RegistryVersion string     `json:"registry_version"`
	ProcessedEvents int64      `json:"processed_events"`
	EmittedResults  int64      `json:"emitted_results"`
	StartedAt       time.Time  `json:"started_at"`
	LastHeartbeatAt time.Time  `json:"last_heartbeat_at"`
	StoppedAt       *time.Time `json:"stopped_at,omitempty"`
}

type AnalysisCheckpointStatus struct {
	Topic     string     `json:"topic"`
	Partition int        `json:"partition"`
	Offset    int64      `json:"offset"`
	Watermark *time.Time `json:"watermark,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type AnalysisRuntime struct {
	Run         *AnalysisRunStatus         `json:"run,omitempty"`
	Checkpoints []AnalysisCheckpointStatus `json:"checkpoints"`
}

func (s *Store) AnalysisRuntime(ctx context.Context) (AnalysisRuntime, error) {
	result := AnalysisRuntime{Checkpoints: []AnalysisCheckpointStatus{}}
	var run AnalysisRunStatus
	err := s.Pool.QueryRow(ctx, `
		SELECT run_id,worker_instance,status,registry_version,processed_events,emitted_results,
		       started_at,last_heartbeat_at,stopped_at
		FROM analysis_runs
		ORDER BY last_heartbeat_at DESC
		LIMIT 1`).Scan(
		&run.RunID,
		&run.WorkerInstance,
		&run.Status,
		&run.RegistryVersion,
		&run.ProcessedEvents,
		&run.EmittedResults,
		&run.StartedAt,
		&run.LastHeartbeatAt,
		&run.StoppedAt,
	)
	if err == nil {
		result.Run = &run
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	rows, queryErr := s.Pool.Query(ctx, `
		SELECT topic,partition,"offset",watermark,updated_at
		FROM analysis_checkpoints
		ORDER BY topic,partition`)
	if queryErr != nil {
		return result, queryErr
	}
	defer rows.Close()
	for rows.Next() {
		var checkpoint AnalysisCheckpointStatus
		if scanErr := rows.Scan(&checkpoint.Topic, &checkpoint.Partition, &checkpoint.Offset, &checkpoint.Watermark, &checkpoint.UpdatedAt); scanErr != nil {
			return result, scanErr
		}
		result.Checkpoints = append(result.Checkpoints, checkpoint)
	}
	return result, rows.Err()
}
