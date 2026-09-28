-- +goose Up
CREATE TABLE analysis_runs (
  run_id text PRIMARY KEY,
  worker_type text NOT NULL,
  worker_instance text NOT NULL,
  consumer_group text NOT NULL,
  registry_version text NOT NULL,
  status text NOT NULL CHECK (status IN ('running','stopped','failed')),
  processed_events bigint NOT NULL DEFAULT 0,
  emitted_results bigint NOT NULL DEFAULT 0,
  last_error text,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
  stopped_at timestamptz
);
CREATE INDEX analysis_runs_heartbeat_idx ON analysis_runs(last_heartbeat_at DESC);

CREATE TABLE analysis_checkpoints (
  consumer_group text NOT NULL,
  topic text NOT NULL,
  partition integer NOT NULL,
  "offset" bigint NOT NULL,
  watermark timestamptz,
  run_id text REFERENCES analysis_runs(run_id) ON DELETE SET NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, topic, partition)
);

CREATE TABLE analysis_processor_states (
  consumer_group text NOT NULL,
  topic text NOT NULL,
  partition integer NOT NULL,
  state_version integer NOT NULL DEFAULT 1,
  state jsonb NOT NULL,
  watermark timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, topic, partition)
);

CREATE TABLE analysis_feedback (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  anomaly_id text NOT NULL,
  feedback_type text NOT NULL CHECK (feedback_type IN ('true_positive','false_positive','false_negative','inconclusive')),
  reason text NOT NULL DEFAULT '',
  actor_identity_id uuid REFERENCES identities(id) ON DELETE SET NULL,
  source_case_id uuid REFERENCES cases(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX analysis_feedback_tenant_anomaly_idx ON analysis_feedback(organization_id, anomaly_id, created_at DESC);
CREATE UNIQUE INDEX analysis_feedback_case_anomaly_idx ON analysis_feedback(source_case_id, anomaly_id, feedback_type) WHERE source_case_id IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS analysis_feedback,analysis_processor_states,analysis_checkpoints,analysis_runs CASCADE;
