-- +goose Up
-- Feature samples are the persisted, windowed feature records that baseline
-- training reads (design baseline §6): training must not depend on the
-- 14-day retention of Raw/standard events or Kafka. A row is written when a
-- window closes; a late correction of the same business key rewrites the row
-- only with a strictly higher revision (same revision with different content
-- is rejected by the application fail-closed). Training reads only rows that
-- are closed, quality-qualified, of one feature version/generation, and
-- whose window ended at or before the training deadline watermark.
CREATE TABLE feature_samples (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  feature_id text NOT NULL CHECK (feature_id ~ '^[a-z][a-z0-9_.]{0,63}$'),
  feature_version text NOT NULL CHECK (feature_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
  generation text NOT NULL CHECK (generation ~ '^g[0-9]+$'),
  entity_id text NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  revision integer NOT NULL CHECK (revision >= 1),
  quality text NOT NULL CHECK (quality IN ('qualified', 'partial')),
  values jsonb NOT NULL,
  inputs jsonb NOT NULL,
  content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  closed_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, feature_id, feature_version, generation, entity_id, window_start),
  CHECK (window_end > window_start)
);
CREATE INDEX feature_samples_training_idx
  ON feature_samples(organization_id, feature_id, feature_version, generation, window_end)
  WHERE quality = 'qualified';

-- Baseline model versions are immutable once published: a version row is
-- insert-only for its content (statistics, metrics, sample range, training
-- cutoff); the only allowed mutation is the lifecycle status transition to
-- retired (revision/withdrawal retraining publishes a NEW version and the
-- old version is kept with the superseding reference for audit). cold_start
-- is an explicit, queryable status row of the model lineage. Publication
-- carries the training deadline, sample range, and evaluation metrics.
CREATE TABLE baseline_models (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  model_id text NOT NULL CHECK (model_id ~ '^[a-z][a-z0-9_.]{0,63}$'),
  model_version text NOT NULL CHECK (model_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
  feature_id text NOT NULL CHECK (feature_id ~ '^[a-z][a-z0-9_.]{0,63}$'),
  feature_version text NOT NULL CHECK (feature_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
  generation text NOT NULL CHECK (generation ~ '^g[0-9]+$'),
  status text NOT NULL CHECK (status IN ('cold_start', 'training', 'ready', 'retired')),
  sample_count integer NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
  complete_days integer NOT NULL DEFAULT 0 CHECK (complete_days >= 0),
  trained_at timestamptz,
  training_cutoff timestamptz,
  sample_range jsonb NOT NULL DEFAULT '{}'::jsonb,
  metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  statistics jsonb NOT NULL DEFAULT '{}'::jsonb,
  content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  supersedes_model_version text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, model_id, model_version),
  CHECK (status <> 'ready' OR (sample_count >= 1 AND trained_at IS NOT NULL AND training_cutoff IS NOT NULL))
);
CREATE INDEX baseline_models_lineage_idx
  ON baseline_models(organization_id, model_id, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS baseline_models_lineage_idx;
DROP TABLE IF EXISTS baseline_models;
DROP INDEX IF EXISTS feature_samples_training_idx;
DROP TABLE IF EXISTS feature_samples;
