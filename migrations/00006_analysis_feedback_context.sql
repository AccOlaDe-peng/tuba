-- +goose Up
ALTER TABLE analysis_feedback ADD COLUMN rule_id text;
ALTER TABLE analysis_feedback ADD COLUMN entity_id text;
ALTER TABLE analysis_feedback ADD COLUMN event_ids text[] NOT NULL DEFAULT '{}';
ALTER TABLE analysis_feedback ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE analysis_feedback
  DROP COLUMN IF EXISTS metadata,
  DROP COLUMN IF EXISTS event_ids,
  DROP COLUMN IF EXISTS entity_id,
  DROP COLUMN IF EXISTS rule_id;
