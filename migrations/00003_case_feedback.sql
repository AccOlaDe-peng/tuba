-- +goose Up
ALTER TABLE cases ADD COLUMN verdict text CHECK(verdict IN ('true_positive','benign_positive','false_positive','inconclusive'));
ALTER TABLE cases ADD COLUMN verdict_reason text;
ALTER TABLE cases ADD COLUMN verdict_by uuid REFERENCES identities(id);
ALTER TABLE cases ADD COLUMN verdict_at timestamptz;
-- +goose Down
ALTER TABLE cases DROP COLUMN IF EXISTS verdict_at,DROP COLUMN IF EXISTS verdict_by,DROP COLUMN IF EXISTS verdict_reason,DROP COLUMN IF EXISTS verdict;
