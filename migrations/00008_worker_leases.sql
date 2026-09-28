-- +goose Up
ALTER TABLE processor_outbox
  ADD COLUMN lease_owner text,
  ADD COLUMN lease_until timestamptz,
  ADD COLUMN fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
  ADD CONSTRAINT processor_outbox_lease_pair CHECK ((lease_owner IS NULL) = (lease_until IS NULL));
CREATE INDEX processor_outbox_claim_idx
  ON processor_outbox(next_attempt_at, id)
  WHERE sent_at IS NULL;

ALTER TABLE processing_jobs
  ADD COLUMN max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX processing_jobs_retry_claim_idx
  ON processing_jobs(next_attempt_at, created_at)
  WHERE state = 'queued';

-- +goose Down
DROP INDEX IF EXISTS processing_jobs_retry_claim_idx;
ALTER TABLE processing_jobs DROP COLUMN IF EXISTS next_attempt_at, DROP COLUMN IF EXISTS max_attempts;
DROP INDEX IF EXISTS processor_outbox_claim_idx;
ALTER TABLE processor_outbox DROP CONSTRAINT IF EXISTS processor_outbox_lease_pair;
ALTER TABLE processor_outbox DROP COLUMN IF EXISTS fencing_token, DROP COLUMN IF EXISTS lease_until, DROP COLUMN IF EXISTS lease_owner;
