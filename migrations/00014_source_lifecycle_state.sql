-- +goose Up
-- Source lifecycle is a small state machine, not a boolean.
--
-- `enabled` was carrying two different facts and the ingest path could not tell
-- them apart: it filtered on `enabled=true`, so a source the platform had
-- deliberately revoked was indistinguishable from a topic the platform knew
-- nothing about. Both surfaced as "not found", both were classified retryable,
-- and a revoked source therefore held its consumer offset forever — until the
-- 24-hour topic retention deleted the records, taking the decision with them and
-- leaving no trace that anything had been refused. The same shape showed up
-- outside the database: a collector whose Kafka ACL had been revoked retried
-- every message for three days (2026-09-30, 49,144 authorization errors, 71 MB
-- of logs) because "not authorized" stayed in the retryable class.
--
-- The three states mean different things, and the difference is what the wire
-- needs to carry:
--   active  — accept
--   paused  — known, deliberately held; retry, because the operator intends to
--             resume and the answer will change
--   revoked — known, deliberately refused; quarantine and advance, because
--             retrying cannot change this answer and the records must not be
--             handed to the retention window unreported
--
-- `enabled` is kept and constrained to agree with `state` so a binary built
-- before this migration keeps working while both versions run. A write that sets
-- only `enabled` now fails loudly instead of silently disagreeing — the same
-- rolling-deploy discipline the payload-hash change needed. A later migration
-- drops `enabled` once no such binary remains.

ALTER TABLE source_instances ADD COLUMN state text NOT NULL DEFAULT 'active';

ALTER TABLE source_instances
  ADD CONSTRAINT source_instances_state_check CHECK (state IN ('active', 'paused', 'revoked'));

UPDATE source_instances SET state = CASE WHEN enabled THEN 'active' ELSE 'revoked' END;

-- `enabled` is the pre-migration view of the same fact, so it may not disagree
-- with `state`. Added after the backfill above: before it, every row still
-- carried the default `active` and would have failed the check.
ALTER TABLE source_instances
  ADD CONSTRAINT source_instances_enabled_matches_state CHECK (enabled = (state = 'active'));

-- The existing (organization_id, namespace, enabled) index still narrows every
-- query this code issues, so no new index is added for a three-valued column
-- over a table of tens of rows.

-- +goose Down
-- Fold the state back into the boolean before dropping it. A downgrade must not
-- re-enable a revoked source: 'paused' also collapses to disabled, because the
-- older schema has no way to express "held but expected to resume".
UPDATE source_instances SET enabled = (state = 'active');

ALTER TABLE source_instances DROP CONSTRAINT IF EXISTS source_instances_enabled_matches_state;
ALTER TABLE source_instances DROP CONSTRAINT IF EXISTS source_instances_state_check;
ALTER TABLE source_instances DROP COLUMN state;
