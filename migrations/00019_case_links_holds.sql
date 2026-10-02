-- +goose Up
-- R03 case authority completion: PostgreSQL remains the case system of
-- record. This migration adds (a) typed, idempotent links from a case to
-- entities, risk contributions, and evidence references, (b) insert-only
-- forensic snapshots that freeze the case evidence view at a moment in
-- time, and (c) a legal-hold flag whose protection semantics are enforced
-- by the retention cleaner (a processing job referenced as evidence of a
-- held case is never swept).
--
-- Link identity is (case_id, link_type, ref_kind, target_id), so replaying
-- the same association is a no-op instead of a duplicate. ref_kind is
-- constrained per link_type: entity links carry ref_kind 'entity', risk
-- contribution links 'risk_contribution', evidence links one of
-- event_id/raw_event_id/attribution/job_id.
CREATE TABLE case_links (
  case_id uuid NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
  link_type text NOT NULL CHECK (link_type IN ('entity','risk_contribution','evidence')),
  ref_kind text NOT NULL CHECK (ref_kind IN ('entity','risk_contribution','event_id','raw_event_id','attribution','job_id')),
  target_id text NOT NULL CHECK (length(target_id) BETWEEN 1 AND 256),
  linked_by uuid NOT NULL REFERENCES identities(id),
  linked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (case_id, link_type, ref_kind, target_id),
  CHECK (
    (link_type = 'entity' AND ref_kind = 'entity') OR
    (link_type = 'risk_contribution' AND ref_kind = 'risk_contribution') OR
    (link_type = 'evidence' AND ref_kind IN ('event_id','raw_event_id','attribution','job_id'))
  )
);
CREATE INDEX case_links_target_idx ON case_links(link_type, ref_kind, target_id);

-- Forensic snapshots freeze the case header plus its full link set at
-- creation time. Rows are insert-only while their case exists: the trigger
-- rejects any UPDATE and any DELETE whose parent case still exists, so a
-- snapshot can never be rewritten or selectively erased. The only way a
-- snapshot row disappears is deletion of the case itself (ON DELETE
-- CASCADE), which no API exposes; case deletion removes the case and its
-- forensic record together, never the snapshot alone.
CREATE TABLE case_snapshots (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  case_id uuid NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
  label text NOT NULL CHECK (length(label) BETWEEN 1 AND 300),
  snapshot jsonb NOT NULL,
  created_by uuid NOT NULL REFERENCES identities(id),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX case_snapshots_case_idx ON case_snapshots(case_id, created_at);

-- +goose StatementBegin
CREATE FUNCTION case_snapshots_immutable() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' AND NOT EXISTS (SELECT 1 FROM cases WHERE id = OLD.case_id) THEN
    RETURN OLD;
  END IF;
  RAISE EXCEPTION 'case_snapshots is insert-only';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER case_snapshots_no_update BEFORE UPDATE ON case_snapshots
  FOR EACH ROW EXECUTE FUNCTION case_snapshots_immutable();
CREATE TRIGGER case_snapshots_no_delete BEFORE DELETE ON case_snapshots
  FOR EACH ROW EXECUTE FUNCTION case_snapshots_immutable();

-- Legal hold. Hold transitions go through the case version guard like any
-- other update, so a concurrent writer cannot silently clear a hold.
ALTER TABLE cases ADD COLUMN hold boolean NOT NULL DEFAULT false;
ALTER TABLE cases ADD COLUMN hold_reason text NOT NULL DEFAULT '';
ALTER TABLE cases ADD COLUMN hold_set_by uuid REFERENCES identities(id);
ALTER TABLE cases ADD COLUMN hold_at timestamptz;
ALTER TABLE cases ADD CONSTRAINT cases_hold_consistent
  CHECK ((hold = false AND hold_set_by IS NULL AND hold_at IS NULL) OR (hold = true AND hold_set_by IS NOT NULL AND hold_at IS NOT NULL));

-- +goose Down
ALTER TABLE cases DROP CONSTRAINT IF EXISTS cases_hold_consistent;
ALTER TABLE cases DROP COLUMN IF EXISTS hold_at;
ALTER TABLE cases DROP COLUMN IF EXISTS hold_set_by;
ALTER TABLE cases DROP COLUMN IF EXISTS hold_reason;
ALTER TABLE cases DROP COLUMN IF EXISTS hold;
DROP TRIGGER IF EXISTS case_snapshots_no_delete ON case_snapshots;
DROP TRIGGER IF EXISTS case_snapshots_no_update ON case_snapshots;
DROP FUNCTION IF EXISTS case_snapshots_immutable();
DROP TABLE IF EXISTS case_snapshots;
DROP TABLE IF EXISTS case_links;
