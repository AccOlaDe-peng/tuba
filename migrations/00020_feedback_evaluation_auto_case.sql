-- +goose Up
-- R04: feedback enters the audit chain and the offline evaluation dataset;
-- it never mutates production models online. Feedback submissions are
-- idempotent via an optional caller-supplied key, may target a case
-- (resolvable in PG) in addition to a finding business key, and annotate
-- finding status in a metadata table decoupled from any model artifact.
-- Auto case creation is default-off; when enabled it dedupes on
-- tenant/entity/policy/time_bucket: one case per key, links accumulate.
ALTER TABLE analysis_feedback ADD COLUMN idempotency_key text CHECK (idempotency_key IS NULL OR length(idempotency_key) BETWEEN 8 AND 128);
ALTER TABLE analysis_feedback ADD COLUMN target_case_id uuid REFERENCES cases(id) ON DELETE SET NULL;
CREATE UNIQUE INDEX analysis_feedback_idempotency_idx ON analysis_feedback(organization_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE finding_labels (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  anomaly_id text NOT NULL CHECK (length(anomaly_id) BETWEEN 1 AND 256),
  label text NOT NULL CHECK (label IN ('confirmed','false_positive','inconclusive')),
  reason text NOT NULL DEFAULT '',
  updated_by uuid REFERENCES identities(id) ON DELETE SET NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, anomaly_id)
);

CREATE TABLE auto_case_dedupe (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
  entity_id text NOT NULL CHECK (entity_id ~ '^ent:[a-f0-9]{64}$'),
  policy_id text NOT NULL CHECK (length(policy_id) BETWEEN 1 AND 256),
  time_bucket timestamptz NOT NULL,
  case_id uuid NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
  linked_anomalies integer NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, entity_id, policy_id, time_bucket)
);

-- Offline evaluation dataset: analyst feedback joined with the (decoupled)
-- finding status annotation and the verdict of the case the feedback is
-- attached to. Findings themselves live in Elasticsearch; the join keys are
-- the finding business key plus the rule/entity context columns of 00006.
CREATE VIEW analysis_feedback_evaluation AS
SELECT f.organization_id,
       f.id AS feedback_id,
       f.feedback_type,
       f.anomaly_id,
       f.rule_id,
       f.entity_id,
       f.event_ids,
       f.reason,
       f.actor_identity_id,
       f.source_case_id,
       f.target_case_id,
       f.created_at,
       l.label AS finding_label,
       l.updated_at AS labeled_at,
       c.verdict AS case_verdict
FROM analysis_feedback f
LEFT JOIN finding_labels l
  ON l.organization_id = f.organization_id AND l.anomaly_id = f.anomaly_id
LEFT JOIN cases c
  ON c.id = coalesce(f.target_case_id, f.source_case_id);

-- +goose Down
DROP VIEW IF EXISTS analysis_feedback_evaluation;
DROP TABLE IF EXISTS auto_case_dedupe;
DROP TABLE IF EXISTS finding_labels;
ALTER TABLE analysis_feedback
  DROP COLUMN IF EXISTS target_case_id,
  DROP COLUMN IF EXISTS idempotency_key;
