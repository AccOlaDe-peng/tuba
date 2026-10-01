-- +goose Up
-- Rule snapshots preserve the exact rule content that produced an
-- attribution/relation/feature result, keyed by (organization, kind,
-- version). A version is immutable once registered: re-registering the same
-- version with different content is rejected, so upgrading rules never
-- rewrites the explanation of historical conclusions.
CREATE TABLE rule_snapshots (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  rule_kind text NOT NULL CHECK (rule_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
  rule_version text NOT NULL CHECK (rule_version ~ '^[0-9]+\.[0-9]+\.[0-9]+$'),
  content jsonb NOT NULL,
  content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[a-f0-9]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, rule_kind, rule_version)
);

-- Temporal relation conflict handling is enforced fail-closed by the
-- application (per-relation-key advisory locks plus an overlap check inside
-- the same transaction); these indexes keep the overlap scans and the
-- reverse (to_entity) temporal lookups bounded.
CREATE INDEX entity_relations_open_idx ON entity_relations(organization_id, from_entity_id, relation_type)
  WHERE valid_to IS NULL;
CREATE INDEX entity_relations_to_time_idx ON entity_relations(organization_id, to_entity_id, valid_from, valid_to);

-- +goose Down
DROP INDEX IF EXISTS entity_relations_to_time_idx;
DROP INDEX IF EXISTS entity_relations_open_idx;
DROP TABLE IF EXISTS rule_snapshots;
