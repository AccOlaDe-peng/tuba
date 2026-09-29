-- +goose Up
-- Release publishing is a global privilege, separate from tenant memberships.
CREATE TABLE platform_release_publishers (
  identity_id uuid PRIMARY KEY REFERENCES identities(id) ON DELETE RESTRICT,
  granted_by uuid REFERENCES identities(id) ON DELETE RESTRICT,
  granted_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz,
  revoked_by uuid REFERENCES identities(id) ON DELETE RESTRICT,
  CHECK ((revoked_at IS NULL) = (revoked_by IS NULL))
);
CREATE INDEX platform_release_publishers_active_idx ON platform_release_publishers(identity_id) WHERE revoked_at IS NULL;
CREATE TABLE platform_release_idempotency (
  publisher_identity_id uuid NOT NULL REFERENCES identities(id) ON DELETE RESTRICT,
  idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 16 AND 128),
  request_sha256 text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
  release_id text NOT NULL REFERENCES release_bundles(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (publisher_identity_id, idempotency_key)
);
CREATE FUNCTION reject_audit_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'audit_events are append-only';
END;
$$;
CREATE TRIGGER audit_events_append_only
  BEFORE UPDATE OR DELETE ON audit_events
  FOR EACH ROW EXECUTE FUNCTION reject_audit_event_mutation();
-- +goose Down
DROP TRIGGER IF EXISTS audit_events_append_only ON audit_events;
DROP FUNCTION IF EXISTS reject_audit_event_mutation();
DROP TABLE IF EXISTS platform_release_idempotency, platform_release_publishers;
