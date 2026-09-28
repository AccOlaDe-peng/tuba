-- +goose Up
CREATE TABLE source_contexts (
  id text PRIMARY KEY CHECK (id ~ '^ctx_[a-f0-9]{32}$'),
  source_instance_id text NOT NULL REFERENCES source_instances(id) ON DELETE RESTRICT,
  source_epoch text NOT NULL,
  organization_slug text NOT NULL REFERENCES organizations(slug) ON DELETE RESTRICT,
  namespace text NOT NULL,
  vendor_name text NOT NULL,
  vendor_product text NOT NULL,
  vendor_dataset text NOT NULL,
  release_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source_instance_id, id)
);
CREATE INDEX source_contexts_source_idx ON source_contexts(source_instance_id, created_at DESC);
CREATE FUNCTION reject_source_context_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'source contexts are immutable';
END;
$$;
CREATE TRIGGER source_contexts_immutable_trg
  BEFORE UPDATE OR DELETE ON source_contexts
  FOR EACH ROW EXECUTE FUNCTION reject_source_context_mutation();

CREATE TABLE source_credentials (
  credential_ref text PRIMARY KEY CHECK (credential_ref ~ '^sha256:[a-f0-9]{64}$'),
  source_instance_id text NOT NULL REFERENCES source_instances(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  valid_until timestamptz
);
CREATE INDEX source_credentials_source_idx ON source_credentials(source_instance_id, valid_until);

ALTER TABLE ingest_receipts ADD COLUMN source_context_id text;
ALTER TABLE ingest_receipts ADD COLUMN trusted_metadata jsonb;
ALTER TABLE ingest_receipts ADD COLUMN kafka_acked_at timestamptz;
ALTER TABLE ingest_receipts ADD CONSTRAINT ingest_receipts_source_context_fk
  FOREIGN KEY(source_instance_id, source_context_id)
  REFERENCES source_contexts(source_instance_id, id) ON DELETE RESTRICT;

INSERT INTO source_credentials(credential_ref, source_instance_id)
SELECT credential_ref, id FROM source_instances;
INSERT INTO source_contexts(id, source_instance_id, source_epoch, organization_slug, namespace,
  vendor_name, vendor_product, vendor_dataset, release_id)
SELECT 'ctx_' || md5(si.id || ':' || si.source_epoch), si.id, si.source_epoch, o.slug, si.namespace,
  si.vendor_name, si.vendor_product, si.vendor_dataset, COALESCE(si.release_id, '')
FROM source_instances si JOIN organizations o ON o.id=si.organization_id;

-- +goose Down
ALTER TABLE ingest_receipts DROP CONSTRAINT IF EXISTS ingest_receipts_source_context_fk;
ALTER TABLE ingest_receipts DROP COLUMN IF EXISTS source_context_id;
ALTER TABLE ingest_receipts DROP COLUMN IF EXISTS trusted_metadata;
ALTER TABLE ingest_receipts DROP COLUMN IF EXISTS kafka_acked_at;
DROP TABLE IF EXISTS source_credentials, source_contexts;
DROP FUNCTION IF EXISTS reject_source_context_mutation();
