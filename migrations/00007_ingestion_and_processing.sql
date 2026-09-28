-- +goose Up
CREATE TABLE source_instances (
  id text PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_.:-]{1,128}$'),
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  namespace text NOT NULL CHECK (namespace ~ '^[A-Za-z0-9_-]{1,64}$'),
  vendor_name text NOT NULL,
  vendor_product text NOT NULL,
  vendor_dataset text NOT NULL,
  source_epoch text NOT NULL DEFAULT '1',
  credential_ref text NOT NULL,
  release_id text,
  enabled boolean NOT NULL DEFAULT true,
  rate_limit integer NOT NULL DEFAULT 1000 CHECK (rate_limit > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (organization_id, id)
);
CREATE INDEX source_instances_tenant_idx ON source_instances(organization_id, namespace, enabled);

CREATE TABLE ingest_receipts (
  organization_slug text NOT NULL REFERENCES organizations(slug) ON DELETE RESTRICT,
  source_instance_id text NOT NULL,
  raw_event_id text NOT NULL CHECK (raw_event_id ~ '^raw:[a-f0-9]{64}$'),
  payload_hash text NOT NULL CHECK (payload_hash ~ '^[a-f0-9]{64}$'),
  received_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(organization_slug, source_instance_id, raw_event_id)
);
CREATE INDEX ingest_receipts_age_idx ON ingest_receipts(received_at);

CREATE TABLE release_bundles (
  id text PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 128),
  version text NOT NULL,
  manifest jsonb NOT NULL,
  sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
  state text NOT NULL CHECK (state IN ('draft','validated','staged','active','retired')),
  created_by uuid REFERENCES identities(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  UNIQUE (id, version)
);
CREATE INDEX release_bundles_state_idx ON release_bundles(state, created_at DESC);
ALTER TABLE source_instances ADD CONSTRAINT source_instances_release_fk
  FOREIGN KEY (release_id) REFERENCES release_bundles(id) ON DELETE RESTRICT;

CREATE TABLE processing_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  namespace text NOT NULL,
  job_type text NOT NULL CHECK (job_type IN ('replay','backfill','baseline','export','retention')),
  state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
  generation text NOT NULL,
  release_id text REFERENCES release_bundles(id) ON DELETE RESTRICT,
  request jsonb NOT NULL,
  attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  lease_owner text,
  lease_until timestamptz,
  fencing_token bigint NOT NULL DEFAULT 0,
  cancel_requested_at timestamptz,
  last_error text,
  created_by uuid REFERENCES identities(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  started_at timestamptz,
  finished_at timestamptz,
  CHECK ((state = 'running') = (lease_owner IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX processing_jobs_claim_idx ON processing_jobs(state, created_at) WHERE state = 'queued';
CREATE INDEX processing_jobs_lease_idx ON processing_jobs(lease_until) WHERE state = 'running';
CREATE INDEX processing_jobs_tenant_idx ON processing_jobs(organization_id, created_at DESC);

CREATE TABLE processing_job_attempts (
  job_id uuid NOT NULL REFERENCES processing_jobs(id) ON DELETE CASCADE,
  attempt integer NOT NULL,
  fencing_token bigint NOT NULL,
  worker_id text NOT NULL,
  state text NOT NULL CHECK (state IN ('running','succeeded','failed','cancelled','lease_expired')),
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error_code text,
  error_message text,
  PRIMARY KEY(job_id, attempt)
);

CREATE TABLE processor_checkpoints (
  consumer_group text NOT NULL,
  topic text NOT NULL,
  partition integer NOT NULL CHECK (partition >= 0),
  next_offset bigint NOT NULL CHECK (next_offset >= 0),
  fencing_token bigint NOT NULL DEFAULT 0,
  watermark timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(consumer_group, topic, partition)
);

CREATE TABLE processor_inbox (
  consumer_group text NOT NULL,
  message_id text NOT NULL,
  topic text NOT NULL,
  partition integer NOT NULL,
  message_offset bigint NOT NULL,
  payload_hash text NOT NULL CHECK (payload_hash ~ '^[a-f0-9]{64}$'),
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(consumer_group, message_id),
  UNIQUE(consumer_group, topic, partition, message_offset)
);
CREATE INDEX processor_inbox_expiry_idx ON processor_inbox(processed_at);

CREATE TABLE processor_outbox (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  producer text NOT NULL,
  aggregate_key text NOT NULL,
  sequence bigint NOT NULL,
  topic text NOT NULL,
  message_key bytea NOT NULL,
  payload jsonb NOT NULL,
  payload_hash text NOT NULL CHECK (payload_hash ~ '^[a-f0-9]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz,
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error text,
  UNIQUE(producer, aggregate_key, sequence)
);
CREATE INDEX processor_outbox_pending_idx ON processor_outbox(next_attempt_at, id) WHERE sent_at IS NULL;

CREATE TABLE entities (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  entity_id text NOT NULL CHECK (entity_id ~ '^ent:[a-f0-9]{64}$'),
  entity_type text NOT NULL CHECK (entity_type IN ('account','device')),
  authority text NOT NULL,
  canonical_key text NOT NULL,
  identity_strength text NOT NULL CHECK (identity_strength IN ('strong','weak')),
  revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
  document jsonb NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to timestamptz,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(organization_id, entity_id),
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE UNIQUE INDEX entities_strong_identity_uq ON entities(organization_id, entity_type, authority, canonical_key)
  WHERE identity_strength = 'strong' AND valid_to IS NULL;
CREATE INDEX entities_identity_lookup_idx ON entities(organization_id, entity_type, authority, canonical_key);

CREATE TABLE entity_attributions (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  attribution_id text NOT NULL CHECK (attribution_id ~ '^att:[a-f0-9]{64}$'),
  event_id text NOT NULL,
  role text NOT NULL,
  status text NOT NULL CHECK (status IN ('resolved','unresolved','ambiguous')),
  entity_id text,
  resolution_snapshot text NOT NULL,
  evidence jsonb NOT NULL DEFAULT '{}',
  event_time timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(organization_id, attribution_id),
  FOREIGN KEY(organization_id, entity_id) REFERENCES entities(organization_id, entity_id),
  CHECK ((status = 'resolved') = (entity_id IS NOT NULL))
);
CREATE INDEX entity_attributions_event_idx ON entity_attributions(organization_id, event_id, event_time);
CREATE INDEX entity_attributions_entity_idx ON entity_attributions(organization_id, entity_id, event_time DESC) WHERE entity_id IS NOT NULL;

CREATE TABLE entity_relations (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  relation_id text NOT NULL CHECK (relation_id ~ '^rel:[a-f0-9]{64}$'),
  from_entity_id text NOT NULL,
  relation_type text NOT NULL,
  to_entity_id text NOT NULL,
  event_id text NOT NULL,
  resolution_snapshot text NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to timestamptz,
  confidence double precision NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  evidence jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY(organization_id, relation_id),
  FOREIGN KEY(organization_id, from_entity_id) REFERENCES entities(organization_id, entity_id),
  FOREIGN KEY(organization_id, to_entity_id) REFERENCES entities(organization_id, entity_id),
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX entity_relations_time_idx ON entity_relations(organization_id, from_entity_id, valid_from, valid_to);

CREATE TABLE analysis_object_revisions (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  object_type text NOT NULL CHECK (object_type IN ('feature','baseline','anomaly','risk_event','entity_risk')),
  object_id text NOT NULL,
  revision bigint NOT NULL CHECK (revision > 0),
  generation text NOT NULL,
  operation text NOT NULL CHECK (operation IN ('upsert','retracted')),
  partition_date date NOT NULL,
  payload_hash text NOT NULL CHECK (payload_hash ~ '^[a-f0-9]{64}$'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(organization_id, object_type, object_id, generation, revision)
);
CREATE INDEX analysis_object_latest_idx ON analysis_object_revisions(organization_id, object_type, object_id, generation, revision DESC);

-- +goose Down
DROP TABLE IF EXISTS analysis_object_revisions, entity_relations, entity_attributions, entities,
  processor_outbox, processor_inbox, processor_checkpoints, processing_job_attempts, processing_jobs,
  ingest_receipts, source_instances, release_bundles CASCADE;
