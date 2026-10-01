-- +goose Up
-- Links a collector to the Kafka write identity of a source it collects for.
-- Disabling the collector revokes the bound principal's WRITE ACL on the source
-- topic (the SCRAM user and DESCRIBE survive, so re-enabling can restore write
-- access exactly); enabling restores it. write_revoked_at is the ledger of the
-- externally applied state: NULL means the ACL is expected to exist.
CREATE TABLE collector_source_kafka_bindings (
  collector_id text NOT NULL REFERENCES collector_agents(id) ON DELETE RESTRICT,
  source_instance_id text NOT NULL REFERENCES source_instances(id) ON DELETE RESTRICT,
  source_context_id text NOT NULL REFERENCES source_contexts(id) ON DELETE RESTRICT,
  kafka_principal text NOT NULL CHECK (length(kafka_principal) BETWEEN 1 AND 255),
  kafka_topic text NOT NULL CHECK (kafka_topic ~ '^tuba\.source\.ctx_[a-f0-9]{32}\.v[0-9]+$'),
  created_by uuid NOT NULL REFERENCES identities(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  write_revoked_at timestamptz,
  write_revoke_error text,
  PRIMARY KEY (collector_id, source_instance_id)
);

-- +goose Down
DROP TABLE IF EXISTS collector_source_kafka_bindings;
