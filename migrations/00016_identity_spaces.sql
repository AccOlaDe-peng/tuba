-- +goose Up
-- Identity spaces are the registered namespaces (e.g. an AD domain, a local
-- machine account database) inside which Account/Device canonical keys are
-- unique. Registration is idempotent per (organization, name); entities
-- reference a space by its normalized name through the composite FK added
-- below, so an entity can never point at an unregistered authority.
CREATE TABLE identity_spaces (
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  space_id text NOT NULL CHECK (space_id ~ '^is:[a-f0-9]{64}$'),
  name text NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
  kind text NOT NULL CHECK (kind IN ('active_directory','local_accounts','cloud_directory','custom')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, space_id),
  UNIQUE (organization_id, name)
);

ALTER TABLE entities
  ADD CONSTRAINT entities_identity_space_fk
  FOREIGN KEY (organization_id, authority) REFERENCES identity_spaces(organization_id, name);

-- +goose Down
ALTER TABLE entities DROP CONSTRAINT IF EXISTS entities_identity_space_fk;
DROP TABLE IF EXISTS identity_spaces;
