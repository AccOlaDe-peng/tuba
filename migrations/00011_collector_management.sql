-- +goose Up
CREATE TABLE collector_enrollment_tokens (
  token_hash text PRIMARY KEY CHECK (token_hash ~ '^sha256:[a-f0-9]{64}$'),
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  namespace text NOT NULL,
  created_by uuid NOT NULL REFERENCES identities(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  CHECK (expires_at > created_at)
);
CREATE INDEX collector_enrollment_expiry_idx ON collector_enrollment_tokens(expires_at) WHERE consumed_at IS NULL;

CREATE TABLE collector_agents (
  id text PRIMARY KEY CHECK (id ~ '^col_[a-f0-9]{32}$'),
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  namespace text NOT NULL,
  install_id text NOT NULL CHECK (length(install_id) BETWEEN 1 AND 128),
  hostname text NOT NULL CHECK (length(hostname) BETWEEN 1 AND 255),
  os text NOT NULL CHECK (os IN ('linux','windows')),
  architecture text NOT NULL CHECK (architecture IN ('amd64','arm64')),
  credential_ref text NOT NULL UNIQUE CHECK (credential_ref ~ '^sha256:[a-f0-9]{64}$'),
  installed_version text NOT NULL,
  desired_version text,
  state text NOT NULL DEFAULT 'enrolled' CHECK (state IN ('enrolled','running','buffering','backpressured','paused','error','disabled')),
  config_version bigint NOT NULL DEFAULT 0,
  last_heartbeat_at timestamptz,
  heartbeat jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (organization_id, install_id),
  UNIQUE (organization_id, id)
);
CREATE INDEX collector_agents_tenant_seen_idx ON collector_agents(organization_id,last_heartbeat_at DESC);

CREATE TABLE collector_agent_configs (
  collector_id text NOT NULL REFERENCES collector_agents(id) ON DELETE RESTRICT,
  version bigint NOT NULL CHECK (version > 0),
  configuration jsonb NOT NULL CHECK (jsonb_typeof(configuration)='object'),
  created_by uuid NOT NULL REFERENCES identities(id) ON DELETE RESTRICT,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(collector_id,version)
);

-- +goose Down
DROP TABLE IF EXISTS collector_agent_configs, collector_agents, collector_enrollment_tokens;
