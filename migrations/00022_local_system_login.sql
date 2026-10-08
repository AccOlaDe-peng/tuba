-- +goose Up
CREATE TABLE local_accounts (
 identity_id uuid PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
 username text UNIQUE NOT NULL CHECK (username ~ '^[a-z0-9_.-]{3,64}$'),
 organization_id uuid NOT NULL REFERENCES organizations(id),
 password_hash text NOT NULL CHECK (password_hash LIKE 'pbkdf2-sha256$600000$%'),
 failed_attempts integer NOT NULL DEFAULT 0 CHECK (failed_attempts >= 0),
 locked_until timestamptz,
 password_changed_at timestamptz NOT NULL DEFAULT now(),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE auth_sessions (
 token_hash bytea PRIMARY KEY CHECK (octet_length(token_hash)=32),
 identity_id uuid NOT NULL REFERENCES local_accounts(identity_id) ON DELETE CASCADE,
 created_at timestamptz NOT NULL DEFAULT now(),
 expires_at timestamptz NOT NULL
);
CREATE INDEX auth_sessions_identity_idx ON auth_sessions(identity_id);
CREATE INDEX auth_sessions_expiry_idx ON auth_sessions(expires_at);
-- +goose Down
DROP TABLE auth_sessions,local_accounts;
