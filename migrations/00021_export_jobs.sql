-- +goose Up
-- Q03 asynchronous query exports. export_jobs carries the export-specific
-- state of a processing_jobs row of job_type 'export' (1:1). The request
-- snapshot (SPL text, fixed time range, dataset, format, limits) and the
-- creator's permission snapshot (include_sensitive/include_raw plus a
-- fingerprint of the download-relevant permissions) are frozen at creation;
-- the retention boundary is pinned at execution; downloads re-authorize
-- against the live principal. Links expire 15 minutes after completion and
-- are single-use (downloaded_at set on first authorized download).
CREATE TABLE export_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id uuid NOT NULL UNIQUE REFERENCES processing_jobs(id) ON DELETE CASCADE,
  organization_id uuid NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
  namespace text NOT NULL,
  created_by uuid NOT NULL REFERENCES identities(id) ON DELETE RESTRICT,
  dataset text NOT NULL,
  format text NOT NULL CHECK (format IN ('ndjson','csv')),
  query text NOT NULL CHECK (length(query) BETWEEN 1 AND 4096),
  from_ts timestamptz NOT NULL,
  to_ts timestamptz NOT NULL,
  CHECK (from_ts < to_ts),
  row_limit integer NOT NULL CHECK (row_limit BETWEEN 1 AND 100000),
  byte_limit bigint NOT NULL CHECK (byte_limit BETWEEN 1 AND 1073741824),
  include_sensitive boolean NOT NULL,
  include_raw boolean NOT NULL,
  permission_fingerprint text NOT NULL CHECK (length(permission_fingerprint) = 64),
  state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','succeeded','failed','expired')),
  retention_from timestamptz,
  limit_reached text CHECK (limit_reached IN ('rows','bytes')),
  file_sha256 text,
  row_count bigint CHECK (row_count >= 0),
  byte_count bigint CHECK (byte_count >= 0),
  expires_at timestamptz,
  downloaded_at timestamptz,
  downloaded_by uuid REFERENCES identities(id) ON DELETE RESTRICT,
  completed_at timestamptz,
  error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (state <> 'succeeded' OR (completed_at IS NOT NULL AND expires_at IS NOT NULL AND file_sha256 IS NOT NULL)),
  CHECK (downloaded_at IS NULL OR state IN ('succeeded','expired'))
);
CREATE INDEX export_jobs_tenant_idx ON export_jobs(organization_id, created_at DESC, id DESC);
CREATE INDEX export_jobs_expiry_idx ON export_jobs(expires_at) WHERE state = 'succeeded' AND downloaded_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS export_jobs;
