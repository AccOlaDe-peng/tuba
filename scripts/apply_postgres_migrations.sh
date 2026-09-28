#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_MIGRATION_URL:?DATABASE_MIGRATION_URL is required; runtime DATABASE_URL is not accepted for migrations}"
database_url="$DATABASE_MIGRATION_URL"
command -v psql >/dev/null || { echo "psql is required" >&2; exit 1; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 1; }

root="$(cd "$(dirname "$0")/.." && pwd)"
up_file="$(mktemp)"
trap 'rm -f "$up_file"' EXIT
psql "$database_url" -X -v ON_ERROR_STOP=1 -q <<'SQL'
SELECT pg_advisory_lock(74190024001) \g /dev/null
SET client_min_messages = warning;
CREATE TABLE IF NOT EXISTS public.tuba_schema_migrations (
  version text PRIMARY KEY,
  checksum char(64) NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SELECT pg_advisory_unlock(74190024001) \g /dev/null
SQL

shopt -s nullglob
files=("$root"/migrations/[0-9]*.sql)
if ((${#files[@]} == 0)); then
  echo "No PostgreSQL migrations found under $root/migrations" >&2
  exit 1
fi

for file in "${files[@]}"; do
  name="$(basename "$file")"
  if [[ ! $name =~ ^([0-9]{5}_[a-z0-9_]+)\.sql$ ]]; then
    echo "Unexpected migration filename: $name" >&2
    exit 1
  fi
  version=${BASH_REMATCH[1]}
  if ! awk '
    { marker = $0; sub(/\r$/, "", marker) }
    marker == "-- +goose Up" { if (found_up) exit 2; in_up = 1; found_up = 1; next }
    marker == "-- +goose Down" && in_up { found_down = 1; exit }
    in_up { print }
    END { if (!found_up || !found_down) exit 2 }
  ' "$file" > "$up_file"; then
    echo "Migration must contain one complete Goose Up section: $name" >&2
    exit 1
  fi
  checksum="$(sha256sum "$file")"
  checksum=${checksum%% *}

  set +e
  {
    cat <<'SQL'
\set ON_ERROR_STOP on
SELECT pg_advisory_lock(74190024001) \g /dev/null
SELECT CASE WHEN EXISTS (
  SELECT 1 FROM public.tuba_schema_migrations
  WHERE version = :'version' AND checksum <> :'checksum'
) THEN 'Migration checksum changed after it was applied: ' || :'version'
  ELSE '1' END::integer \g /dev/null
SELECT EXISTS (
  SELECT 1 FROM public.tuba_schema_migrations WHERE version = :'version'
) AS migration_exists \gset
\if :migration_exists
  \echo Skipping already applied migration :version
  \quit
\endif
BEGIN;
SQL
    cat "$up_file"
    printf "\nINSERT INTO public.tuba_schema_migrations(version, checksum) VALUES (:'version', :'checksum');\nCOMMIT;\nSELECT pg_advisory_unlock(74190024001) \\g /dev/null\n"
  } | psql "$database_url" -X -v ON_ERROR_STOP=1 -q -v version="$version" -v checksum="$checksum"
  pipeline_status=("${PIPESTATUS[@]}")
  set -e
  if ((pipeline_status[0] != 0 || pipeline_status[1] != 0)); then
    echo "Migration failed: $name" >&2
    exit 1
  fi

  echo "Migration state verified: $name"
done
