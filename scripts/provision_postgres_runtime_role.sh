#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_MIGRATION_URL:?DATABASE_MIGRATION_URL is required}"
: "${TUBA_RUNTIME_DB_PASSWORD:?TUBA_RUNTIME_DB_PASSWORD is required}"

runtime_user="${TUBA_RUNTIME_DB_USER:-tuba_runtime}"
migration_user="${TUBA_MIGRATION_DB_USER:-tuba}"
if [[ $runtime_user == "$migration_user" ]]; then
  echo "runtime and migration database users must be different" >&2
  exit 1
fi
export TUBA_RUNTIME_DB_USER="$runtime_user"
export TUBA_MIGRATION_DB_USER="$migration_user"
command -v psql >/dev/null || { echo "psql is required" >&2; exit 1; }

# Run as the database bootstrap/migration administrator against a dedicated TUBA
# database. Keep the runtime role limited to DML; never grant it schema changes.
psql "$DATABASE_MIGRATION_URL" -X -v ON_ERROR_STOP=1 -q <<'SQL'
\getenv runtime_user TUBA_RUNTIME_DB_USER
\getenv runtime_password TUBA_RUNTIME_DB_PASSWORD
\getenv migration_user TUBA_MIGRATION_DB_USER

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'runtime_user', :'runtime_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_user')
\gexec

SELECT format(
  'ALTER ROLE %I LOGIN PASSWORD %L INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
  :'runtime_user', :'runtime_password'
)
\gexec

SELECT format('REVOKE %I FROM %I', granted.rolname, :'runtime_user')
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE member.rolname = :'runtime_user'
\gexec

SELECT format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', current_database())
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'migration_user')
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'runtime_user')
\gexec

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SELECT format('GRANT USAGE, CREATE ON SCHEMA public TO %I', :'migration_user')
\gexec
SELECT format('REVOKE ALL ON SCHEMA public FROM %I', :'runtime_user')
\gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'runtime_user')
\gexec

SELECT format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %I.%I TO %I', schemaname, tablename, :'runtime_user')
FROM pg_tables
WHERE schemaname = 'public' AND tablename <> 'tuba_schema_migrations'
\gexec

SELECT format('GRANT USAGE, SELECT, UPDATE ON SEQUENCE %I.%I TO %I', schemaname, sequencename, :'runtime_user')
FROM pg_sequences
WHERE schemaname = 'public'
\gexec

SELECT format(
  'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I',
  :'migration_user', :'runtime_user'
)
\gexec
SELECT format(
  'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %I',
  :'migration_user', :'runtime_user'
)
\gexec

SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
FROM pg_roles
WHERE rolname = :'runtime_user';
SELECT current_user AS provisioned_by, current_database() AS target_database;
SELECT defaclrole::regrole AS owner_role, defaclnamespace::regnamespace AS schema_name,
       defaclobjtype AS object_type, defaclacl AS default_acl
FROM pg_default_acl
WHERE defaclrole = (SELECT oid FROM pg_roles WHERE rolname = :'migration_user')
  AND defaclnamespace = 'public'::regnamespace;
SQL
