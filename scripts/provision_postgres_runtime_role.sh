#!/usr/bin/env bash
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
product_root=$(cd -- "${script_dir}/.." && pwd -P)
# shellcheck source=lib/tuba_pg_adoption.sh
source "${script_dir}/lib/tuba_pg_adoption.sh"

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

# The block below revokes CONNECT on the database and CREATE on schema public for
# PUBLIC. On a dedicated TUBA database that is hardening. On a database shared
# with another product -- 248's PostgreSQL is shared -- it takes that product
# down with it. So both share-conditions are refused by default and need an
# explicit acknowledgement; this is the step that motivated the prerequisite
# check in the first place.
expected_tables_sql=$(tuba_pg_expected_tables_sql "$product_root")
foreign_tables=$(tuba_pg_foreign_tables "$DATABASE_MIGRATION_URL" "$expected_tables_sql")
if [[ -n $foreign_tables ]]; then
  if [[ ${TUBA_ADOPT_SHARED_DATABASE:-} != yes ]]; then
    cat >&2 <<EOF
Refusing to provision against a shared database.

Schema public contains tables this product did not create:
  $foreign_tables

Provisioning revokes CONNECT on the database and CREATE on schema public from
PUBLIC, and revokes ALL on schema public from ${runtime_user}. If another product
relies on PUBLIC privileges here, those steps break it.

If a dedicated TUBA database is not available, re-run with
TUBA_ADOPT_SHARED_DATABASE=yes to accept that risk knowingly.
EOF
    exit 1
  fi
  echo "Adopting a shared database (TUBA_ADOPT_SHARED_DATABASE=yes); other products' tables: $foreign_tables" >&2
fi

# A runtime role that owns the tables can DDL, which defeats the DML-only
# boundary the separate runtime identity exists to enforce. 248 is in exactly
# this state. Provisioning does not take ownership away -- that is a migration --
# so the operator has to decide.
if [[ $(tuba_pg_scalar "$DATABASE_MIGRATION_URL" "SELECT 1 FROM pg_roles WHERE rolname = '${runtime_user//\'/\'\'}'") == 1 ]]; then
  owned=$(tuba_pg_runtime_owned_tables "$DATABASE_MIGRATION_URL" "$expected_tables_sql" "$runtime_user")
  if [[ ${owned:-0} -gt 0 && ${TUBA_ALLOW_RUNTIME_OWNER:-} != yes ]]; then
    cat >&2 <<EOF
Refusing to provision a runtime role that owns the schema.

${runtime_user} owns ${owned} table(s) in schema public, so it can issue DDL. The
runtime account is supposed to hold DML only; owning the tables makes that
boundary nominal.

Set TUBA_ALLOW_RUNTIME_OWNER=yes to accept a runtime role that is also the schema
owner, or move ownership to the migration identity first.
EOF
    exit 1
  fi
  [[ ${owned:-0} -gt 0 ]] && echo "Accepted: ${runtime_user} owns ${owned} table(s) and can DDL." >&2
fi

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
