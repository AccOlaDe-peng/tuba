#!/bin/sh
set -eu

: "${TUBA_KEYCLOAK_DB_NAME:?TUBA_KEYCLOAK_DB_NAME is required}"
: "${TUBA_KEYCLOAK_DB_USER:?TUBA_KEYCLOAK_DB_USER is required}"
: "${TUBA_KEYCLOAK_DB_PASSWORD:?TUBA_KEYCLOAK_DB_PASSWORD is required}"

# psql's \getenv keeps identifiers and the password out of the SQL command line.
psql --no-psqlrc --set=ON_ERROR_STOP=1 --dbname=postgres <<'SQL'
\getenv kc_db_name TUBA_KEYCLOAK_DB_NAME
\getenv kc_db_user TUBA_KEYCLOAK_DB_USER
\getenv kc_db_password TUBA_KEYCLOAK_DB_PASSWORD

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'kc_db_user', :'kc_db_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'kc_db_user')
\gexec

SELECT format(
  'ALTER ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
  :'kc_db_user', :'kc_db_password'
)
\gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'kc_db_name', :'kc_db_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'kc_db_name')
\gexec

SELECT format('ALTER DATABASE %I OWNER TO %I', :'kc_db_name', :'kc_db_user')
\gexec

SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
FROM pg_roles
WHERE rolname = :'kc_db_user';
SQL
