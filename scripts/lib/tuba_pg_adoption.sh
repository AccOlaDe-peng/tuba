#!/usr/bin/env bash
# Adoption guards for the two scripts that touch a pre-existing PostgreSQL: the
# read-only prerequisite check and the provisioning script itself.
#
# The detection lives here in one place because a guard that disagrees with the
# preflight that preceded it is worse than having no preflight at all. Only the
# detection and the raw queries are shared; each caller decides its own wording,
# since the checker reports every finding and the provisioner needs one verdict.
#
# 248's PostgreSQL is shared with another product, and the provisioning script
# revokes CONNECT on the database and CREATE on schema public from PUBLIC. On a
# shared database that is not a hardening step, it is an outage for the other
# product. Hence the guards below.

# The product's own tables, derived from migrations/*.sql. A public table that is
# not here belongs to somebody else.
tuba_pg_expected_tables() {
  local root=$1
  grep -hoiE 'create table( if not exists)? +[a-z_"][a-z0-9_"]*' "$root"/migrations/*.sql 2>/dev/null |
    sed -E 's/.*[[:space:]]//' | tr -d '"' | sort -u
}

# The same list rendered as a PostgreSQL ARRAY[...] literal.
tuba_pg_expected_tables_sql() {
  tuba_pg_expected_tables "$1" |
    awk 'BEGIN { printf "ARRAY["; first = 1 }
         NF { gsub(/'"'"'/, "'"'"''"'"'"); printf "%s'"'"'%s'"'"'", (first ? "" : ","), $0; first = 0 }
         END { printf "]" }'
}

tuba_pg_scalar() { # url, sql -> single value on stdout, or empty
  psql "$1" -X -A -t -v ON_ERROR_STOP=1 -c "$2" 2>/dev/null | head -n 1
}

# Tables in schema public that this product did not create.
tuba_pg_foreign_tables() { # url, expected_tables_sql
  tuba_pg_scalar "$1" "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public' AND tablename <> ALL($2)"
}

# How many product tables the runtime role owns. Owning them means it can DDL,
# which contradicts the DML-only boundary the runtime role exists to enforce.
tuba_pg_runtime_owned_tables() { # url, expected_tables_sql, runtime_user
  local user=${3//\'/\'\'}
  tuba_pg_scalar "$1" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_roles r ON r.oid = c.relowner WHERE n.nspname = 'public' AND c.relkind = 'r' AND r.rolname = '$user'"
}
