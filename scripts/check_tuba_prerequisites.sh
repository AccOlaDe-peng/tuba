#!/usr/bin/env bash
# Preflight for existing, externally-installed dependencies.
#
# TUBA installs exactly one thing: its own versioned product tree. PostgreSQL,
# Kafka, Elasticsearch and Keycloak are *adopted* — they already exist, they are
# larger than TUBA (248's PostgreSQL is shared with another product), and the
# platform is a guest on them. So this script never installs, upgrades or
# reconfigures any of them. It answers one question per dependency: does what is
# already running satisfy what TUBA requires, and does anything about it need an
# explicit operator acknowledgement before the initializer touches it.
#
# It is read-only by construction: no DDL, no DML, no topic creation, no cluster
# setting. The one thing it cannot check without mutating is Kafka authentication
# — that is exercised by the initializer's own topic step, where creating topics
# is the intended action.
#
# Every finding is one line, either PASS, WARN (accepted, recorded) or FAIL
# (refused). Failures are the ones that would cost data or break another product
# if the initializer went ahead anyway, so they stop the run.
#
# The risks that motivate the FAILs — all observed on 248 in September 2026:
#   - provisioning against a database another product owns would revoke that
#     product's access (the provisioning script revokes CONNECT/CREATE for PUBLIC);
#   - a runtime role that is also the schema owner can DDL, which contradicts the
#     whole point of a separate runtime identity;
#   - a single-node ES cluster with the default disk watermarks stops allocating
#     shards at 85% full, which on 248 presented as a "failed restore" that was
#     really a watermark.
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
product_root=$(cd -- "${script_dir}/.." && pwd -P)
# shellcheck source=lib/tuba_pg_adoption.sh
source "${script_dir}/lib/tuba_pg_adoption.sh"

usage() {
  cat >&2 <<'USAGE'
Usage: check_tuba_prerequisites.sh [--json]

Adopts, does not install. Verifies the pre-existing PostgreSQL, Kafka,
Elasticsearch and Keycloak instances against what TUBA requires, and refuses the
combinations that are unsafe to initialize against. Read-only: it never creates
a topic, applies a migration or changes a cluster setting.

Required environment (the same variables the initializer consumes):
  DATABASE_MIGRATION_URL   DDL identity
  KAFKA_BROKERS            broker list, for example 10.6.68.248:29292
  ES_URL                   Elasticsearch base URL
  KEYCLOAK_URL             Keycloak base URL
Optional:
  ES_API_KEY                 used only for the health/watermark queries
  KEYCLOAK_REALM             realm to verify discovery for (default: tuba)
  TUBA_RUNTIME_DB_USER       runtime role to inspect (default: tuba_runtime)
  TUBA_ADOPT_SHARED_DATABASE=yes   acknowledge a database shared with other products
  TUBA_ALLOW_RUNTIME_OWNER=yes     acknowledge a runtime role that owns the schema
  TUBA_PREREQ_MIN_FREE_PCT         free-space floor (default: 30)

Exit codes: 0 all checks passed (warnings allowed), 1 a check failed.
USAGE
}

json=0
while [[ $# -gt 0 ]]; do
  case $1 in
    --json) json=1 ;;
    -h | --help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
  shift
done

findings=()
failures=0
warnings=0

record() { # level, component, message
  local level=$1 component=$2 message=$3
  findings+=("${level}|${component}|${message}")
  if [[ $level == FAIL ]]; then
    failures=$((failures + 1))
  elif [[ $level == WARN ]]; then
    warnings=$((warnings + 1))
  fi
  if [[ $json -eq 0 ]]; then
    printf '%-4s %-14s %s\n' "$level" "$component" "$message" >&2
  fi
}

# The product's own tables come from migrations/*.sql via the shared adoption
# lib, so this preflight and the provisioning guard agree on what "ours" means.
expected_tables_sql=$(tuba_pg_expected_tables_sql "$product_root")

psql_scalar() { tuba_pg_scalar "$@"; }

check_postgres() {
  local url=${DATABASE_MIGRATION_URL:-}
  if [[ -z $url ]]; then
    record FAIL postgres "DATABASE_MIGRATION_URL is not set"
    return
  fi
  if ! command -v psql >/dev/null 2>&1; then
    record FAIL postgres "psql is not available on this host"
    return
  fi
  if [[ $(psql_scalar "$url" 'SELECT 1') != 1 ]]; then
    record FAIL postgres "cannot connect with the migration identity"
    return
  fi
  local server_version
  server_version=$(psql_scalar "$url" 'SHOW server_version')
  record PASS postgres "connected; server version ${server_version:-unknown}"

  # Which tables exist, and do any of them belong to somebody else? Checked
  # first because it decides how much the rest of the answer matters.
  local present_count total foreign_list
  total=$(printf '%s\n' "$(tuba_pg_expected_tables "$product_root")" | grep -c . || true)
  present_count=$(psql_scalar "$url" "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = ANY($expected_tables_sql)")
  foreign_list=$(tuba_pg_foreign_tables "$url" "$expected_tables_sql")
  if [[ -n $foreign_list ]]; then
    # 248's PostgreSQL is shared with another product. Provisioning revokes
    # CONNECT on the database and CREATE on schema public for PUBLIC, which on a
    # shared database takes those products down with it.
    if [[ ${TUBA_ADOPT_SHARED_DATABASE:-} == yes ]]; then
      record WARN postgres "database is shared with other products (acknowledged); public also contains: ${foreign_list}"
    else
      record FAIL postgres "database is shared: public contains unrelated tables (${foreign_list}); provisioning would revoke CONNECT/CREATE for PUBLIC and break those products. Set TUBA_ADOPT_SHARED_DATABASE=yes only after confirming they do not rely on PUBLIC privileges"
    fi
  fi
  if [[ ${present_count:-0} -eq 0 ]]; then
    if [[ -n $foreign_list ]]; then
      record PASS postgres "no TUBA tables yet, but the database is not empty (see the share finding above)"
    else
      record PASS postgres "no TUBA tables yet; fresh database"
    fi
  elif [[ ${present_count:-0} -eq $total ]]; then
    record PASS postgres "all ${total} TUBA tables are present"
  else
    record WARN postgres "${present_count} of ${total} TUBA tables are present; migrations will fill the rest"
  fi

  # The runtime role must exist, must not be privileged, and must not own the
  # schema. 248's runtime role is the schema owner, so it can DDL — that defeats
  # the point of a DML-only identity and nothing in the initializer reported it.
  local runtime_user=${TUBA_RUNTIME_DB_USER:-tuba_runtime} attrs memberships owned
  # One query, not two: the attributes and the memberships are read together so a
  # role that exists and a role that does not cannot be confused, and so the
  # ${owned:-0} branches below only run for a role that is really there.
  attrs=$(psql_scalar "$url" "SELECT format('%s,%s,%s,%s,%s', rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls) || '|' || COALESCE((SELECT string_agg(granted.rolname, ',') FROM pg_auth_members m JOIN pg_roles granted ON granted.oid = m.roleid JOIN pg_roles member ON member.oid = m.member WHERE member.rolname = '${runtime_user//\'/\'\'}'), '') FROM pg_roles WHERE rolname = '${runtime_user//\'/\'\'}'")
  if [[ -z $attrs ]]; then
    record PASS postgres "runtime role ${runtime_user} does not exist yet; provisioning will create it"
    return
  fi
  local role_attrs=${attrs%%|*}
  memberships=${attrs#*|}
  if [[ $role_attrs != "f,f,f,f,f" ]]; then
    record FAIL postgres "runtime role ${runtime_user} has dangerous attributes (superuser,createdb,createrole,replication,bypassrls = ${role_attrs})"
    return
  fi
  record PASS postgres "runtime role ${runtime_user} exists with no dangerous attributes"
  if [[ -n $memberships ]]; then
    record WARN postgres "runtime role ${runtime_user} inherits from: ${memberships}; provisioning revokes these"
  fi
  owned=$(tuba_pg_runtime_owned_tables "$url" "$expected_tables_sql" "$runtime_user")
  if [[ ${owned:-0} -gt 0 ]]; then
    if [[ ${TUBA_ALLOW_RUNTIME_OWNER:-} == yes ]]; then
      record WARN postgres "runtime role ${runtime_user} owns ${owned} table(s) and can therefore DDL (acknowledged)"
    else
      record FAIL postgres "runtime role ${runtime_user} owns ${owned} table(s), so the DML-only boundary does not hold; set TUBA_ALLOW_RUNTIME_OWNER=yes to accept that knowingly"
    fi
  fi
}

broker_reachable() { # host:port
  timeout 5 bash -c "exec 3<>/dev/tcp/${1%:*}/${1##*:}" 2>/dev/null
}

check_kafka() {
  local brokers=${KAFKA_BROKERS:-}
  if [[ -z $brokers ]]; then
    record FAIL kafka "KAFKA_BROKERS is not set"
    return
  fi
  # Without `timeout` every probe below would hang or fail, which would read as
  # "no broker is reachable" — a misleading refusal. Say what is actually wrong.
  if ! command -v timeout >/dev/null 2>&1; then
    record WARN kafka "timeout is not available; skipping the broker reachability probe"
    return
  fi
  # Reachability only. An authenticated admin call would prove the SASL
  # credentials work, but the only client that does so creates topics — and this
  # preflight is read-only. Authentication is exercised by the initializer's own
  # topic step, where creating topics is the intended action.
  local any_reachable=0 broker
  IFS=',' read -ra broker_list <<<"$brokers"
  for broker in "${broker_list[@]}"; do
    broker=${broker// /}
    [[ -z $broker ]] && continue
    if [[ $broker != *:* ]]; then
      record FAIL kafka "broker ${broker} is not in host:port form"
      continue
    fi
    if broker_reachable "$broker"; then
      any_reachable=1
    else
      record WARN kafka "broker ${broker} did not accept a TCP connection within 5s"
    fi
  done
  if [[ $any_reachable -eq 1 ]]; then
    record PASS kafka "at least one broker in ${brokers} is reachable"
  else
    record FAIL kafka "no broker in ${brokers} accepted a TCP connection"
  fi
}

check_elasticsearch() {
  local url=${ES_URL:-}
  if [[ -z $url ]]; then
    record FAIL elasticsearch "ES_URL is not set"
    return
  fi
  if ! command -v curl >/dev/null 2>&1; then
    record FAIL elasticsearch "curl is not available on this host"
    return
  fi
  local auth=()
  [[ -n ${ES_API_KEY:-} ]] && auth=(-H "Authorization: ApiKey ${ES_API_KEY}")
  local health
  if ! health=$(curl --fail --silent --show-error --max-time 10 "${auth[@]}" "${url%/}/_cluster/health"); then
    record FAIL elasticsearch "cluster health is not reachable"
    return
  fi
  local status data_nodes
  status=$(printf '%s' "$health" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))' 2>/dev/null || true)
  data_nodes=$(printf '%s' "$health" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("number_of_data_nodes",""))' 2>/dev/null || true)
  if [[ $status == red ]]; then
    record FAIL elasticsearch "cluster status is red"
    return
  fi
  record PASS elasticsearch "cluster status is ${status:-unknown} with ${data_nodes:-?} data node(s)"

  # The single-data-node trap: with the default watermarks ES stops allocating
  # new shards at 85% used, and on 248 that surfaced as restores that "failed"
  # to allocate rather than as a disk problem.
  if [[ ${data_nodes:-0} == 1 ]]; then
    local settings low high
    settings=$(curl --fail --silent --show-error --max-time 10 "${auth[@]}" "${url%/}/_cluster/settings?include_defaults=true" 2>/dev/null || true)
    if [[ -z $settings ]]; then
      record WARN elasticsearch "could not read cluster settings to check the disk watermarks"
      return
    fi
    low=$(printf '%s' "$settings" | python3 "$script_dir/lib/read_es_watermark.py" low 2>/dev/null || true)
    high=$(printf '%s' "$settings" | python3 "$script_dir/lib/read_es_watermark.py" high 2>/dev/null || true)
    if [[ $low == 85%* && $high == 90%* ]]; then
      record WARN elasticsearch "single data node is on the default 85%/90% watermarks, so new shards simply stop allocating at 85% used; scripts/tuba_capacity_guard.py sets 75/78/80 deliberately"
    else
      record PASS elasticsearch "single data node with non-default watermarks (low=${low:-unset}, high=${high:-unset})"
    fi
  fi
}

check_keycloak() {
  local url=${KEYCLOAK_URL:-}
  if [[ -z $url ]]; then
    record FAIL keycloak "KEYCLOAK_URL is not set"
    return
  fi
  local realm=${KEYCLOAK_REALM:-tuba}
  local issuer=${url%/}/realms/${realm}
  local discovery
  if ! discovery=$(curl --fail --silent --show-error --max-time 10 "${issuer}/.well-known/openid-configuration"); then
    record FAIL keycloak "realm ${realm} discovery is not reachable at ${issuer}"
    return
  fi
  if printf '%s' "$discovery" | python3 -c 'import json,sys; raise SystemExit(0 if json.load(sys.stdin).get("issuer") == sys.argv[1] else 1)' "$issuer"; then
    record PASS keycloak "realm ${realm} discovery matches the issuer"
  else
    record FAIL keycloak "realm ${realm} discovery issuer does not match ${issuer}"
  fi
  # Identity is adopted, not installed: TUBA never provisions users here, so a
  # realm with no members is a legitimate state, not a failure.
  record PASS keycloak "identity is adopted; TUBA does not manage users in this realm"
}

check_host() {
  local min_free=${TUBA_PREREQ_MIN_FREE_PCT:-30}
  local target=${TUBA_STATE_DIR:-/var/lib/tuba}
  # Split from the line above on purpose: bash expands a whole `local a=1 b=$a`
  # before assigning any of it, so a single line leaves $target unbound.
  local probe=$target
  while [[ ! -d $probe && $probe != / ]]; do probe=$(dirname -- "$probe"); done
  local free_pct
  free_pct=$(df -P "$probe" 2>/dev/null | awk 'NR==2 {gsub(/%/,"",$5); print 100-$5}')
  if [[ -z $free_pct ]]; then
    record WARN host "could not measure free space for ${probe}"
  elif [[ $free_pct -lt $min_free ]]; then
    record FAIL host "only ${free_pct}% free on ${probe} (minimum ${min_free}%)"
  else
    record PASS host "${free_pct}% free on ${probe}"
  fi
  if command -v python3 >/dev/null 2>&1 && command -v curl >/dev/null 2>&1; then
    record PASS host "python3 and curl are available"
  else
    record FAIL host "python3 and curl are both required by the initializer"
  fi
}

check_postgres
check_kafka
check_elasticsearch
check_keycloak
check_host

if [[ $json -eq 1 ]]; then
  python3 - "${findings[@]+"${findings[@]}"}" <<'PY'
import json, sys
items = []
for raw in sys.argv[1:]:
    level, component, message = raw.split("|", 2)
    items.append({"level": level, "component": component, "message": message})
print(json.dumps({
    "findings": items,
    "failures": sum(1 for i in items if i["level"] == "FAIL"),
    "warnings": sum(1 for i in items if i["level"] == "WARN"),
}, indent=2))
PY
fi

if [[ $failures -gt 0 ]]; then
  echo "Prerequisite check refused: ${failures} failure(s), ${warnings} warning(s). Nothing was installed or modified." >&2
  exit 1
fi
echo "Prerequisite check passed: dependencies are adoptable (${warnings} warning(s)). Nothing was installed or modified." >&2
