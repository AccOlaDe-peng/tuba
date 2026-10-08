#!/usr/bin/env bash
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

# Scope: TUBA installs only its own product tree, and even that is done by
# install_tuba_linux.sh. PostgreSQL, Kafka, Elasticsearch are
# adopted — they are pre-existing, they are larger than TUBA, and the platform is
# a guest on them (248's PostgreSQL is shared with another product). So this
# entry point coordinates the already-idempotent component initializers against
# those dependencies, checks first that they are safe to touch, and never
# installs or upgrades them.
dependencies_only=0
if [[ ${1:-} == --check-dependencies-only ]]; then
  dependencies_only=1
  set -- "${@:2}"
fi
if [[ ${1:-} == -h || ${1:-} == --help ]]; then
  cat >&2 <<'USAGE'
Usage: initialize_tuba_single_node.sh [--check-dependencies-only]

Runs the read-only adoption preflight and then the component initializers.
With --check-dependencies-only it stops after the preflight and touches nothing.
See scripts/check_tuba_prerequisites.sh for the environment and the
TUBA_ADOPT_SHARED_DATABASE / TUBA_ALLOW_RUNTIME_OWNER acknowledgements.
USAGE
  exit 0
fi
if [[ $# -ne 0 ]]; then
  echo "Unexpected arguments: $*" >&2
  exit 2
fi

# This entry point does not install retention/ILM policy; production retention
# remains gated by A03. Until that decision is recorded, only the bounded
# validation profile is accepted.
: "${DATABASE_MIGRATION_URL:?DATABASE_MIGRATION_URL is required}"
: "${TUBA_RUNTIME_DB_PASSWORD:?TUBA_RUNTIME_DB_PASSWORD is required}"
: "${KAFKA_BROKERS:?KAFKA_BROKERS is required}"
: "${ES_URL:?ES_URL is required}"
: "${ES_API_KEY:?ES_API_KEY is required}"
: "${TUBA_TOPIC_NAMESPACE:?TUBA_TOPIC_NAMESPACE is required}"

profile=${TUBA_TOPIC_PROFILE:-zeek_validation_single_node}
if [[ $profile != zeek_validation_single_node ]]; then
  echo "Production topic profiles require an approved A03 capacity/retention decision; refusing profile: $profile" >&2
  exit 1
fi
if [[ ! $TUBA_TOPIC_NAMESPACE =~ ^[a-z0-9][a-z0-9._-]{0,62}$ ]]; then
  echo "TUBA_TOPIC_NAMESPACE must be a lowercase deployment slug" >&2
  exit 1
fi

root=$(cd "$(dirname "$0")/.." && pwd)
topic_admin=${TUBA_TOPIC_ADMIN:-}
if [[ -z $topic_admin ]]; then
  if [[ -x /opt/tuba/current/bin/tuba-topic-admin ]]; then
    topic_admin=/opt/tuba/current/bin/tuba-topic-admin
  elif command -v tuba-topic-admin >/dev/null 2>&1; then
    topic_admin=$(command -v tuba-topic-admin)
  else
    echo "TUBA_TOPIC_ADMIN must point to the packaged tuba-topic-admin executable" >&2
    exit 1
  fi
fi
if [[ ! -x $topic_admin ]]; then
  echo "TUBA_TOPIC_ADMIN is not executable: $topic_admin" >&2
  exit 1
fi
command -v curl >/dev/null || { echo "curl is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }

echo "[1/5] Checking that the existing dependencies are adoptable"
"$script_dir/check_tuba_prerequisites.sh"
if [[ $dependencies_only -eq 1 ]]; then
  echo "Dependency check complete; nothing else was touched."
  exit 0
fi

echo "[2/5] Applying PostgreSQL migrations"
"$root/scripts/apply_postgres_migrations.sh"
echo "[3/5] Provisioning PostgreSQL runtime role"
"$root/scripts/provision_postgres_runtime_role.sh"
echo "[4/5] Reconciling bounded Kafka topics"
"$topic_admin" \
  --contract "$root/contracts/events/topics.v1.json" \
  --profile "$profile" \
  --namespace "$TUBA_TOPIC_NAMESPACE" \
  --apply
echo "[5/5] Applying Elasticsearch templates"
"$root/scripts/apply_elasticsearch_assets.sh"
echo "TUBA single-node initialization completed. Use tuba-bootstrap-operator for the first system administrator."
