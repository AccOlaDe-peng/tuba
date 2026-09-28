#!/usr/bin/env bash
set -euo pipefail

# This entry point coordinates the already idempotent component initializers.
# It does not install retention/ILM policy; production retention remains gated
# by A03. Until that decision is recorded, only the bounded validation profile
# is accepted.
: "${DATABASE_MIGRATION_URL:?DATABASE_MIGRATION_URL is required}"
: "${TUBA_RUNTIME_DB_PASSWORD:?TUBA_RUNTIME_DB_PASSWORD is required}"
: "${KAFKA_BROKERS:?KAFKA_BROKERS is required}"
: "${ES_URL:?ES_URL is required}"
: "${ES_API_KEY:?ES_API_KEY is required}"
: "${KEYCLOAK_URL:?KEYCLOAK_URL is required}"
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
realm=${KEYCLOAK_REALM:-tuba}
if [[ ! $realm =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$ ]]; then
  echo "KEYCLOAK_REALM is invalid" >&2
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

echo "[1/5] Applying PostgreSQL migrations"
"$root/scripts/apply_postgres_migrations.sh"
echo "[2/5] Provisioning PostgreSQL runtime role"
"$root/scripts/provision_postgres_runtime_role.sh"
echo "[3/5] Reconciling bounded Kafka topics"
"$topic_admin" \
  --contract "$root/contracts/events/topics.v1.json" \
  --profile "$profile" \
  --namespace "$TUBA_TOPIC_NAMESPACE" \
  --apply
echo "[4/5] Applying Elasticsearch templates"
"$root/scripts/apply_elasticsearch_assets.sh"
echo "[5/5] Verifying Keycloak realm discovery"

discovery=$(mktemp)
trap 'rm -f -- "$discovery"' EXIT
issuer=${KEYCLOAK_URL%/}/realms/${realm}
curl --fail --silent --show-error --max-time 10 \
  "${issuer}/.well-known/openid-configuration" >"$discovery"
python3 - "$discovery" "$issuer" <<'PY'
import json
import sys

path, expected_issuer = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    document = json.load(handle)
if document.get("issuer") != expected_issuer:
    raise SystemExit("Keycloak discovery issuer does not match the requested realm")
for field in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
    value = document.get(field)
    if not isinstance(value, str) or not value.startswith(expected_issuer + "/"):
        raise SystemExit(f"Keycloak discovery field is missing or outside the realm: {field}")
PY

echo "TUBA single-node bounded initialization completed and Keycloak discovery was verified."
