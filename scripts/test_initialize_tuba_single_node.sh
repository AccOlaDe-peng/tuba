#!/usr/bin/env bash
set -euo pipefail

source_root=$(cd "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d /tmp/tuba-single-node-init-test.XXXXXX)
cleanup() { rm -rf -- "$test_root"; }
trap cleanup EXIT
mkdir -p "$test_root/scripts" "$test_root/contracts/events" "$test_root/bin"
cp "$source_root/scripts/initialize_tuba_single_node.sh" "$test_root/scripts/"
printf '{}\n' >"$test_root/contracts/events/topics.v1.json"

make_step() {
  local path=$1 name=$2
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' '$name' >>"\$TUBA_INIT_TEST_LOG"
EOF
  chmod 0755 "$path"
}
make_step "$test_root/scripts/apply_postgres_migrations.sh" postgres-migrations
make_step "$test_root/scripts/provision_postgres_runtime_role.sh" postgres-runtime-role
make_step "$test_root/scripts/apply_elasticsearch_assets.sh" elasticsearch-assets
make_step "$test_root/bin/tuba-topic-admin" kafka-topics
cat >"$test_root/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat <<JSON
{"issuer":"http://keycloak.test/realms/tuba","authorization_endpoint":"http://keycloak.test/realms/tuba/protocol/openid-connect/auth","token_endpoint":"http://keycloak.test/realms/tuba/protocol/openid-connect/token","jwks_uri":"http://keycloak.test/realms/tuba/protocol/openid-connect/certs"}
JSON
EOF
chmod 0755 "$test_root/bin/curl"

export PATH="$test_root/bin:$PATH"
export TUBA_INIT_TEST_LOG="$test_root/order.log"
export DATABASE_MIGRATION_URL=postgres://migration.invalid/tuba
export TUBA_RUNTIME_DB_PASSWORD=not-logged
export KAFKA_BROKERS=kafka.invalid:9092
export ES_URL=http://elasticsearch.invalid:9200
export ES_API_KEY=not-logged
export KEYCLOAK_URL=http://keycloak.test
export TUBA_TOPIC_NAMESPACE=single_node_validation
export TUBA_TOPIC_ADMIN="$test_root/bin/tuba-topic-admin"

bash "$test_root/scripts/initialize_tuba_single_node.sh" >/dev/null
expected=$'postgres-migrations\npostgres-runtime-role\nkafka-topics\nelasticsearch-assets'
actual=$(cat "$TUBA_INIT_TEST_LOG")
[[ $actual == "$expected" ]] || { echo "Unexpected initializer order: $actual" >&2; exit 1; }

if TUBA_TOPIC_PROFILE=production_single_node bash "$test_root/scripts/initialize_tuba_single_node.sh" >/dev/null 2>&1; then
  echo "Initializer accepted a production profile before A03 approval" >&2
  exit 1
fi
if TUBA_TOPIC_NAMESPACE='Invalid Namespace' bash "$test_root/scripts/initialize_tuba_single_node.sh" >/dev/null 2>&1; then
  echo "Initializer accepted an invalid namespace" >&2
  exit 1
fi

echo "Single-node initializer orchestration test passed: fixed order, discovery validation, and A03 profile gate."
