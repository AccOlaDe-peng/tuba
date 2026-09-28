#!/usr/bin/env bash
set -euo pipefail

: "${KAFKA_BROKERS:?set one reachable Kafka bootstrap address}"
: "${KAFKA_ADMIN_CONFIG:?set path to an admin Kafka CLI config}"
KAFKA_BIN="${KAFKA_BIN:-}"
if [[ -z "$KAFKA_BIN" ]]; then
  command -v kafka-configs.sh >/dev/null || { echo "kafka-configs.sh is required" >&2; exit 2; }
  KAFKA_BIN="$(dirname "$(command -v kafka-configs.sh)")"
fi
configs="$KAFKA_BIN/kafka-configs.sh"
topics="$KAFKA_BIN/kafka-topics.sh"
producer="$KAFKA_BIN/kafka-console-producer.sh"
for tool in "$configs" "$topics" "$producer"; do
  [[ -x "$tool" ]] || { echo "Kafka CLI not executable: $tool" >&2; exit 2; }
done

probe_suffix="$(date -u +%Y%m%d%H%M%S)-$$"
probe_user="tuba-acl-probe-$probe_suffix"
probe_topic="tuba.acl-probe.$probe_suffix.v1"
probe_password="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
probe_config="$(mktemp)"
probe_error="$(mktemp)"
umask 077
cleanup() {
  "$configs" --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_ADMIN_CONFIG" \
    --alter --entity-type users --entity-name "$probe_user" --delete-config SCRAM-SHA-512 >/dev/null 2>&1 || true
  "$topics" --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_ADMIN_CONFIG" \
    --delete --topic "$probe_topic" >/dev/null 2>&1 || true
  rm -f -- "$probe_config" "$probe_error"
}
trap cleanup EXIT

# Use a disposable topic so the probe never reads or writes production events.
"$topics" --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_ADMIN_CONFIG" \
  --create --topic "$probe_topic" --partitions 1 --replication-factor 1 \
  --config retention.ms=60000 --config max.message.bytes=1048576 >/dev/null

"$configs" --bootstrap-server "$KAFKA_BROKERS" --command-config "$KAFKA_ADMIN_CONFIG" \
  --alter --entity-type users --entity-name "$probe_user" \
  --add-config "SCRAM-SHA-512=[password=$probe_password]" >/dev/null

cat >"$probe_config" <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$probe_user" password="$probe_password";
EOF
chmod 600 "$probe_config"

producer_rc=0
printf '{"probe":"acl-default-deny"}\n' | "$producer" --bootstrap-server "$KAFKA_BROKERS" \
  --producer.config "$probe_config" --topic "$probe_topic" --timeout 5000 >/dev/null 2>"$probe_error" || producer_rc=$?
if ! grep -Eiq 'TopicAuthorizationException|not authorized to access topic|topic authorization failed' "$probe_error"; then
  if [[ "$producer_rc" -eq 0 ]]; then
    echo "FAIL: authenticated principal without ACL could write to $probe_topic" >&2
    exit 1
  fi
  echo "FAIL: produce failed for a reason other than ACL denial" >&2
  cat "$probe_error" >&2
  exit 1
fi
echo "PASS: authenticated principal without topic ACL was denied produce on a disposable topic"
