#!/usr/bin/env bash
# Driving the adoption preflight against fake dependencies.
#
# The fake psql doubles as the read-only proof: it fails the run if it is ever
# handed DDL or a write, so "this preflight installs nothing" is checked rather
# than asserted in a comment. The fake curl answers health, settings and
# discovery from a scenario file so each risk can be staged independently.
set -euo pipefail

source_root=$(cd "$(dirname "$0")/.." && pwd -P)
test_root=$(mktemp -d /tmp/tuba-prereq-test.XXXXXX)
cleanup() { rm -rf -- "$test_root"; }
trap cleanup EXIT
mkdir -p "$test_root/bin" "$test_root/state"

# --- fakes ------------------------------------------------------------------

cat >"$test_root/bin/psql" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
sql=""
while [[ $# -gt 0 ]]; do
  case $1 in
    -c) sql=$2; shift 2 ;;
    -X|-A|-t|-q|-v) shift ;;
    ON_ERROR_STOP=1) shift ;;
    *) shift ;;
  esac
done
# Read-only proof: the preflight must never send a mutation. psql_scalar in the
# checker discards stderr and pipes through head, so a mutating statement's exit
# code never reaches the caller — asserting on exit codes cannot catch it. The
# fake therefore logs every statement it is handed, and the test asserts on the
# log: whatever the exit code, the preflight must never have asked for a write.
[[ -n ${FAKE_PG_LOG-} ]] && printf '%s\n' "$sql" >>"$FAKE_PG_LOG"
[[ ${FAKE_PG_CONNECT:-1} == 1 ]] || exit 2
if [[ $sql == *"format("* && $sql == *"FROM pg_roles WHERE rolname"* ]]; then
  # The checker reads attributes and memberships in one row, "|"-joined. The
  # format( marker is what separates this from the plain existence check below —
  # both mention FROM pg_roles WHERE rolname.
  if [[ -n ${FAKE_PG_ATTRS-} ]]; then printf '%s|%s' "$FAKE_PG_ATTRS" "${FAKE_PG_MEMBERSHIPS-}"; fi
elif [[ $sql == *"SELECT 1 FROM pg_roles"* ]]; then
  echo "${FAKE_PG_ROLE_EXISTS-1}"
elif [[ $sql == *"SELECT 1"* ]]; then
  echo 1
elif [[ $sql == *"SHOW server_version"* ]]; then
  echo "${FAKE_PG_VERSION-14.23}"
elif [[ $sql == *"FROM pg_tables"* && $sql == *"count("* ]]; then
  echo "${FAKE_PG_PRESENT-0}"
elif [[ $sql == *"string_agg(tablename"* ]]; then
  echo "${FAKE_PG_FOREIGN-}"
elif [[ $sql == *"FROM pg_class"* ]]; then
  echo "${FAKE_PG_OWNED-0}"
else
  echo ""
fi
FAKE
chmod 0755 "$test_root/bin/psql"

cat >"$test_root/bin/curl" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
# No ${VAR:-{...}} defaults here: the braces in a JSON default terminate the
# parameter expansion early and leave a stray trailing brace. Assign explicitly.
url=""
for arg in "$@"; do case $arg in http*) url=$arg ;; esac; done
case $url in
  */_cluster/health)
    [[ ${FAKE_ES_HEALTH:-ok} == down ]] && exit 7
    body=${FAKE_ES_HEALTH_BODY-}
    [[ -n $body ]] || body='{"status":"green","number_of_data_nodes":3}'
    printf '%s' "$body"
    ;;
  */_cluster/settings*)
    [[ ${FAKE_ES_SETTINGS-d} == down ]] && exit 7
    body=${FAKE_ES_SETTINGS-}
    [[ -n $body ]] || body='{"defaults":{"cluster":{"routing":{"allocation":{"disk":{"watermark":{"low":"75%","high":"78%"}}}}}}}'
    printf '%s' "$body"
    ;;
  */.well-known/openid-configuration)
    [[ ${FAKE_KC:-ok} == down ]] && exit 7
    body=${FAKE_KC_BODY-}
    [[ -n $body ]] || body='{"issuer":"http://keycloak.test/realms/tuba"}'
    printf '%s' "$body"
    ;;
  *) exit 7 ;;
esac
FAKE
chmod 0755 "$test_root/bin/curl"

export PATH="$test_root/bin:$PATH"
export FAKE_PG_LOG="$test_root/sql.log"
: >"$FAKE_PG_LOG"
export TUBA_STATE_DIR="$test_root/state"
export TUBA_PREREQ_MIN_FREE_PCT=0
export DATABASE_MIGRATION_URL=postgres://migration.invalid/tuba
export KAFKA_BROKERS=broker.test:9092
export ES_URL=http://es.test:9200
export KEYCLOAK_URL=http://keycloak.test

# Kafka reachability uses /dev/tcp, which no fake can intercept, so point it at a
# listener this test controls: a refused connection is then a real scenario.
run_check() { # captures stdout+stderr, returns the checker's exit code
  local out=$1
  shift
  set +e
  bash "$source_root/scripts/check_tuba_prerequisites.sh" "$@" >"$out" 2>&1
  local code=$?
  set -e
  echo "$code"
}

# The checker's contract is stdout = the --json report, stderr = the human
# report, so the JSON case has to read stdout on its own.
run_check_stdout() {
  local out=$1
  shift
  set +e
  bash "$source_root/scripts/check_tuba_prerequisites.sh" "$@" >"$out" 2>"$out.err"
  local code=$?
  set -e
  echo "$code"
}

expect_contains() { # file, needle, description
  grep -qF -- "$2" "$1" || { echo "FAIL: $3 (missing: $2)" >&2; cat "$1" >&2; exit 1; }
}
expect_not_contains() {
  grep -qF -- "$2" "$1" && { echo "FAIL: $3 (unexpected: $2)" >&2; cat "$1" >&2; exit 1; }
  return 0
}

# The preflight is read-only. Checked against the SQL the fake was actually
# handed, not against an exit code the pipe would have swallowed. The non-empty
# guard matters too: if logging ever breaks, the grep would pass vacuously.
assert_preflight_read_only() {
  [[ -s $FAKE_PG_LOG ]] || { echo "FAIL: no SQL reached the fake psql; logging is broken" >&2; exit 1; }
  if grep -qiE '\b(insert|update|delete|drop|alter|revoke|grant|truncate|create)\b' "$FAKE_PG_LOG"; then
    echo "FAIL: the preflight sent a mutating statement:" >&2
    grep -inE '\b(insert|update|delete|drop|alter|revoke|grant|truncate|create)\b' "$FAKE_PG_LOG" >&2
    exit 1
  fi
}

# The Kafka probe wants a real TCP listener; without one every scenario would
# fail on Kafka and mask what it is testing. Start one and use its port.
port_file=$test_root/port
python3 - "$port_file" <<'PY' &
import socket, sys, threading, time
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", 0))
s.listen(128)
open(sys.argv[1], "w").write(str(s.getsockname()[1]))
while True:
    try:
        conn, _ = s.accept()
        conn.close()
    except OSError:
        break
PY
KAFKA_PID=$!
for _ in $(seq 1 100); do [[ -s $port_file ]] && break; sleep 0.1; done
KAFKA_PORT=$(cat "$port_file")
export KAFKA_BROKERS="127.0.0.1:${KAFKA_PORT}"
trap 'kill '"$KAFKA_PID"' 2>/dev/null || true; cleanup' EXIT

fail() { echo "FAIL: $1" >&2; cat "${2:-/dev/null}" >&2; exit 1; }

# --- 1. fresh dedicated database, healthy dependencies ----------------------
unset FAKE_PG_FOREIGN FAKE_PG_ATTRS FAKE_PG_MEMBERSHIPS FAKE_PG_OWNED FAKE_PG_PRESENT
out=$test_root/fresh.txt
[[ $(run_check "$out") == 0 ]] || fail "a fresh healthy environment must pass, not fail" "$out"
expect_contains "$out" "no TUBA tables yet" "fresh database recognised"
expect_contains "$out" "does not exist yet" "absent runtime role reported as fine"
expect_contains "$out" "dependencies are adoptable" "summary printed"

# --- 2. shared database is refused, then acknowledged ----------------------
export FAKE_PG_FOREIGN="orders,order_items"
out=$test_root/shared.txt
[[ $(run_check "$out") == 1 ]] || fail "a shared database must be refused by default" "$out"
expect_contains "$out" "database is shared" "shared database detected"
expect_contains "$out" "orders,order_items" "the other product's tables are named"
expect_contains "$out" "Nothing was installed or modified" "refusal states nothing changed"

export TUBA_ADOPT_SHARED_DATABASE=yes
out=$test_root/shared-ack.txt
[[ $(run_check "$out") == 0 ]] || fail "an acknowledged shared database must be accepted" "$out"
expect_contains "$out" "acknowledged" "acknowledgement recorded"
unset TUBA_ADOPT_SHARED_DATABASE FAKE_PG_FOREIGN

# --- 3. runtime role that owns the schema is refused -----------------------
export FAKE_PG_ATTRS="f,f,f,f,f" FAKE_PG_OWNED=33
out=$test_root/owner.txt
[[ $(run_check "$out") == 1 ]] || fail "a runtime role owning the schema must be refused" "$out"
expect_contains "$out" "owns 33 table(s)" "ownership counted"
expect_contains "$out" "DML-only boundary" "the reason is spelled out"

export TUBA_ALLOW_RUNTIME_OWNER=yes
out=$test_root/owner-ack.txt
[[ $(run_check "$out") == 0 ]] || fail "acknowledged ownership must be accepted" "$out"
expect_contains "$out" "acknowledged" "ownership acknowledgement recorded"
unset TUBA_ALLOW_RUNTIME_OWNER FAKE_PG_OWNED FAKE_PG_ATTRS

# --- 4. privileged runtime role is refused --------------------------------
export FAKE_PG_ATTRS="t,f,f,f,f"
out=$test_root/priv.txt
[[ $(run_check "$out") == 1 ]] || fail "a superuser runtime role must be refused" "$out"
expect_contains "$out" "dangerous attributes" "privileged role detected"
unset FAKE_PG_ATTRS

# --- 5. established database is reported as established -------------------
export FAKE_PG_PRESENT=31 FAKE_PG_ATTRS="f,f,f,f,f"
out=$test_root/established.txt
[[ $(run_check "$out") == 0 ]] || fail "an established database must pass" "$out"
expect_contains "$out" "all 31 TUBA tables are present" "existing migrations recognised"
unset FAKE_PG_PRESENT FAKE_PG_ATTRS

# --- 6. single data node on default watermarks warns ----------------------
export FAKE_ES_HEALTH_BODY='{"status":"yellow","number_of_data_nodes":1}'
export FAKE_ES_SETTINGS='{"defaults":{"cluster":{"routing":{"allocation":{"disk":{"watermark":{"low":"85%","high":"90%"}}}}}}}'
out=$test_root/watermark.txt
[[ $(run_check "$out") == 0 ]] || fail "a watermark warning must not be fatal" "$out"
expect_contains "$out" "default 85%/90% watermarks" "the single-node trap is flagged"
unset FAKE_ES_HEALTH_BODY FAKE_ES_SETTINGS

# --- 7. red cluster and bad issuer are refused ----------------------------
export FAKE_ES_HEALTH_BODY='{"status":"red","number_of_data_nodes":3}'
out=$test_root/red.txt
[[ $(run_check "$out") == 1 ]] || fail "a red cluster must be refused" "$out"
expect_contains "$out" "status is red" "red cluster detected"
unset FAKE_ES_HEALTH_BODY

export FAKE_KC_BODY='{"issuer":"http://other/realms/tuba"}'
out=$test_root/issuer.txt
[[ $(run_check "$out") == 1 ]] || fail "an issuer mismatch must be refused" "$out"
expect_contains "$out" "does not match" "issuer mismatch detected"
unset FAKE_KC_BODY

# --- 8. unreachable dependencies are refused ------------------------------
unset KAFKA_BROKERS
out=$test_root/nokafka.txt
[[ $(run_check "$out") == 1 ]] || fail "a missing KAFKA_BROKERS must be refused" "$out"
expect_contains "$out" "KAFKA_BROKERS is not set" "missing broker config detected"
export KAFKA_BROKERS="127.0.0.1:1"
out=$test_root/kafkadown.txt
[[ $(run_check "$out") == 1 ]] || fail "an unreachable broker must be refused" "$out"
expect_contains "$out" "no broker" "unreachable broker detected"
export KAFKA_BROKERS="127.0.0.1:${KAFKA_PORT}"

# --- 9. --json is machine-readable and consistent -------------------------
export FAKE_PG_FOREIGN="orders" FAKE_PG_ATTRS="f,f,f,f,f" FAKE_PG_MEMBERSHIPS="legacy_reader"
out=$test_root/report.json
[[ $(run_check_stdout "$out" --json) == 1 ]] || fail "--json must still exit non-zero on failure" "$out"
expect_contains "$out" '"failures"' "json went to stdout"
expect_not_contains "$out" "PASS postgres" "--json stdout stays machine-readable"
expect_contains "$out.err" "Nothing was installed or modified" "the refusal summary stayed on stderr"
python3 - "$out" <<'PY' || fail "--json output was not usable"
import json, sys
report = json.load(open(sys.argv[1], encoding="utf-8"))
assert report["failures"] >= 1, report
assert report["warnings"] >= 1, report
assert all({"level", "component", "message"} <= set(f) for f in report["findings"]), report
print("json ok:", report["failures"], "failures,", report["warnings"], "warnings")
PY
unset FAKE_PG_FOREIGN FAKE_PG_ATTRS FAKE_PG_MEMBERSHIPS

# --- 10. the provisioning guard agrees on what is shared ------------------
# Isolated on purpose: the runtime role is clean here, so a shared database is
# the only reason to refuse. With an ownership failure also in play, a broken
# shared guard would be masked by the ownership guard exiting for its own reason.
export FAKE_PG_FOREIGN="orders,order_items" FAKE_PG_ATTRS="f,f,f,f,f" FAKE_PG_OWNED=0
prov=$test_root/provision.txt
set +e
DATABASE_MIGRATION_URL=postgres://migration.invalid/tuba TUBA_RUNTIME_DB_PASSWORD=x \
  bash "$source_root/scripts/provision_postgres_runtime_role.sh" >"$prov" 2>&1
prov_rc=$?
set -e
# Assert on the message *and* the exit code. Neither alone is enough: the guard
# prints its refusal before exiting, so a guard whose exit was dropped still
# matches the message and then falls through; and a guard whose message was
# dropped still exits non-zero. Together they pin both halves.
expect_contains "$prov" "Refusing to provision against a shared database" "provisioning guard fires"
expect_contains "$prov" "orders,order_items" "guard names the other product's tables"
[[ $prov_rc == 1 ]] || fail "provisioning must exit 1 when refusing a shared database (got $prov_rc)" "$prov"

# --- 10b. the ownership guard, on a database that is not shared ------------
export FAKE_PG_FOREIGN="" FAKE_PG_OWNED=33
set +e
DATABASE_MIGRATION_URL=postgres://migration.invalid/tuba TUBA_RUNTIME_DB_PASSWORD=x \
  bash "$source_root/scripts/provision_postgres_runtime_role.sh" >"$prov" 2>&1
prov_rc=$?
set -e
expect_contains "$prov" "owns the schema" "ownership guard fires without a shared database"
expect_contains "$prov" "owns 33 table(s)" "the guard counts what the runtime role owns"
[[ $prov_rc == 1 ]] || fail "provisioning must exit 1 when refusing runtime ownership (got $prov_rc)" "$prov"

# --- 10c. acknowledging both lets it reach the real provisioning SQL -------
export TUBA_ADOPT_SHARED_DATABASE=yes TUBA_ALLOW_RUNTIME_OWNER=yes
set +e
DATABASE_MIGRATION_URL=postgres://migration.invalid/tuba TUBA_RUNTIME_DB_PASSWORD=x \
  bash "$source_root/scripts/provision_postgres_runtime_role.sh" >"$prov" 2>&1
set -e
expect_contains "$prov" "Accepted: tuba_runtime owns 33 table(s)" "an acknowledged owner is reported, not silent"
unset TUBA_ADOPT_SHARED_DATABASE TUBA_ALLOW_RUNTIME_OWNER FAKE_PG_OWNED FAKE_PG_ATTRS

# --- 11. everything above was read-only -----------------------------------
assert_preflight_read_only

echo "Adoption preflight test passed: read-only, shared database and runtime ownership refused by default, acknowledgements honoured."
