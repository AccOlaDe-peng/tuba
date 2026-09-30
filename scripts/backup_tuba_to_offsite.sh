#!/usr/bin/env bash
# Off-host backup of the TUBA single node.
#
# Covers the four things a rebuild needs, all of which live only on the node the
# backup is meant to survive: the PostgreSQL database (receipts, sources,
# releases), an Elasticsearch snapshot (raw and standard evidence plus the index
# templates), the identity realm, and the release bundles. The destination is a
# different host, so losing this node's disk does not take the backup with it.
#
# RPO is one run interval. There is no WAL archiving and no continuous shipping:
# the PostgreSQL instance is shared with another product on this host, and its
# server configuration must not be changed for TUBA's benefit. Losing this node
# between two runs therefore loses the events ingested in that window; the
# measured RPO/RTO belong in the acceptance record, not in this comment.
#
# Runs on the TUBA node and pushes to the backup host over a key restricted to
# that source address. Credentials are read from a root-only environment file so
# the schedule needs no environment of its own.
#
# Usage:
#   scripts/backup_tuba_to_offsite.sh              # run the backup
#   KEEP=7 scripts/backup_tuba_to_offsite.sh       # keep 7 copies on the target
set -euo pipefail

TARGET_HOST="${TUBA_BACKUP_HOST:-10.6.69.21}"
TARGET_ROOT="${TUBA_BACKUP_ROOT:-/opt/tuba-backup/248}"
SSH_KEY="${TUBA_BACKUP_KEY:-/root/.ssh/tuba_backup_ed25519}"
KEEP="${KEEP:-7}"
PSQL="${PSQL:-/opt/adms/postgresql/bin/psql}"
PG_DUMP="${PG_DUMP:-/opt/adms/postgresql/bin/pg_dump}"
ES_URL="${ES_URL:-http://127.0.0.1:9200}"
ES_REPO="${ES_REPO:-tuba_offsite}"
ES_REPO_PATH="${ES_REPO_PATH:-/var/lib/elasticsearch/backups}"
RELEASES_DIR="${RELEASES_DIR:-/opt/tuba/releases}"
# Lower case throughout: Elasticsearch rejects a snapshot name containing any
# upper-case letter, and the run stamp carries T and Z separators.
STAMP="$(date -u +%Y%m%dT%H%M%SZ | tr A-Z a-z)"
WORK="$(mktemp -d /tmp/tuba-backup.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# The schedule has no environment; read the credentials the same way the other
# operational scripts do, and fall back to a root-only file for the realm export
# which needs credentials the API process does not carry.
if [[ -f "${TUBA_BACKUP_ENV:-/opt/tuba/backup.env}" ]]; then
  set -a
  # shellcheck disable=SC1090
  . "${TUBA_BACKUP_ENV:-/opt/tuba/backup.env}"
  set +a
fi
if [[ -z "${DATABASE_URL:-}" ]]; then
  DATABASE_URL="$("${PYTHON:-python3}" - <<'READ_DSN'
import os
for entry in os.listdir("/proc"):
    if not entry.isdigit():
        continue
    try:
        if b"/opt/tuba/bin/tuba-api" not in open("/proc/%s/cmdline" % entry, "rb").read():
            continue
        environ = dict(item.split(b"=", 1)
                       for item in open("/proc/%s/environ" % entry, "rb").read().split(b"\x00")
                       if b"=" in item)
        value = environ.get(b"DATABASE_URL", b"").decode()
        if value:
            print(value)
            break
    except (OSError, PermissionError):
        continue
READ_DSN
)"
fi
if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is not set and could not be read from the running tuba-api" >&2
  exit 2
fi

say() { printf '%s\n' "$*"; }
# Report the response body on failure: curl -f discards it, which turns a
# diagnosable Elasticsearch error into a bare 400 Bad Request.
es() {
  local response code
  response="$(curl -sS -m "${ES_TIMEOUT:-30}" -H 'Content-Type: application/json' -w '\n%{http_code}' "$@")" || return 1
  code="${response##*$'\n'}"
  response="${response%$'\n'*}"
  if [[ "$code" != 2* ]]; then
    printf 'elasticsearch returned HTTP %s: %s\n' "$code" "$response" >&2
    return 1
  fi
  printf '%s\n' "$response"
}

say "backup ${STAMP} -> ${TARGET_HOST}:${TARGET_ROOT}/${STAMP}"

# 1. PostgreSQL, in the custom format so a restore can be selective.
say "postgres: dumping"
"$PG_DUMP" --format=custom --no-owner --no-privileges -f "$WORK/postgres-tuba.dump" "$DATABASE_URL"
# Read the archive back before shipping it: a dump that cannot be listed cannot
# be restored, and finding that out during a recovery is too late.
"${PG_RESTORE:-${PG_DUMP%pg_dump}pg_restore}" --list "$WORK/postgres-tuba.dump" > "$WORK/postgres-toc.txt"
say "  dump verified: $(wc -l < "$WORK/postgres-toc.txt") archive entries"
gzip -9 "$WORK/postgres-tuba.dump"

# 2. Elasticsearch snapshot. The filesystem repository path is already declared
#    in path.repo, so registering it needs no restart.
say "elasticsearch: snapshot"
es -X PUT "${ES_URL}/_snapshot/${ES_REPO}" -d "{\"type\":\"fs\",\"settings\":{\"location\":\"${ES_REPO_PATH}\",\"compress\":true}}" >/dev/null
ES_TIMEOUT=3600 es -X PUT "${ES_URL}/_snapshot/${ES_REPO}/tuba-${STAMP}?wait_for_completion=true" \
   -d '{"indices":"logs-ueba.*,tuba-v1-*","include_global_state":false,"ignore_unavailable":true}' \
   > "$WORK/es-snapshot.json"
"${PYTHON:-python3}" - "$WORK/es-snapshot.json" <<'CHECK_SNAPSHOT'
import json, sys
doc = json.load(open(sys.argv[1]))
snapshot = doc.get("snapshot", {})
# wait_for_completion=true returns the snapshot status, which carries state but
# no "accepted" field; that field only exists on the asynchronous response.
if not snapshot or snapshot.get("state") != "SUCCESS":
    sys.exit("snapshot did not complete successfully: %s" % json.dumps(doc)[:400])
print("  snapshot %s: %d indices, %d shards successful"
      % (snapshot.get("snapshot"), len(snapshot.get("indices", [])), snapshot.get("shards", {}).get("successful", 0)))
CHECK_SNAPSHOT

# 3. Identity realm, exported through the admin API. The realm is the authority
#    for who may reach the data, so it belongs in the same backup as the data.
if [[ -n "${KC_BOOTSTRAP_ADMIN_USERNAME:-}" && -n "${KC_BOOTSTRAP_ADMIN_PASSWORD:-}" ]]; then
  say "keycloak: exporting realm"
  KC_BASE="${KC_BASE:-http://10.6.68.247:8180}"
  TOKEN="$(curl -fsS -m 20 -X POST "${KC_BASE}/realms/master/protocol/openid-connect/token" \
      -d grant_type=password -d client_id=admin-cli \
      --data-urlencode "username=${KC_BOOTSTRAP_ADMIN_USERNAME}" \
      --data-urlencode "password=${KC_BOOTSTRAP_ADMIN_PASSWORD}" \
      | "${PYTHON:-python3}" -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
  curl -fsS -m 60 -X POST "${KC_BASE}/admin/realms/${KC_REALM:-tuba}/partial-export?exportClients=true&exportGroupsAndRoles=true" \
      -H "Authorization: Bearer ${TOKEN}" -o "$WORK/keycloak-realm-${KC_REALM:-tuba}.json"
  say "  realm exported: $(wc -c < "$WORK/keycloak-realm-${KC_REALM:-tuba}.json") bytes"
else
  say "keycloak: SKIPPED (no admin credential in the environment); the realm is not in this backup"
fi

# 4. Release bundles, which the published release rows point at.
if [[ -d "$RELEASES_DIR" ]]; then
  say "releases: copying"
  cp -a "$RELEASES_DIR" "$WORK/releases"
fi

# Watermarks: what the backup corresponds to, so a restore can be compared
# against live Kafka and ES rather than assumed complete.
say "watermarks: recording"
{
  echo "stamp_utc: ${STAMP}"
  echo "host: $(hostname)"
  echo "postgres_database_size: $("$PSQL" "$DATABASE_URL" -Atc "SELECT pg_size_pretty(pg_database_size(current_database()));")"
  echo "ingest_receipts_rows: $("$PSQL" "$DATABASE_URL" -Atc 'SELECT count(*) FROM ingest_receipts;')"
  echo "source_instances: $("$PSQL" "$DATABASE_URL" -Atc 'SELECT count(*) FROM source_instances;')"
  echo "release_bundles: $("$PSQL" "$DATABASE_URL" -Atc 'SELECT count(*) FROM release_bundles;')"
  echo "es_snapshot: tuba-${STAMP}"
  echo "es_repo: ${ES_REPO}"
} > "$WORK/MANIFEST.txt"
cat "$WORK/MANIFEST.txt" | sed 's/^/  /'

say "transfer to ${TARGET_HOST}"
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 \
    "root@${TARGET_HOST}" "mkdir -p '${TARGET_ROOT}/${STAMP}'"
rsync -a -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
      --stats "$WORK/" "root@${TARGET_HOST}:${TARGET_ROOT}/${STAMP}/" >/dev/null
rsync -a --delete -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
      "${ES_REPO_PATH}/" "root@${TARGET_HOST}:${TARGET_ROOT}/${STAMP}/elasticsearch/" >/dev/null

# Drop the local snapshot once the off-host copy is complete. Elasticsearch
# writes the snapshot into the repository before it is shipped, so leaving it
# behind duplicates the evidence store on the node under backup and, measured on
# 2026-09-30, added 4.6 GB and pushed the root filesystem past the 80% stop
# writing watermark. Restoring copies it back from the target; see RUNBOOK.
es -X DELETE "${ES_URL}/_snapshot/${ES_REPO}/tuba-${STAMP}" >/dev/null
say "  local snapshot released after transfer"

# Keep the most recent KEEP copies on the target.
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@${TARGET_HOST}" \
  "cd '${TARGET_ROOT}' && ls -1d */ 2>/dev/null | sort | head -n -${KEEP} | xargs -r rm -rf" || true

say "done: ${TARGET_HOST}:${TARGET_ROOT}/${STAMP}"
