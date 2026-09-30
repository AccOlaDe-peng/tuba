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
# The snapshot is written into a repository on this node's own filesystem before
# it is shipped, so the run briefly needs room for a whole copy of the evidence
# store. That copy is not free: on 2026-09-30 it took the root filesystem from
# 70% to 81%, past the Elasticsearch flood-stage watermark of 80%, and
# Elasticsearch answered every indexer with 429 read-only-allow-delete for the
# eleven minutes it took to ship and release it. The run therefore refuses to
# start unless the projected usage stays below the watermark, and it releases
# the local copy even when it fails part way. See RUNBOOK "异机备份与恢复".
#
# Usage:
#   scripts/backup_tuba_to_offsite.sh              # run the backup
#   scripts/backup_tuba_to_offsite.sh --check-only # report whether it fits, then stop
#   KEEP=7 scripts/backup_tuba_to_offsite.sh       # keep 7 copies on the target
#
# Exit codes: 0 ran (or, with --check-only, fits); 2 no database credential;
# 3 could not measure the filesystem, the snapshot size or the watermark;
# 4 refused to run because the copy would cross the watermark.
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
# Where Elasticsearch keeps its indices, used only to estimate how large the
# snapshot will be. The estimate does not have to be exact: it has to be within
# the margin kept below the watermark.
ES_DATA_PATH="${ES_DATA_PATH:-/var/lib/elasticsearch}"
REPO_HEADROOM_MARGIN_PCT="${REPO_HEADROOM_MARGIN_PCT:-2}"
# Lower case throughout: Elasticsearch rejects a snapshot name containing any
# upper-case letter, and the run stamp carries T and Z separators.
STAMP="$(date -u +%Y%m%dT%H%M%SZ | tr A-Z a-z)"
WORK="$(mktemp -d /tmp/tuba-backup.XXXXXX)"

# The local copy of the snapshot is the one thing this run must not leave
# behind: it is what pushed the filesystem over the watermark. Releasing it has
# to survive a failure at any later step, including a failed or interrupted
# transfer, so it is done from the exit trap rather than inline at the end.
LOCAL_SNAPSHOT=""
release_local_snapshot() {
  [[ -n "$LOCAL_SNAPSHOT" ]] || return 0
  local name="$LOCAL_SNAPSHOT"
  LOCAL_SNAPSHOT=""
  # Not the `es` helper: the trap can fire before that is defined, and it must
  # not itself abort under `set -e`.
  local code
  code="$(curl -sS -m "${ES_TIMEOUT:-120}" -o /dev/null -w '%{http_code}' \
          -X DELETE "${ES_URL}/_snapshot/${ES_REPO}/${name}" 2>/dev/null || echo 000)"
  if [[ "$code" == 2* ]]; then
    printf '  local snapshot %s released\n' "$name"
  else
    printf '  WARNING: could not release local snapshot %s (HTTP %s); it is still consuming disk and may keep Elasticsearch above the watermark\n' \
           "$name" "$code" >&2
  fi
}
cleanup() { release_local_snapshot; rm -rf "$WORK"; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

say() { printf '%s\n' "$*"; }

# Size check, before anything else is done. The local copy of the snapshot costs
# about what the evidence store costs, and this run is not allowed to be the
# reason the node crosses the flood-stage watermark: at that watermark
# Elasticsearch marks every index read-only and the indexers' writes are rejected
# until the space comes back. A night without a backup is recoverable; a
# silently read-only cluster is not. It runs first so a run that cannot fit
# fails before spending a minute on a database dump.
say "headroom check"
repo_fs_total_kb="$(df -Pk "$ES_REPO_PATH" | awk 'NR==2 {print $2}')"
repo_fs_used_kb="$(df -Pk "$ES_REPO_PATH" | awk 'NR==2 {print $3}')"
repo_fs_free_kb="$(df -Pk "$ES_REPO_PATH" | awk 'NR==2 {print $4}')"
if [[ -z "$repo_fs_total_kb" || "$repo_fs_total_kb" == "0" ]]; then
  say "  cannot size the filesystem holding ${ES_REPO_PATH}; refusing to snapshot"
  exit 3
fi
estimate_kb="$(du -sk "$ES_DATA_PATH" 2>/dev/null | awk '{print $1}')"
if [[ -z "${estimate_kb:-}" ]]; then
  say "  cannot estimate the snapshot size from ${ES_DATA_PATH}; refusing to snapshot"
  exit 3
fi
# The watermark is read from the cluster rather than assumed, so this guard
# tracks the operational policy instead of duplicating it.
flood_pct="$("${PYTHON:-python3}" - "$ES_URL" <<'READ_FLOOD'
import json, sys, urllib.request
try:
    doc = json.load(urllib.request.urlopen(
        sys.argv[1] + "/_cluster/settings?include_defaults=true&flat_settings=true", timeout=10))
except Exception:
    sys.exit(0)
key = "cluster.routing.allocation.disk.watermark.flood_stage"
for scope in ("persistent", "transient", "defaults"):
    value = doc.get(scope, {}).get(key)
    if value:
        print(str(value).rstrip("%"))
        break
READ_FLOOD
)"
if ! [[ "${flood_pct:-}" =~ ^[0-9]+$ ]]; then
  say "  cannot read the flood-stage watermark from Elasticsearch; refusing to snapshot"
  exit 3
fi
projected_pct=$(( (repo_fs_used_kb + estimate_kb) * 100 / repo_fs_total_kb ))
say "  filesystem: ${repo_fs_used_kb}K used of ${repo_fs_total_kb}K, ${repo_fs_free_kb}K free"
say "  snapshot estimate: ${estimate_kb}K; projected use ${projected_pct}%, limit $(( flood_pct - REPO_HEADROOM_MARGIN_PCT ))%"
if (( projected_pct > flood_pct - REPO_HEADROOM_MARGIN_PCT )); then
  say "REFUSING to snapshot: the local copy would take the filesystem to ${projected_pct}%," >&2
  say "within ${REPO_HEADROOM_MARGIN_PCT} points of the flood-stage watermark (${flood_pct}%), at which" >&2
  say "Elasticsearch marks every index read-only and the indexers' writes are rejected." >&2
  say "Free space on the filesystem holding ${ES_REPO_PATH}, or move the repository off" >&2
  say "this node's root filesystem, then run again. No backup was taken." >&2
  exit 4
fi
if [[ "${1:-}" == "--check-only" ]]; then
  say "check only: the run fits; no backup was taken"
  exit 0
fi

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
#    in path.repo, so registering it needs no restart. The size check above has
#    already established that the copy fits below the watermark.
say "elasticsearch: snapshot"
es -X PUT "${ES_URL}/_snapshot/${ES_REPO}" -d "{\"type\":\"fs\",\"settings\":{\"location\":\"${ES_REPO_PATH}\",\"compress\":true}}" >/dev/null
# From here the local copy exists and the exit trap owns releasing it.
LOCAL_SNAPSHOT="tuba-${STAMP}"
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
  echo "repo_fs_used_pct_before: ${projected_pct} (projected including the snapshot)"
  echo "repo_fs_free_kb_before: ${repo_fs_free_kb}"
  echo "snapshot_estimate_kb: ${estimate_kb}"
} > "$WORK/MANIFEST.txt"
cat "$WORK/MANIFEST.txt" | sed 's/^/  /'

say "transfer to ${TARGET_HOST}"
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 \
    "root@${TARGET_HOST}" "mkdir -p '${TARGET_ROOT}/${STAMP}'"
rsync -a -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
      --stats "$WORK/" "root@${TARGET_HOST}:${TARGET_ROOT}/${STAMP}/" >/dev/null
rsync -a --delete -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
      "${ES_REPO_PATH}/" "root@${TARGET_HOST}:${TARGET_ROOT}/${STAMP}/elasticsearch/" >/dev/null

# Drop the local snapshot once the off-host copy is complete. Restoring copies
# it back from the target; see RUNBOOK. The exit trap also does this, so a
# failure above still releases the space instead of leaving the node over the
# watermark until someone notices.
release_local_snapshot

# Keep the most recent KEEP copies on the target.
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@${TARGET_HOST}" \
  "cd '${TARGET_ROOT}' && ls -1d */ 2>/dev/null | sort | head -n -${KEEP} | xargs -r rm -rf" || true

say "done: ${TARGET_HOST}:${TARGET_ROOT}/${STAMP}"
