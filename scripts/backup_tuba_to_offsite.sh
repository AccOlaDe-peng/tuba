#!/usr/bin/env bash
# Off-host backup of the TUBA single node.
#
# Covers the four things a rebuild needs, all of which live only on the node the
# backup is meant to survive: the PostgreSQL database (receipts, sources,
# releases), an Elasticsearch snapshot (raw and standard evidence plus the index
# templates), the identity realm, and the release bundles. Everything ends up on
# a different host, so losing this node's disk does not take the backup with it.
#
# RPO is one run interval. There is no WAL archiving and no continuous shipping:
# the PostgreSQL instance is shared with another product on this host, and its
# server configuration must not be changed for TUBA's benefit. Losing this node
# between two runs therefore loses the events ingested in that window; the
# measured RPO/RTO belong in the acceptance record, not in this comment.
#
# The Elasticsearch snapshot goes straight into a repository that lives on the
# backup host and is mounted at ES_REPO_PATH. It must not be written here: on
# 2026-09-30 a version of this script that staged the snapshot on the node took
# the root filesystem from 70% to 81%, past the Elasticsearch flood-stage
# watermark, and every indexer was answered with 429 read-only-allow-delete for
# the eleven minutes it took to ship and release the copy. The mount check below
# exists so that cannot happen again by accident.
#
# Because the repository is shared and incremental, the snapshot is kept, not
# deleted: later snapshots reuse its segments. Retention is applied through the
# Elasticsearch API (the newest KEEP snapshots), not by deleting directories.
#
# Usage:
#   scripts/backup_tuba_to_offsite.sh              # run the backup
#   scripts/backup_tuba_to_offsite.sh --check-only # report whether it fits, then stop
#   KEEP=7 scripts/backup_tuba_to_offsite.sh       # keep 7 snapshots on the target
#
# Exit codes: 0 ran (or, with --check-only, fits); 2 no database credential;
# 3 could not measure the filesystem or the snapshot size; 4 the repository
# filesystem is too full to take another copy; 5 ES_REPO_PATH is not a separate
# mount, so the snapshot would land on this node's root filesystem.
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
# snapshot will be. The estimate does not have to be exact: it only has to be
# the right order of magnitude for the free-space check.
ES_DATA_PATH="${ES_DATA_PATH:-/var/lib/elasticsearch}"
# The backup host is not under the node's capacity policy, so this is an
# ordinary "do not fill the backup target" bound rather than a watermark.
REPO_TARGET_MAX_PCT="${REPO_TARGET_MAX_PCT:-90}"
# Lower case throughout: Elasticsearch rejects a snapshot name containing any
# upper-case letter, and the run stamp carries T and Z separators.
STAMP="$(date -u +%Y%m%dT%H%M%SZ | tr A-Z a-z)"
WORK="$(mktemp -d /tmp/tuba-backup.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

say() { printf '%s\n' "$*"; }

# Pre-flight, before anything else is done, so a run that cannot fit fails
# before spending a minute on a database dump.
say "pre-flight"
# The one check that matters most. If the repository is not a separate mount
# then ES_REPO_PATH is an ordinary directory on the root filesystem, and the
# snapshot would be written here — the exact failure this script exists to avoid.
if ! mountpoint -q "$ES_REPO_PATH"; then
  say "REFUSING to run: ${ES_REPO_PATH} is not a mount point." >&2
  say "An Elasticsearch snapshot written there would consume this node's root filesystem," >&2
  say "and at the flood-stage watermark that marks every index read-only and rejects the" >&2
  say "indexers' writes. Mount the backup host's repository at ${ES_REPO_PATH} first" >&2
  say "(see RUNBOOK, 异机备份与恢复). No backup was taken." >&2
  exit 5
fi
repo_fs_total_kb="$(df -Pk "$ES_REPO_PATH" | awk 'NR==2 {print $2}')"
repo_fs_used_kb="$(df -Pk "$ES_REPO_PATH" | awk 'NR==2 {print $3}')"
repo_fs_free_kb="$(df -Pk "$ES_REPO_PATH" | awk 'NR==2 {print $4}')"
if [[ -z "$repo_fs_total_kb" || "$repo_fs_total_kb" == "0" ]]; then
  say "  cannot size the filesystem holding ${ES_REPO_PATH}; refusing to run" >&2
  exit 3
fi
estimate_kb="$(du -sk "$ES_DATA_PATH" 2>/dev/null | awk '{print $1}')"
if [[ -z "${estimate_kb:-}" ]]; then
  say "  cannot estimate the snapshot size from ${ES_DATA_PATH}; refusing to run" >&2
  exit 3
fi
# The repository is incremental, so a later snapshot usually costs far less than
# this; the estimate is the pessimistic whole-store figure, which is the right
# thing to refuse on.
repo_fs_after_pct=$(( (repo_fs_used_kb + estimate_kb) * 100 / repo_fs_total_kb ))
say "  repository filesystem: ${repo_fs_used_kb}K used of ${repo_fs_total_kb}K, ${repo_fs_free_kb}K free"
say "  snapshot estimate: ${estimate_kb}K; worst-case use ${repo_fs_after_pct}%, limit ${REPO_TARGET_MAX_PCT}%"
if (( repo_fs_after_pct > REPO_TARGET_MAX_PCT )); then
  say "REFUSING to run: another snapshot could take ${ES_REPO_PATH} to ${repo_fs_after_pct}%," >&2
  say "past the ${REPO_TARGET_MAX_PCT}% bound for the backup target. Free space on" >&2
  say "${TARGET_HOST}, or lower KEEP, then run again. No backup was taken." >&2
  exit 4
fi
if [[ "${1:-}" == "--check-only" ]]; then
  say "  snapshots currently in ${ES_REPO}:"
  # Reach the repository the same way the run does, so this reports what the run
  # would see rather than what the config says it should see.
  if curl -sS -m 20 "${ES_URL}/_snapshot/${ES_REPO}/_all" -o "$WORK/es-snapshots.json" 2>/dev/null; then
    "${PYTHON:-python3}" - "$WORK/es-snapshots.json" <<'LIST_SNAPSHOTS'
import json, sys
doc = json.load(open(sys.argv[1]))
snaps = [s for s in doc.get("snapshots", []) if s.get("snapshot", "").startswith("tuba-")]
snaps.sort(key=lambda s: s.get("start_time_in_millis", 0))
if not snaps:
    print("    (none)")
for s in snaps:
    print("    %s %s" % (s.get("state"), s.get("snapshot")))
LIST_SNAPSHOTS
  else
    say "    (repository not registered yet)"
  fi
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

# 2. Elasticsearch snapshot, written into the mounted off-host repository. The
#    filesystem repository path is already declared in path.repo, so registering
#    it needs no restart, and the pre-flight above established it fits.
say "elasticsearch: snapshot"
es -X PUT "${ES_URL}/_snapshot/${ES_REPO}" \
   -d "{\"type\":\"fs\",\"settings\":{\"location\":\"${ES_REPO_PATH}\",\"compress\":true}}" >/dev/null
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

# 2b. Retention, through the API rather than by deleting files. Snapshots in a
#     filesystem repository share segments, so dropping the oldest lets
#     Elasticsearch release only the segments no remaining snapshot needs. Only
#     snapshots this script created are considered; anything else in the
#     repository is left alone.
say "elasticsearch: retention (keep ${KEEP})"
es -X GET "${ES_URL}/_snapshot/${ES_REPO}/_all" > "$WORK/es-snapshots.json"
# The listing goes through a file rather than a pipe: `python3 -` takes its
# program from stdin, so a heredoc and a pipe cannot both supply it.
"${PYTHON:-python3}" - "$WORK/es-snapshots.json" "$KEEP" <<'LIST_STALE' > "$WORK/es-stale.txt"
import json, sys
doc = json.load(open(sys.argv[1]))
keep = int(sys.argv[2])
# The REST listing names each snapshot in "snapshot", not "name" — the
# repository's own index file uses "name", which is why this looks wrong but is
# not. Getting it wrong silently produces an empty list and no retention at all.
snaps = [s for s in doc.get("snapshots", [])
         if s.get("snapshot", "").startswith("tuba-") and s.get("state") == "SUCCESS"]
snaps.sort(key=lambda s: s.get("start_time_in_millis", 0), reverse=True)
for s in snaps[keep:]:
    print(s["snapshot"])
LIST_STALE
while read -r stale; do
  [[ -n "$stale" ]] || continue
  say "  expiring ${stale}"
  # Asynchronous on purpose: the run should not wait for a large segment delete,
  # and a failure here leaves a surplus snapshot rather than an invalid backup.
  es -X DELETE "${ES_URL}/_snapshot/${ES_REPO}/${stale}" >/dev/null || \
    say "  WARNING: could not expire ${stale}; it still counts against the target's space" >&2
done < "$WORK/es-stale.txt"

# 3. Native accounts and password digests are included in the PostgreSQL dump.
# External Linux authentication systems are not TUBA login dependencies.

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
  echo "es_repository: ${ES_REPO} at ${TARGET_HOST}:${ES_REPO_PATH} (mounted here)"
  echo "es_snapshot_note: the snapshot stays in the shared repository; restore it from there"
  echo "snapshot_estimate_kb: ${estimate_kb}"
  echo "repo_fs_free_kb_before: ${repo_fs_free_kb}"
} > "$WORK/MANIFEST.txt"
cat "$WORK/MANIFEST.txt" | sed 's/^/  /'

say "transfer artefacts to ${TARGET_HOST}"
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 \
    "root@${TARGET_HOST}" "mkdir -p '${TARGET_ROOT}/${STAMP}'"
# Only the small artefacts are copied; the snapshot is already on the target and
# copying the repository here would create a second, divergent generation of it.
rsync -a -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
      --stats "$WORK/" "root@${TARGET_HOST}:${TARGET_ROOT}/${STAMP}/" >/dev/null

# Keep the most recent KEEP artefact directories on the target. The Elasticsearch
# snapshots are pruned above, by the API; these directories hold the dump, the
# realm export and the release bundles.
ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new "root@${TARGET_HOST}" \
  "cd '${TARGET_ROOT}' && ls -1d */ 2>/dev/null | sort | head -n -${KEEP} | xargs -r rm -rf" || true

say "done: ${TARGET_HOST}:${TARGET_ROOT}/${STAMP}"
say "snapshot tuba-${STAMP} is in ${ES_REPO} on ${TARGET_HOST}:${ES_REPO_PATH}"
