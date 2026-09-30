#!/usr/bin/env bash
# Prune ingest receipts older than the de-duplication window.
#
# ingest_receipts holds one row per accepted event and is never pruned, so it
# grows with every event the platform ever ingests: 2.5 GB after 1.75M events,
# still climbing. Kafka and Elasticsearch are bounded by their own retention and
# plateau; this table does not, and it is what filled the root filesystem.
#
# A receipt is the authority that stops a re-delivered event from being indexed
# twice, so the window is not free to choose. The bound is the retention of the
# topics a redelivery could come from: Kafka keeps source and raw topics for 24
# hours, and the collectors re-read at most 24 hours of their own backlog
# (winlogbeat ignore_older). An event older than that cannot be redelivered, so
# its receipt has no de-duplication work left. Two days is twice that bound.
#
# Do not widen the window without checking the data: the table only started
# accumulating on 2026-09-27, so a seven-day window selects nothing at all while
# the filesystem keeps filling, and pruning only begins once the oldest rows
# pass the window.
#
# Space is not returned to the operating system by DELETE alone: PostgreSQL
# marks the pages reusable inside the table's existing files. That is enough to
# stop the growth, because later inserts reuse them, so the table plateaus
# instead of filling the disk. Reclaiming the files themselves needs VACUUM
# FULL, which takes an ACCESS EXCLUSIVE lock and blocks ingestion while it
# rewrites the table; VACUUM_FULL=true does that as an explicit choice.
#
# Usage:
#   DATABASE_URL=... scripts/prune_ingest_receipts.sh                          # report only
#   DATABASE_URL=... APPLY=true scripts/prune_ingest_receipts.sh               # delete
#   DATABASE_URL=... APPLY=true VACUUM_FULL=true scripts/prune_ingest_receipts.sh
set -euo pipefail

RETENTION_DAYS="${RETENTION_DAYS:-2}"
BATCH_SIZE="${BATCH_SIZE:-20000}"
APPLY="${APPLY:-false}"
VACUUM_FULL="${VACUUM_FULL:-false}"
PSQL="${PSQL:-psql}"

# A scheduled run has no environment of its own, and copying the DSN to disk
# just to reach the scheduler would put the database credential somewhere the
# deployment never intended it. Read it from the running API instead, the same
# way the deployment scripts do; if the API is down nothing is being ingested,
# so there is nothing to prune.
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
if ! [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]] || (( RETENTION_DAYS < 1 )); then
  echo "RETENTION_DAYS must be a positive integer (got '${RETENTION_DAYS}')" >&2
  exit 2
fi
if ! [[ "$BATCH_SIZE" =~ ^[0-9]+$ ]] || (( BATCH_SIZE < 1 || BATCH_SIZE > 500000 )); then
  echo "BATCH_SIZE must be between 1 and 500000 (got '${BATCH_SIZE}')" >&2
  exit 2
fi

query() { "$PSQL" "$DATABASE_URL" -Atc "$1"; }

# Resolved once, not as an expression re-evaluated per batch. A moving cutoff
# keeps admitting rows that age past it while the loop runs, so the batch count
# never reaches zero and the loop does not terminate.
CUTOFF_TS=$(query "SELECT (now() - interval '${RETENTION_DAYS} days')::timestamptz::text;")
CUTOFF="TIMESTAMPTZ '${CUTOFF_TS}'"

total=$(query "SELECT count(*) FROM ingest_receipts;")
older=$(query "SELECT count(*) FROM ingest_receipts WHERE received_at < ${CUTOFF};")
size=$(query "SELECT pg_size_pretty(pg_total_relation_size('ingest_receipts'));")

echo "ingest_receipts: ${total} rows, ${size}"
echo "window          : keep ${RETENTION_DAYS} days (cutoff ${CUTOFF_TS})"
echo "eligible        : ${older} rows"

if [[ "$older" == "0" ]]; then
  echo "nothing older than the window; no action"
  exit 0
fi

if [[ "$APPLY" != "true" ]]; then
  echo
  echo "report only: set APPLY=true to delete these ${older} rows"
  exit 0
fi

echo
echo "deleting in batches of ${BATCH_SIZE}..."
deleted=0
iterations=0
# A fixed cutoff makes the loop terminate, and the cap is a backstop so a future
# change cannot turn this into an endless delete against a live table.
MAX_ITERATIONS=$(( older / BATCH_SIZE + 1000 ))
while :; do
  iterations=$((iterations + 1))
  if (( iterations > MAX_ITERATIONS )); then
    echo "stopped after ${MAX_ITERATIONS} batches with rows still eligible; inspect before retrying" >&2
    exit 1
  fi
  # psql -c also prints a command tag ("DELETE 0"), so the count takes only the
  # RETURNING rows. Counting every output line made an empty batch look like one
  # deleted row and the loop never terminated.
  # ctid bounds each statement to one batch, so no single transaction holds
  # locks across the whole table while ingestion is writing to it.
  n=$(query "WITH doomed AS (SELECT ctid FROM ingest_receipts WHERE received_at < ${CUTOFF} LIMIT ${BATCH_SIZE}) DELETE FROM ingest_receipts WHERE ctid IN (SELECT ctid FROM doomed) RETURNING 1;" | grep -c '^1$' || true)
  n=${n//[[:space:]]/}
  if [[ -z "$n" || "$n" == "0" ]]; then
    break
  fi
  deleted=$((deleted + n))
  echo "  deleted ${deleted}/${older}"
  sleep 0.2
done

# Plain VACUUM is enough to make the freed pages reusable, which is what stops
# the file from growing. It does not shrink the files and does not need an
# exclusive lock.
echo "vacuuming (plain; does not shrink the files, makes the space reusable)..."
"$PSQL" "$DATABASE_URL" -c "VACUUM (ANALYZE) ingest_receipts;" >/dev/null

if [[ "$VACUUM_FULL" == "true" ]]; then
  # Rewrites the table to hand the freed pages back to the filesystem. It takes
  # an ACCESS EXCLUSIVE lock, so ingestion stalls until it finishes; the adapter
  # keeps its source offsets uncommitted and retries, so nothing is lost, but
  # the stall is real and this is why it is opt-in.
  echo "VACUUM FULL: rewriting the table; ingestion will stall until it finishes..."
  "$PSQL" "$DATABASE_URL" -c "VACUUM FULL ingest_receipts;" >/dev/null
  echo "VACUUM FULL done"
fi

echo
echo "deleted         : ${deleted} rows"
echo "remaining       : $(query "SELECT count(*) FROM ingest_receipts;") rows"
echo "table size now  : $(query "SELECT pg_size_pretty(pg_total_relation_size('ingest_receipts'));")"
if [[ "$VACUUM_FULL" != "true" ]]; then
  echo "note: the files keep their size until VACUUM FULL; growth stops because new rows reuse the freed pages"
fi
