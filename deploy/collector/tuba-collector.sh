#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BIN="$ROOT/bin/linux-amd64/tuba-collector"
[ -x "$BIN" ] || { echo "Linux Collector binary is missing from this bundle" >&2; exit 1; }
exec "$BIN" "$@" --config "$ROOT/config/collector.json"
