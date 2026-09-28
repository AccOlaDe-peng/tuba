#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -x $1 ]]; then
  echo "Usage: verify_launcher_boot_recovery.sh EXECUTABLE_LAUNCHER" >&2
  exit 2
fi
launcher=$(realpath "$1")
root=$(mktemp -d /tmp/tuba-launcher-boot-recovery.XXXXXX)
manifest=${root}/manifest.json
state_dir=${root}/state
log_dir=${root}/logs
state_file=${state_dir}/launcher-state.json
old_runner_pid=''
old_service_pid=''
new_runner_pid=''
new_service_pid=''

read_runner_pid() {
  sed -n 's/.*"runner_pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$state_file" | head -n 1
}
read_service_pid() {
  awk '
    /"helper"[[:space:]]*:/ { in_service=1; next }
    in_service && /"pid"[[:space:]]*:/ { gsub(/[^0-9]/, "", $0); print; exit }
    in_service && /}/ { exit }
  ' "$state_file"
}
cleanup() {
  for pid in "$new_runner_pid" "$new_service_pid" "$old_runner_pid" "$old_service_pid"; do
    if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
  done
  rm -rf -- "$root"
}
trap cleanup EXIT

cat >"$manifest" <<JSON
{
  "version": 1,
  "state_dir": "$state_dir",
  "log_dir": "$log_dir",
  "services": [{
    "name": "helper",
    "command": "/bin/sh",
    "args": ["-c", "exec sleep 300"],
    "working_dir": "/",
    "restart_min": "100ms",
    "restart_max": "1s"
  }]
}
JSON

"$launcher" start --manifest "$manifest"
for _ in $(seq 1 50); do
  old_runner_pid=$(read_runner_pid)
  old_service_pid=$(read_service_pid)
  if [[ -n $old_runner_pid && -n $old_service_pid ]] && kill -0 "$old_service_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
if [[ -z $old_runner_pid || -z $old_service_pid ]] || ! kill -0 "$old_runner_pid" 2>/dev/null; then
  echo "Initial Launcher supervisor/helper did not start." >&2
  exit 1
fi

# Simulate a host reboot: the supervisor and child disappear while persisted
# state still contains the old PID and old process identity.
kill -KILL "$old_service_pid" "$old_runner_pid" 2>/dev/null || true
for _ in $(seq 1 50); do
  if ! kill -0 "$old_runner_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
# Model an upgrade from a pre-identity Launcher after PID reuse: point stale
# state at this live shell and remove runner_identity entirely. The new
# Launcher must not trust a legacy PID-only state as a running supervisor.
sed -i -E \
  -e "s/(\"runner_pid\"[[:space:]]*:[[:space:]]*)[0-9]+/\1$$/" \
  -e '/"runner_identity"[[:space:]]*:/d' \
  "$state_file"

"$launcher" start --manifest "$manifest"
for _ in $(seq 1 50); do
  new_runner_pid=$(read_runner_pid)
  new_service_pid=$(read_service_pid)
  if [[ -n $new_runner_pid && $new_runner_pid != "$$" && -n $new_service_pid ]] && kill -0 "$new_service_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
if [[ -z $new_runner_pid || $new_runner_pid == "$$" || -z $new_service_pid ]] || ! kill -0 "$new_runner_pid" 2>/dev/null; then
  echo "Launcher did not recover from prior-boot PID reuse state." >&2
  exit 1
fi
status=$("$launcher" status --manifest "$manifest")
if [[ $status != *"helper"*"running"* ]]; then
  echo "Recovered Launcher did not report its helper running: $status" >&2
  exit 1
fi

# Stop through the host signal path and verify supervisor drain/state commit.
kill -TERM "$new_runner_pid"
for _ in $(seq 1 100); do
  if ! kill -0 "$new_runner_pid" 2>/dev/null; then break; fi
  sleep 0.1
done
if kill -0 "$new_runner_pid" 2>/dev/null; then
  echo "Recovered Launcher did not exit after SIGTERM." >&2
  exit 1
fi
status=$("$launcher" status --manifest "$manifest")
if [[ $status != *"stopped"* ]]; then
  echo "Recovered Launcher did not persist a stopped state: $status" >&2
  exit 1
fi
new_runner_pid=''
new_service_pid=''
echo "Launcher boot-recovery smoke passed: legacy PID-only state was rejected after PID reuse, manual start restored service supervision, and SIGTERM drained the helper."
