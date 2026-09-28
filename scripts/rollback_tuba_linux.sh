#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run this rollback as root." >&2
  exit 1
fi
if [[ $# -gt 0 ]]; then
  echo "Usage: rollback_tuba_linux.sh" >&2
  exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
source "${script_dir}/lib/tuba_release_linux.sh"

install_root=/opt/tuba
config_dir=/etc/tuba
account=tuba
current_link=${install_root}/current
previous_link=${install_root}/previous
manifest=${config_dir}/tuba-services.json

tuba_release_target "$install_root" "$current_link" >/dev/null || {
  echo "Current is not a valid TUBA release link." >&2
  exit 1
}
tuba_release_target "$install_root" "$previous_link" >/dev/null || {
  echo "Previous is not a valid TUBA release link." >&2
  exit 1
}
if [[ ! -f $manifest ]]; then
  echo "Launcher manifest not found: $manifest" >&2
  exit 1
fi

runuser -u "$account" -- "${current_link}/bin/tuba-launcher" stop --manifest "$manifest" --timeout 30s
if ! tuba_switch_current_previous "$install_root"; then
  runuser -u "$account" -- "${current_link}/bin/tuba-launcher" start --manifest "$manifest" || \
    echo "Current release was retained but could not be restarted; inspect ${install_root}/current and ${log_dir:-/var/log/tuba}." >&2
  exit 1
fi
chown -h root:"$account" "$current_link" "$previous_link"
if ! runuser -u "$account" -- "${current_link}/bin/tuba-launcher" start --manifest "$manifest"; then
  echo "Previous release is active at ${current_link} but could not start; inspect ${log_dir:-/var/log/tuba}." >&2
  exit 1
fi

echo "Rollback completed. Current now uses the previous release; the former release is retained at ${previous_link}."
