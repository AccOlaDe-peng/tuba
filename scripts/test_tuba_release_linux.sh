#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
source "${script_dir}/lib/tuba_release_linux.sh"
root=$(mktemp -d /tmp/tuba-release-linux-test.XXXXXX)
cleanup() {
  local resolved
  if [[ -z ${root:-} || ! -d $root ]]; then return; fi
  resolved=$(realpath -e "$root") || return
  case "$resolved" in
    /tmp/tuba-release-linux-test.*) rm -rf -- "$resolved" ;;
    *) echo "Refusing to remove unexpected test path: $resolved" >&2; return 1 ;;
  esac
}
trap cleanup EXIT

mkdir -p "${root}/releases/v1" "${root}/releases/v2"
printf 'v1\n' > "${root}/releases/v1/version.txt"
printf 'v2\n' > "${root}/releases/v2/version.txt"

tuba_activate_release "$root" v1
[[ $(cat "${root}/current/version.txt") == v1 ]]
[[ ! -e ${root}/previous && ! -L ${root}/previous ]]

tuba_activate_release "$root" v2
[[ $(cat "${root}/current/version.txt") == v2 ]]
[[ $(cat "${root}/previous/version.txt") == v1 ]]

tuba_switch_current_previous "$root"
[[ $(cat "${root}/current/version.txt") == v1 ]]
[[ $(cat "${root}/previous/version.txt") == v2 ]]

if tuba_activate_release "$root" missing; then
  echo 'Activation accepted a missing release.' >&2
  exit 1
fi
[[ $(cat "${root}/current/version.txt") == v1 ]]
[[ $(cat "${root}/previous/version.txt") == v2 ]]

ln -s /tmp "${root}/untrusted"
if tuba_release_target "$root" "${root}/untrusted" >/dev/null; then
  echo 'Release target validation accepted a link outside releases/.' >&2
  exit 1
fi

mv -- "${root}/current" "${root}/current.saved"
if tuba_activate_release "$root" v2; then
  echo 'Activation accepted a missing current with a retained previous link.' >&2
  exit 1
fi
mv -- "${root}/current.saved" "${root}/current"

echo 'Linux release activation checks passed: initial install, upgrade, rollback, failure preservation, and path containment.'
