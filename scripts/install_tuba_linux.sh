#!/usr/bin/env bash
set -euo pipefail
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
source "${script_dir}/lib/tuba_release_linux.sh"

if [[ $EUID -ne 0 ]]; then
  echo "Run this installer as root." >&2
  exit 1
fi
if [[ $# -ne 2 ]]; then
  echo "Usage: install_tuba_linux.sh VERSION PACKAGE.tar.gz" >&2
  exit 2
fi

version=$1
archive=$(realpath "$2")
if [[ ! $version =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]]; then
  echo "Invalid version identifier." >&2
  exit 2
fi
if [[ ! -f $archive || ! -f ${archive}.sha256 ]]; then
  echo "Package or its .sha256 file is missing." >&2
  exit 1
fi
archive_name=$(basename "$archive")
mapfile -t sidecar_lines < "${archive}.sha256"
if [[ ${#sidecar_lines[@]} -ne 1 ]]; then
  echo "Package SHA-256 sidecar must contain exactly one line." >&2
  exit 1
fi
sidecar_line=${sidecar_lines[0]%$'\r'}
if [[ ! $sidecar_line =~ ^([[:xdigit:]]{64})[[:space:]]{2}(.+)$ ]]; then
  echo "Package SHA-256 sidecar has an invalid format." >&2
  exit 1
fi
expected_hash=${BASH_REMATCH[1],,}
sidecar_name=${BASH_REMATCH[2]}
if [[ $sidecar_name != "$archive_name" ]]; then
  echo "Package SHA-256 sidecar names a different archive." >&2
  exit 1
fi
actual_hash=$(sha256sum "$archive")
actual_hash=${actual_hash%% *}
if [[ ${actual_hash,,} != "$expected_hash" ]]; then
  echo "Package SHA-256 does not match its sidecar." >&2
  exit 1
fi
echo "Package SHA-256 verified: ${archive_name}."

tar -tzf "$archive" | awk '
  substr($0, 1, 1) == "/" || /(^|\/)\.\.(\/|$)/ { print "Unsafe package path: " $0 > "/dev/stderr"; exit 1 }
'
tar -tvzf "$archive" | awk '
  substr($0, 1, 1) != "-" && substr($0, 1, 1) != "d" {
    print "Unsafe package entry type: " $0 > "/dev/stderr"; exit 1
  }
'

install_root=/opt/tuba
release_dir=${install_root}/releases/${version}
config_dir=/etc/tuba
state_dir=/var/lib/tuba
log_dir=/var/log/tuba
account=tuba
nologin_shell=$(command -v nologin || true)
if [[ -z $nologin_shell ]]; then
  for candidate in /usr/sbin/nologin /sbin/nologin; do
    if [[ -x $candidate ]]; then
      nologin_shell=$candidate
      break
    fi
  done
fi
if [[ -z $nologin_shell ]]; then
  echo "Could not find a nologin shell (expected nologin, /usr/sbin/nologin, or /sbin/nologin)." >&2
  exit 1
fi

if [[ -e $release_dir ]]; then
  echo "Release already exists: $release_dir" >&2
  exit 1
fi
if ! getent group "$account" >/dev/null; then
  groupadd --system "$account"
fi
if ! id "$account" >/dev/null 2>&1; then
  useradd --system --gid "$account" --home-dir "$state_dir" --no-create-home --shell "$nologin_shell" "$account"
fi
if [[ $(id -u "$account") -eq 0 ]]; then
  echo "Refusing to use a privileged root-equivalent TUBA account." >&2
  exit 1
fi
account_groups=$(id -nG "$account")
for privileged_group in root sudo wheel; do
  case " $account_groups " in
    *" $privileged_group "*) echo "Refusing to use TUBA account in privileged group: $privileged_group" >&2; exit 1 ;;
  esac
done
account_shell=$(getent passwd "$account" | awk -F: '{print $7}')
if [[ $(basename -- "$account_shell") != nologin ]]; then
  echo "TUBA account must use a nologin shell (found: $account_shell)." >&2
  exit 1
fi

install -d -o root -g "$account" -m 0750 "$install_root" "${install_root}/releases" "$config_dir"
install -d -o "$account" -g "$account" -m 0700 "$state_dir" "${state_dir}/launcher" "$log_dir"
stage=$(mktemp -d "${install_root}/.stage-${version}.XXXXXX")
cleanup_stage() {
  if [[ -n $stage && -d $stage ]]; then
    rm -rf -- "$stage"
  fi
}
trap cleanup_stage EXIT
tar -xzf "$archive" -C "$stage"
if [[ ! -f ${stage}/bin/tuba-launcher || ! -f ${stage}/bin/tuba-api ]]; then
  echo "Package is missing required TUBA binaries." >&2
  exit 1
fi
chown -R root:"$account" "$stage"
find "$stage" -type d -exec chmod 0750 {} +
find "$stage" -type f -exec chmod 0640 {} +
find "${stage}/bin" -type f -exec chmod 0750 {} +
mv -- "$stage" "$release_dir"
stage=""

if [[ ! -f ${config_dir}/tuba-services.json ]]; then
  install -o "$account" -g "$account" -m 0640 \
    "${release_dir}/deploy/launcher/tuba-services.linux.example.json" \
    "${config_dir}/tuba-services.json"
fi
if [[ ! -f ${config_dir}/tuba.env ]]; then
  install -o "$account" -g "$account" -m 0600 \
    "${release_dir}/deploy/launcher/tuba.env.example" "${config_dir}/tuba.env"
fi
if [[ ! -f ${config_dir}/source-adapter.json ]]; then
  install -o "$account" -g "$account" -m 0640 \
    "${release_dir}/deploy/launcher/source-adapter.example.json" "${config_dir}/source-adapter.json"
fi

current_link=${install_root}/current
previous_link=${install_root}/previous
manifest_path=${config_dir}/tuba-services.json
migrated_manifest=""
if [[ -e $current_link || -L $current_link ]]; then
  tuba_release_target "$install_root" "$current_link" >/dev/null || {
    echo "Refusing to upgrade because current is not a valid release link." >&2
    exit 1
  }
  old_release=$(tuba_release_target "$install_root" "$current_link")
  # merge-manifest validates through a sibling temporary file. The dedicated
  # account can read /etc/tuba but must not be able to create files there, so
  # build the candidate in its private state directory and let root commit it.
  migrated_manifest=${state_dir}/.tuba-services.${version}.candidate.json
  if [[ -e $migrated_manifest ]]; then
    echo "Refusing to overwrite existing manifest migration candidate: $migrated_manifest" >&2
    exit 1
  fi
  runuser -u "$account" -- "${release_dir}/bin/tuba-launcher" merge-manifest \
    --manifest "$manifest_path" \
    --previous-defaults "${old_release}/deploy/launcher/tuba-services.linux.example.json" \
    --defaults "${release_dir}/deploy/launcher/tuba-services.linux.example.json" \
    --output "$migrated_manifest" \
    --current-path "$current_link" \
    --release-path "$release_dir"
  chmod 0640 "$migrated_manifest"
  chown "$account:$account" "$migrated_manifest"
  if [[ -e $previous_link || -L $previous_link ]]; then
    tuba_release_target "$install_root" "$previous_link" >/dev/null || {
      echo "Refusing to upgrade because previous is not a valid release link." >&2
      exit 1
    }
  fi
  runuser -u "$account" -- "${current_link}/bin/tuba-launcher" validate --manifest "$manifest_path"
  runuser -u "$account" -- "${current_link}/bin/tuba-launcher" stop --manifest "$manifest_path" --timeout 30s
fi

if ! tuba_activate_release "$install_root" "$version"; then
  if [[ -n $migrated_manifest ]]; then rm -f -- "$migrated_manifest"; fi
  if [[ -L $current_link ]]; then
    runuser -u "$account" -- "${current_link}/bin/tuba-launcher" start --manifest "${config_dir}/tuba-services.json" || \
      echo "Previous release remains selected but could not restart; inspect /var/log/tuba." >&2
  fi
  exit 1
fi
if [[ -n $migrated_manifest ]]; then
  backup_manifest=${manifest_path}.pre-${version}
  if [[ -e $backup_manifest ]]; then
    echo "Refusing to overwrite manifest backup: $backup_manifest" >&2
    rm -f -- "$migrated_manifest"
    tuba_switch_current_previous "$install_root" || true
    runuser -u "$account" -- "${current_link}/bin/tuba-launcher" start --manifest "$manifest_path" || true
    exit 1
  fi
  if ! cp -p -- "$manifest_path" "$backup_manifest" || ! mv -f -- "$migrated_manifest" "$manifest_path"; then
    rm -f -- "$backup_manifest"
    tuba_switch_current_previous "$install_root" || true
    runuser -u "$account" -- "${current_link}/bin/tuba-launcher" start --manifest "$manifest_path" || true
    echo "Manifest migration could not be committed; previous release was restored." >&2
    exit 1
  fi
fi
chown -h root:"$account" "$current_link"
if [[ -L $previous_link ]]; then chown -h root:"$account" "$previous_link"; fi

echo "Installed TUBA ${version} under ${release_dir}."
if [[ -L $previous_link ]]; then echo "Previous release link retained at ${previous_link} for rollback."; fi
echo "Review ${config_dir}/tuba-services.json and replace placeholders in ${config_dir}/tuba.env."
echo "Start as the dedicated account: runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher start --manifest /etc/tuba/tuba-services.json"
