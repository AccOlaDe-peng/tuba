#!/usr/bin/env bash

tuba_release_target() {
  local install_root=$1 link_path=$2 release_root resolved
  [[ -L $link_path ]] || return 1
  release_root=$(realpath -e "${install_root}/releases") || return 1
  resolved=$(realpath -e "${link_path}") || return 1
  case "$resolved" in
    "$release_root"/*) printf '%s\n' "$resolved" ;;
    *) return 1 ;;
  esac
}

tuba_activate_release() {
  local install_root=$1 version=$2 release_root release_dir current_link previous_link
  local current_target next_current next_previous
  [[ $version =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ ]] || {
    echo "Invalid version identifier." >&2
    return 2
  }
  release_root=$(realpath -e "${install_root}/releases") || return 1
  release_dir=$(realpath -e "${release_root}/${version}" 2>/dev/null) || {
    echo "Release directory does not exist: ${release_root}/${version}" >&2
    return 1
  }
  case "$release_dir" in "$release_root"/*) ;; *) echo "Release path escaped release root." >&2; return 1 ;; esac

  current_link=${install_root}/current
  previous_link=${install_root}/previous
  if [[ -e $current_link || -L $current_link ]]; then
    current_target=$(tuba_release_target "$install_root" "$current_link") || {
      echo "Refusing to replace current because it is not a valid release link." >&2
      return 1
    }
  elif [[ -e $previous_link || -L $previous_link ]]; then
    echo "Current is missing while a previous release link exists; restore current first." >&2
    return 1
  else
    current_target=''
  fi
  if [[ -e $previous_link || -L $previous_link ]]; then
    tuba_release_target "$install_root" "$previous_link" >/dev/null || {
      echo "Refusing to replace previous because it is not a valid release link." >&2
      return 1
    }
  fi

  next_current=${install_root}/.current.$$.${RANDOM}
  next_previous=${install_root}/.previous.$$.${RANDOM}
  if ! ln -s "$release_dir" "$next_current"; then
    echo "Could not stage current release link." >&2
    return 1
  fi
  if [[ -n $current_target ]]; then
    if ! ln -s "$current_target" "$next_previous" || ! mv -Tf -- "$next_previous" "$previous_link"; then
      rm -f -- "$next_current" "$next_previous"
      echo "Could not stage previous release link; current was not changed." >&2
      return 1
    fi
  fi
  if ! mv -Tf -- "$next_current" "$current_link"; then
    rm -f -- "$next_current" "$next_previous"
    echo "Could not activate release; current was not changed." >&2
    return 1
  fi
  rm -f -- "$next_previous"
}

tuba_switch_current_previous() {
  local install_root=$1 current_link previous_link current_target previous_target
  local next_current next_previous
  current_link=${install_root}/current
  previous_link=${install_root}/previous
  current_target=$(tuba_release_target "$install_root" "$current_link") || {
    echo "Current is not a valid release link." >&2
    return 1
  }
  previous_target=$(tuba_release_target "$install_root" "$previous_link") || {
    echo "Previous is not a valid release link." >&2
    return 1
  }
  next_current=${install_root}/.current.rollback.$$.${RANDOM}
  next_previous=${install_root}/.previous.rollback.$$.${RANDOM}
  if ! ln -s "$previous_target" "$next_current" || ! ln -s "$current_target" "$next_previous"; then
    rm -f -- "$next_current" "$next_previous"
    echo "Could not stage rollback links; current was not changed." >&2
    return 1
  fi
  if ! mv -Tf -- "$next_current" "$current_link"; then
    rm -f -- "$next_current" "$next_previous"
    echo "Could not switch current release; current was not changed." >&2
    return 1
  fi
  if ! mv -Tf -- "$next_previous" "$previous_link"; then
    next_current=${install_root}/.current.restore.$$.${RANDOM}
    if ln -s "$current_target" "$next_current" && mv -Tf -- "$next_current" "$current_link"; then
      rm -f -- "$next_previous"
      echo "Could not switch previous release; current was restored." >&2
    else
      rm -f -- "$next_current" "$next_previous"
      echo "Could not switch previous release and current restoration failed; inspect release links." >&2
    fi
    return 1
  fi
}
