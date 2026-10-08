#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: verify_tuba_linux_web_package.sh PACKAGE.tar.gz" >&2
  exit 2
fi
archive=$(realpath "$1")
if [[ ! -f $archive || ! -f ${archive}.sha256 ]]; then
  echo "Package archive or SHA-256 sidecar is missing." >&2
  exit 1
fi
archive_name=$(basename "$archive")
mapfile -t sidecar_lines < "${archive}.sha256"
if [[ ${#sidecar_lines[@]} -ne 1 || ! ${sidecar_lines[0]} =~ ^([[:xdigit:]]{64})[[:space:]]{2}(.+)$ || ${BASH_REMATCH[2]} != "$archive_name" ]]; then
  echo "Package SHA-256 sidecar is invalid." >&2
  exit 1
fi
expected_hash=${BASH_REMATCH[1],,}
actual_hash=$(sha256sum "$archive")
actual_hash=${actual_hash%% *}
if [[ ${actual_hash,,} != "$expected_hash" ]]; then
  echo "Package SHA-256 does not match its sidecar." >&2
  exit 1
fi
tar -tzf "$archive" | awk '
  substr($0, 1, 1) == "/" || /(^|\/)\.\.(\/|$)/ { print "Unsafe package path: " $0 > "/dev/stderr"; exit 1 }
'
tar -tvzf "$archive" | awk '
  substr($0, 1, 1) != "-" && substr($0, 1, 1) != "d" {
    print "Unsafe package entry type: " $0 > "/dev/stderr"; exit 1
  }
'

temp_root=$(mktemp -d /tmp/tuba-linux-web-smoke.XXXXXX)
web_pid=''
cleanup() {
  if [[ -n $web_pid ]] && kill -0 "$web_pid" 2>/dev/null; then
    kill "$web_pid" 2>/dev/null || true
    wait "$web_pid" 2>/dev/null || true
  fi
  rm -rf -- "$temp_root"
}
trap cleanup EXIT
tar -xzf "$archive" -C "$temp_root"
chmod 0750 "${temp_root}/bin/tuba-web"

port=${TUBA_WEB_SMOKE_PORT:-18088}
api_port=$((port + 1000))
ingest_port=$((port + 2000))
if (( port < 1024 || port > 40000 )); then
  echo "TUBA_WEB_SMOKE_PORT must be between 1024 and 40000." >&2
  exit 2
fi
WEB_LISTEN="127.0.0.1:${port}" \
WEB_ROOT="${temp_root}/web" \
API_UPSTREAM="http://127.0.0.1:${api_port}" \
INGEST_UPSTREAM="http://127.0.0.1:${ingest_port}" \
  "${temp_root}/bin/tuba-web" >"${temp_root}/web.log" 2>&1 &
web_pid=$!

http_get() {
  local request_path=$1 response
  response=$(
    exec 3<>"/dev/tcp/127.0.0.1/${port}"
    printf 'GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' "$request_path" >&3
    cat <&3
  )
  printf '%s' "$response"
}

response=''
for _ in $(seq 1 50); do
  if ! kill -0 "$web_pid" 2>/dev/null; then
    cat "${temp_root}/web.log" >&2
    echo "Packaged tuba-web exited before becoming ready." >&2
    exit 1
  fi
  if response=$(http_get /health/live 2>/dev/null) && [[ $response == *$'\r\n\r\n'* ]]; then
    break
  fi
  sleep 0.1
done
if [[ $response != *'200 OK'* || $response != *$'\r\n\r\nok'* ]]; then
  cat "${temp_root}/web.log" >&2
  echo "Packaged tuba-web did not return a live response." >&2
  exit 1
fi

home=$(http_get /)
spa=$(http_get /overview)
runtime=$(http_get /config.js)
ready=$(http_get /health/ready)
if [[ $home != *'200 OK'* || $home != *'id="root"'* ]]; then echo "Packaged home page failed." >&2; exit 1; fi
if [[ $spa != *'200 OK'* || $spa != *'id="root"'* ]]; then echo "Packaged SPA fallback failed." >&2; exit 1; fi
if [[ $runtime != *'200 OK'* || $runtime != *'Cache-Control: no-store'* ||
      $runtime != *'"basePath":"/"'* || $runtime == *'oidc'* ]]; then
  echo "Packaged runtime configuration failed." >&2
  exit 1
fi
if [[ $ready != *'503 Service Unavailable'* ]]; then echo "Readiness must fail while upstreams are unavailable." >&2; exit 1; fi

exec 3<>"/dev/tcp/127.0.0.1/${port}"
printf 'POST /api/v1/internal/ingest/beat-events HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n' >&3
internal=$(cat <&3)
if [[ $internal != *'404 Not Found'* ]]; then echo "Packaged internal ingest route was not denied." >&2; exit 1; fi

echo "Linux package web smoke passed: hash verified, live, static home, SPA fallback, runtime config, internal route denied, and upstream readiness fails closed."
