#!/usr/bin/env python3
"""Install two Linux package releases in a disposable Ubuntu container."""

from __future__ import annotations

import argparse
import pathlib
import re
import shlex
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]


def workspace_path(value: str) -> str:
    path = (ROOT / value).resolve()
    try:
        relative = path.relative_to(ROOT)
    except ValueError as exc:
        raise ValueError("package paths must remain inside the workspace") from exc
    if not path.is_file() or path.suffix != ".gz":
        raise ValueError(f"Linux package archive is missing or invalid: {value}")
    if not pathlib.Path(str(path) + ".sha256").is_file():
        raise ValueError(f"Linux package SHA-256 sidecar is missing: {value}.sha256")
    return "/workspace/" + relative.as_posix()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-version", required=True)
    parser.add_argument("--first-package", required=True)
    parser.add_argument("--second-version", required=True)
    parser.add_argument("--second-package", required=True)
    parser.add_argument("--image", default="ubuntu:24.04")
    args = parser.parse_args()
    version_pattern = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
    if not version_pattern.fullmatch(args.first_version) or not version_pattern.fullmatch(args.second_version):
        parser.error("version identifiers must use letters, digits, dot, underscore, or hyphen")
    if args.first_version == args.second_version:
        parser.error("first and second versions must differ")

    first = shlex.quote(workspace_path(args.first_package))
    second = shlex.quote(workspace_path(args.second_package))
    first_version = shlex.quote(args.first_version)
    second_version = shlex.quote(args.second_version)
    script = f"""set -euo pipefail
probe_dir=$(mktemp -d /tmp/tuba-sidecar-probe.XXXXXX)
trap 'rm -rf -- "$probe_dir"' EXIT
cp {first} "$probe_dir/probe.tar.gz"
printf 'not the requested package\n' > "$probe_dir/other.bin"
(cd "$probe_dir" && sha256sum other.bin > probe.tar.gz.sha256)
if bash /workspace/scripts/install_tuba_linux.sh sidecar-probe "$probe_dir/probe.tar.gz"; then
  echo 'Installer accepted a sidecar that checks a different file.' >&2
  exit 1
fi
[[ ! -e /opt/tuba/releases/sidecar-probe ]]
if id tuba >/dev/null 2>&1; then
  echo 'Rejected package sidecar changed the system TUBA account.' >&2
  exit 1
fi
ln -s /etc "$probe_dir/unsafe-link"
tar -czf "$probe_dir/unsafe.tar.gz" -C "$probe_dir" unsafe-link
(cd "$probe_dir" && sha256sum unsafe.tar.gz > unsafe.tar.gz.sha256)
if bash /workspace/scripts/install_tuba_linux.sh unsafe-probe "$probe_dir/unsafe.tar.gz"; then
  echo 'Installer accepted a package containing a symbolic link.' >&2
  exit 1
fi
[[ ! -e /opt/tuba/releases/unsafe-probe ]]
if id tuba >/dev/null 2>&1; then
  echo 'Rejected symbolic-link package changed the system TUBA account.' >&2
  exit 1
fi
rm -rf -- "$probe_dir"
trap - EXIT

bash /workspace/scripts/install_tuba_linux.sh {first_version} {first}
[[ $(id -u tuba) -ne 0 ]]
[[ $(basename -- "$(getent passwd tuba | cut -d: -f7)") == nologin ]]
[[ $(stat -c '%a' /var/lib/tuba) == 700 ]]
[[ $(stat -c '%a' /etc/tuba/tuba.env) == 600 ]]
[[ $(readlink -f /opt/tuba/current) == "/opt/tuba/releases/{args.first_version}" ]]
bash /workspace/scripts/install_tuba_linux.sh {second_version} {second}
[[ $(readlink -f /opt/tuba/current) == "/opt/tuba/releases/{args.second_version}" ]]
[[ $(readlink -f /opt/tuba/previous) == "/opt/tuba/releases/{args.first_version}" ]]
[[ $(stat -c '%U:%G:%a' /opt/tuba/current) == 'root:tuba:777' ]]

cat > /tmp/tuba-launcher-smoke.json <<'JSON'
{{
  "version": 1,
  "state_dir": "/var/lib/tuba/acceptance/state",
  "log_dir": "/var/log/tuba/acceptance",
  "services": [{{
    "name": "smoke-helper",
    "command": "/bin/sh",
    "args": ["-c", "echo launcher-smoke-ready; exec sleep 60"],
    "restart_min": "100ms",
    "restart_max": "1s"
  }}]
}}
JSON
chown tuba:tuba /tmp/tuba-launcher-smoke.json
chmod 0640 /tmp/tuba-launcher-smoke.json
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher validate --manifest /tmp/tuba-launcher-smoke.json
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher start --manifest /tmp/tuba-launcher-smoke.json
status=''
for attempt in $(seq 1 50); do
  status=$(runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher status --manifest /tmp/tuba-launcher-smoke.json)
  if grep -q 'smoke-helper.*running' <<<"$status"; then break; fi
  sleep 0.1
done
grep -q 'smoke-helper.*running' <<<"$status"
logs=$(runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher logs --service smoke-helper --manifest /tmp/tuba-launcher-smoke.json)
grep -q 'launcher-smoke-ready' <<<"$logs"
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher stop --manifest /tmp/tuba-launcher-smoke.json --timeout 10s
status=$(runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher status --manifest /tmp/tuba-launcher-smoke.json)
grep -q 'TUBA launcher is stopped' <<<"$status"
install -o tuba -g tuba -m 0640 /tmp/tuba-launcher-smoke.json /etc/tuba/tuba-services.json
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher start --manifest /etc/tuba/tuba-services.json
sleep 0.2
bash /workspace/scripts/rollback_tuba_linux.sh
[[ $(readlink -f /opt/tuba/current) == "/opt/tuba/releases/{args.first_version}" ]]
[[ $(readlink -f /opt/tuba/previous) == "/opt/tuba/releases/{args.second_version}" ]]
runuser -u tuba -- /opt/tuba/current/bin/tuba-launcher stop --manifest /etc/tuba/tuba-services.json --timeout 10s
echo 'Ubuntu package acceptance passed: mismatched sidecar and symbolic-link archive rejected before install, install, upgrade, current/previous links, dedicated non-root account, protected environment file, Launcher CLI lifecycle, and standalone rollback.'
"""
    result = subprocess.run(
        [
            "docker", "run", "--rm", "--init", "--network", "none",
            "-v", f"{ROOT}:/workspace:ro", args.image, "bash", "-lc", script,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, end="")
        raise SystemExit(result.returncode)
    if result.stderr:
        print(result.stderr, end="")


if __name__ == "__main__":
    main()

