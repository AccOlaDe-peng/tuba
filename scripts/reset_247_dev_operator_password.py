#!/usr/bin/env python3
"""Reset the 247 development TUBA operator password and store it in .env.local.

Sibling to provision_247_dev_operator.py. That script creates the operator and
prints its generated password exactly once; the password is never written to
disk, so a lost terminal transcript means the credential is gone for good. This
script recovers from that situation by rotating the password through the
Keycloak Admin API and persisting it to the gitignored .env.local instead of
stdout, so it cannot be lost the same way twice.

The Keycloak admin credential is read from .env.local first, then the process
environment:

    KC_BOOTSTRAP_ADMIN_USERNAME / KC_BOOTSTRAP_ADMIN_PASSWORD

247's Keycloak listens on the same port as its admin API, and that port is
reachable directly from the development machine, so no SSH hop is involved.

After rotating, the script proves the credential works by running a password
grant and checking the resulting token subject against the subject that
platform_release_publishers was bootstrapped for. A mismatch means the release
publisher grant points at a different identity, which would surface as 403
rather than 401 on the release API.
"""
import argparse
import base64
import json
import os
import secrets
import string
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_LOCAL = REPO_ROOT / ".env.local"

DEFAULT_BASE = "http://10.6.68.247:8180"
DEFAULT_REALM = "tuba"
DEFAULT_USERNAME = "tuba-operator-20260929"
DEFAULT_CLIENT = "tuba-web"
# The subject granted via cmd/tuba-bootstrap-release-publisher on 2026-09-29.
DEFAULT_PUBLISHER_SUBJECT = "26a64c67-b05a-4d52-b08f-201a8657ec03"


def load_env_local() -> dict:
    """Parse .env.local into a mapping. Absent file yields an empty mapping."""
    values = {}
    if not ENV_LOCAL.exists():
        return values
    for line in ENV_LOCAL.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = value.strip()
    return values


def setting(name: str, file_values: dict, default: str = "") -> str:
    """Prefer the process environment, then .env.local, then the default."""
    return os.environ.get(name) or file_values.get(name) or default


def request(path, token, data=None, form=None, method=None):
    body = None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    if form is not None:
        body = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers, exc.read()
    except urllib.error.URLError as exc:
        print(f"could not reach {path}: {exc.reason}", file=sys.stderr)
        return 0, {}, b""


def token_subject(access_token: str) -> str:
    """Read the sub claim out of a JWT without verifying its signature."""
    try:
        payload = access_token.split(".")[1]
        padded = payload + "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(padded)).get("sub", "")
    except (IndexError, ValueError):
        return ""


def upsert_env_local(updates: dict) -> None:
    """Set keys in .env.local, preserving unrelated lines and comments."""
    lines = ENV_LOCAL.read_text(encoding="utf-8").splitlines() if ENV_LOCAL.exists() else []
    remaining = dict(updates)
    output = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.partition("=")[0].strip()
            if key in remaining:
                output.append(f"{key}={remaining.pop(key)}")
                continue
        output.append(line)
    if remaining and output and output[-1].strip():
        output.append("")
    for key, value in remaining.items():
        output.append(f"{key}={value}")
    ENV_LOCAL.write_text("\n".join(output) + "\n", encoding="utf-8")
    try:
        ENV_LOCAL.chmod(0o600)
    except OSError:
        # Windows does not implement POSIX modes; the file is gitignored either way.
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", default=None, help=f"Keycloak base URL (default {DEFAULT_BASE})")
    parser.add_argument("--realm", default=None, help=f"realm to operate on (default {DEFAULT_REALM})")
    parser.add_argument("--username", default=None, help=f"operator username (default {DEFAULT_USERNAME})")
    parser.add_argument("--expected-subject", default=None,
                        help="publisher subject to verify against; empty string skips the check")
    args = parser.parse_args()

    file_values = load_env_local()
    base = args.base or setting("TUBA_OPERATOR_KC_BASE", file_values, DEFAULT_BASE)
    realm = args.realm or setting("TUBA_OPERATOR_KC_REALM", file_values, DEFAULT_REALM)
    username = args.username or setting("TUBA_OPERATOR_USERNAME", file_values, DEFAULT_USERNAME)
    if args.expected_subject is not None:
        expected_subject = args.expected_subject
    else:
        expected_subject = setting("TUBA_PUBLISHER_SUBJECT", file_values, DEFAULT_PUBLISHER_SUBJECT)

    admin_user = setting("KC_BOOTSTRAP_ADMIN_USERNAME", file_values)
    admin_password = setting("KC_BOOTSTRAP_ADMIN_PASSWORD", file_values)
    if not admin_user or not admin_password:
        print(
            "Keycloak admin credential is missing.\n"
            f"Put KC_BOOTSTRAP_ADMIN_USERNAME and KC_BOOTSTRAP_ADMIN_PASSWORD into {ENV_LOCAL}\n"
            "(the 247 master-realm admin, stored on the host in /etc/keycloak/admin.env).",
            file=sys.stderr,
        )
        return 2

    status, _, body = request(
        f"{base}/realms/master/protocol/openid-connect/token",
        "",
        form={"grant_type": "password", "client_id": "admin-cli",
              "username": admin_user, "password": admin_password},
    )
    if status != 200:
        print(f"Keycloak admin authentication failed (HTTP {status}); "
              "this credential is the 247 master realm admin, not the local dev admin", file=sys.stderr)
        return 1
    admin_token = json.loads(body)["access_token"]

    status, _, body = request(f"{base}/admin/realms/{realm}/users?username={username}&exact=true", admin_token)
    if status != 200:
        print(f"could not look up user {username} (HTTP {status})", file=sys.stderr)
        return 1
    users = json.loads(body)
    if len(users) != 1:
        print(f"expected exactly one user named {username}, found {len(users)}; "
              "run provision_247_dev_operator.py instead of resetting", file=sys.stderr)
        return 1
    user = users[0]
    user_id = user["id"]
    if not user.get("enabled", False):
        print(f"user {username} is disabled; enable it before resetting its password", file=sys.stderr)
        return 1

    password = "T!" + "".join(secrets.choice(string.ascii_letters + string.digits + "-_@#") for _ in range(30))
    status, _, _ = request(f"{base}/admin/realms/{realm}/users/{user_id}/reset-password", admin_token,
                           data={"type": "password", "value": password, "temporary": False}, method="PUT")
    if status not in (200, 204):
        print(f"password reset failed (HTTP {status})", file=sys.stderr)
        return 1

    upsert_env_local({"TUBA_OPERATOR_USERNAME": username, "TUBA_OPERATOR_PASSWORD": password})
    print(f"password rotated and stored in {ENV_LOCAL} (never printed here)")

    # Proving the new credential works is the whole point of the reset; a stored
    # password nobody validated is how this situation started.
    status, _, body = request(
        f"{base}/realms/{realm}/protocol/openid-connect/token",
        "",
        form={"grant_type": "password", "client_id": DEFAULT_CLIENT,
              "username": username, "password": password},
    )
    if status != 200:
        print(f"password grant failed after reset (HTTP {status}); "
              "check that Direct Access Grants are still enabled for the client", file=sys.stderr)
        return 1
    subject = token_subject(json.loads(body)["access_token"])
    print("password grant: OK")
    print(f"subject: {subject}")

    if not expected_subject:
        print("publisher subject check: skipped")
        return 0
    if subject != expected_subject:
        print(f"publisher subject check: MISMATCH (expected {expected_subject})", file=sys.stderr)
        print("platform_release_publishers is granted to a different identity; the release API "
              "will answer 403 until that subject is granted. Re-run the one-time bootstrap tool, "
              "or grant this subject through the platform publisher endpoint.", file=sys.stderr)
        return 1
    print("publisher subject check: matches the bootstrapped release publisher")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
