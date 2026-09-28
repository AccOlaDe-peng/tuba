#!/usr/bin/env python3
"""Apply an auditable redirect/origin update to an existing TUBA Keycloak realm."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def protected_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        user = os.environ.get("USERNAME", "")
        if not user:
            raise RuntimeError("USERNAME is unavailable; refusing to write realm snapshot")
        account = os.environ.get("USERDOMAIN", ".") + "\\" + user
        result = subprocess.run(
            ["icacls", str(path.parent), "/inheritance:r", "/grant:r", f"{account}:(OI)(CI)(F)"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise RuntimeError("could not restrict realm snapshot directory ACL")
    else:
        path.parent.chmod(0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
        if os.name == "nt":
            result = subprocess.run(
                ["icacls", str(path), "/inheritance:r", "/grant:r", f"{account}:(F)"],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode:
                raise RuntimeError("could not restrict realm export ACL")
        else:
            path.chmod(0o600)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def request(base: str, token: str, method: str, path: str, body: Any = None) -> tuple[int, bytes]:
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    req = urllib.request.Request(base.rstrip("/") + path, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        # Do not echo request/response bodies into logs; identity-provider
        # errors can contain configuration details or user identifiers.
        raise RuntimeError(f"Keycloak returned HTTP {exc.code} for {method} {path}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Keycloak request failed for {method} {path}: {exc.reason}") from None


def validate_redirect(value: str) -> tuple[str, str]:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("redirect URI must be an absolute http(s) URL without userinfo, query, or fragment")
    if parsed.path and not (parsed.path == "/" or parsed.path.endswith("/*")):
        raise ValueError("redirect URI path must be '/' or end with '/*' for an application base path")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return value, origin


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("KEYCLOAK_URL", ""))
    parser.add_argument("--realm", default="tuba")
    parser.add_argument("--client-id", default="tuba-web")
    parser.add_argument("--redirect-uri", required=True)
    parser.add_argument("--database-backup", required=True, help="fresh non-empty PostgreSQL dump made before this change")
    parser.add_argument("--snapshot-dir", required=True, help="restricted directory for the pre-change realm export")
    args = parser.parse_args()
    if not args.base_url:
        parser.error("--base-url or KEYCLOAK_URL is required")
    backup = Path(args.database_backup).resolve()
    if not backup.is_file() or backup.stat().st_size == 0:
        raise RuntimeError("database backup must exist and be non-empty before applying realm changes")
    username = os.environ.get("KEYCLOAK_ADMIN_USERNAME", "")
    password = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "")
    if not username or not password:
        raise RuntimeError("KEYCLOAK_ADMIN_USERNAME and KEYCLOAK_ADMIN_PASSWORD are required")
    redirect, origin = validate_redirect(args.redirect_uri)
    token_url = args.base_url.rstrip("/") + "/realms/master/protocol/openid-connect/token"
    data = urllib.parse.urlencode({
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": username,
        "password": password,
    }).encode()
    token_req = urllib.request.Request(token_url, data=data, method="POST")
    token_req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(token_req, timeout=20) as response:
            token = json.loads(response.read())["access_token"]
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Keycloak admin authentication failed with HTTP {exc.code}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Keycloak admin authentication request failed: {exc.reason}") from None

    realm_path = "/admin/realms/" + urllib.parse.quote(args.realm, safe="")
    status, raw_realm = request(args.base_url, token, "GET", realm_path)
    if status != 200:
        raise RuntimeError(f"could not export realm before update: HTTP {status}")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot = Path(args.snapshot_dir).resolve() / f"{args.realm}-before-{timestamp}.json"
    protected_write(snapshot, raw_realm + b"\n")

    clients_path = realm_path + "/clients?clientId=" + urllib.parse.quote(args.client_id, safe="")
    status, raw_clients = request(args.base_url, token, "GET", clients_path)
    if status != 200:
        raise RuntimeError(f"could not look up client: HTTP {status}")
    clients = json.loads(raw_clients)
    if len(clients) != 1 or not clients[0].get("id"):
        raise RuntimeError(f"expected exactly one client named {args.client_id}")
    client = clients[0]
    if not client.get("publicClient") or not client.get("standardFlowEnabled"):
        raise RuntimeError("target client is not an enabled public OIDC authorization-code client")
    if client.get("directAccessGrantsEnabled"):
        raise RuntimeError("refusing to update a client with Direct Access Grants enabled")
    client["redirectUris"] = [redirect]
    client["webOrigins"] = [origin]
    client_id = urllib.parse.quote(client["id"], safe="")
    status, _ = request(args.base_url, token, "PUT", realm_path + "/clients/" + client_id, client)
    if status not in (200, 204):
        raise RuntimeError(f"Keycloak client update failed with HTTP {status}")

    status, raw_clients = request(args.base_url, token, "GET", clients_path)
    if status != 200:
        raise RuntimeError(f"could not read back client after update: HTTP {status}")
    readback = json.loads(raw_clients)
    if len(readback) != 1 or readback[0].get("redirectUris") != [redirect] or readback[0].get("webOrigins") != [origin]:
        raise RuntimeError("client update readback did not match the requested redirect/origin")
    if readback[0].get("directAccessGrantsEnabled"):
        raise RuntimeError("client readback unexpectedly enabled Direct Access Grants")
    print(f"Updated {args.realm}/{args.client_id}; readback verified. Pre-change realm export: {snapshot}")
    print(f"Database backup prerequisite: {backup}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, OSError, KeyError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
