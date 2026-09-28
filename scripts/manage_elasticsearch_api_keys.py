#!/usr/bin/env python3
"""Create and revoke namespace-scoped Elasticsearch API keys for TUBA services.

The bootstrap API key is read from ES_ADMIN_API_KEY and is never written to
the generated keyring. Secret API key material is only written to the requested
file, which is restricted to the current OS identity.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


NAMESPACE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,62}$")
SERVICES = ("api", "raw-indexer", "quarantine-indexer", "standard-indexer", "analysis-sink")


def role_descriptors(namespace: str) -> dict[str, dict[str, Any]]:
    if not NAMESPACE_RE.fullmatch(namespace):
        raise ValueError("namespace must be 1-63 ASCII letters, digits, '_' or '-', starting with a letter or digit")

    def index(names: list[str], privileges: list[str]) -> dict[str, Any]:
        return {"names": names, "privileges": privileges, "allow_restricted_indices": False}

    uim_physical = f"tuba-v1-uim-*-{namespace}-g1-*"
    raw_physical = f"tuba-v1-raw-{namespace}-g1-*"
    quarantine_physical = f"tuba-v1-quarantine-{namespace}-g1-*"
    aliases = f"logs-ueba.*-{namespace}"
    return {
        "api": {
            "cluster": ["monitor"],
            "indices": [
                index([aliases, uim_physical, raw_physical, quarantine_physical], ["read"]),
                index([f"ueba-anomalies-{namespace}"], ["read"]),
                # The API's legacy case fallback reads and updates a case with
                # optimistic concurrency. Normal deployments use PostgreSQL.
                index([f"ueba-cases-{namespace}"], ["create_index", "read", "index"]),
            ],
        },
        "raw-indexer": {
            # ES has no alias-only fixed privilege, so request only the alias
            # action pattern instead of granting the broader index `manage`.
            "indices": [index([raw_physical, f"logs-ueba.raw-{namespace}"], ["indices:admin/create", "indices:admin/aliases*", "indices:data/write/bulk*", "indices:data/write/index:op_type/create", "read"])],
        },
        "quarantine-indexer": {
            "indices": [index([quarantine_physical, f"logs-ueba.quarantine-{namespace}"], ["indices:admin/create", "indices:admin/aliases*", "indices:data/write/bulk*", "indices:data/write/index:op_type/create"])],
        },
        "standard-indexer": {
            "indices": [index([uim_physical, f"logs-ueba.*-{namespace}"], ["indices:admin/create", "indices:admin/aliases*", "indices:data/write/bulk*", "indices:data/write/index:op_type/create", "read"])],
        },
        "analysis-sink": {
            # PutAnalysis uses op_type=index so stable IDs can be updated on a
            # replay; create_doc alone would reject that operation.
            "indices": [index([f"ueba-anomalies-{namespace}"], ["create_index", "index"])],
        },
    }


def _request(base_url: str, authorization: str, method: str, path: str, payload: Any = None) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(base_url.rstrip("/") + path, data=body, method=method)
    req.add_header("Authorization", authorization)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", "replace")
        raise RuntimeError(f"Elasticsearch returned HTTP {exc.code} for {method} {path}: {detail}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Elasticsearch request failed for {method} {path}: {exc.reason}") from None
    return json.loads(raw) if raw else {}


def _restrict_file(path: Path) -> None:
    if os.name == "nt":
        identity = os.environ.get("USERNAME")
        if not identity:
            raise RuntimeError("USERNAME is unavailable; refusing to write API key material")
        account = os.environ.get("USERDOMAIN", ".") + "\\" + identity
        result = subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", f"{account}:(F)"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            path.unlink(missing_ok=True)
            raise RuntimeError("could not restrict keyring ACL; keyring file removed")
    else:
        path.chmod(0o600)


def _write_keyring(path: Path, body: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(body, stream, indent=2)
            stream.write("\n")
        _restrict_file(path)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _admin_authorization() -> str:
    api_key = os.environ.get("ES_ADMIN_API_KEY", "")
    if api_key:
        return "ApiKey " + api_key
    username = os.environ.get("ES_ADMIN_USERNAME", "")
    password = os.environ.get("ES_ADMIN_PASSWORD", "")
    if username and password:
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return "Basic " + token
    raise RuntimeError("set ES_ADMIN_API_KEY or both ES_ADMIN_USERNAME and ES_ADMIN_PASSWORD")


def _create(args: argparse.Namespace) -> int:
    admin_auth = _admin_authorization()
    descriptors = role_descriptors(args.namespace)
    created: dict[str, Any] = {}
    try:
        for service in SERVICES:
            suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            response = _request(
                args.es_url,
                admin_auth,
                "POST",
                "/_security/api_key",
                {
                    "name": f"tuba-{service}-{args.namespace}-{suffix}",
                    "expiration": args.expiration,
                    "role_descriptors": {f"tuba_{service.replace('-', '_')}": descriptors[service]},
                    "metadata": {"product": "tuba", "service": service, "namespace": args.namespace},
                },
            )
            created[service] = {
                "id": response["id"],
                "name": response["name"],
                "encoded": response["encoded"],
            }
        keyring = {
            "schema_version": 1,
            "namespace": args.namespace,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "services": created,
        }
        _write_keyring(Path(args.output).resolve(), keyring)
    except Exception:
        # Avoid leaving a partial generation active if writing the protected
        # keyring failed. The bootstrap key never appears in this error path.
        ids = [value["id"] for value in created.values()]
        if ids:
            try:
                _request(args.es_url, admin_auth, "DELETE", "/_security/api_key", {"ids": ids})
            except Exception:
                print("WARNING: could not revoke partially created API keys; inspect the ES API-key inventory", file=sys.stderr)
        raise
    print(f"Created {len(created)} namespace-scoped API keys; secret keyring saved with restricted access: {Path(args.output).resolve()}")
    print("Install each service's 'encoded' value in its matching Launcher environment variable; do not copy the bootstrap key.")
    return 0


def _revoke(args: argparse.Namespace) -> int:
    admin_auth = _admin_authorization()
    body = json.loads(Path(args.keyring).read_text(encoding="utf-8"))
    ids = [entry["id"] for entry in body.get("services", {}).values()]
    if not ids:
        raise RuntimeError("keyring contains no API key IDs")
    result = _request(args.es_url, admin_auth, "DELETE", "/_security/api_key", {"ids": ids})
    print(f"Invalidated {len(result.get('invalidated_api_keys', []))} API keys; already-invalidated: {len(result.get('previously_invalidated_api_keys', []))}.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subs = parser.add_subparsers(dest="command", required=True)
    plan = subs.add_parser("plan", help="print namespace-scoped role descriptors; no secrets or changes")
    plan.add_argument("--namespace", required=True)
    create = subs.add_parser("create", help="create a new key generation and write a protected keyring")
    create.add_argument("--namespace", required=True)
    create.add_argument("--es-url", default=os.environ.get("ES_URL", ""))
    create.add_argument("--expiration", default="90d")
    create.add_argument("--output", required=True)
    revoke = subs.add_parser("revoke", help="invalidate every key ID in a prior keyring")
    revoke.add_argument("--es-url", default=os.environ.get("ES_URL", ""))
    revoke.add_argument("--keyring", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps({"namespace": args.namespace, "services": role_descriptors(args.namespace)}, indent=2))
        return 0
    if not args.es_url:
        parser.error("--es-url or ES_URL is required")
    return _create(args) if args.command == "create" else _revoke(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, KeyError, json.JSONDecodeError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
