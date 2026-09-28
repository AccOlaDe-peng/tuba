#!/usr/bin/env python3
"""Run namespace isolation and key-rotation acceptance against isolated ES."""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
MANAGER = ROOT / "scripts" / "manage_elasticsearch_api_keys.py"


def admin_auth() -> str:
    username = os.environ.get("ES_ADMIN_USERNAME", "")
    password = os.environ.get("ES_ADMIN_PASSWORD", "")
    api_key = os.environ.get("ES_ADMIN_API_KEY", "")
    if api_key:
        return "ApiKey " + api_key
    if username and password:
        return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
    raise RuntimeError("set ES_ADMIN_API_KEY or ES_ADMIN_USERNAME and ES_ADMIN_PASSWORD")


def call(url: str, auth: str, method: str, path: str, payload: Any = None) -> tuple[int, dict[str, Any]]:
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url.rstrip("/") + path, data=body, method=method)
    req.add_header("Authorization", auth)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            data = response.read()
            return response.status, json.loads(data) if data else {}
    except urllib.error.HTTPError as exc:
        data = exc.read(4096)
        try:
            decoded = json.loads(data)
        except ValueError:
            decoded = {}
        return exc.code, decoded


def expect(url: str, key: str, method: str, path: str, allowed: bool, payload: Any = None) -> dict[str, Any]:
    status, body = call(url, "ApiKey " + key, method, path, payload)
    if allowed and not 200 <= status < 300:
        raise AssertionError(f"expected allowed {method} {path}, got HTTP {status}")
    if not allowed and status not in (401, 403):
        raise AssertionError(f"expected denied {method} {path}, got HTTP {status}")
    return body


def bulk_create(url: str, key: str, index: str, document_id: str, document: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps({"create": {"_index": index, "_id": document_id}}) + "\n" + json.dumps(document) + "\n"
    req = urllib.request.Request(url.rstrip("/") + "/_bulk", data=payload.encode(), method="POST")
    req.add_header("Authorization", "ApiKey " + key)
    req.add_header("Content-Type", "application/x-ndjson")
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", "replace")
        raise AssertionError(f"bulk create denied: HTTP {exc.code}: {detail}") from None


def run_manager(url: str, namespace: str, output: Path, command: str, keyring: Path | None = None) -> None:
    args = [sys.executable, str(MANAGER), command, "--es-url", url]
    if command == "create":
        args += ["--namespace", namespace, "--output", str(output)]
    else:
        assert keyring is not None
        args += ["--keyring", str(keyring)]
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError("API-key manager failed: " + result.stderr.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--es-url", default=os.environ.get("ES_URL", "http://127.0.0.1:19200"))
    parser.add_argument("--namespace", default="o03_authz_validation")
    args = parser.parse_args()
    auth = admin_auth()
    deadline = time.monotonic() + 40
    while True:
        try:
            status, _ = call(args.es_url, auth, "GET", "/_cluster/health")
            if status == 200:
                break
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("isolated Elasticsearch did not become ready")
        time.sleep(1)

    ns = args.namespace
    suffix = f"{ns}-g1-2026.09.28"
    names = {
        "raw": f"tuba-v1-raw-{suffix}",
        "quarantine": f"tuba-v1-quarantine-{suffix}",
        "uim": f"tuba-v1-uim-network-{suffix}",
        "other_raw": f"tuba-v1-raw-other_{ns}-g1-2026.09.28",
        "other_uim": f"tuba-v1-uim-network-other_{ns}-g1-2026.09.28",
    }
    made: list[str] = []
    made_templates: list[str] = []
    keyrings: list[Path] = []
    try:
        with tempfile.TemporaryDirectory(prefix="tuba-o03-es-") as temp:
            old_file = Path(temp) / "generation-old.json"
            active_file = Path(temp) / "generation-active.json"
            run_manager(args.es_url, ns, old_file, "create")
            run_manager(args.es_url, ns, active_file, "create")
            keyrings.extend((old_file, active_file))
            old = json.loads(old_file.read_text(encoding="utf-8"))["services"]
            active = json.loads(active_file.read_text(encoding="utf-8"))["services"]

            # The active generation can create/write only its own namespace.
            raw = active["raw-indexer"]["encoded"]
            code, response = call(args.es_url, "ApiKey " + raw, "PUT", "/" + names["raw"], {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"dynamic": "strict", "properties": {"payload_hash": {"type": "keyword"}}},
                "aliases": {f"logs-ueba.raw-{ns}": {}},
            })
            assert code in (200, 201), f"raw index creation denied: HTTP {code}: {response.get('error', {}).get('reason', response)}"
            made.append(names["raw"])
            result = bulk_create(args.es_url, raw, names["raw"], "raw-1", {"payload_hash": "abc"})
            assert result["items"][0]["create"]["status"] == 201, f"raw bulk item failed: {result['items'][0]}"
            expect(args.es_url, raw, "GET", f"/{names['raw']}/_doc/raw-1", True)
            expect(args.es_url, raw, "PUT", "/" + names["other_raw"], False, {"settings": {"number_of_shards": 1}})
            expect(args.es_url, raw, "GET", f"/{names['uim']}/_search", False)
            expect(args.es_url, raw, "PUT", f"/{names['raw']}/_mapping", False, {"properties": {"forbidden": {"type": "keyword"}}})
            expect(args.es_url, raw, "DELETE", "/" + names["raw"], False)

            quarantine = active["quarantine-indexer"]["encoded"]
            code, _ = call(args.es_url, "ApiKey " + quarantine, "PUT", "/" + names["quarantine"], {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"dynamic": "strict", "properties": {
                    "id": {"type": "keyword"}, "raw_event_id": {"type": "keyword"},
                    "stage": {"type": "keyword"}, "code": {"type": "keyword"},
                    "reason": {"type": "keyword"}, "occurred_at": {"type": "date"},
                }},
                "aliases": {f"logs-ueba.quarantine-{ns}": {}},
            })
            assert code in (200, 201), f"quarantine index creation denied: HTTP {code}"
            made.append(names["quarantine"])
            result = bulk_create(args.es_url, quarantine, names["quarantine"], "quarantine-1", {
                "id": "quarantine-1", "raw_event_id": "raw-1", "stage": "uim",
                "code": "FIELD_INVALID", "reason": "invalid field", "occurred_at": "2026-09-28T00:00:00Z",
            })
            assert result["items"][0]["create"]["status"] == 201, f"quarantine bulk item failed: {result['items'][0]}"
            expect(args.es_url, quarantine, "GET", f"/{names['quarantine']}/_search", False)
            expect(args.es_url, quarantine, "PUT", "/" + names["raw"], False, {"settings": {"number_of_shards": 1}})

            standard = active["standard-indexer"]["encoded"]
            code, _ = call(args.es_url, "ApiKey " + standard, "PUT", "/" + names["uim"], {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"dynamic": "strict", "properties": {"event": {"properties": {"id": {"type": "keyword"}}}}},
                "aliases": {f"logs-ueba.network-{ns}": {}},
            })
            assert code in (200, 201), f"standard index creation denied: HTTP {code}"
            made.append(names["uim"])
            result = bulk_create(args.es_url, standard, names["uim"], "event-1", {"event": {"id": "event-1"}})
            assert result["items"][0]["create"]["status"] == 201, f"standard bulk item failed: {result['items'][0]}"
            expect(args.es_url, standard, "GET", f"/{names['uim']}/_doc/event-1", True)
            expect(args.es_url, standard, "PUT", "/" + names["other_uim"], False, {"settings": {"number_of_shards": 1}})
            expect(args.es_url, standard, "PUT", f"/{names['uim']}/_mapping", False, {"properties": {"forbidden": {"type": "keyword"}}})
            expect(args.es_url, standard, "DELETE", "/" + names["uim"], False)

            analysis = active["analysis-sink"]["encoded"]
            expect(args.es_url, analysis, "PUT", f"/ueba-anomalies-{ns}/_doc/anomaly-1?op_type=index", True, {"anomaly": {"id": "anomaly-1"}})
            made.append(f"ueba-anomalies-{ns}")
            expect(args.es_url, analysis, "GET", f"/ueba-anomalies-{ns}/_search", False)
            expect(args.es_url, analysis, "PUT", f"/ueba-anomalies-other_{ns}/_doc/a?op_type=index", False, {"id": "a"})

            api = active["api"]["encoded"]
            expect(args.es_url, api, "GET", "/_cluster/health", True)
            expect(args.es_url, api, "GET", f"/{names['raw']}/_search", True)
            expect(args.es_url, api, "GET", f"/logs-ueba.raw-{ns}/_search", True)
            status, _ = call(args.es_url, auth, "PUT", "/" + names["other_raw"], {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"dynamic": True},
            })
            assert status in (200, 201), f"could not seed cross-namespace read-deny check: HTTP {status}"
            made.append(names["other_raw"])
            expect(args.es_url, api, "GET", f"/{names['other_raw']}/_search", False)
            expect(args.es_url, api, "PUT", f"/ueba-cases-{ns}/_doc/case-1", True, {"organization": {"id": "org-test"}})
            made.append(f"ueba-cases-{ns}")
            expect(args.es_url, api, "PUT", f"/ueba-cases-other_{ns}/_doc/case-1", False, {"id": "case-1"})
            expect(args.es_url, api, "POST", f"/{names['raw']}/_doc/forbidden", False, {"id": "forbidden"})
            template_name = "o03-security-validation-protected"
            status, _ = call(args.es_url, auth, "PUT", "/_component_template/" + template_name, {
                "template": {"settings": {"number_of_shards": 1}},
            })
            assert status in (200, 201), f"could not seed template-admin denial check: HTTP {status}"
            made_templates.append(template_name)
            expect(args.es_url, api, "PUT", "/_component_template/" + template_name, False, {
                "template": {"settings": {"number_of_shards": 2}},
            })

            # Both generations work during rollout; revoke old only after the
            # new keyring has passed all service operation checks.
            for service in active:
                expect(args.es_url, active[service]["encoded"], "GET", "/_security/_authenticate", True)
                expect(args.es_url, old[service]["encoded"], "GET", "/_security/_authenticate", True)
            run_manager(args.es_url, ns, old_file, "revoke", old_file)
            for service in active:
                status, _ = call(args.es_url, "ApiKey " + old[service]["encoded"], "GET", "/_security/_authenticate")
                assert status == 401, f"old {service} key still authenticates after rotation: HTTP {status}"
                expect(args.es_url, active[service]["encoded"], "GET", "/_security/_authenticate", True)

            # Keep the current keyring available to the caller only for the
            # duration of this isolated verifier; revoke it after assertions.
            print("PASS: five service keys have namespace-scoped allow/deny behavior")
            print("PASS: API key rotation preserves the new generation and invalidates the old generation")
            run_manager(args.es_url, ns, active_file, "revoke", active_file)
        return 0
    finally:
        # Only remove exact disposable names generated above and invalidate
        # any key IDs left by an assertion failure.
        for index in reversed(made):
            try:
                call(args.es_url, auth, "DELETE", "/" + index)
            except Exception:
                pass
        for template in made_templates:
            try:
                call(args.es_url, auth, "DELETE", "/_component_template/" + template)
            except Exception:
                pass
        for keyring in keyrings:
            if keyring.exists():
                try:
                    run_manager(args.es_url, ns, keyring, "revoke", keyring)
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, AssertionError, OSError, ValueError, KeyError, json.JSONDecodeError, urllib.error.HTTPError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
