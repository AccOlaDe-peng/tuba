#!/usr/bin/env python3
"""Run TUBA indexers with their own ES keys against isolated dependencies."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy" / "validation" / "compose.analysis-sink-security.yaml"
SERVICES = {
    "raw-indexer": "tuba-raw-indexer",
    "quarantine-indexer": "tuba-quarantine-indexer",
    "standard-indexer": "tuba-standard-indexer",
    "analysis-sink": "tuba-analysis-sink",
}
EVENT_TOPICS = (
    "authentication", "session", "iam", "directory", "network", "dns", "web", "tls"
)


def run(command: list[str], *, env: dict[str, str], input_text: str | None = None) -> str:
    result = subprocess.run(
        command, cwd=ROOT, env=env, input=input_text, text=True,
        capture_output=True, timeout=90, check=False,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"command failed ({result.returncode}): {Path(command[0]).name}: {detail[-2000:]}")
    return result.stdout.strip()


def request(url: str, auth: str, method: str = "GET", body: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", auth)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(2048)


def wait_for_document(url: str, auth: str, processes: dict[str, subprocess.Popen[str]]) -> dict:
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        status, body = request(url, auth)
        if status == 200:
            return json.loads(body)
        exited = [name for name, process in processes.items() if process.poll() is not None]
        if exited:
            raise RuntimeError(f"indexer exited before writing its document: {', '.join(exited)}")
        time.sleep(0.5)
    raise RuntimeError(f"indexer did not create expected document: {url.rsplit('/', 1)[-1]}")


def raw_envelope(organization: str, namespace: str) -> dict:
    source, dataset, epoch, position = "runtime-zeek", "zeek.conn", "1", "runtime:" + uuid.uuid4().hex
    fields = ["raw-v1", organization, source, dataset, epoch, position]
    digest = hashlib.sha256("".join(f"{len(value)}:{value}" for value in fields).encode()).hexdigest()
    payload = {"uid": "runtime-" + uuid.uuid4().hex, "id.orig_h": "192.0.2.10", "id.resp_h": "198.51.100.20"}
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode()
    return {
        "schema_version": "1.0.0", "raw_event_id": "raw:" + digest,
        "organization": {"id": organization}, "namespace": namespace,
        "source_instance_id": source, "source_context_id": "ctx_0123456789abcdef0123456789abcdef",
        "source_position": position, "source_epoch": epoch,
        "vendor": {"name": "zeek", "product": "zeek", "dataset": dataset},
        "received_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "release_id": "runtime-validation-v1", "payload_hash": hashlib.sha256(payload_bytes).hexdigest(),
        "payload": payload, "encoding": "json",
    }


def launch(name: str, binary: Path, key: str, brokers: str, es_url: str,
           organization: str, namespace: str, log_path: Path) -> tuple[subprocess.Popen[str], object]:
    env = os.environ.copy()
    env.update({
        "KAFKA_BROKERS": brokers, "TUBA_ORGANIZATION_ID": organization,
        "TUBA_NAMESPACE": namespace, "ES_URL": es_url, "ES_API_KEY": key,
        "KAFKA_DLQ_TOPIC": "tuba.indexing.dlq.v1",
        "RAW_INDEXER_METRICS_LISTEN": "127.0.0.1:0",
        "QUARANTINE_INDEXER_METRICS_LISTEN": "127.0.0.1:0",
        "STANDARD_INDEXER_METRICS_LISTEN": "127.0.0.1:0",
        "ANALYSIS_SINK_METRICS_LISTEN": "127.0.0.1:0",
    })
    log_file = open(log_path, "w", encoding="utf-8")
    options: dict[str, object] = {
        "cwd": ROOT, "env": env, "stdin": subprocess.DEVNULL,
        "stdout": log_file, "stderr": subprocess.STDOUT, "text": True,
    }
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options["start_new_session"] = True
    process = subprocess.Popen([str(binary)], **options)  # type: ignore[arg-type]
    return process, log_file


def stop(processes: dict[str, subprocess.Popen[str]]) -> None:
    for process in processes.values():
        if process.poll() is None:
            process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
    for name, process in processes.items():
        try:
            code = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            raise RuntimeError(f"{name} did not stop gracefully") from None
        if code != 0:
            raise RuntimeError(f"{name} graceful stop returned {code}")


def main() -> int:
    project = "tuba-indexer-check-" + uuid.uuid4().hex[:10]
    namespace = "idxcheck_" + uuid.uuid4().hex[:8]
    organization = "idxcheck_org"
    es_url, brokers = "http://127.0.0.1:19201", "127.0.0.1:19095"
    admin_password = secrets.token_urlsafe(28)
    compose_env = os.environ.copy()
    compose_env["ES_VALIDATION_PASSWORD"] = admin_password
    compose_env["DB_VALIDATION_PASSWORD"] = secrets.token_urlsafe(28)
    compose_env["KEYCLOAK_VALIDATION_PASSWORD"] = secrets.token_urlsafe(28)
    key_env = compose_env.copy()
    key_env.update({"ES_ADMIN_USERNAME": "elastic", "ES_ADMIN_PASSWORD": admin_password})
    compose = ["docker", "compose", "--project-name", project, "--file", str(COMPOSE)]
    workdir = Path(tempfile.mkdtemp(prefix="tuba-indexer-check-"))
    keyring = workdir / "keyring.json"
    binaries = {name: workdir / (name + (".exe" if os.name == "nt" else "")) for name in SERVICES}
    processes: dict[str, subprocess.Popen[str]] = {}
    logs: list[object] = []
    compose_started = False
    keys_revoked = False
    try:
        for name, package in SERVICES.items():
            run(["go", "build", "-o", str(binaries[name]), "./cmd/" + package], env=compose_env)
        compose_started = True
        run(compose + ["up", "--detach", "--wait"], env=compose_env)

        admin_auth = "Basic " + base64.b64encode(f"elastic:{admin_password}".encode()).decode()
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if request(es_url + "/_cluster/health", admin_auth)[0] == 200:
                break
            time.sleep(1)
        else:
            raise RuntimeError("secured Elasticsearch did not become healthy")

        run([sys.executable, str(ROOT / "scripts" / "manage_elasticsearch_api_keys.py"),
             "create", "--namespace", namespace, "--es-url", es_url,
             "--expiration", "1h", "--output", str(keyring)], env=key_env)
        keyring_data = json.loads(keyring.read_text(encoding="utf-8"))
        kafka_id = run(compose + ["ps", "--all", "--quiet", "kafka"], env=compose_env).splitlines()[0]

        # Fresh consumer groups start at the beginning; start them before producing fixtures.
        for name, package in SERVICES.items():
            processes[name], log_file = launch(
                name, binaries[name], keyring_data["services"][name]["encoded"],
                brokers, es_url, organization, namespace, workdir / (name + ".log"),
            )
            logs.append(log_file)

        raw = raw_envelope(organization, namespace)
        quarantine_id = "quarantine:runtime:" + uuid.uuid4().hex
        quarantine = {
            "id": quarantine_id, "organization_id": organization, "namespace": namespace,
            "raw_event_id": raw["raw_event_id"], "release_id": "runtime-validation-v1",
            "stage": "uim", "code": "RUNTIME_CHECK", "reason": "isolated indexer identity check",
            "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        standard_id = "evt:runtime:" + uuid.uuid4().hex
        standard = {
            "@timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "organization": {"id": organization},
            "event": {"id": standard_id, "kind": "event", "category": ["network"],
                      "dataset": "network", "duration": 10},
            "vendor": {"name": "zeek", "product": "zeek", "dataset": "zeek.conn"},
            "ueba": {"schema": {"version": "1.0.0"},
                     "quality": {"status": "qualified", "reasons": [], "usable_for": ["event_query"]},
                     "route": {"domain": "network", "generation": "g1"},
                     "event": {"type": "network.connection"},
                     "provenance": {"raw_event_id": raw["raw_event_id"], "release_id": "runtime-validation-v1",
                                    "dip_version": "1.0.0", "uim_version": "1.0.0"}},
            "source": {"ip": "192.0.2.10", "port": 12345},
            "destination": {"ip": "198.51.100.20", "port": 443},
            "network": {"transport": "tcp", "protocol": "tls", "bytes": 128, "packets": 2},
        }
        analysis_id = "anomaly:runtime:" + uuid.uuid4().hex
        analysis_result = {
            "contract_version": "1.0.0", "result_type": "anomaly", "result_id": analysis_id,
            "organization_id": organization, "namespace": namespace,
            "rule_id": "runtime.es-api-key", "rule_version": "1.0.0", "run_id": "run:" + uuid.uuid4().hex,
            "document": {"@timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                         "organization": {"id": organization}, "detection": {"rule_id": "runtime.es-api-key"}},
        }
        fixtures = [
            ("tuba.raw.events.v1", raw), ("tuba.quarantine.v1", quarantine),
            ("tuba.events.network.v1", standard), ("tuba.analysis.results.v1", analysis_result),
        ]
        for topic, payload in fixtures:
            run(["docker", "exec", "-i", kafka_id, "/opt/kafka/bin/kafka-console-producer.sh",
                 "--bootstrap-server", "kafka:29092", "--topic", topic],
                env=compose_env, input_text=json.dumps(payload, separators=(",", ":")) + "\n")

        physical_day = datetime.now(timezone.utc).strftime("%Y.%m.%d")
        expected_docs = {
            "raw-indexer": (f"{es_url}/tuba-v1-raw-{namespace}-g1-{physical_day}/_doc/{raw['raw_event_id']}", "organization"),
            "quarantine-indexer": (f"{es_url}/tuba-v1-quarantine-{namespace}-g1-{physical_day}/_doc/{quarantine_id}", "organization_id"),
            "standard-indexer": (f"{es_url}/tuba-v1-uim-network-{namespace}-g1-{physical_day}/_doc/{standard_id}", "organization"),
            "analysis-sink": (f"{es_url}/ueba-anomalies-{namespace}/_doc/{analysis_id}", "organization"),
        }
        for name, (url, scope_field) in expected_docs.items():
            stored = wait_for_document(url, admin_auth, processes)
            source = stored.get("_source", {})
            stored_organization = source.get("organization_id") if scope_field == "organization_id" else source.get("organization", {}).get("id")
            if stored_organization != organization:
                raise RuntimeError(f"{name} stored a document with the wrong organization")

        # Prove the analysis-sink service key cannot write another namespace.
        service_auth = "ApiKey " + keyring_data["services"]["analysis-sink"]["encoded"]
        denied_status, _ = request(
            f"{es_url}/ueba-anomalies-outside-{namespace}/_doc/forbidden?op_type=index",
            service_auth, "PUT", json.dumps({"organization": {"id": organization}}).encode(),
        )
        if denied_status not in (401, 403):
            raise RuntimeError(f"analysis-sink key unexpectedly wrote outside namespace (HTTP {denied_status})")

        groups = {
            "raw-indexer": "tuba-raw-indexer-" + namespace,
            "quarantine-indexer": "tuba-quarantine-indexer-" + namespace,
            "standard-indexer": "tuba-standard-indexer-network-" + namespace,
            "analysis-sink": "tuba-analysis-sink-" + namespace,
        }
        for name, group in groups.items():
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                output = run(["docker", "exec", kafka_id, "/opt/kafka/bin/kafka-consumer-groups.sh",
                              "--bootstrap-server", "kafka:29092", "--describe", "--group", group], env=compose_env)
                data = next((line.split() for line in output.splitlines()
                             if line.split() and line.split()[0] == group), None)
                if data and len(data) >= 6 and data[5] == "0":
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError(f"{name} did not commit its Kafka offset")

        stop(processes)
        processes.clear()
        for log in logs:
            log.close()  # type: ignore[attr-defined]
        logs.clear()
        run([sys.executable, str(ROOT / "scripts" / "manage_elasticsearch_api_keys.py"),
             "revoke", "--es-url", es_url, "--keyring", str(keyring)], env=key_env)
        keys_revoked = True
        print("PASS: Raw, Quarantine, Standard Indexer and Analysis Sink wrote through their own ES keys; offsets committed; out-of-namespace write denied; all four processes stopped gracefully.")
        return 0
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        for log in logs:
            log.close()  # type: ignore[attr-defined]
        if keyring.exists() and compose_started and not keys_revoked:
            try:
                run([sys.executable, str(ROOT / "scripts" / "manage_elasticsearch_api_keys.py"),
                     "revoke", "--es-url", es_url, "--keyring", str(keyring)], env=key_env)
                keys_revoked = True
            except Exception as exc:  # noqa: BLE001 - cleanup diagnostic only
                print(f"WARNING: isolated Elasticsearch API key revocation needs attention: {exc}", file=sys.stderr)
        if compose_started:
            try:
                run(compose + ["down", "--volumes", "--remove-orphans"], env=compose_env)
            except Exception as exc:  # noqa: BLE001 - cleanup diagnostic only
                print(f"WARNING: isolated Compose cleanup needs attention: {exc}", file=sys.stderr)
        keyring.unlink(missing_ok=True)
        for path in binaries.values():
            path.unlink(missing_ok=True)
        for name in SERVICES:
            (workdir / (name + ".log")).unlink(missing_ok=True)
        workdir.rmdir()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError, KeyError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
