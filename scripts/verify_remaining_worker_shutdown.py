#!/usr/bin/env python3
"""Start normalizer, control worker, and source adapter on isolated dependencies, then stop them."""

import hashlib
import json
import errno
import os
import secrets
import signal
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Dict, List, Optional


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILES = [ROOT / "compose.yaml", ROOT / "deploy/validation/compose.one-node.yaml",
                 ROOT / "deploy/validation/compose.runtime.yaml"]
WORKERS = {
    "normalizer": "tuba-normalizer",
    "control-worker": "tuba-control-worker",
    "source-adapter": "tuba-source-adapter",
}


def run(command: List[str], env: Dict[str, str], input_text: Optional[str] = None) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, input=input_text, universal_newlines=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, check=False)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"command failed ({result.returncode}): {Path(command[0]).name}: {detail[-2000:]}")
    return result.stdout.strip()


def unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def wait_ready(url: str, process: subprocess.Popen, log_path: Path) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = "\n".join(log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-20:])
            raise RuntimeError(f"worker exited before readiness: {url}; log tail:\n{tail}")
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"worker readiness did not become healthy: {url}")


def wait_status(url: str, process: subprocess.Popen, log_path: Path, expected: int,
                timeout: int = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = "\n".join(log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-20:])
            raise RuntimeError(f"worker exited while waiting for HTTP {expected}: {url}; log tail:\n{tail}")
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                status = response.status
        except urllib.error.HTTPError as exc:
            status = exc.code
        except (OSError, urllib.error.URLError):
            status = 0
        if status == expected:
            return
        time.sleep(0.2)
    raise RuntimeError(f"worker readiness did not become HTTP {expected}: {url}")


def stop_all(processes: Dict[str, subprocess.Popen]) -> None:
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


def kill_managed_child(pid: int) -> None:
    if pid <= 1:
        raise RuntimeError(f"refusing to terminate invalid managed child PID: {pid}")
    if os.name == "nt":
        result = subprocess.run(["taskkill.exe", "/PID", str(pid), "/F"], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True,
                                timeout=10, check=False)
        if result.returncode:
            raise RuntimeError(f"could not terminate isolated managed child PID {pid}: {result.stderr.strip()}")
    else:
        os.kill(pid, signal.SIGKILL)


def main() -> int:
    project = "tuba-workers-stop-" + uuid.uuid4().hex[:10]
    namespace = "stopcheck_" + uuid.uuid4().hex[:8]
    organization = "stopcheck_org"
    pg_password = secrets.token_urlsafe(28)
    env = os.environ.copy()
    env.update({
        "POSTGRES_DB": "tuba", "POSTGRES_USER": "tuba", "POSTGRES_PASSWORD": pg_password,
        "ES_VALIDATION_PASSWORD": secrets.token_urlsafe(28),
        "DB_VALIDATION_PASSWORD": secrets.token_urlsafe(28),
        "KEYCLOAK_VALIDATION_PASSWORD": secrets.token_urlsafe(28),
    })
    compose = ["docker", "compose", "--project-name", project]
    for path in COMPOSE_FILES:
        compose.extend(["--file", str(path)])
    workdir = Path(tempfile.mkdtemp(prefix="tuba-workers-stop-"))
    binary_suffix = ".exe" if os.name == "nt" else ""
    prebuilt_dir_value = os.environ.get("TUBA_VALIDATION_BIN_DIR", "").strip()
    prebuilt_dir = Path(prebuilt_dir_value).resolve() if prebuilt_dir_value else None
    binaries = {
        name: ((prebuilt_dir / (binary_name + binary_suffix)) if prebuilt_dir else
               workdir / (binary_name + binary_suffix))
        for name, binary_name in WORKERS.items()
    }
    launcher_binary = ((prebuilt_dir / ("tuba-launcher" + binary_suffix)) if prebuilt_dir else
                       workdir / ("tuba-launcher" + binary_suffix))
    config_path = workdir / "source-adapter.json"
    manifest_path = workdir / "tuba-services.json"
    state_dir = workdir / "launcher-state"
    log_dir = workdir / "launcher-logs"
    processes: Dict[str, subprocess.Popen] = {}
    logs: List[object] = []
    started = False
    try:
        if prebuilt_dir:
            for binary in list(binaries.values()) + [launcher_binary]:
                if not binary.is_file() or (os.name != "nt" and not os.access(str(binary), os.X_OK)):
                    raise RuntimeError("prebuilt validation binary is missing or not executable: " + str(binary))
        else:
            for name, package in WORKERS.items():
                run(["go", "build", "-o", str(binaries[name]), "./cmd/" + package], env)
            run(["go", "build", "-o", str(launcher_binary), "./cmd/tuba-launcher"], env)
        started = True
        run(compose + ["up", "--detach", "--wait", "kafka", "postgres", "kafka-init"], env)
        pg_id = run(compose + ["ps", "--all", "--quiet", "postgres"], env).splitlines()[0]
        kafka_id = run(compose + ["ps", "--all", "--quiet", "kafka"], env).splitlines()[0]

        # Apply only each migration's Goose Up block to the disposable database.
        for migration in sorted((ROOT / "migrations").glob("*.sql")):
            lines = migration.read_text(encoding="utf-8").splitlines()
            try:
                up_start = lines.index("-- +goose Up") + 1
                up_end = lines.index("-- +goose Down", up_start)
            except ValueError as exc:
                raise RuntimeError(f"migration has no complete Goose Up section: {migration.name}") from exc
            run(["docker", "exec", "-i", pg_id, "psql", "--username", "tuba", "--dbname", "tuba",
                 "--set", "ON_ERROR_STOP=1"], env, "\n".join(lines[up_start:up_end]))

        source_topic = "tuba.source.ctx_0123456789abcdef0123456789abcdef.v1"
        run(["docker", "exec", kafka_id, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092",
             "--create", "--if-not-exists", "--topic", source_topic, "--partitions", "1", "--replication-factor", "1"], env)
        config_path.write_text(json.dumps({
            "ingest_url": "http://127.0.0.1:18789/api/v1/internal/ingest/beat-events",
            "bindings": [{"topic": source_topic}],
        }), encoding="utf-8")

        base_env = {
            "KAFKA_BROKERS": "127.0.0.1:19094", "TUBA_ORGANIZATION_ID": organization,
            "TUBA_NAMESPACE": namespace, "KAFKA_SECURITY_PROTOCOL": "plaintext",
            "KAFKA_SASL_MECHANISM": "none",
        }
        envs = {
            "normalizer": {**base_env, "NORMALIZER_METRICS_LISTEN": "127.0.0.1:19096"},
            "control-worker": {**base_env, "DATABASE_URL": f"postgres://tuba:{pg_password}@127.0.0.1:15434/tuba?sslmode=disable",
                               "PG_STATEMENT_TIMEOUT": "15s", "CONTROL_WORKER_METRICS_LISTEN": "127.0.0.1:19099"},
            "source-adapter": {**base_env, "SOURCE_ADAPTER_CONFIG": str(config_path),
                                "SOURCE_ADAPTER_TOKEN": secrets.token_urlsafe(36),
                                "SOURCE_ADAPTER_METRICS_LISTEN": "127.0.0.1:19185"},
        }

        manifest_path.write_text(json.dumps({
            "version": 1,
            "state_dir": str(state_dir),
            "log_dir": str(log_dir),
            "services": [{
                "name": "control-worker",
                "command": str(binaries["control-worker"]),
                "working_dir": str(ROOT),
                "environment": {
                    "KAFKA_BROKERS": base_env["KAFKA_BROKERS"],
                    "TUBA_ORGANIZATION_ID": organization,
                    "TUBA_NAMESPACE": namespace,
                    "KAFKA_SECURITY_PROTOCOL": "plaintext",
                    "KAFKA_SASL_MECHANISM": "none",
                    "DATABASE_URL": "${TUBA_STAGE2_VALIDATION_DATABASE_URL}",
                    "PG_STATEMENT_TIMEOUT": "15s",
                    "CONTROL_WORKER_METRICS_LISTEN": "127.0.0.1:19099",
                },
            }],
        }), encoding="utf-8")

        launcher_env = os.environ.copy()
        launcher_env["TUBA_STAGE2_VALIDATION_DATABASE_URL"] = envs["control-worker"]["DATABASE_URL"]
        launcher_log = open(workdir / "launcher.log", "w", encoding="utf-8")
        logs.append(launcher_log)
        launcher_options: Dict[str, object] = {"cwd": ROOT, "env": launcher_env, "stdin": subprocess.DEVNULL,
                                               "stdout": launcher_log, "stderr": subprocess.STDOUT}
        if os.name == "nt":
            launcher_options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            launcher_options["start_new_session"] = True
        processes["control-worker"] = subprocess.Popen(
            [str(launcher_binary), "run", "--manifest", str(manifest_path)], **launcher_options
        )

        def start_worker(name: str) -> None:
            child_env = os.environ.copy()
            child_env.update(envs[name])
            log_file = open(workdir / (name + ".log"), "w", encoding="utf-8")
            logs.append(log_file)
            options: Dict[str, object] = {"cwd": ROOT, "env": child_env, "stdin": subprocess.DEVNULL,
                                          "stdout": log_file, "stderr": subprocess.STDOUT}
            if os.name == "nt":
                options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                options["start_new_session"] = True
            processes[name] = subprocess.Popen([str(binaries[name])], **options)

        control_ready = "http://127.0.0.1:19099/health/ready"
        wait_ready(control_ready, processes["control-worker"], workdir / "launcher.log")

        # Control-worker readiness must follow the required PostgreSQL and Kafka
        # dependencies, then recover without restarting the process.
        run(["docker", "stop", pg_id], env)
        wait_status(control_ready, processes["control-worker"], workdir / "launcher.log", 503, timeout=20)
        run(["docker", "start", pg_id], env)
        run(compose + ["up", "--detach", "--wait", "postgres"], env)
        wait_status(control_ready, processes["control-worker"], workdir / "launcher.log", 200, timeout=30)
        run(["docker", "stop", kafka_id], env)
        wait_status(control_ready, processes["control-worker"], workdir / "launcher.log", 503, timeout=20)
        run(["docker", "start", kafka_id], env)
        run(compose + ["up", "--detach", "--wait", "kafka"], env)
        wait_status(control_ready, processes["control-worker"], workdir / "launcher.log", 200, timeout=30)

        # Verify Launcher restarts the actual managed product process after an
        # unexpected exit, using only the child PID from this unique test state.
        state_path = state_dir / "launcher-state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        service_state = state["services"]["control-worker"]
        previous_pid = int(service_state["pid"])
        if service_state["state"] != "running" or previous_pid <= 1:
            raise RuntimeError(f"Launcher did not record a running control-worker: {service_state}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        managed_command = Path(manifest["services"][0]["command"]).resolve()
        if managed_command != binaries["control-worker"].resolve():
            raise RuntimeError("refusing child termination because manifest does not target the isolated control-worker binary")
        kill_managed_child(previous_pid)
        deadline = time.monotonic() + 30
        restarted = False
        while time.monotonic() < deadline:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            service_state = state["services"]["control-worker"]
            if int(service_state.get("restarts", 0)) >= 1 and int(service_state.get("pid", 0)) not in (0, previous_pid):
                restarted = True
                break
            time.sleep(0.2)
        if not restarted:
            raise RuntimeError(f"Launcher did not restart the isolated control-worker: {service_state}")
        wait_ready(control_ready, processes["control-worker"], workdir / "launcher.log")

        # Keep the Kafka fault window isolated to control-worker; Kafka readers
        # in the other workers intentionally fail fast so Launcher can restart them.
        start_worker("normalizer")
        start_worker("source-adapter")
        readiness = {"normalizer": 19096, "source-adapter": 19185}
        for name, port in readiness.items():
            wait_ready(f"http://127.0.0.1:{port}/health/ready", processes[name], workdir / (name + ".log"))

        stop_all(processes)
        processes.clear()
        status = subprocess.run([str(launcher_binary), "status", "--manifest", str(manifest_path)],
                                cwd=ROOT, env=launcher_env, universal_newlines=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15, check=False)
        if status.returncode != 0 or "TUBA launcher is stopped" not in status.stdout:
            raise RuntimeError(f"Launcher did not report stopped after supervisor signal: {status.stdout} {status.stderr}")
        for log_file in logs:
            log_file.close()  # type: ignore[attr-defined]
        logs.clear()
        print("PASS: Launcher supervised the real control-worker through PostgreSQL/Kafka loss and recovery, restarted it after forced child exit, gracefully stopped it on Launcher signal, and the other workers exited cleanly.")
        return 0
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        for log_file in logs:
            log_file.close()  # type: ignore[attr-defined]
        if started:
            try:
                run(compose + ["down", "--volumes", "--remove-orphans"], env)
            except Exception as exc:  # noqa: BLE001 - cleanup diagnostic only
                print(f"WARNING: isolated Compose cleanup needs attention: {exc}", file=sys.stderr)
        unlink_if_exists(config_path)
        unlink_if_exists(manifest_path)
        if not prebuilt_dir:
            unlink_if_exists(launcher_binary)
            for path in binaries.values():
                unlink_if_exists(path)
        for directory in (state_dir, log_dir):
            if directory.parent == workdir and directory.exists():
                shutil.rmtree(directory)
        unlink_if_exists(workdir / "launcher.log")
        for name in WORKERS:
            unlink_if_exists(workdir / (name + ".log"))
        workdir.rmdir()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError, IndexError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
