#!/usr/bin/env python3
"""Manually control an isolated, synthetic-only Zeek validation pipeline on Linux."""

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import time


ROOT = "/opt/tuba/collector-validation-v2"
BIN = os.path.join(ROOT, "bin")
RUN = os.path.join(ROOT, "run")
LOGS = os.path.join(ROOT, "logs")
CONFIG = os.path.join(ROOT, "config", "source-adapter.json")
STATE = os.path.join(RUN, "processes.json")
NAMESPACE = "zeek_validation_20260927_001"
ORGANIZATION = NAMESPACE
BROKER = "10.6.68.71:9092"
RAW_TOPIC = "tuba.collector." + NAMESPACE + ".raw.v1"
EVENT_PREFIX = "tuba.collector." + NAMESPACE + ".events"
QUARANTINE_TOPIC = "tuba.collector." + NAMESPACE + ".quarantine.v1"
DLQ_TOPIC = "tuba.collector." + NAMESPACE + ".dlq.v1"
ADAPTER_DLQ_TOPIC = "tuba.collector." + NAMESPACE + ".source-adapter.dlq.v1"
CONTEXTS = {
    "conn": "ctx_50000000000000000000000000000005",
    "dns": "ctx_60000000000000000000000000000006",
    "http": "ctx_70000000000000000000000000000007",
    "ssl": "ctx_80000000000000000000000000000008",
}


def read_api_environment():
    proc_root = "/proc"
    for entry in os.listdir(proc_root):
        if not entry.isdigit():
            continue
        base = os.path.join(proc_root, entry)
        try:
            cmdline = open(os.path.join(base, "cmdline"), "rb").read()
            if b"/opt/tuba/bin/tuba-api" not in cmdline:
                continue
            raw = open(os.path.join(base, "environ"), "rb").read()
            source = {}
            for item in raw.split(bytes([0])):
                if b"=" in item:
                    key, value = item.split(b"=", 1)
                    source[key.decode()] = value.decode(errors="replace")
            required = ("DATABASE_URL", "ES_URL", "ES_API_KEY")
            missing = [name for name in required if not source.get(name)]
            if missing:
                raise RuntimeError("API process is missing required runtime settings: " + ", ".join(missing))
            return {name: source[name] for name in required}
        except (OSError, PermissionError):
            continue
    raise RuntimeError("could not find the running /opt/tuba/bin/tuba-api process")


def process_matches(pid, binary):
    try:
        stat = open("/proc/%d/stat" % pid, "r").read().rsplit(")", 1)[1].strip().split()[0]
        if stat == "Z":
            return False
        cmdline = open("/proc/%d/cmdline" % pid, "rb").read().split(bytes([0]))
        return bool(cmdline and os.path.realpath(cmdline[0].decode()) == os.path.realpath(binary))
    except (OSError, ValueError):
        return False


def load_state():
    try:
        with open(STATE, "r") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {}


def save_state(state):
    temporary = STATE + ".tmp"
    with open(temporary, "w") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, STATE)


def component_env(base, token):
    env = dict(base)
    env.update({
        "KAFKA_BROKERS": BROKER,
        "KAFKA_SECURITY_PROTOCOL": "plaintext",
        "KAFKA_SASL_MECHANISM": "none",
        "KAFKA_RAW_TOPIC": RAW_TOPIC,
        "KAFKA_QUARANTINE_TOPIC": QUARANTINE_TOPIC,
        "KAFKA_EVENTS_TOPIC_PREFIX": EVENT_PREFIX,
        "KAFKA_DLQ_TOPIC": DLQ_TOPIC,
        "KAFKA_SOURCE_ADAPTER_DLQ_TOPIC": ADAPTER_DLQ_TOPIC,
        "TUBA_ORGANIZATION_ID": ORGANIZATION,
        "TUBA_NAMESPACE": NAMESPACE,
        "KAFKA_CONSUMER_GROUP_SUFFIX": "zeekvalidation20260927",
        "HTTP_LISTEN": "127.0.0.1:8080",
        "RAW_INDEXER_METRICS_LISTEN": "127.0.0.1:19095",
        "SOURCE_ADAPTER_METRICS_LISTEN": "127.0.0.1:19185",
        "SOURCE_ADAPTER_TOKEN": token,
        "SOURCE_ADAPTER_CONFIG": CONFIG,
    })
    return env


def stop_processes(state):
    remaining = []
    for name, details in reversed(list(state.items())):
        pid = int(details["pid"])
        binary = os.path.join(BIN, name)
        if not process_matches(pid, binary):
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        deadline = time.time() + 10
        while time.time() < deadline and process_matches(pid, binary):
            time.sleep(0.2)
        if process_matches(pid, binary):
            # Components run in their own session/process group. Escalate only
            # within that recorded validation process group after SIGTERM timed out.
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.time() + 5
            while time.time() < deadline and process_matches(pid, binary):
                time.sleep(0.2)
            if process_matches(pid, binary):
                remaining.append(name)
    if remaining:
        raise RuntimeError("processes did not stop cleanly: " + ", ".join(remaining))
    try:
        os.remove(STATE)
    except FileNotFoundError:
        pass


def start_pipeline(allow_plaintext):
    if not allow_plaintext:
        raise RuntimeError("this isolated profile uses a shared plaintext broker; pass --allow-shared-plaintext only for synthetic events")
    os.makedirs(RUN, mode=0o750, exist_ok=True)
    os.makedirs(LOGS, mode=0o750, exist_ok=True)
    state = load_state()
    for name, details in state.items():
        if process_matches(int(details["pid"]), os.path.join(BIN, name)):
            raise RuntimeError("validation process already running: " + name)
    if state:
        save_state({})
    base = read_api_environment()
    token = secrets.token_urlsafe(48)
    config = {
        "ingest_url": "http://127.0.0.1:8080/api/v1/internal/ingest/beat-events",
        "bindings": [
            {"topic": "tuba.source." + context_id + ".v1"}
            for context_id in CONTEXTS.values()
        ],
    }
    with open(CONFIG, "w") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")
    os.chmod(CONFIG, 0o640)
    env = component_env(base, token)
    processes = ["tuba-ingest", "tuba-raw-indexer", "tuba-normalizer", "tuba-quarantine-indexer", "tuba-standard-indexer", "tuba-source-adapter"]
    state = {}
    try:
        for name in processes:
            binary = os.path.join(BIN, name)
            if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
                raise RuntimeError("component binary is missing or not executable: " + binary)
            log_path = os.path.join(LOGS, name + ".log")
            output = open(log_path, "ab", buffering=0)
            process = subprocess.Popen([binary], cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                       stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            output.close()
            state[name] = {"pid": process.pid, "binary": binary}
            save_state(state)
            time.sleep(0.6)
            if process.poll() is not None:
                raise RuntimeError(name + " exited during startup; inspect " + log_path)
        print("started synthetic-only Zeek validation pipeline")
        for name, details in state.items():
            print(name + " pid=" + str(details["pid"]))
    except Exception:
        stop_processes(state)
        raise


def show_status():
    state = load_state()
    if not state:
        print("stopped")
        return
    for name, details in state.items():
        pid = int(details["pid"])
        status = "running" if process_matches(pid, os.path.join(BIN, name)) else "stopped/stale"
        print(name + " pid=" + str(pid) + " " + status)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status"))
    parser.add_argument("--allow-shared-plaintext", action="store_true",
                        help="required to start this synthetic-only profile on the no-ACL validation broker")
    args = parser.parse_args()
    try:
        if args.action == "start":
            start_pipeline(args.allow_shared_plaintext)
        elif args.action == "stop":
            stop_processes(load_state())
            print("stopped")
        else:
            show_status()
    except Exception as exc:
        print("error: " + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
