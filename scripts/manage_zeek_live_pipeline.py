#!/usr/bin/env python3
"""Start/stop the manually controlled TUBA data plane for real Zeek logs."""

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import time


ROOT = "/opt/tuba/collector-live/pipeline"
KAFKA_ROOT = "/opt/tuba/collector-live/kafka"
BIN = os.path.join(ROOT, "bin")
RUN = os.path.join(ROOT, "run")
LOGS = os.path.join(ROOT, "logs")
CONFIG = os.path.join(ROOT, "config", "source-adapter.json")
STATE = os.path.join(RUN, "processes.json")
SECRETS = os.path.join(KAFKA_ROOT, "secrets.json")
NAMESPACE = "zeek_validation_20260927_001"
ORGANIZATION = NAMESPACE
BROKER = "10.6.68.248:29292"
RAW_TOPIC = "tuba.collector." + NAMESPACE + ".raw.live2.v1"
EVENT_PREFIX = "tuba.collector." + NAMESPACE + ".events"
QUARANTINE_TOPIC = "tuba.collector." + NAMESPACE + ".quarantine.v1"
DLQ_TOPIC = "tuba.collector." + NAMESPACE + ".dlq.v1"
ADAPTER_DLQ_TOPIC = "tuba.collector." + NAMESPACE + ".source-adapter.dlq.v1"
CONTEXTS = (
	"ctx_9173765dafede7b01176206fc49f70d9",
	"ctx_df595c138c0ecac68cf9e8af95b7b881",
	"ctx_5abc8a06f8a0088878248d4504d22a62",
	"ctx_5fadf1a689e7900ff74af27b5674c1c5",
)
SERVICE_ROLES = {
    "tuba-ingest": "ingest",
    "tuba-raw-indexer": "raw-indexer",
    "tuba-normalizer": "normalizer",
    "tuba-quarantine-indexer": "quarantine-indexer",
    "tuba-standard-indexer": "standard-indexer",
    "tuba-source-adapter": "source-adapter",
}


def read_api_environment():
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        base = "/proc/" + entry
        try:
            cmdline = open(os.path.join(base, "cmdline"), "rb").read()
            if b"/opt/tuba/bin/tuba-api" not in cmdline:
                continue
            raw = open(os.path.join(base, "environ"), "rb").read()
            values = dict(item.split(b"=", 1) for item in raw.split(bytes([0])) if b"=" in item)
            required = (b"DATABASE_URL", b"ES_URL", b"ES_API_KEY")
            if any(not values.get(name) for name in required):
                raise RuntimeError("API process lacks required database/ES settings")
            return {name.decode(): values[name].decode(errors="replace") for name in required}
        except (OSError, PermissionError):
            continue
    raise RuntimeError("could not find the running /opt/tuba/bin/tuba-api process")


def process_matches(pid, binary):
    try:
        state = open("/proc/%d/stat" % pid, "r").read().rsplit(")", 1)[1].strip().split()[0]
        if state == "Z":
            return False
        args = open("/proc/%d/cmdline" % pid, "rb").read().split(bytes([0]))
        return bool(args and os.path.realpath(args[0].decode()) == os.path.realpath(binary))
    except (OSError, ValueError):
        return False


def load_state():
    try:
        with open(STATE, "r") as handle:
            return json.load(handle)
    except IOError:
        return {}


def save_state(state):
    temporary = STATE + ".tmp"
    with open(temporary, "w") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, STATE)


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
            time.sleep(.2)
        if process_matches(pid, binary):
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.time() + 5
            while time.time() < deadline and process_matches(pid, binary):
                time.sleep(.2)
            if process_matches(pid, binary):
                remaining.append(name)
    if remaining:
        raise RuntimeError("data-plane processes did not stop: " + ", ".join(remaining))
    try:
        os.remove(STATE)
    except IOError:
        pass


def start():
    for directory in (RUN, LOGS, os.path.dirname(CONFIG)):
        os.makedirs(directory, mode=0o750, exist_ok=True)
    state = load_state()
    for name, details in state.items():
        if process_matches(int(details["pid"]), os.path.join(BIN, name)):
            raise RuntimeError("data-plane process already running: " + name)
    if state:
        save_state({})
    secrets_data = json.load(open(SECRETS, "r"))
    services = secrets_data["services"]
    base = read_api_environment()
    token = secrets.token_urlsafe(48)
    config = {
        "ingest_url": "http://127.0.0.1:8080/api/v1/internal/ingest/beat-events",
        "bindings": [{"topic": "tuba.source." + context + ".v1"} for context in CONTEXTS],
    }
    with open(CONFIG, "w") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")
    os.chmod(CONFIG, 0o640)
    env = dict(base)
    env.update({
        "KAFKA_BROKERS": BROKER,
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "KAFKA_RAW_TOPIC": RAW_TOPIC,
        "KAFKA_QUARANTINE_TOPIC": QUARANTINE_TOPIC,
        "KAFKA_EVENTS_TOPIC_PREFIX": EVENT_PREFIX,
        "KAFKA_DLQ_TOPIC": DLQ_TOPIC,
        "KAFKA_SOURCE_ADAPTER_DLQ_TOPIC": ADAPTER_DLQ_TOPIC,
        "TUBA_ORGANIZATION_ID": ORGANIZATION,
        "TUBA_NAMESPACE": NAMESPACE,
        "KAFKA_CONSUMER_GROUP_SUFFIX": "zeeklive20260927",
        "HTTP_LISTEN": "127.0.0.1:8080",
        "RAW_INDEXER_METRICS_LISTEN": "127.0.0.1:19095",
        "SOURCE_ADAPTER_METRICS_LISTEN": "127.0.0.1:19185",
        "SOURCE_ADAPTER_TOKEN": token,
        "SOURCE_ADAPTER_CONSUMER_GROUP_SUFFIX": "zeeklive20260927r2",
        "SOURCE_ADAPTER_CONFIG": CONFIG,
    })
    processes = ("tuba-ingest", "tuba-raw-indexer", "tuba-normalizer",
                 "tuba-quarantine-indexer", "tuba-standard-indexer", "tuba-source-adapter")
    running = {}
    try:
        for name in processes:
            binary = os.path.join(BIN, name)
            if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
                raise RuntimeError("missing or non-executable component: " + name)
            log = open(os.path.join(LOGS, name + ".log"), "ab", buffering=0)
            component_env = dict(env)
            service = services[SERVICE_ROLES[name]]
            component_env["KAFKA_SASL_USERNAME"] = service["username"]
            component_env["KAFKA_SASL_PASSWORD"] = service["password"]
            # Re-read the retained Raw topic under new groups after an envelope
            # contract repair, while keeping quarantine/source-adapter watermarks.
            if name in ("tuba-raw-indexer", "tuba-normalizer", "tuba-standard-indexer"):
                component_env["KAFKA_CONSUMER_GROUP_SUFFIX"] = "zeeklive20260927r2"
            process = subprocess.Popen([binary], cwd=ROOT, env=component_env, stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            log.close()
            running[name] = {"pid": process.pid, "binary": binary}
            save_state(running)
            time.sleep(.75)
            if process.poll() is not None:
                raise RuntimeError(name + " exited; inspect its validation log")
        print("started TUBA data plane for four real Zeek datasets over SCRAM-authenticated Kafka")
        for name, details in running.items():
            print(name + " pid=" + str(details["pid"]))
    except Exception:
        stop_processes(running)
        raise


def status():
    state = load_state()
    if not state:
        print("stopped")
        return
    for name, details in state.items():
        pid = int(details["pid"])
        current = "running" if process_matches(pid, os.path.join(BIN, name)) else "stopped/stale"
        print(name + " pid=" + str(pid) + " " + current)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status"))
    args = parser.parse_args()
    try:
        if args.action == "start":
            start()
        elif args.action == "stop":
            stop_processes(load_state())
            print("stopped")
        else:
            status()
    except Exception as error:
        print("error: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
