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
# The ingest routes each source to a raw topic derived from its own namespace,
# while the consumers below read one concrete topic. The pattern resolves to
# exactly RAW_TOPIC for this namespace, so this chain is unaffected; a source
# registered under another namespace lands on that namespace's own topic
# instead of being rejected downstream as a tenant scope mismatch.
RAW_TOPIC_PATTERN = "tuba.collector.{namespace}.raw.live2.v1"
EVENT_PREFIX = "tuba.collector." + NAMESPACE + ".events"
QUARANTINE_TOPIC = "tuba.collector." + NAMESPACE + ".quarantine.v1"
DLQ_TOPIC = "tuba.collector." + NAMESPACE + ".dlq.v1"
ADAPTER_DLQ_TOPIC = "tuba.collector." + NAMESPACE + ".source-adapter.dlq.v1"
PSQL = os.environ.get("TUBA_PSQL", "/opt/adms/postgresql/bin/psql")


def enabled_source_contexts(database_url):
    """Topic bindings, read from the source registry instead of a fixed list.

    The registry is the same authority ingest resolves topics against, so
    registering a source through the API is enough to have the adapter consume
    it. A hardcoded list here silently drops any source registered elsewhere as
    soon as the pipeline is restarted, which is exactly the failure this
    replaces. One instance keeps every context it has ever had, because contexts
    are immutable and a reset mints a new one, so only the newest is live.
    """
    query = ("SELECT DISTINCT ON (si.id) sc.id FROM source_contexts sc "
             "JOIN source_instances si ON si.id = sc.source_instance_id "
             "WHERE si.enabled = true ORDER BY si.id, sc.created_at DESC")
    result = subprocess.run([PSQL, database_url, "-Atc", query],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            universal_newlines=True, timeout=60)
    if result.returncode:
        raise RuntimeError("could not read enabled source contexts: " + result.stdout.strip()[-200:])
    contexts = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not contexts:
        raise RuntimeError("source registry has no enabled source contexts")
    return contexts
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


def supervisor_matches(pid):
    try:
        state = open("/proc/%d/stat" % pid, "r").read().rsplit(")", 1)[1].strip().split()[0]
        if state == "Z":
            return False
        args = open("/proc/%d/cmdline" % pid, "rb").read()
        return os.path.realpath(__file__).encode() in args and b"--supervise-binary" in args
    except (OSError, ValueError):
        return False


def state_process_matches(name, details):
    pid = int(details["pid"])
    if details.get("supervised"):
        if not supervisor_matches(pid):
            return False
        child_pid = supervised_child_pid(name)
        return child_pid is not None and process_matches(child_pid, os.path.join(BIN, name))
    return process_matches(pid, os.path.join(BIN, name))


def state_process_alive(name, details):
    pid = int(details["pid"])
    return supervisor_matches(pid) if details.get("supervised") else process_matches(pid, os.path.join(BIN, name))


def child_pid_path(name):
    return os.path.join(RUN, name + ".child.pid")


def supervised_child_pid(name):
    try:
        return int(open(child_pid_path(name), "r").read().strip())
    except (OSError, ValueError):
        return None


def wait_component_started(name, details, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if details.get("supervised"):
            child_pid = supervised_child_pid(name)
            if child_pid is not None and process_matches(child_pid, os.path.join(BIN, name)):
                return
            if not supervisor_matches(int(details["pid"])):
                break
        elif process_matches(int(details["pid"]), os.path.join(BIN, name)):
            return
        time.sleep(.1)
    raise RuntimeError(name + " child failed to start; inspect its validation log")


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
        matches = state_process_alive(name, details)
        if not matches:
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        deadline = time.time() + 10
        while time.time() < deadline and state_process_alive(name, details):
            time.sleep(.2)
        if state_process_alive(name, details):
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.time() + 5
            while time.time() < deadline and state_process_alive(name, details):
                time.sleep(.2)
            if state_process_alive(name, details):
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
        if state_process_alive(name, details):
            raise RuntimeError("data-plane process already running: " + name)
    if state:
        save_state({})
    secrets_data = json.load(open(SECRETS, "r"))
    services = secrets_data["services"]
    base = read_api_environment()
    token = secrets.token_urlsafe(48)
    config = {
        "ingest_url": "http://127.0.0.1:8080/api/v1/internal/ingest/beat-events",
        "bindings": [{"topic": "tuba.source." + context + ".v1"}
                     for context in enabled_source_contexts(base["DATABASE_URL"])],
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
        "KAFKA_RAW_TOPIC_PATTERN": RAW_TOPIC_PATTERN,
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
            process = subprocess.Popen([sys.executable, os.path.abspath(__file__),
                                        "--supervise-binary", binary], cwd=ROOT,
                                       env=component_env, stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            log.close()
            running[name] = {"pid": process.pid, "binary": binary, "supervised": True}
            save_state(running)
            time.sleep(.75)
            wait_component_started(name, running[name])
        print("started TUBA data plane for four real Zeek datasets over SCRAM-authenticated Kafka")
        for name, details in running.items():
            print(name + " pid=" + str(details["pid"]))
    except Exception:
        stop_processes(running)
        raise


def process_environment(pid):
    raw = open("/proc/%d/environ" % pid, "rb").read()
    return dict(item.split(b"=", 1) for item in raw.split(bytes([0])) if b"=" in item)


def existing_adapter_token(state, live):
    preferred = ("tuba-ingest", "tuba-source-adapter", "tuba-normalizer",
                 "tuba-raw-indexer", "tuba-standard-indexer", "tuba-quarantine-indexer")
    for name in preferred:
        if name not in live:
            continue
        token = process_environment(int(state[name]["pid"])).get(b"SOURCE_ADAPTER_TOKEN", b"").decode()
        if len(token) >= 32:
            return token
    raise RuntimeError("cannot recover the existing adapter token from any running data-plane process")


def start_components(names, restart=False):
    """Recover selected stopped workers without touching live peers or offsets."""
    state = load_state()
    live = {name for name, details in state.items() if state_process_alive(name, details)}
    requested = list(dict.fromkeys(names))
    unknown = [name for name in requested if name not in SERVICE_ROLES]
    if unknown:
        raise RuntimeError("unknown component: " + ", ".join(unknown))
    token = existing_adapter_token(state, live)
    if restart:
        for name in requested:
            if name not in live:
                continue
            details = state[name]
            pid = int(details["pid"])
            alive = lambda: (supervisor_matches(pid) if details.get("supervised")
                             else process_matches(pid, os.path.join(BIN, name)))
            os.killpg(pid, signal.SIGTERM)
            deadline = time.time() + 10
            while time.time() < deadline and alive():
                time.sleep(.2)
            if alive():
                os.killpg(pid, signal.SIGKILL)
                deadline = time.time() + 5
                while time.time() < deadline and alive():
                    time.sleep(.2)
                if alive():
                    raise RuntimeError("could not stop component for supervised restart: " + name)
            live.remove(name)
    missing = [name for name in requested if name not in live]
    if not missing:
        print("requested components already running")
        return
    secrets_data = json.load(open(SECRETS, "r"))
    services = secrets_data["services"]
    base = read_api_environment()
    env = dict(base)
    env.update({
        "KAFKA_BROKERS": BROKER,
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "KAFKA_RAW_TOPIC": RAW_TOPIC,
        "KAFKA_RAW_TOPIC_PATTERN": RAW_TOPIC_PATTERN,
        "KAFKA_QUARANTINE_TOPIC": QUARANTINE_TOPIC,
        "KAFKA_EVENTS_TOPIC_PREFIX": EVENT_PREFIX,
        "KAFKA_DLQ_TOPIC": DLQ_TOPIC,
        "KAFKA_SOURCE_ADAPTER_DLQ_TOPIC": ADAPTER_DLQ_TOPIC,
        "TUBA_ORGANIZATION_ID": ORGANIZATION,
        "TUBA_NAMESPACE": NAMESPACE,
        "HTTP_LISTEN": "127.0.0.1:8080",
        "RAW_INDEXER_METRICS_LISTEN": "127.0.0.1:19095",
        "SOURCE_ADAPTER_METRICS_LISTEN": "127.0.0.1:19185",
        "SOURCE_ADAPTER_TOKEN": token,
        "SOURCE_ADAPTER_CONSUMER_GROUP_SUFFIX": "zeeklive20260927r2",
        "SOURCE_ADAPTER_CONFIG": CONFIG,
    })
    started = {}
    try:
        for name in missing:
            binary = os.path.join(BIN, name)
            if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
                raise RuntimeError("missing or non-executable component: " + name)
            component_env = dict(env)
            component_env["KAFKA_SASL_USERNAME"] = services[SERVICE_ROLES[name]]["username"]
            component_env["KAFKA_SASL_PASSWORD"] = services[SERVICE_ROLES[name]]["password"]
            if name in ("tuba-raw-indexer", "tuba-normalizer", "tuba-standard-indexer"):
                component_env["KAFKA_CONSUMER_GROUP_SUFFIX"] = "zeeklive20260927r2"
            elif name == "tuba-quarantine-indexer":
                component_env["KAFKA_CONSUMER_GROUP_SUFFIX"] = "zeeklive20260927"
            else:
                component_env["KAFKA_CONSUMER_GROUP_SUFFIX"] = "zeeklive20260927"
            with open(os.path.join(LOGS, name + ".log"), "ab", buffering=0) as log:
                process = subprocess.Popen([sys.executable, os.path.abspath(__file__),
                                            "--supervise-binary", binary], cwd=ROOT,
                                           env=component_env, stdin=subprocess.DEVNULL,
                                           stdout=log, stderr=subprocess.STDOUT,
                                           start_new_session=True)
            started[name] = {"pid": process.pid, "binary": binary, "supervised": True}
            wait_component_started(name, started[name])
        state.update(started)
        save_state(state)
        for name, details in started.items():
            print("started " + name + " pid=" + str(details["pid"]))
    except Exception:
        for details in started.values():
            pid = int(details["pid"])
            if supervisor_matches(pid):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        raise


def status():
    state = load_state()
    if not state:
        print("stopped")
        return
    for name, details in state.items():
        pid = int(details["pid"])
        if details.get("supervised") and state_process_alive(name, details):
            current = "running" if state_process_matches(name, details) else "restarting"
        else:
            current = "running" if state_process_matches(name, details) else "stopped/stale"
        print(name + " pid=" + str(pid) + " " + current)


def supervise(binary):
    delay = 2
    name = os.path.basename(binary)
    os.makedirs(RUN, mode=0o750, exist_ok=True)
    pid_path = child_pid_path(name)
    while True:
        started_at = time.monotonic()
        process = subprocess.Popen([binary], cwd=ROOT, env=os.environ,
                                   stdin=subprocess.DEVNULL, start_new_session=False)
        temporary = pid_path + ".tmp"
        with open(temporary, "w") as handle:
            handle.write(str(process.pid) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, pid_path)
        result = process.wait()
        try:
            if supervised_child_pid(name) == process.pid:
                os.remove(pid_path)
        except OSError:
            pass
        if time.monotonic() - started_at >= 300:
            delay = 2
        print("component exited with code=" + str(result) + "; restarting in " + str(delay) + " seconds", flush=True)
        time.sleep(delay)
        delay = min(delay * 2, 60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--supervise-binary")
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("action", nargs="?", choices=("start", "start-components", "stop", "status"))
    parser.add_argument("components", nargs="*")
    args = parser.parse_args()
    try:
        if args.supervise_binary:
            supervise(args.supervise_binary)
            return 0
        if not args.action:
            parser.error("action is required")
        if args.action == "start":
            start()
        elif args.action == "start-components":
            if not args.components:
                raise RuntimeError("start-components requires one or more component names")
            start_components(args.components, restart=args.restart)
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
