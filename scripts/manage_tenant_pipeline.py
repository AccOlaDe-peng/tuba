#!/usr/bin/env python3
"""Start/stop the TUBA data plane for one additional namespace.

The Zeek pipeline script is single-namespace by construction: its namespace,
topic names, consumer group suffix, metrics ports and run directory are module
constants. This runs the same four consumer components for another namespace
alongside it, so a source registered under that namespace has a chain to land in
instead of being rejected by the Zeek namespace's indexers as a tenant scope
mismatch.

The ingest and the source adapter are shared: one ingest routes each source to
its own namespace's raw topic, and the existing adapter already consumes every
source topic.

Each component runs under a supervisor that restarts it with bounded backoff,
mirroring the Zeek chain. The supervisor lives here rather than being borrowed
from the Zeek script because that script keys its child pid files by binary
name, and both chains run binaries with the same names.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

ROOT = "/opt/tuba/collector-live/pipeline"
KAFKA_ROOT = "/opt/tuba/collector-live/kafka"
BIN = os.path.join(ROOT, "bin")
RUN = os.path.join(ROOT, "run")
LOGS = os.path.join(ROOT, "logs")
SECRETS = os.path.join(KAFKA_ROOT, "secrets.json")
BROKER = "10.6.68.248:29292"

NAMESPACE = os.environ.get("TUBA_PIPELINE_NAMESPACE", "tenant_a")
GROUP_SUFFIX = os.environ.get("TUBA_PIPELINE_GROUP_SUFFIX", "tenanta20260929")
# One metrics port per component, offset from a base so a second namespace gets a
# block that does not overlap the first. The defaults are distinct from the Zeek
# chain's 19095/19185, so both planes can run at once.
METRICS_ORDER = ("tuba-raw-indexer", "tuba-normalizer",
                 "tuba-quarantine-indexer", "tuba-standard-indexer")
METRICS_BASE = int(os.environ.get("TUBA_PIPELINE_METRICS_BASE", "19295"))
METRICS = {name: "127.0.0.1:%d" % (METRICS_BASE + 100 * i)
           for i, name in enumerate(METRICS_ORDER)}
METRICS_VAR = {
    "tuba-raw-indexer": "RAW_INDEXER_METRICS_LISTEN",
    "tuba-normalizer": "NORMALIZER_METRICS_LISTEN",
    "tuba-quarantine-indexer": "QUARANTINE_INDEXER_METRICS_LISTEN",
    "tuba-standard-indexer": "STANDARD_INDEXER_METRICS_LISTEN",
}
SERVICE_ROLES = {
    "tuba-raw-indexer": "raw-indexer",
    "tuba-normalizer": "normalizer",
    "tuba-quarantine-indexer": "quarantine-indexer",
    "tuba-standard-indexer": "standard-indexer",
}
# Namespaced state, so nothing here can collide with the Zeek chain's files.
STATE = os.path.join(RUN, "tenant_a_chain.json")
CHILD_RUN = os.path.join(RUN, NAMESPACE)

BACKOFF_INITIAL = 2
BACKOFF_MAX = 60
BACKOFF_RESET_AFTER = 300


def read_api_environment():
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        base = "/proc/" + entry
        try:
            if b"/opt/tuba/bin/tuba-api" not in open(os.path.join(base, "cmdline"), "rb").read():
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


def environment_for(name):
    """Environment for one component of this namespace's chain."""
    secrets = json.load(open(SECRETS, encoding="utf-8"))
    env = dict(os.environ)
    env.update(read_api_environment())
    env.update({
        "KAFKA_BROKERS": BROKER,
        "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "TUBA_ORGANIZATION_ID": NAMESPACE,
        "TUBA_NAMESPACE": NAMESPACE,
        "KAFKA_RAW_TOPIC": "tuba.collector.%s.raw.live2.v1" % NAMESPACE,
        "KAFKA_QUARANTINE_TOPIC": "tuba.collector.%s.quarantine.v1" % NAMESPACE,
        "KAFKA_DLQ_TOPIC": "tuba.collector.%s.dlq.v1" % NAMESPACE,
        "KAFKA_EVENTS_TOPIC_PREFIX": "tuba.collector.%s.events" % NAMESPACE,
        "KAFKA_CONSUMER_GROUP_SUFFIX": GROUP_SUFFIX,
    })
    role = SERVICE_ROLES[name]
    env["KAFKA_SASL_USERNAME"] = secrets["services"][role]["username"]
    env["KAFKA_SASL_PASSWORD"] = secrets["services"][role]["password"]
    env[METRICS_VAR[name]] = METRICS[name]
    return env


def child_pid_path(name):
    return os.path.join(CHILD_RUN, name + ".child.pid")


def read_child_pid(name):
    try:
        return int(open(child_pid_path(name), "r").read().strip())
    except (OSError, ValueError):
        return None


def alive(pid):
    try:
        state = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].strip().split()[0]
        return state != "Z"
    except (OSError, IOError, IndexError, ValueError):
        return False


def load_state():
    try:
        with open(STATE) as handle:
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


def spawn_supervisor(name):
    binary = os.path.join(BIN, name)
    if not (os.path.isfile(binary) and os.access(binary, os.X_OK)):
        raise RuntimeError("missing or non-executable component: " + name)
    log = open(os.path.join(LOGS, "%s-%s.log" % (name, NAMESPACE)), "ab", buffering=0)
    return subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "--supervise-binary", binary],
        cwd=ROOT, env=environment_for(name), stdout=log, stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL, start_new_session=True)


def terminate(supervisor_pid, name):
    """Stop a supervisor and its child.

    The child shares the supervisor's process group, so signalling the group
    stops both and cannot leave an orphan holding the consumer group.
    """
    os.killpg(supervisor_pid, signal.SIGTERM)
    deadline = time.time() + 10
    while time.time() < deadline and alive(supervisor_pid):
        time.sleep(.2)
    if alive(supervisor_pid):
        os.killpg(supervisor_pid, signal.SIGKILL)
    child = read_child_pid(name)
    if child and alive(child):
        os.kill(child, signal.SIGKILL)


def supervise(binary):
    """Restart one component with bounded backoff until the supervisor is killed.

    A component that stayed up long enough counts as healthy, so the backoff
    resets instead of drifting towards the cap across unrelated restarts.
    """
    name = os.path.basename(binary)
    os.makedirs(CHILD_RUN, mode=0o750, exist_ok=True)
    delay = BACKOFF_INITIAL
    while True:
        started_at = time.monotonic()
        process = subprocess.Popen([binary], cwd=ROOT, env=os.environ,
                                   stdin=subprocess.DEVNULL, start_new_session=False)
        temporary = child_pid_path(name) + ".tmp"
        with open(temporary, "w") as handle:
            handle.write(str(process.pid) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, child_pid_path(name))
        result = process.wait()
        if read_child_pid(name) == process.pid:
            try:
                os.remove(child_pid_path(name))
            except OSError:
                pass
        if time.monotonic() - started_at >= BACKOFF_RESET_AFTER:
            delay = BACKOFF_INITIAL
        print("component exited with code=%d; restarting in %d seconds" % (result, delay), flush=True)
        time.sleep(delay)
        delay = min(delay * 2, BACKOFF_MAX)


def start():
    state = load_state()
    if any(alive(int(d["pid"])) for d in state.values()):
        raise RuntimeError("tenant_a chain already running; stop it first")
    os.makedirs(CHILD_RUN, mode=0o750, exist_ok=True)
    state = {}
    try:
        for name in SERVICE_ROLES:
            supervisor = spawn_supervisor(name)
            state[name] = {"pid": supervisor.pid, "supervised": True}
            print("started supervisor %-26s pid=%d" % (name, supervisor.pid))
    finally:
        save_state(state)
    return 0


def stop():
    state = load_state()
    for name, details in reversed(list(state.items())):
        pid = int(details["pid"])
        if not alive(pid):
            print("%s supervisor already gone" % name)
            continue
        terminate(pid, name)
        print("stopped %s" % name)
    save_state({})
    return 0


def restart(names):
    if not names:
        raise SystemExit("restart needs at least one component name")
    state = load_state()
    for name in names:
        if name not in SERVICE_ROLES:
            raise SystemExit("unknown component: " + name)
        details = state.get(name)
        if details and alive(int(details["pid"])):
            terminate(int(details["pid"]), name)
            print("stopped %s" % name)
        supervisor = spawn_supervisor(name)
        state[name] = {"pid": supervisor.pid, "supervised": True}
        print("restarted %s supervisor pid=%d" % (name, supervisor.pid))
    save_state(state)
    return 0


def status():
    state = load_state()
    for name in SERVICE_ROLES:
        details = state.get(name)
        if not details:
            print("  %-26s not started by this tool" % name)
            continue
        pid = int(details["pid"])
        child = read_child_pid(name)
        child_state = "child=%d" % child if child and alive(child) else "child=missing"
        print("  %-26s supervisor=%-8s %s" % (name, pid if alive(pid) else "dead", child_state))
        log = os.path.join(LOGS, "%s-%s.log" % (name, NAMESPACE))
        if os.path.isfile(log):
            with open(log, "rb") as handle:
                handle.seek(max(0, os.path.getsize(log) - 2000))
                tail = handle.read().decode("utf-8", "replace").strip().splitlines()[-2:]
            for line in tail:
                print("       | " + line[:170])
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("action", nargs="?", choices=("start", "stop", "status", "restart"))
    parser.add_argument("components", nargs="*")
    parser.add_argument("--supervise-binary", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.supervise_binary:
        supervise(args.supervise_binary)
        return 0
    if not args.action:
        parser.error("action is required")
    if args.action == "restart":
        return restart(args.components)
    return {"start": start, "stop": stop, "status": status}[args.action]()


if __name__ == "__main__":
    raise SystemExit(main())
