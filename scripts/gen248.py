#!/usr/bin/env python3
"""Derive a Launcher manifest + env file from the processes actually running.

The authoritative configuration for the live data plane is not any config file:
each service's environment is composed at start from the API's own environment,
the Kafka secrets file, and constants in the supervisor scripts. So this reads
/proc for the running services and reconstructs it exactly. It writes the two
files on this host and prints only a redacted copy; secret values never leave.
"""
import glob
import json
import os
import re
import sys

OUT_ENV = os.environ.get("TUBA_GEN_ENV", "/etc/tuba/tuba.env")
OUT_MANIFEST = os.environ.get("TUBA_GEN_MANIFEST", "/etc/tuba/tuba-services.json")
STATE_DIR = "/var/lib/tuba/launcher"
LOG_DIR = "/var/log/tuba"
PYTHON = "/usr/bin/python3"

# Secrets that are identical across services and must stay one variable: the
# zeek chain shares a single SOURCE_ADAPTER_TOKEN between ingest and adapter, and
# splitting it per service would let them drift apart, which breaks the handshake
# with no symptom except rejected events.
SHARED_SECRETS = ["DATABASE_URL", "ES_API_KEY", "TUBA_INGEST_API_KEY", "SOURCE_ADAPTER_TOKEN"]
# Secrets that legitimately differ per service: each service has its own ACL
# identity, so each has its own SCRAM password.
PER_SERVICE_SECRETS = ["KAFKA_SASL_PASSWORD"]
SECRET = set(SHARED_SECRETS) | set(PER_SERVICE_SECRETS)

# Per-service literals the supervisors set from code. Kept as literals so each of
# the eleven services keeps its own namespace, topics and consumer group.
LITERAL_KEYS = [
    "KAFKA_BROKERS",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_RAW_TOPIC",
    "KAFKA_RAW_TOPIC_PATTERN",
    "KAFKA_QUARANTINE_TOPIC",
    "KAFKA_EVENTS_TOPIC_PREFIX",
    "KAFKA_DLQ_TOPIC",
    "KAFKA_SOURCE_ADAPTER_DLQ_TOPIC",
    "KAFKA_CONSUMER_GROUP_SUFFIX",
    "KAFKA_ANALYSIS_RESULTS_TOPIC",
    "KAFKA_EVENTS_TOPIC",
    "SOURCE_ADAPTER_CONFIG",
    "SOURCE_ADAPTER_CONSUMER_GROUP_SUFFIX",
    "TUBA_NAMESPACE",
    "TUBA_ORGANIZATION_ID",
    "TUBA_RELEASE_ROOT",
    "API_LISTEN",
    "INDEX_BATCH_SIZE",
    "INDEX_BATCH_WAIT",
    "INDEX_MAX_ATTEMPTS",
]

# Each listener needs a distinct port; a service that had none gets port 0 so a
# Launcher restart cannot collide on a default port.
LISTENER_KEYS = ["HTTP_LISTEN", "API_LISTEN", "WEB_LISTEN", "METRICS_LISTEN"]
SUFFIX_LISTENERS = re.compile(r"^[A-Z_][A-Z0-9_]*_METRICS_LISTEN$")
ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
# Deliberately not ENV_NAME. The guard below must not share a filter with the
# loop it audits: narrowing ENV_NAME would otherwise hide a variable from both
# at once, which is how a silent drop gets through unnoticed.
GUARD_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Ambient shell noise that came along for the ride because the supervisor seeded
# each service from the API's environment. Reproducing it would hand a service a
# PWD of /root while it runs in /opt/tuba, so it is dropped deliberately -- this
# is the only category that may be dropped without checking the binary reads it.
AMBIENT_DENY = set("""PWD SHLVL _ OLDPWD TERM SHELL HOSTNAME HOME USER LOGNAME PATH
LANG LANGUAGE LS_COLORS LESSOPEN LESSCLOSE MAIL PS1 PS2 SSH_CLIENT SSH_CONNECTION
SSH_TTY XDG_SESSION_ID XDG_RUNTIME_DIR SUDO_USER SUDO_UID SUDO_GID""".split())

# Deliberately a *second* copy of the same names, not an alias of AMBIENT_DENY.
# The copy loop skips AMBIENT_DENY; the guard compares what actually went missing
# against THIS list. Widening the skip list without also widening this one makes
# the tool refuse to write instead of emitting a manifest with a hole in it --
# and a hole is the whole failure mode here: ES_URL reached no service and
# nothing said so, it just surfaced later as "ES_API_KEY is required".
MAY_DROP = set("""PWD SHLVL _ OLDPWD TERM SHELL HOSTNAME HOME USER LOGNAME PATH
LANG LANGUAGE LS_COLORS LESSOPEN LESSCLOSE MAIL PS1 PS2 SSH_CLIENT SSH_CONNECTION
SSH_TTY XDG_SESSION_ID XDG_RUNTIME_DIR SUDO_USER SUDO_UID SUDO_GID""".split())

SHORT = {
    "tuba-ingest": "ingest",
    "tuba-source-adapter": "source-adapter",
    "tuba-raw-indexer": "raw-indexer",
    "tuba-normalizer": "normalizer",
    "tuba-quarantine-indexer": "quarantine-indexer",
    "tuba-standard-indexer": "standard-indexer",
    "tuba-api": "api",
}


# Listener values dropped because they were wildcard binds that nothing respects.
dropped_listeners = []


def read_env(pid):
    try:
        raw = open("/proc/%d/environ" % pid, "rb").read()
    except (IOError, OSError):
        return None
    out = {}
    for item in raw.split(b"\0"):
        if b"=" in item:
            key, value = item.split(b"=", 1)
            out[key.decode("utf-8", "replace")] = value.decode("utf-8", "replace")
    return out


def ppid_of(pid):
    try:
        for line in open("/proc/%d/status" % pid):
            if line.startswith("PPid:"):
                return int(line.split()[1])
    except (IOError, OSError):
        pass
    return 0


def cmdline_of(pid):
    try:
        return open("/proc/%d/cmdline" % pid, "rb").read().decode("utf-8", "replace").replace("\0", " ").strip()
    except (IOError, OSError):
        return ""


def exe_of(pid):
    try:
        return os.readlink("/proc/%d/exe" % pid)
    except (IOError, OSError):
        return ""


def chain_of(pid):
    """Which supervisor started this binary, which decides its namespace."""
    parent = ppid_of(pid)
    for _ in range(3):
        if parent <= 1:
            break
        text = cmdline_of(parent)
        if "manage_zeek_live_pipeline" in text:
            return "zeek"
        if "tenant_a_chain" in text:
            return "tenant-a"
        parent = ppid_of(parent)
    return "standalone"


def collect():
    found = {}
    for path in glob.glob("/proc/[0-9]*"):
        pid = int(path.split("/")[-1])
        exe = exe_of(pid)
        if not exe.startswith("/opt/tuba/") or "/bin/" not in exe:
            continue
        base = os.path.basename(exe)
        if base not in SHORT:
            continue
        env = read_env(pid)
        if not env:
            continue
        chain = chain_of(pid)
        if base == "tuba-api":
            name, chain = "api", "standalone"
        elif chain != "standalone":
            name = chain + "-" + SHORT[base]
        else:
            name = SHORT[base]
        found[name] = {"command": exe, "env": env, "chain": chain, "pid": pid}
    return found


def env_var_for(service, key):
    return ("TUBA_" + service.upper().replace("-", "_") + "_" + key).replace("__", "_")


def build(services):
    file_env, manifest_services = {}, []
    del dropped_listeners[:]
    # A shared secret with two different values in the live plane means the plane
    # is already inconsistent; refuse rather than silently pick one.
    for key in SHARED_SECRETS:
        values = set(svc["env"][key] for svc in services.values() if key in svc["env"])
        if len(values) > 1:
            raise SystemExit("refusing to write: %s has %d different live values" % (key, len(values)))
        if values:
            file_env[key] = values.pop()

    for name in sorted(services):
        svc = services[name]
        per_service = {}
        # Listener values are reproduced as the supervisor set them, with one
        # exception: a wildcard bind is dropped. The launcher refuses wildcards
        # unless non-loopback is explicitly allowed, and on this host the only
        # wildcard entries (api's HTTP_LISTEN and METRICS_LISTEN) are dead --
        # the api binds only its API_LISTEN address.
        for key in list(svc["env"]):
            if key in LISTENER_KEYS or SUFFIX_LISTENERS.match(key):
                if svc["env"][key].startswith(":") or svc["env"][key].startswith("0.0.0.0:"):
                    dropped_listeners.append("%s.%s=%s" % (name, key, svc["env"][key]))
                    continue
                per_service[key] = svc["env"][key]
        for key in LITERAL_KEYS:
            if key in svc["env"]:
                per_service[key] = svc["env"][key]
        for key in SHARED_SECRETS:
            if key in svc["env"]:
                per_service[key] = "${%s}" % key
        for key in PER_SERVICE_SECRETS:
            if key in svc["env"]:
                var = env_var_for(name, key)
                file_env[var] = svc["env"][key]
                per_service[key] = "${%s}" % var
        # The supervisor seeded every service from the API's environment, so the
        # live environment -- not a list of names somebody thought to write down
        # -- is the specification. The hand-maintained whitelist this replaces is
        # what dropped ES_URL and took the whole data plane down with "ES_URL and
        # ES_API_KEY are required": a name missing from a whitelist fails at run
        # time somewhere else entirely, or not at all. Everything not already
        # emitted, not secret, not a listener, and not ambient shell noise is now
        # reproduced verbatim.
        for key, value in svc["env"].items():
            if key in per_service or key in SECRET or key in AMBIENT_DENY:
                continue
            if key in LISTENER_KEYS or SUFFIX_LISTENERS.match(key):
                continue
            if ENV_NAME.match(key):
                per_service[key] = value
        # Every service runs its binary directly. The python3 supervisor is the
        # process being replaced, not part of the service: its supervise() loop
        # does Popen([binary]) itself. Wrapping the binary in python3 here would
        # make the interpreter parse a Go executable and die at once.
        entry = {
            "name": name,
            "command": svc["command"],
            "working_dir": "/opt/tuba",
            "environment": per_service,
        }
        manifest_services.append(entry)

    # Fail closed rather than write a manifest that is quietly missing something.
    # A variable that some live service had and no built service carries is
    # either a whitelist bug or a binding nobody has thought about yet; both are
    # worth stopping for, since the symptom is a service that starts and then
    # dies in a way that names a different variable.
    live_keys = set()
    for svc in services.values():
        live_keys |= {key for key in svc["env"] if GUARD_NAME.match(key)}
    built_keys = set()
    for entry in manifest_services:
        built_keys |= set(entry["environment"])
    dropped = sorted(k for k in live_keys - built_keys
                     if k not in MAY_DROP and k not in LISTENER_KEYS
                     and not SUFFIX_LISTENERS.match(k))
    if dropped:
        raise SystemExit("refusing to write: live variables reached no service: " + ", ".join(dropped))
    return file_env, manifest_services


def write_env(path, values, header):
    lines = ["# " + header, "# Generated from the running processes; contains credentials."]
    for key in sorted(values):
        lines.append("%s=%s" % (key, values[key]))
    tmp = path + ".new"
    with open(tmp, "w") as handle:
        handle.write("\n".join(lines) + "\n")
    os.chmod(tmp, 0o600)
    os.rename(tmp, path)
    try:
        os.chown(path, 0, 0)
    except OSError:
        pass


def redacted(value):
    return "<redacted:%d>" % len(value) if value else value


def main():
    services = collect()
    if not services:
        print("no running services found; refusing to write an empty manifest", file=sys.stderr)
        return 1
    file_env, manifest_services = build(services)
    manifest = {
        "version": 1,
        "state_dir": STATE_DIR,
        "log_dir": LOG_DIR,
        "environment_file": OUT_ENV,
        "services": manifest_services,
    }
    if not os.path.isdir("/etc/tuba"):
        os.makedirs("/etc/tuba")
    write_env(OUT_ENV, file_env, "TUBA service credentials and shared environment")
    tmp = OUT_MANIFEST + ".new"
    with open(tmp, "w") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(tmp, 0o600)
    os.rename(tmp, OUT_MANIFEST)

    print("services: %d" % len(manifest_services))
    for svc in manifest_services:
        print("\n## %s" % svc["name"])
        print("   command: %s" % svc["command"])
        for key in (
            "TUBA_NAMESPACE",
            "KAFKA_SASL_USERNAME",
            "KAFKA_CONSUMER_GROUP_SUFFIX",
            "KAFKA_RAW_TOPIC",
            "HTTP_LISTEN",
            "METRICS_LISTEN",
            "API_LISTEN",
            "KAFKA_SASL_PASSWORD",
            "SOURCE_ADAPTER_TOKEN",
        ):
            if key in svc["environment"]:
                value = svc["environment"][key]
                print("   %-34s %s" % (key + ":", redacted(value) if key in SECRET else value))
    print("\nenv file variables: %d" % len(file_env))
    print("   " + ", ".join(sorted(file_env)))
    if dropped_listeners:
        print("\ndropped wildcard listener values (not bound in practice):")
        for item in dropped_listeners:
            print("   " + item)
    return 0


if __name__ == "__main__":
    sys.exit(main())
