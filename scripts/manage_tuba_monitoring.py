#!/usr/bin/env python3
"""Manually manage loopback-only O05 monitoring processes on the single node."""

import argparse
import json
import os
import pwd
import secrets
import signal
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request


ROOT = os.environ.get("TUBA_MONITORING_ROOT", "/opt/tuba/monitoring")
BIN = os.path.join(ROOT, "bin")
PROM = os.path.join(ROOT, "prometheus")
RUN = os.path.join(ROOT, "run")
LOG = os.path.join(ROOT, "logs")
KAFKA_SECRETS = "/opt/tuba/collector-live/kafka/secrets.json"
BROKER = "10.6.68.248:29292"
# The consumer groups monitoring is allowed to scrape and alert on.
#
# An allow-list rather than `tuba-.*` on purpose: a retired generation leaves its
# consumer groups behind with a committed offset and no members, so their lag
# freezes at whatever it was and never moves. `--all-groups` still lists them,
# and TubaKafkaConsumerInactiveWithBacklog ("lag > 0 and no members") fires on
# them immediately. Keeping them out is what this list is for.
#
# It has to be extended when a namespace is added, and it was missing the whole
# tenant_a (Windows Security) chain until 2026-09-30 — the busiest ingestion path
# had no lag monitoring at all. The same alternation is repeated verbatim in the
# Prometheus rules and the Grafana dashboard, which cannot import from here;
# when this changes, those change too.
MONITORED_CONSUMER_GROUPS = "|".join((
    # Active source adapters all carry the current generation suffix. The
    # unsuffixed tuba-source-adapter-* names in the same range are retired
    # placeholders whose committed offsets stopped moving: their lag is frozen
    # and they have no members, so listing them would make
    # TubaKafkaConsumerInactiveWithBacklog fire forever. A pattern rather than
    # six hashes also covers sources registered later.
    r"tuba-source-adapter-[0-9a-f]{16}-zeeklive20260927r2",
    r"tuba-raw-indexer-zeek_validation_20260927_001-zeeklive20260927r2",
    r"tuba-normalizer-zeek_validation_20260927_001-zeeklive20260927r2",
    # The quarantine indexer kept its pre-generation group name on purpose, so
    # renaming it would restart its offsets.
    r"tuba-quarantine-indexer-zeek_validation_20260927_001-zeeklive20260927",
    r"tuba-raw-indexer-tenant_a-tenanta20260929",
    r"tuba-normalizer-tenant_a-tenanta20260929",
    r"tuba-quarantine-indexer-tenant_a-tenanta20260929",
    r"tuba-standard-indexer-(authentication|session|iam|directory|network|dns|web|tls)-zeek_validation_20260927_001-zeeklive20260927r2",
    r"tuba-standard-indexer-(authentication|session|iam|directory|network|dns|web|tls)-tenant_a-tenanta20260929",
))
KAFKA_GROUP_FILTER = "^(" + MONITORED_CONSUMER_GROUPS + ")$"
SERVICES = {
    "prometheus": {
        "binary": os.path.join(BIN, "prometheus"), "port": 19090,
        "run_as": "tuba-prometheus",
        "args": ["--config.file=" + os.path.join(PROM, "prometheus.yml"),
                 "--storage.tsdb.path=" + os.path.join(ROOT, "data", "prometheus"),
                 "--storage.tsdb.retention.time=15d", "--storage.tsdb.retention.size=1GB",
                 "--web.listen-address=127.0.0.1:19090",
                 "--web.external-url=http://127.0.0.1:19090/", "--log.level=warn"],
    },
    "node-exporter": {
        "binary": os.path.join(BIN, "node_exporter"), "port": 19101,
        "run_as": "tuba-node-exporter",
        "args": ["--web.listen-address=127.0.0.1:19101", "--log.level=warn",
                 "--collector.filesystem.mount-points-exclude=^/(dev|proc|sys|run/.+|var/lib/containers/.+)($|/)",
                 "--collector.filesystem.fs-types-exclude=^(autofs|binfmt_misc|bpf|cgroup2?|configfs|debugfs|devpts|devtmpfs|fusectl|hugetlbfs|mqueue|nsfs|overlay|proc|procfs|pstore|rpc_pipefs|securityfs|selinuxfs|squashfs|sysfs|tracefs)$"],
    },
    "kafka-exporter": {
        "binary": os.path.join(BIN, "kafka_exporter"), "port": 19102,
        "run_as": "tuba-kafka-exporter",
        "args": ["--kafka.server=" + BROKER, "--sasl.enabled",
                 "--sasl.username=tuba-kafka-observer", "--sasl.mechanism=scram-sha512",
                 "--group.filter=" + KAFKA_GROUP_FILTER, "--topic.filter=tuba\\..*",
                 "--web.listen-address=127.0.0.1:19102"],
    },
}
SERVICES["log-rotator"] = {
    "binary": sys.executable, "port": None, "process_match": b"manage_tuba_monitoring.py rotate-logs",
    "args": [os.path.abspath(__file__), "rotate-logs"],
}
GRAFANA_HOME = os.path.join(ROOT, "grafana")
if os.path.isfile(os.path.join(GRAFANA_HOME, "bin", "grafana-server")):
    SERVICES["grafana"] = {
        "binary": os.path.join(GRAFANA_HOME, "bin", "grafana-server"), "port": 13000,
        "process_match": b"grafana server",
        "run_as": "tuba-grafana",
        "health_path": "/api/health",
        "args": ["--homepath=" + GRAFANA_HOME,
                 "--config=" + os.path.join(ROOT, "grafana", "conf", "custom.ini"),
                 "cfg:server.http_addr=127.0.0.1", "cfg:server.http_port=13000",
                 ],
    }


def pid_path(name):
    return os.path.join(RUN, name + ".pid")


def read_pid(name):
    try:
        pid = int(open(pid_path(name), encoding="ascii").read().strip())
        raw = open("/proc/%d/cmdline" % pid, "rb").read().replace(b"\0", b" ")
        if SERVICES[name].get("process_match", os.path.basename(SERVICES[name]["binary"]).encode()) not in raw:
            return None
        return pid
    except (OSError, ValueError):
        return None


def healthy(name):
    if SERVICES[name]["port"] is None:
        return bool(read_pid(name))
    suffix = SERVICES[name].get("health_path", "/-/healthy" if name == "prometheus" else "/metrics")
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d%s" % (SERVICES[name]["port"], suffix), timeout=10) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def grafana_admin_password():
    path = os.path.join(ROOT, "secrets.json")
    try:
        with open(path, encoding="utf-8") as handle:
            values = json.load(handle)
    except FileNotFoundError:
        values = {}
    password = values.get("grafana_admin_password")
    if not password:
        password = secrets.token_urlsafe(32)
        values["grafana_admin_password"] = password
        temporary = path + ".tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(values, handle, indent=2)
            handle.write("\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    return password


def rotate_monitoring_logs():
    maximum = 16 * 1024 * 1024
    backups = 3
    for filename in os.listdir(LOG):
        if not filename.endswith(".log"):
            continue
        path = os.path.join(LOG, filename)
        try:
            if os.path.getsize(path) < maximum:
                continue
        except OSError:
            continue
        oldest = path + ".%d" % backups
        try:
            os.remove(oldest)
        except FileNotFoundError:
            pass
        for index in range(backups - 1, 0, -1):
            try:
                os.replace(path + ".%d" % index, path + ".%d" % (index + 1))
            except FileNotFoundError:
                pass
        shutil.copy2(path, path + ".1")
        with open(path, "wb"):
            pass


def run_log_rotator():
    while True:
        rotate_monitoring_logs()
        time.sleep(60)


def start_one(name):
    spec = SERVICES[name]
    if read_pid(name):
        print("%s already running pid=%s healthy=%s" % (name, read_pid(name), healthy(name)))
        return
    if not os.path.isfile(spec["binary"]):
        raise RuntimeError("missing binary: " + spec["binary"])
    if name == "log-rotator":
        process = subprocess.Popen([sys.executable, os.path.abspath(__file__), "rotate-logs"],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL, cwd=ROOT, start_new_session=True)
        temporary = pid_path(name) + ".tmp"
        with open(temporary, "w", encoding="ascii") as handle:
            handle.write(str(process.pid) + "\n")
        os.chmod(temporary, 0o600)
        os.replace(temporary, pid_path(name))
        time.sleep(.2)
        if not healthy(name):
            raise RuntimeError("monitoring log rotator did not start")
        print("log-rotator running pid=%d; threshold=16MiB; backups=3" % process.pid)
        return
    env = dict(os.environ)
    if name == "kafka-exporter":
        with open(KAFKA_SECRETS, encoding="utf-8") as handle:
            secrets = json.load(handle)
        monitoring = secrets.get("monitoring", {}).get("kafka_exporter")
        if not monitoring or not monitoring.get("password"):
            raise RuntimeError("Kafka exporter credentials are not provisioned")
        env["SASL_USER_PASSWORD"] = monitoring["password"]
    if name == "grafana":
        env["GF_SECURITY_ADMIN_PASSWORD"] = grafana_admin_password()
        env["GF_USERS_ALLOW_SIGN_UP"] = "false"
        env["GF_AUTH_ANONYMOUS_ENABLED"] = "false"
        env["GF_PATHS_DATA"] = os.path.join(ROOT, "data", "grafana")
        env["GF_PATHS_LOGS"] = os.path.join(ROOT, "logs", "grafana")
        env["GF_PATHS_PLUGINS"] = os.path.join(ROOT, "data", "grafana", "plugins")
        env["GF_PATHS_PROVISIONING"] = os.path.join(ROOT, "grafana", "provisioning")
        env["GF_LOG_MODE"] = "file"
        env["GF_LOG_FILE_LOG_ROTATE"] = "true"
        env["GF_LOG_FILE_MAX_SIZE_SHIFT"] = "24"
        env["GF_LOG_FILE_DAILY_ROTATE"] = "true"
        env["GF_LOG_FILE_MAX_DAYS"] = "3"
    logfile = open(os.path.join(LOG, name + ".log"), "ab", buffering=0)
    account = pwd.getpwnam(spec["run_as"])
    data_path = os.path.join(ROOT, "data", name)
    os.makedirs(data_path, mode=0o750, exist_ok=True)
    os.chown(data_path, account.pw_uid, account.pw_gid)
    if name == "grafana":
        grafana_logs = os.path.join(LOG, "grafana")
        os.makedirs(grafana_logs, mode=0o750, exist_ok=True)
        os.chown(grafana_logs, account.pw_uid, account.pw_gid)

    def drop_privileges():
        os.setgroups([])
        os.setgid(account.pw_gid)
        os.setuid(account.pw_uid)

    process = subprocess.Popen([spec["binary"]] + spec["args"], stdin=subprocess.DEVNULL,
                               stdout=logfile, stderr=subprocess.STDOUT,
                               cwd=ROOT, env=env, start_new_session=True,
                               preexec_fn=drop_privileges)
    logfile.close()
    temporary = pid_path(name) + ".tmp"
    with open(temporary, "w", encoding="ascii") as handle:
        handle.write(str(process.pid) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, pid_path(name))
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError("%s exited; inspect %s" % (name, os.path.join(LOG, name + ".log")))
        if healthy(name):
            print("%s running pid=%d healthy=yes" % (name, process.pid))
            return
        time.sleep(.5)
    raise RuntimeError("%s started but health endpoint did not become ready" % name)


def stop_one(name):
    pid = read_pid(name)
    if not pid:
        try:
            os.remove(pid_path(name))
        except OSError:
            pass
        print(name + " stopped")
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.time() + 10
    while time.time() < deadline and read_pid(name):
        time.sleep(.2)
    if read_pid(name):
        os.killpg(pid, signal.SIGKILL)
        time.sleep(.5)
    if read_pid(name):
        raise RuntimeError(name + " did not stop")
    try:
        os.remove(pid_path(name))
    except OSError:
        pass
    print(name + " stopped")


def status():
    for name in SERVICES:
        pid = read_pid(name)
        print("%s state=%s pid=%s healthy=%s" %
              (name, "running" if pid else "stopped", pid or "-", healthy(name)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "stop", "status", "rotate-logs"))
    parser.add_argument("service", nargs="?", choices=("all",) + tuple(SERVICES), default="all")
    args = parser.parse_args()
    os.makedirs(RUN, mode=0o700, exist_ok=True)
    os.makedirs(LOG, mode=0o750, exist_ok=True)
    if args.action == "rotate-logs":
        run_log_rotator()
        return
    names = list(SERVICES) if args.service == "all" else [args.service]
    if args.action == "status":
        status()
        return
    if args.action == "start":
        for name in names:
            start_one(name)
    else:
        for name in reversed(names):
            stop_one(name)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("error: " + str(exc), file=sys.stderr)
        sys.exit(1)
