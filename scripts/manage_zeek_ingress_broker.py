#!/usr/bin/env python3
"""Manually manage a separate, ACL-protected single-node KRaft broker for Zeek."""

import argparse
import hashlib
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import time


ROOT = os.environ.get("TUBA_KAFKA_ROOT", "/opt/tuba/collector-live/kafka")
KAFKA = os.environ.get("TUBA_KAFKA_BIN", "/opt/adms/kafka/bin")
JAVA_HOME = os.environ.get("JAVA_HOME", "/opt/adms/adms-jdk")
HOST = os.environ.get("TUBA_KAFKA_BIND_HOST", "10.6.68.248")
PORT = int(os.environ.get("TUBA_KAFKA_PORT", "29292"))
ADMIN = "tuba-kafka-admin"
WORKER = "tuba-zeek-worker"
SERVICE_USERS = {
    "source-adapter": "tuba-zeek-source-adapter",
    "ingest": "tuba-zeek-ingest",
    "raw-indexer": "tuba-zeek-raw-indexer",
    "normalizer": "tuba-zeek-normalizer",
    "quarantine-indexer": "tuba-zeek-quarantine-indexer",
    "standard-indexer": "tuba-zeek-standard-indexer",
}
NAMESPACE = os.environ.get("TUBA_KAFKA_NAMESPACE", "zeek_validation_20260927_001")
CONTEXTS = {
	"conn": "ctx_9173765dafede7b01176206fc49f70d9",
	"dns": "ctx_df595c138c0ecac68cf9e8af95b7b881",
	"http": "ctx_5abc8a06f8a0088878248d4504d22a62",
	"ssl": "ctx_5fadf1a689e7900ff74af27b5674c1c5",
}
HISTORIC_CONTEXTS = (
	"ctx_50000000000000000000000000000005",
	"ctx_60000000000000000000000000000006",
	"ctx_70000000000000000000000000000007",
	"ctx_80000000000000000000000000000008",
)
SOURCE_GROUP_SUFFIX = "zeeklive20260927r2"
PIPELINE_GROUP_SUFFIX = "zeeklive20260927r2"
QUARANTINE_GROUP_SUFFIX = "zeeklive20260927"
DOMAINS = ("authentication", "session", "iam", "directory", "network", "dns", "web", "tls")
SECRETS = os.path.join(ROOT, "secrets.json")
CONFIG = os.path.join(ROOT, "server.properties")
CLIENT_CONFIG = os.path.join(ROOT, "admin.properties")
PID_FILE = os.path.join(ROOT, "run", "broker.pid")
LOG_FILE = os.path.join(ROOT, "logs", "broker.log")
DATA = os.path.join(ROOT, "data")
PIPELINE_STATE = os.environ.get("TUBA_KAFKA_PIPELINE_STATE", "/opt/tuba/collector-live/pipeline/run/processes.json")
PIPELINE_BIN = os.environ.get("TUBA_KAFKA_PIPELINE_BIN", "/opt/tuba/collector-live/pipeline/bin")
PROCESS_ROLES = {
    "tuba-ingest": "ingest",
    "tuba-raw-indexer": "raw-indexer",
    "tuba-normalizer": "normalizer",
    "tuba-quarantine-indexer": "quarantine-indexer",
    "tuba-standard-indexer": "standard-indexer",
    "tuba-source-adapter": "source-adapter",
}


def write_private(path, content):
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        handle.write(content)
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def load_or_create_secrets():
    if os.path.exists(SECRETS):
        with open(SECRETS, encoding="utf-8") as handle:
            values = json.load(handle)
        services = values.setdefault("services", {})
        changed = False
        for role, username in SERVICE_USERS.items():
            if role not in services:
                services[role] = {"username": username, "password": secrets.token_urlsafe(36)}
                changed = True
        if changed:
            write_private(SECRETS, json.dumps(values, indent=2) + "\n")
        return values
    values = {
        "admin": {"username": ADMIN, "password": secrets.token_urlsafe(36)},
        "worker": {"username": WORKER, "password": secrets.token_urlsafe(36)},
        "services": {
            role: {"username": username, "password": secrets.token_urlsafe(36)}
            for role, username in SERVICE_USERS.items()
        },
        "filebeat": {
            name: {"username": "tuba-zeek-" + name, "password": secrets.token_urlsafe(36)}
            for name in CONTEXTS
        },
    }
    write_private(SECRETS, json.dumps(values, indent=2) + "\n")
    return values


def server_config(admin_password):
    jaas = ('org.apache.kafka.common.security.scram.ScramLoginModule required '
            'username="%s" password="%s";') % (ADMIN, admin_password)
    return "\n".join([
        "process.roles=broker,controller",
        "node.id=2",
        "controller.quorum.bootstrap.servers=127.0.0.1:29293",
        "listeners=SASL_PLAINTEXT://%s:%d,CONTROLLER://127.0.0.1:29293" % (HOST, PORT),
        "advertised.listeners=SASL_PLAINTEXT://%s:%d" % (HOST, PORT),
        "listener.security.protocol.map=SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
        "inter.broker.listener.name=SASL_PLAINTEXT",
        "controller.listener.names=CONTROLLER",
        "log.dirs=" + DATA,
        "metadata.log.dir=" + DATA,
        "num.partitions=1",
        "default.replication.factor=1",
        "min.insync.replicas=1",
        "offsets.topic.replication.factor=1",
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "auto.create.topics.enable=false",
        "log.retention.hours=24",
        "log.segment.bytes=536870912",
        "message.max.bytes=2097152",
        "replica.fetch.max.bytes=4194304",
        "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "allow.everyone.if.no.acl.found=false",
        # The controller listener is bound to loopback only and uses PLAINTEXT;
        # KRaft's local broker/controller channel therefore authenticates as ANONYMOUS.
        "super.users=User:%s;User:ANONYMOUS" % ADMIN,
        "sasl.enabled.mechanisms=SCRAM-SHA-512",
        "sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512",
        "listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=" + jaas,
        "group.initial.rebalance.delay.ms=0",
        "",
    ])


def client_config(username, password):
    jaas = ('org.apache.kafka.common.security.scram.ScramLoginModule required '
            'username="%s" password="%s";') % (username, password)
    return "\n".join([
        "security.protocol=SASL_PLAINTEXT",
        "sasl.mechanism=SCRAM-SHA-512",
        "sasl.jaas.config=" + jaas,
        "",
    ])


def env():
    result = dict(os.environ)
    result["JAVA_HOME"] = JAVA_HOME
    result["PATH"] = JAVA_HOME + "/bin:" + result.get("PATH", "/usr/bin:/bin")
    return result


def cli(name, *args, check=True):
    try:
        return subprocess.run([os.path.join(KAFKA, name + ".sh"), *args], env=env(),
                              check=check, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              universal_newlines=True, timeout=90)
    except subprocess.TimeoutExpired:
        raise RuntimeError("Kafka administration command timed out")


def is_broker(pid):
    try:
        args = open("/proc/%d/cmdline" % pid, "rb").read()
        return b"kafka.Kafka" in args and CONFIG.encode() in args
    except OSError:
        return False


def broker_pid():
    try:
        pid = int(open(PID_FILE, encoding="ascii").read().strip())
        return pid if is_broker(pid) else None
    except (OSError, ValueError):
        return None


def wait_ready(process, log_offset, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Kafka broker exited during startup; inspect " + LOG_FILE)
        try:
            with socket.create_connection((HOST, PORT), timeout=1):
                with open(LOG_FILE, "rb") as log:
                    log.seek(log_offset)
                    if b"Kafka Server started" in log.read():
                        return
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError("Kafka broker listener did not become ready; inspect " + LOG_FILE)


def ensure_user(username, password, admin_config):
    result = cli("kafka-configs", "--bootstrap-server", "%s:%d" % (HOST, PORT),
                 "--command-config", admin_config, "--alter", "--entity-type", "users",
                 "--entity-name", username, "--add-config",
                 "SCRAM-SHA-512=[password=%s]" % password, check=False)
    if result.returncode:
        raise RuntimeError("Kafka user configuration failed for %s (return code %d)" % (username, result.returncode))


def acl(admin_config, principal, operations, resource_type, resource, pattern="literal"):
    args = ["--bootstrap-server", "%s:%d" % (HOST, PORT), "--command-config", admin_config,
            "--add", "--allow-principal", "User:" + principal]
    for operation in operations:
        args += ["--operation", operation]
    args += ["--%s" % resource_type, resource, "--resource-pattern-type", pattern]
    result = cli("kafka-acls", *args, check=False)
    if result.returncode:
        raise RuntimeError("could not create Kafka ACL for %s: %s" % (principal, result.stdout[-1200:]))


def remove_acl(admin_config, principal, operation, resource_type, resource, pattern):
    args = ["--bootstrap-server", "%s:%d" % (HOST, PORT), "--command-config", admin_config,
            "--remove", "--allow-principal", "User:" + principal,
            "--operation", operation, "--%s" % resource_type, resource,
            "--resource-pattern-type", pattern, "--force"]
    result = cli("kafka-acls", *args, check=False)
    if result.returncode:
        raise RuntimeError("could not remove legacy Kafka ACL for %s (return code %d)" % (principal, result.returncode))


def source_group_id(topic, suffix=SOURCE_GROUP_SUFFIX):
    value = "tuba-source-adapter-" + hashlib.sha256(topic.encode("utf-8")).hexdigest()[:16]
    return value + ("-" + suffix if suffix else "")


def worker_group_ids():
    groups = [source_group_id("tuba.source.%s.v1" % context) for context in CONTEXTS.values()]
    groups += ["tuba-raw-indexer-%s-%s" % (NAMESPACE, PIPELINE_GROUP_SUFFIX),
               "tuba-normalizer-%s-%s" % (NAMESPACE, PIPELINE_GROUP_SUFFIX),
               "tuba-quarantine-indexer-%s-%s" % (NAMESPACE, QUARANTINE_GROUP_SUFFIX)]
    groups += ["tuba-standard-indexer-%s-%s-%s" % (domain, NAMESPACE, PIPELINE_GROUP_SUFFIX)
               for domain in DOMAINS]
    return groups


def service_group_ids():
    return {
        "source-adapter": [source_group_id("tuba.source.%s.v1" % context) for context in CONTEXTS.values()],
        "raw-indexer": ["tuba-raw-indexer-%s-%s" % (NAMESPACE, PIPELINE_GROUP_SUFFIX)],
        "normalizer": ["tuba-normalizer-%s-%s" % (NAMESPACE, PIPELINE_GROUP_SUFFIX)],
        "quarantine-indexer": ["tuba-quarantine-indexer-%s-%s" % (NAMESPACE, QUARANTINE_GROUP_SUFFIX)],
        "standard-indexer": ["tuba-standard-indexer-%s-%s-%s" % (domain, NAMESPACE, PIPELINE_GROUP_SUFFIX)
                             for domain in DOMAINS],
    }


def grant_topic(admin_config, principal, topic, operations):
    acl(admin_config, principal, operations, "topic", topic)


def prepare_service_acls(admin_config, secrets_data):
    """Add exact per-component ACLs without retiring the live shared identity."""
    for role, user in secrets_data["services"].items():
        ensure_user(user["username"], user["password"], admin_config)

    raw = "tuba.collector.%s.raw.live2.v1" % NAMESPACE
    quarantine = "tuba.collector.%s.quarantine.v1" % NAMESPACE
    indexing_dlq = "tuba.collector.%s.dlq.v1" % NAMESPACE
    adapter_dlq = "tuba.collector.%s.source-adapter.dlq.v1" % NAMESPACE
    events = ["tuba.collector.%s.events.%s.v1" % (NAMESPACE, domain) for domain in DOMAINS]
    source_topics = ["tuba.source.%s.v1" % context for context in CONTEXTS.values()]

    permissions = {
        "ingest": {raw: ("Write", "Describe")},
        "source-adapter": {adapter_dlq: ("Write", "Describe")},
        "raw-indexer": {raw: ("Read", "Describe"), indexing_dlq: ("Write", "Describe")},
        "normalizer": {raw: ("Read", "Describe"), quarantine: ("Write", "Describe")},
        "quarantine-indexer": {quarantine: ("Read", "Describe"), indexing_dlq: ("Write", "Describe")},
        "standard-indexer": {indexing_dlq: ("Write", "Describe")},
    }
    for topic in source_topics:
        permissions["source-adapter"][topic] = ("Read", "Describe")
    for topic in events:
        permissions["normalizer"][topic] = ("Write", "Describe")
        permissions["standard-indexer"][topic] = ("Read", "Describe")

    for role, topic_permissions in permissions.items():
        principal = secrets_data["services"][role]["username"]
        for topic, operations in topic_permissions.items():
            grant_topic(admin_config, principal, topic, operations)
    for role, groups in service_group_ids().items():
        principal = secrets_data["services"][role]["username"]
        for group in groups:
            acl(admin_config, principal, ("Read",), "group", group)


def retire_legacy_worker_acls(admin_config, worker):
    """Remove broad ACLs while retaining literal read access for historical offsets."""
    for operation in ("Read", "Write", "Describe"):
        remove_acl(admin_config, worker, operation, "topic", "tuba.collector." + NAMESPACE + ".", "prefixed")
    for operation in ("Read", "Describe"):
        remove_acl(admin_config, worker, operation, "topic", "tuba.source.", "prefixed")
    remove_acl(admin_config, worker, "Read", "group", "tuba-", "prefixed")
    for context in CONTEXTS.values():
        remove_acl(admin_config, worker, "Read", "topic", "tuba.source.%s.v1" % context, "literal")
        remove_acl(admin_config, worker, "Describe", "topic", "tuba.source.%s.v1" % context, "literal")
    for group in worker_group_ids():
        remove_acl(admin_config, worker, "Read", "group", group, "literal")

    legacy_raw_topic = "tuba.collector.%s.raw.v1" % NAMESPACE
    grant_topic(admin_config, worker, legacy_raw_topic, ("Read", "Describe"))
    legacy_groups = [
        "tuba-raw-indexer-%s-zeeklive20260927" % NAMESPACE,
        "tuba-normalizer-%s-zeeklive20260927" % NAMESPACE,
    ]
    for context in HISTORIC_CONTEXTS:
        source_topic = "tuba.source.%s.v1" % context
        grant_topic(admin_config, worker, source_topic, ("Read", "Describe"))
        legacy_groups.append(source_group_id(source_topic, ""))
    for group in legacy_groups:
        acl(admin_config, worker, ("Read",), "group", group)


def retire_historical_source_writer_acls(admin_config, secrets_data):
    """Keep old source data/offsets but revoke obsolete Beat write credentials."""
    for dataset, context in zip(("conn", "dns", "http", "ssl"), HISTORIC_CONTEXTS):
        user = secrets_data["filebeat"][dataset]["username"]
        topic = "tuba.source.%s.v1" % context
        remove_acl(admin_config, user, "Write", "topic", topic, "literal")
        remove_acl(admin_config, user, "Describe", "topic", topic, "literal")


def reconcile_acls():
    if broker_pid() is None:
        raise RuntimeError("dedicated Kafka broker is not running")
    secrets_data = load_or_create_secrets()
    admin = secrets_data["admin"]
    write_private(CLIENT_CONFIG, client_config(admin["username"], admin["password"]))
    prepare_service_acls(CLIENT_CONFIG, secrets_data)
    print("exact per-service ACLs prepared; shared validation ACLs remain until clients are switched")


def retire_worker_acls():
    if broker_pid() is None:
        raise RuntimeError("dedicated Kafka broker is not running")
    secrets_data = load_or_create_secrets()
    admin = secrets_data["admin"]
    write_private(CLIENT_CONFIG, client_config(admin["username"], admin["password"]))
    assert_service_clients_active(secrets_data)
    retire_legacy_worker_acls(CLIENT_CONFIG, secrets_data["worker"]["username"])
    retire_historical_source_writer_acls(CLIENT_CONFIG, secrets_data)
    print("shared worker ACLs removed; service-specific identities remain")


def assert_service_clients_active(secrets_data):
    try:
        with open(PIPELINE_STATE, encoding="utf-8") as handle:
            state = json.load(handle)
    except (OSError, ValueError) as error:
        raise RuntimeError("refusing to retire shared ACLs: data-plane state unavailable") from error
    missing = []
    for process, role in PROCESS_ROLES.items():
        details = state.get(process)
        try:
            pid = int(details["pid"])
            cmdline = open("/proc/%d/cmdline" % pid, "rb").read().split(bytes([0]))
            environ = open("/proc/%d/environ" % pid, "rb").read().split(bytes([0]))
            values = dict(item.split(b"=", 1) for item in environ if b"=" in item)
            username = values.get(b"KAFKA_SASL_USERNAME", b"").decode()
        except (KeyError, OSError, ValueError, TypeError):
            missing.append(process)
            continue
        expected_binary = os.path.realpath(os.path.join(PIPELINE_BIN, process))
        actual_binary = os.path.realpath(cmdline[0].decode()) if cmdline and cmdline[0] else ""
        expected_user = secrets_data["services"][role]["username"]
        if actual_binary != expected_binary or username != expected_user:
            missing.append(process)
    if missing:
        raise RuntimeError("refusing to retire shared ACLs; service-role processes not confirmed: " + ", ".join(missing))


def ensure_topic(admin_config, topic):
    result = cli("kafka-topics", "--bootstrap-server", "%s:%d" % (HOST, PORT),
                 "--command-config", admin_config, "--create", "--if-not-exists",
                 "--topic", topic, "--partitions", "1", "--replication-factor", "1",
                 "--config", "retention.ms=86400000", "--config", "max.message.bytes=2097152",
                 check=False)
    if result.returncode:
        raise RuntimeError("could not create Kafka topic %s: %s" % (topic, result.stdout[-1200:]))


def start():
    os.makedirs(os.path.join(ROOT, "run"), mode=0o700, exist_ok=True)
    os.makedirs(os.path.join(ROOT, "logs"), mode=0o700, exist_ok=True)
    os.makedirs(DATA, mode=0o700, exist_ok=True)
    if broker_pid():
        raise RuntimeError("dedicated Zeek ingress broker is already running")
    secrets_data = load_or_create_secrets()
    admin = secrets_data["admin"]
    write_private(CONFIG, server_config(admin["password"]))
    if not os.path.exists(os.path.join(DATA, "meta.properties")):
        unexpected = [name for name in os.listdir(DATA) if name != ".keep"]
        if unexpected:
            raise RuntimeError("Kafka data directory is partially initialized; refusing to format")
        cluster_id = cli("kafka-storage", "random-uuid").stdout.strip().splitlines()[-1]
        formatted = cli("kafka-storage", "format", "--config", CONFIG, "--cluster-id", cluster_id,
                        "--standalone", "--add-scram",
                        "SCRAM-SHA-512=[name=%s,password=%s]" % (admin["username"], admin["password"]),
                        check=False)
        if formatted.returncode:
            raise RuntimeError("Kafka storage formatting failed (return code %d)" % formatted.returncode)
    log_offset = os.path.getsize(LOG_FILE) if os.path.exists(LOG_FILE) else 0
    log = open(LOG_FILE, "ab", buffering=0)
    process = subprocess.Popen([os.path.join(KAFKA, "kafka-server-start.sh"), CONFIG],
                               cwd=ROOT, env=env(), stdin=subprocess.DEVNULL,
                               stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    write_private(PID_FILE, str(process.pid) + "\n")
    wait_ready(process, log_offset)
    write_private(CLIENT_CONFIG, client_config(admin["username"], admin["password"]))
    for dataset, user in secrets_data["filebeat"].items():
        ensure_user(user["username"], user["password"], CLIENT_CONFIG)
    for user in secrets_data["services"].values():
        ensure_user(user["username"], user["password"], CLIENT_CONFIG)

    topics = [
        "tuba.collector.%s.raw.v1" % NAMESPACE,
        "tuba.collector.%s.quarantine.v1" % NAMESPACE,
        "tuba.collector.%s.dlq.v1" % NAMESPACE,
        "tuba.collector.%s.source-adapter.dlq.v1" % NAMESPACE,
    ]
    topics += ["tuba.collector.%s.events.%s.v1" % (NAMESPACE, domain) for domain in DOMAINS]
    topics += ["tuba.source.%s.v1" % context for context in CONTEXTS.values()]
    for topic in topics:
        ensure_topic(CLIENT_CONFIG, topic)

    for dataset, context in CONTEXTS.items():
        user = secrets_data["filebeat"][dataset]["username"]
        acl(CLIENT_CONFIG, user, ("Write", "Describe"), "topic", "tuba.source.%s.v1" % context)
    prepare_service_acls(CLIENT_CONFIG, secrets_data)
    print("dedicated Kafka ready; topics=%d; source ACLs=4; exact service groups=%d" % (len(topics), sum(len(v) for v in service_group_ids().values())))
    print("credentials are stored in the root-only secrets file; values were not printed")


def stop():
    pid = broker_pid()
    if pid is None:
        print("stopped")
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.time() + 30
    while time.time() < deadline and is_broker(pid):
        time.sleep(.25)
    if is_broker(pid):
        os.killpg(pid, signal.SIGKILL)
        deadline = time.time() + 5
        while time.time() < deadline and is_broker(pid):
            time.sleep(.25)
    if is_broker(pid):
        raise RuntimeError("Kafka broker did not stop")
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass
    print("stopped")


def status():
    pid = broker_pid()
    print("running pid=%d listener=%s:%d" % (pid, HOST, PORT) if pid else "stopped")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status", "reconcile-acls", "retire-worker-acls"))
    args = parser.parse_args()
    try:
        {"start": start, "stop": stop, "status": status, "reconcile-acls": reconcile_acls,
         "retire-worker-acls": retire_worker_acls}[args.action]()
    except Exception as error:
        print("error: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
