#!/usr/bin/env python3
"""Exercise an isolated Filebeat disk queue across a Kafka outage and hard kill."""

import argparse
import hashlib
import json
import os
import signal
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


FILEBEAT_DEFAULT = "/usr/share/filebeat/bin/filebeat"
KAFKA_IMAGE = "apache/kafka:4.3.1"
KAFKA_BIN = "/opt/kafka/bin"


def unused_local_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def start_filebeat(binary, config, root):
    os.makedirs(os.path.join(root, "data"), mode=0o700, exist_ok=True)
    os.makedirs(os.path.join(root, "logs"), mode=0o700, exist_ok=True)
    log = open(os.path.join(root, "filebeat.log"), "ab", buffering=0)
    process = subprocess.Popen(
        [binary, "-e", "-c", config,
         "--path.home", os.path.dirname(os.path.dirname(binary)),
         "--path.config", root,
         "--path.data", os.path.join(root, "data"),
         "--path.logs", os.path.join(root, "logs")],
        cwd=root, env=os.environ.copy(), stdin=subprocess.DEVNULL,
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    log.close()
    return process


def stop_filebeat(process, hard=False):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGKILL if hard else signal.SIGTERM)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def write_config(path, root, bootstrap, monitor_port):
    input_path = os.path.join(root, "input", "events.jsonl")
    data_path = os.path.join(root, "data", "diskqueue")
    config = """http.enabled: true
http.host: 127.0.0.1
http.port: {monitor_port}

filebeat.inputs:
  - type: filestream
    id: tuba-reliability-diskqueue
    enabled: true
    paths:
      - {input_path}
    prospector.scanner.check_interval: 1s

queue.disk:
  path: {data_path}
  max_size: 16MB
  segment_size: 1MB

filebeat.registry.flush: 1s
setup.template.enabled: false
setup.ilm.enabled: false
logging.level: error

output.kafka:
  hosts: [{bootstrap}]
  topic: tuba.reliability.diskqueue.v1
  required_acks: -1
  compression: none
  bulk_max_size: 128
""".format(
        monitor_port=monitor_port,
        input_path=json.dumps(input_path),
        data_path=json.dumps(data_path),
        bootstrap=json.dumps(bootstrap))
    with open(path, "w") as handle:
        handle.write(config)
    os.chmod(path, 0o600)


def docker(*args, check=True, capture=False, timeout=30):
    return subprocess.run(["docker"] + list(args), check=check,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None,
                          universal_newlines=True, timeout=timeout)


def start_test_kafka():
    port = unused_local_port()
    name = "tuba-filebeat-reliability-%d" % os.getpid()
    args = ["run", "--detach", "--name", name,
            "--publish", "127.0.0.1:%d:9092" % port,
            "--env", "KAFKA_NODE_ID=1",
            "--env", "KAFKA_PROCESS_ROLES=broker,controller",
            "--env", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "--env", "KAFKA_LISTENERS=PLAINTEXT_HOST://:9092,PLAINTEXT_INTERNAL://:29092,CONTROLLER://:9093",
            "--env", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://127.0.0.1:%d,PLAINTEXT_INTERNAL://localhost:29092" % port,
            "--env", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT_HOST:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "--env", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "--env", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL",
            "--env", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "--env", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
            KAFKA_IMAGE]
    try:
        result = docker(*args, capture=True, timeout=180)
    except Exception:
        docker("rm", "--force", name, check=False)
        raise
    container_id = result.stdout.strip()
    deadline = time.time() + 120
    last_error = "broker is starting"
    while time.time() < deadline:
        check = docker("exec", name, KAFKA_BIN + "/kafka-topics.sh",
                       "--bootstrap-server", "localhost:29092", "--list",
                       check=False, capture=True)
        if check.returncode == 0:
            docker("exec", name, KAFKA_BIN + "/kafka-topics.sh",
                   "--bootstrap-server", "localhost:29092", "--create",
                   "--topic", "tuba.reliability.diskqueue.v1",
                   "--partitions", "1", "--replication-factor", "1")
            return name, container_id, port
        last_error = (check.stderr or check.stdout or last_error).strip()
        time.sleep(1)
    docker("rm", "--force", name, check=False)
    raise RuntimeError("isolated Kafka did not become ready: " + last_error)


def wait_test_kafka(name, timeout=120):
    deadline = time.time() + timeout
    last_error = "broker is starting"
    while time.time() < deadline:
        check = docker("exec", name, KAFKA_BIN + "/kafka-topics.sh",
                       "--bootstrap-server", "localhost:29092", "--list",
                       check=False, capture=True)
        if check.returncode == 0:
            return
        last_error = (check.stderr or check.stdout or last_error).strip()
        time.sleep(1)
    raise RuntimeError("isolated Kafka did not become ready: " + last_error)


def topic_end_offset(name):
    result = docker("exec", name, KAFKA_BIN + "/kafka-get-offsets.sh",
                    "--bootstrap-server", "localhost:29092",
                    "--topic", "tuba.reliability.diskqueue.v1", "--time", "-1",
                    check=False, capture=True)
    if result.returncode != 0:
        return 0
    total = 0
    for line in (result.stdout or "").splitlines():
        try:
            total += int(line.rsplit(":", 1)[1])
        except (IndexError, ValueError):
            continue
    return total


def verify_kafka_records(name, events):
    result = docker("exec", name, KAFKA_BIN + "/kafka-console-consumer.sh",
                    "--bootstrap-server", "localhost:29092",
                    "--topic", "tuba.reliability.diskqueue.v1",
                    "--from-beginning", "--timeout-ms", "10000",
                    check=False, capture=True)
    seen = {}
    first_record = {}
    duplicate_sources = []
    malformed = 0
    for line in (result.stdout or "").splitlines():
        try:
            event = json.loads(line)
            message = event.get("message")
            if isinstance(message, str):
                message = json.loads(message)
            event_id = message.get("test_id") if isinstance(message, dict) else None
            if isinstance(event_id, str) and event_id.startswith("rel-"):
                log = event.get("log") if isinstance(event, dict) else None
                source = log.get("file", {}) if isinstance(log, dict) else {}
                agent = event.get("agent", {}) if isinstance(event, dict) else {}
                message_bytes = message.encode("utf-8") if isinstance(message, str) else json.dumps(message, sort_keys=True).encode("utf-8")
                record = {"id": event_id, "timestamp": event.get("@timestamp"),
                          "agent_id": agent.get("id") if isinstance(agent, dict) else None,
                          "agent_ephemeral_id": agent.get("ephemeral_id") if isinstance(agent, dict) else None,
                          "message_sha256": hashlib.sha256(message_bytes).hexdigest(),
                          "path": source.get("path"),
                          "inode": source.get("inode"), "device_id": source.get("device_id"),
                          "fingerprint": source.get("fingerprint"), "offset": log.get("offset") if isinstance(log, dict) else None}
                if event_id in seen and len(duplicate_sources) < 10:
                    duplicate_sources.append({"first": first_record[event_id], "duplicate": record})
                else:
                    first_record[event_id] = record
                seen[event_id] = seen.get(event_id, 0) + 1
            else:
                malformed += 1
        except (TypeError, ValueError):
            malformed += 1
    expected = {"rel-%08d" % index for index in range(events)}
    missing = sorted(expected - set(seen))
    duplicates = sum(count - 1 for count in seen.values() if count > 1)
    if missing:
        raise RuntimeError("Kafka is missing %d/%d synthetic source records; duplicate records=%d; inspect isolated test logs" %
                           (len(missing), events, duplicates))
    if malformed:
        raise RuntimeError("Kafka contains %d malformed/unrecognized records; inspect isolated test logs" % malformed)
    return len(seen), duplicates, malformed, duplicate_sources


def output_acked(monitor_port):
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/stats" % monitor_port, timeout=2) as response:
            stats = json.load(response)
        return int(stats.get("libbeat", {}).get("output", {}).get("events", {}).get("acked", 0))
    except (OSError, ValueError, urllib.error.URLError):
        return 0


def wait_for(predicate, timeout, description):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise RuntimeError("timed out waiting for " + description)


def diskqueue_bytes(path):
    total = 0
    for current, _, files in os.walk(path):
        for name in files:
            if name.endswith(".seg"):
                try:
                    total += os.path.getsize(os.path.join(current, name))
                except OSError:
                    pass
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="", help="isolated test Kafka address")
    parser.add_argument("--filebeat", default=FILEBEAT_DEFAULT)
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--wait-queue-full", action="store_true",
                        help="wait until the isolated 16 MB disk queue reaches 75%% before killing Filebeat")
    parser.add_argument("--restart-kafka-during-outage", action="store_true",
                        help="stop the disposable Kafka broker after initial delivery, fill queue, hard-kill Filebeat, then restart both")
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--start-disposable-kafka", action="store_true",
                        help="start and remove a loopback-only Apache Kafka 4.3.1 test container")
    args = parser.parse_args()
    if args.events < 100 or args.payload_bytes < 128 or args.timeout < 10:
        parser.error("events>=100, payload-bytes>=128, and timeout>=10 are required")
    if not args.bootstrap and not args.start_disposable_kafka:
        parser.error("provide --bootstrap or use --start-disposable-kafka")
    if args.restart_kafka_during_outage and not args.start_disposable_kafka:
        parser.error("--restart-kafka-during-outage requires --start-disposable-kafka")
    if not os.path.isfile(args.filebeat) or not os.access(args.filebeat, os.X_OK):
        parser.error("Filebeat executable is unavailable: " + args.filebeat)

    root = tempfile.mkdtemp(prefix="tuba-filebeat-diskqueue-")
    process = None
    container = None
    try:
        bootstrap = args.bootstrap
        if args.start_disposable_kafka:
            container, _, port = start_test_kafka()
            bootstrap = "127.0.0.1:%d" % port

        input_dir = os.path.join(root, "input")
        os.makedirs(input_dir, mode=0o700)
        input_path = os.path.join(input_dir, "events.jsonl")
        padding = "x" * args.payload_bytes
        with open(input_path, "w") as handle:
            for index in range(args.events):
                handle.write(json.dumps({"test_id": "rel-%08d" % index, "padding": padding}, separators=(",", ":")) + "\n")

        monitor_port = unused_local_port()
        config = os.path.join(root, "filebeat.yml")
        offline_port = unused_local_port()
        initial_bootstrap = bootstrap if args.restart_kafka_during_outage else "127.0.0.1:%d" % offline_port
        write_config(config, root, initial_bootstrap, monitor_port)
        process = start_filebeat(args.filebeat, config, root)
        wait_for(lambda: process.poll() is not None or os.path.exists(os.path.join(root, "data", "diskqueue")), 15, "offline Filebeat initialization")
        if process.poll() is not None:
            raise RuntimeError("Filebeat exited before entering offline buffering; inspect test filebeat.log")
        if args.restart_kafka_during_outage:
            initial_ack_target = 100
            wait_for(lambda: process.poll() is not None or output_acked(monitor_port) >= initial_ack_target,
                     args.timeout, "initial events to reach Kafka before broker outage")
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited before broker outage; inspect test filebeat.log")
            docker("stop", container)
            target_bytes = 12 * 1024 * 1024
            wait_for(lambda: process.poll() is not None or
                     diskqueue_bytes(os.path.join(root, "data", "diskqueue")) >= target_bytes,
                     args.timeout, "disk queue to fill during real Kafka outage")
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited during broker outage; inspect test filebeat.log")
            pending_bytes = diskqueue_bytes(os.path.join(root, "data", "diskqueue"))
            stop_filebeat(process, hard=True)
            process = None
            docker("start", container)
            wait_test_kafka(container)
            write_config(config, root, bootstrap, monitor_port)
            process = start_filebeat(args.filebeat, config, root)
            wait_for(lambda: process.poll() is not None or topic_end_offset(container) >= args.events,
                     args.timeout, "all source events to reach Kafka after broker and Filebeat recovery")
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited during broker recovery; inspect test filebeat.log")
            recovered_acked = output_acked(monitor_port)
        elif args.wait_queue_full:
            target_bytes = 12 * 1024 * 1024
            wait_for(lambda: process.poll() is not None or
                     diskqueue_bytes(os.path.join(root, "data", "diskqueue")) >= target_bytes,
                     args.timeout, "isolated disk queue to reach 12 MiB under offline backpressure")
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited before the disk queue reached its fill target; inspect test filebeat.log")
        else:
            wait_for(lambda: process.poll() is not None or
                     diskqueue_bytes(os.path.join(root, "data", "diskqueue")) > 0,
                     args.timeout, "offline events to persist in disk queue")
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited before entering offline buffering; inspect test filebeat.log")
        if not args.restart_kafka_during_outage:
            pending_bytes = diskqueue_bytes(os.path.join(root, "data", "diskqueue"))
            stop_filebeat(process, hard=True)
            process = None

            write_config(config, root, bootstrap, monitor_port)
            process = start_filebeat(args.filebeat, config, root)
            wait_for(lambda: process.poll() is not None or output_acked(monitor_port) >= args.events,
                     args.timeout, "all synthetic events to be acknowledged by the isolated Kafka output")
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited during recovery; inspect test filebeat.log")
            recovered_acked = output_acked(monitor_port)
        stop_filebeat(process, hard=False)
        process = None
        delivered, duplicates, malformed, duplicate_sources = verify_kafka_records(container, args.events) if container else (0, 0, 0, [])
        status = "PASS_AT_LEAST_ONCE_DUPLICATES_OBSERVED" if duplicates else "PASS_NO_DUPLICATES_OBSERVED"
        print("%s filebeat=%s events=%d outage_queue_bytes=%d recovery_output_acked=%d kafka_unique=%d duplicates=%d malformed=%d duplicate_sources=%s" %
              (status, args.filebeat, args.events, pending_bytes, recovered_acked, delivered, duplicates, malformed, json.dumps(duplicate_sources, separators=(",", ":"))))
        if not args.keep_artifacts:
            shutil.rmtree(root)
            print("test artifacts removed")
        else:
            print("ARTIFACTS " + root)
    except Exception:
        print("FAIL artifacts retained at " + root)
        raise
    finally:
        if process is not None:
            stop_filebeat(process, hard=True)
        if container is not None:
            docker("rm", "--force", container, check=False)


if __name__ == "__main__":
    main()
