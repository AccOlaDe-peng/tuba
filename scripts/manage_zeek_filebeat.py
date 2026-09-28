#!/usr/bin/env python3
"""Manually control four isolated Filebeat processes reading active/recent Zeek logs."""

import argparse
import gzip
import json
import os
import glob
import shutil
import signal
import subprocess
import sys
import time


ROOT = "/opt/tuba/collector-live/filebeat-r2"
RUN = os.path.join(ROOT, "run")
CONFIGS = os.path.join(ROOT, "config")
LOGS = os.path.join(ROOT, "logs")
DATA = os.path.join(ROOT, "data")
ARCHIVE_DATA = os.path.join(ROOT, "archive")
ARCHIVE_SOURCE = "/opt/zeek/logs"
SECRETS = os.path.join(ROOT, "secrets.json")
STATE = os.path.join(RUN, "processes.json")
FILEBEAT = "/usr/share/filebeat/bin/filebeat"
FILEBEAT_HOME = "/usr/share/filebeat"
FILEBEAT_CONFIG = "/etc/filebeat"
BROKER = "10.6.68.248:29292"
CONTEXTS = {
	"conn": "ctx_9173765dafede7b01176206fc49f70d9",
	"dns": "ctx_df595c138c0ecac68cf9e8af95b7b881",
	"http": "ctx_5abc8a06f8a0088878248d4504d22a62",
	"ssl": "ctx_5fadf1a689e7900ff74af27b5674c1c5",
}


def process_matches(pid, dataset):
    try:
        if open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].strip().split()[0] == "Z":
            return False
        args = open("/proc/%d/cmdline" % pid, "rb").read().split(bytes([0]))
        if dataset == "archive-sync":
            return any(argument.endswith(b"manage_zeek_filebeat.py") for argument in args) and b"sync-archives" in args
        executable = os.path.basename(args[0].decode(errors="replace")) if args and args[0] else ""
        config = (CONFIGS + "/" + dataset + ".yml").encode()
        return executable == "filebeat" and config in args
    except (OSError, ValueError):
        return False


def load_state():
    try:
        return json.load(open(STATE, "r"))
    except IOError:
        return {}


def save_state(state):
    tmp = STATE + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, STATE)


def yaml_config(dataset, context, username, password):
    active = "/opt/zeek/spool/zeek/%s.log" % dataset
    archives = os.path.join(ARCHIVE_DATA, dataset, "*.log")
    return """filebeat.inputs:
  - type: filestream
    id: tuba-zeek-%s
    enabled: true
    paths:
      - "%s"
      - "%s"
    ignore_older: 90m
    prospector.scanner:
      check_interval: 10s
      symlinks: true
    fields_under_root: true
    fields:
      event.dataset: zeek.%s
    processors:
      - copy_fields:
          fields:
            - from: message
              to: event.original
          fail_on_error: false
          ignore_missing: true
      - decode_json_fields:
          fields: [message]
          target: ""
          overwrite_keys: true
          add_error_key: true
      - timestamp:
          field: ts
          layouts: [UNIX]
          target_field: "@timestamp"
          ignore_failure: true

queue.disk:
  path: ${path.data}/diskqueue
  max_size: 256MB
  segment_size: 10MB
  read_ahead: 512

filebeat.registry.flush: 1s
setup.template.enabled: false
setup.ilm.enabled: false

output.kafka:
  hosts: ["%s"]
  topic: "tuba.source.%s.v1"
  username: "%s"
  password: "%s"
  sasl.mechanism: SCRAM-SHA-512
  security.protocol: SASL_PLAINTEXT
  required_acks: -1
  compression: gzip
  max_message_bytes: 2097152
  bulk_max_size: 128
""" % (dataset, active, archives, dataset, BROKER, context, username, password)


def prepare_instance(dataset, context, user):
    config_path = os.path.join(CONFIGS, dataset + ".yml")
    with open(config_path, "w") as handle:
        handle.write(yaml_config(dataset, context, user["username"], user["password"]))
    os.chmod(config_path, 0o600)
    data_path = os.path.join(DATA, dataset)
    log_path = os.path.join(LOGS, dataset)
    os.makedirs(data_path, mode=0o750, exist_ok=True)
    os.makedirs(log_path, mode=0o750, exist_ok=True)
    return config_path, data_path, log_path


def sync_archives_once():
    now = time.time()
    for dataset in CONTEXTS:
        destination_dir = os.path.join(ARCHIVE_DATA, dataset)
        os.makedirs(destination_dir, mode=0o750, exist_ok=True)
        pattern = os.path.join(ARCHIVE_SOURCE, "*", dataset + ".*.log.gz")
        for source in glob.glob(pattern):
            try:
                details = os.stat(source)
                if now - details.st_mtime > 90 * 60:
                    continue
                archive_name = os.path.basename(source)[:-3]
                destination = os.path.join(destination_dir, archive_name)
                marker = destination + ".source.json"
                identity = {"path": source, "size": details.st_size, "mtime_ns": details.st_mtime_ns}
                if os.path.exists(destination) and os.path.exists(marker):
                    if json.load(open(marker, "r")) == identity:
                        continue
                temporary = destination + ".partial"
                try:
                    with gzip.open(source, "rb") as compressed, open(temporary, "wb") as expanded:
                        shutil.copyfileobj(compressed, expanded, 1024 * 1024)
                    os.replace(temporary, destination)
                    with open(marker + ".tmp", "w") as handle:
                        json.dump(identity, handle, sort_keys=True)
                        handle.write("\n")
                    os.replace(marker + ".tmp", marker)
                    os.chmod(destination, 0o640)
                    os.chmod(marker, 0o640)
                    print("decompressed %s" % source, flush=True)
                except Exception:
                    try:
                        os.remove(temporary)
                    except OSError:
                        pass
                    raise
            except Exception as error:
                print("archive pending %s: %s" % (source, error), flush=True)


def sync_archives_loop():
    while True:
        sync_archives_once()
        time.sleep(30)


def validate_instance(dataset, config_path, data_path, log_path):
    check = subprocess.run([FILEBEAT, "test", "config", "-c", config_path,
                            "--path.home", FILEBEAT_HOME, "--path.config", FILEBEAT_CONFIG,
                            "--path.data", data_path, "--path.logs", log_path],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True)
    if check.returncode:
        raise RuntimeError("Filebeat config validation failed for " + dataset + "; inspect its isolated config")
    check = subprocess.run([FILEBEAT, "test", "output", "-c", config_path,
                            "--path.home", FILEBEAT_HOME, "--path.config", FILEBEAT_CONFIG,
                            "--path.data", data_path, "--path.logs", log_path],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True, timeout=30)
    if check.returncode:
        raise RuntimeError("Filebeat Kafka output validation failed for " + dataset + "; inspect its isolated config")


def test():
    for directory in (RUN, CONFIGS, LOGS, DATA):
        os.makedirs(directory, mode=0o750, exist_ok=True)
    secrets_data = json.load(open(SECRETS, "r"))
    for dataset, context in CONTEXTS.items():
        config_path, data_path, log_path = prepare_instance(dataset, context, secrets_data[dataset])
        validate_instance(dataset, config_path, data_path, log_path)
        print(dataset + ": Filebeat config valid; Kafka authentication/Topic ACL confirmed")


def stop_processes(state):
    remaining = []
    for dataset, details in reversed(list(state.items())):
        pid = int(details["pid"])
        if not process_matches(pid, dataset):
            continue
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        deadline = time.time() + 10
        while time.time() < deadline and process_matches(pid, dataset):
            time.sleep(.2)
        if process_matches(pid, dataset):
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            deadline = time.time() + 5
            while time.time() < deadline and process_matches(pid, dataset):
                time.sleep(.2)
            if process_matches(pid, dataset):
                remaining.append(dataset)
    if remaining:
        raise RuntimeError("isolated Filebeat processes did not stop: " + ", ".join(remaining))
    try:
        os.remove(STATE)
    except IOError:
        pass


def start():
    for directory in (RUN, CONFIGS, LOGS, DATA, ARCHIVE_DATA):
        os.makedirs(directory, mode=0o750, exist_ok=True)
    state = load_state()
    if any(process_matches(int(details["pid"]), dataset) for dataset, details in state.items()):
        raise RuntimeError("one or more TUBA Zeek Filebeat instances are already running")
    if state:
        save_state({})
    if not os.path.isfile(FILEBEAT):
        raise RuntimeError("Filebeat binary was not found at " + FILEBEAT)
    secrets_data = json.load(open(SECRETS, "r"))
    running = {}
    try:
        archive_log = open(os.path.join(LOGS, "archive-sync.log"), "ab", buffering=0)
        archive = subprocess.Popen([sys.executable, os.path.abspath(__file__), "sync-archives"],
                                   cwd=ROOT, env=os.environ.copy(), stdin=subprocess.DEVNULL,
                                   stdout=archive_log, stderr=subprocess.STDOUT, start_new_session=True)
        archive_log.close()
        running["archive-sync"] = {"pid": archive.pid}
        save_state(running)
        for dataset, context in CONTEXTS.items():
            config_path, data_path, log_path = prepare_instance(dataset, context, secrets_data[dataset])
            validate_instance(dataset, config_path, data_path, log_path)
            log = open(os.path.join(LOGS, dataset + ".log"), "ab", buffering=0)
            process = subprocess.Popen([FILEBEAT, "--path.home", FILEBEAT_HOME,
                                         "--path.config", FILEBEAT_CONFIG, "-e", "-c", config_path,
                                         "--path.data", data_path, "--path.logs", log_path],
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
            log.close()
            running[dataset] = {"pid": process.pid, "context": context}
            save_state(running)
            time.sleep(.8)
            if process.poll() is not None:
                raise RuntimeError("Filebeat exited during startup for " + dataset + "; inspect its local log")
        print("started four TUBA Filebeat processes with independent config, registry, queue and Kafka ACL")
        for dataset, details in running.items():
            print(dataset + " pid=" + str(details["pid"]))
    except Exception:
        stop_processes(running)
        raise


def status():
    state = load_state()
    if not state:
        print("stopped")
        return
    for dataset, details in state.items():
        pid = int(details["pid"])
        print(dataset + " pid=" + str(pid) + " " + ("running" if process_matches(pid, dataset) else "stopped/stale"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("test", "start", "stop", "status", "sync-archives"))
    args = parser.parse_args()
    try:
        if args.action == "test":
            test()
        elif args.action == "sync-archives":
            sync_archives_loop()
        elif args.action == "start":
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
