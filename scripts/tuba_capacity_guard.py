#!/usr/bin/env python3
"""Single-node ES retention, disk watermark and write-protection guard."""

import argparse
import datetime as dt
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread


INDEX = re.compile(r"^tuba-v1-(?:raw|quarantine|uim-(?:authentication|session|iam|directory|network|dns|web|tls))-.+-g[0-9]+-([0-9]{4}\.[0-9]{2}\.[0-9]{2})$")
STATE = {"disk_used_percent": 0.0, "level": "unknown", "delete_candidates": 0,
         "deleted_total": 0, "last_check": 0, "last_error": ""}
STOP = False


def request(url, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.loads(response.read().decode("utf-8") or "{}")


def apply_es_watermarks(es_url):
    request(es_url.rstrip("/") + "/_cluster/settings", "PUT", {"persistent": {
        "cluster.routing.allocation.disk.threshold_enabled": True,
        "cluster.routing.allocation.disk.watermark.low": "70%",
        "cluster.routing.allocation.disk.watermark.high": "75%",
        "cluster.routing.allocation.disk.watermark.flood_stage": "80%",
    }})


def eligible_indices(es_url, retention_days, namespace):
    rows = request(es_url.rstrip("/") + "/_cat/indices/tuba-v1-*?format=json&h=index")
    cutoff = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=retention_days - 1)
    result = []
    for row in rows:
        name = row.get("index", "")
        if ("-" + namespace + "-g") not in name:
            continue
        match = INDEX.fullmatch(name)
        if not match:
            continue
        day = dt.datetime.strptime(match.group(1), "%Y.%m.%d").date()
        if day < cutoff:
            result.append(name)
    return sorted(result)


def check_once(es_url, disk_path, retention_days, namespace, delete_enabled, audit_path):
    usage = shutil.disk_usage(disk_path)
    used = 100.0 * (usage.total - usage.free) / usage.total
    level = "normal" if used < 70 else "warning" if used < 75 else "critical" if used < 80 else "protected"
    if level != STATE["level"]:
        print("capacity level changed old=%s new=%s disk_used_percent=%.2f" % (STATE["level"], level, used), flush=True)
    STATE.update({"disk_used_percent": used, "level": level, "last_check": int(time.time()), "last_error": ""})
    candidates = eligible_indices(es_url, retention_days, namespace)
    STATE["delete_candidates"] = len(candidates)
    if not delete_enabled:
        return
    for name in candidates:
        encoded = urllib.parse.quote(name, safe="")
        request(es_url.rstrip("/") + "/" + encoded, "DELETE")
        STATE["deleted_total"] += 1
        record = {"timestamp": dt.datetime.now(dt.timezone.utc).isoformat(), "action": "delete_index", "index": name}
        with open(audit_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        print("deleted expired index %s" % name, flush=True)


class Metrics(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health/live":
            body, status, content_type = b'{"status":"live"}\n', 200, "application/json"
        elif self.path == "/health/ready":
            ok = bool(STATE["last_check"] and not STATE["last_error"])
            body, status, content_type = json.dumps({"status": "ready" if ok else "not_ready", "level": STATE["level"]}).encode() + b"\n", 200 if ok else 503, "application/json"
        elif self.path == "/metrics":
            levels = {name: int(STATE["level"] == name) for name in ("normal", "warning", "critical", "protected")}
            text = "tuba_capacity_disk_used_percent %.3f\n" % STATE["disk_used_percent"]
            text += "tuba_capacity_delete_candidates %d\n" % STATE["delete_candidates"]
            text += "tuba_capacity_deleted_indices_total %d\n" % STATE["deleted_total"]
            for name, value in levels.items():
                text += 'tuba_capacity_level{level="%s"} %d\n' % (name, value)
            body, status, content_type = text.encode(), 200, "text/plain; version=0.0.4"
        else:
            body, status, content_type = b"not found\n", 404, "text/plain"
        self.send_response(status); self.send_header("Content-Type", content_type); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def log_message(self, *_args):
        return


def run(args):
    os.makedirs(os.path.dirname(args.audit_log), mode=0o750, exist_ok=True)
    apply_es_watermarks(args.es_url)
    server = HTTPServer((args.metrics_host, args.metrics_port), Metrics)
    Thread(target=server.serve_forever, daemon=True).start()
    def stop(_signum, _frame):
        global STOP
        STOP = True
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    while not STOP:
        try:
            check_once(args.es_url, args.disk_path, args.retention_days, args.namespace, args.delete, args.audit_log)
        except Exception as error:
            STATE["last_error"] = str(error)
            print("capacity check failed: %s" % error, file=sys.stderr, flush=True)
        deadline = time.time() + args.interval
        while not STOP and time.time() < deadline:
            time.sleep(1)
    server.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--es-url", default="http://127.0.0.1:9200")
    parser.add_argument("--disk-path", default="/")
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--namespace", default="zeek_validation_20260927_001")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--metrics-host", default="127.0.0.1")
    parser.add_argument("--metrics-port", type=int, default=19100)
    parser.add_argument("--audit-log", default="/opt/tuba/collector-live/capacity-guard/logs/audit.jsonl")
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()
    if args.retention_days != 7 or args.interval < 30:
        parser.error("current A03 profile requires retention-days=7 and interval>=30")
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
