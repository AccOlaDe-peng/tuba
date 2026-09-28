"""Small dependency-free Prometheus metrics endpoint for the analysis worker."""

from __future__ import annotations

import ipaddress
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._watermarks: dict[int, float] = {}
        self._ready = False

    def set_ready(self, ready: bool) -> None:
        with self._lock:
            self._ready = ready

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + value

    def set_watermark(self, partition: int, timestamp: float | None) -> None:
        with self._lock:
            if timestamp is None:
                self._watermarks.pop(partition, None)
            else:
                self._watermarks[partition] = timestamp

    def render(self) -> str:
        with self._lock:
            counters = sorted(self._counters.items())
            watermarks = sorted(self._watermarks.items())
        lines = [f"{name} {value}" for name, value in counters]
        lines.extend(
            f'tuba_analysis_watermark_timestamp_seconds{{partition="{partition}"}} {value}'
            for partition, value in watermarks
        )
        return "\n".join(lines) + "\n"


class _Handler(BaseHTTPRequestHandler):
    metrics: Metrics

    def do_GET(self) -> None:
        if self.path == "/health/live":
            self._respond(200, b"ok\n", "text/plain")
        elif self.path == "/health/ready":
            with self.metrics._lock:
                ready = self.metrics._ready
            self._respond(200 if ready else 503, b"ready\n" if ready else b"not ready\n", "text/plain")
        elif self.path == "/metrics":
            self._respond(200, self.metrics.render().encode(), "text/plain; version=0.0.4")
        else:
            self._respond(404, b"not found\n", "text/plain")

    def _respond(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return None


def start_http_server(address: str, metrics: Metrics) -> ThreadingHTTPServer:
    host, _, port = address.rpartition(":")
    if not port:
        raise ValueError("metrics address must include a port")
    try:
        port_number = int(port)
    except ValueError as exc:
        raise ValueError("metrics address must use a TCP port between 1 and 65535") from exc
    if port_number < 1 or port_number > 65535:
        raise ValueError("metrics address must use a TCP port between 1 and 65535")

    allow_value = os.getenv("TUBA_ALLOW_NON_LOOPBACK_LISTEN", "")
    if allow_value not in ("", "false", "true"):
        raise ValueError("TUBA_ALLOW_NON_LOOPBACK_LISTEN must be exactly true or false")
    allow_non_loopback = allow_value == "true"
    if host == "":
        if not allow_non_loopback:
            raise ValueError("wildcard metrics listener requires TUBA_ALLOW_NON_LOOPBACK_LISTEN=true")
        bind_host = "0.0.0.0"
    else:
        try:
            bind_ip = ipaddress.ip_address(host)
        except ValueError as exc:
            raise ValueError("metrics listener must use an IP address") from exc
        if not bind_ip.is_loopback and not allow_non_loopback:
            raise ValueError("non-loopback metrics listener requires TUBA_ALLOW_NON_LOOPBACK_LISTEN=true")
        if bind_ip.version != 4:
            raise ValueError("Python metrics listener currently supports IPv4 addresses only")
        bind_host = str(bind_ip)

    handler = type("TubaMetricsHandler", (_Handler,), {"metrics": metrics})
    server = ThreadingHTTPServer((bind_host, port_number), handler)
    thread = threading.Thread(target=server.serve_forever, name="tuba-metrics", daemon=True)
    thread.start()
    return server
