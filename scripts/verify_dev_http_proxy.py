#!/usr/bin/env python3
"""Verify the Vite development HTTP proxy forwards API path and auth header."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"
API_PORT = 8788
INGEST_PORT = 8080


def port_available(port: int) -> bool:
    with socket.socket() as sock:
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def wait_http(url: str, process: subprocess.Popen[str], timeout: int = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before HTTP server became ready: {url} (exit={process.returncode})")
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    raise TimeoutError(f"HTTP server did not become ready: {url}")


def stop(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main() -> int:
    vite_entry = WEB / "node_modules" / "vite" / "bin" / "vite.js"
    if not vite_entry.is_file():
        raise RuntimeError("web/node_modules is missing; install the locked frontend dependencies first")
    for port in (API_PORT, INGEST_PORT):
        if not port_available(port):
            raise RuntimeError(f"refusing to replace or probe an existing listener on 127.0.0.1:{port}")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        web_port = int(sock.getsockname()[1])

    backend_source = r'''
const http = require("node:http");
const server = http.createServer((req, res) => {
  const chunks = [];
  req.on("data", chunk => chunks.push(chunk));
  req.on("end", () => {
    res.writeHead(200, {"content-type": "application/json"});
    res.end(JSON.stringify({service: process.env.MOCK_SERVICE, method: req.method, path: req.url,
      authorization: req.headers.authorization || "", body: Buffer.concat(chunks).toString()}));
  });
});
server.listen(Number(process.env.MOCK_PORT), "127.0.0.1");
process.on("SIGTERM", () => server.close(() => process.exit(0)));
'''

    with tempfile.TemporaryDirectory(prefix="tuba-dev-http-proxy-") as temp:
        work = Path(temp)
        backend_log = open(work / "backend.log", "w", encoding="utf-8")
        vite_log = open(work / "vite.log", "w", encoding="utf-8")
        ingest_log = open(work / "ingest.log", "w", encoding="utf-8")
        options: dict[str, object] = {"cwd": ROOT, "stdin": subprocess.DEVNULL, "text": True}
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            options["start_new_session"] = True
        backend: subprocess.Popen[str] | None = None
        ingest: subprocess.Popen[str] | None = None
        vite: subprocess.Popen[str] | None = None
        try:
            api_env = os.environ.copy()
            api_env.update({"MOCK_PORT": str(API_PORT), "MOCK_SERVICE": "api"})
            backend = subprocess.Popen(["node", "-e", backend_source], env=api_env, stdout=backend_log,
                                      stderr=subprocess.STDOUT, **options)  # type: ignore[arg-type]
            ingest_env = os.environ.copy()
            ingest_env.update({"MOCK_PORT": str(INGEST_PORT), "MOCK_SERVICE": "ingest"})
            ingest = subprocess.Popen(["node", "-e", backend_source], env=ingest_env, stdout=ingest_log,
                                      stderr=subprocess.STDOUT, **options)  # type: ignore[arg-type]
            wait_http(f"http://127.0.0.1:{API_PORT}/healthz", backend)
            wait_http(f"http://127.0.0.1:{INGEST_PORT}/healthz", ingest)
            vite = subprocess.Popen(["node", str(vite_entry), "--host", "127.0.0.1", "--port", str(web_port),
                                     "--strictPort"], cwd=WEB, stdin=subprocess.DEVNULL, stdout=vite_log,
                                    stderr=subprocess.STDOUT, text=True,
                                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                                    start_new_session=os.name != "nt")
            wait_http(f"http://127.0.0.1:{web_port}/", vite)

            request = urllib.request.Request(
                f"http://127.0.0.1:{web_port}/api/v1/me",
                headers={"Authorization": "Bearer tuba-proxy-canary"},
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                payload = json.loads(response.read())
            if payload != {"service": "api", "method": "GET", "path": "/api/v1/me",
                           "authorization": "Bearer tuba-proxy-canary", "body": ""}:
                raise AssertionError(f"Vite proxy did not preserve the API request: {payload}")
            body = b'{"event":"proxy-check"}'
            ingest_request = urllib.request.Request(
                f"http://127.0.0.1:{web_port}/api/v1/ingest/events", data=body, method="POST",
                headers={"Authorization": "Bearer tuba-ingest-canary", "Content-Type": "application/json"},
            )
            with urllib.request.urlopen(ingest_request, timeout=5) as response:
                ingest_payload = json.loads(response.read())
            if ingest_payload != {"service": "ingest", "method": "POST", "path": "/api/v1/ingest/events",
                                 "authorization": "Bearer tuba-ingest-canary", "body": body.decode()}:
                raise AssertionError(f"Vite proxy did not route the ingestion request correctly: {ingest_payload}")
            print("PASS: Vite HTTP proxy routed API and event-ingest paths to their loopback services, preserving method, body, and Authorization.")
            return 0
        finally:
            stop(vite)
            stop(ingest)
            stop(backend)
            vite_log.close()
            ingest_log.close()
            backend_log.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError, TimeoutError, ValueError) as exc:
        print(f"error: {exc}")
        raise SystemExit(1)
