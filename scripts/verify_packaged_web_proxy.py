#!/usr/bin/env python3
"""Exercise a packaged TUBA web gateway against local HTTP mock upstreams."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def mock_handler(role: str):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            self.respond()

        def do_POST(self) -> None:  # noqa: N802
            self.respond()

        def respond(self) -> None:
            if self.path == "/health/ready":
                payload = b"ready"
                status = 200
            else:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.dumps(
                    {
                        "role": role,
                        "method": self.command,
                        "path": self.path,
                        "authorization": self.headers.get("Authorization"),
                        "body": self.rfile.read(length).decode("utf-8"),
                    }
                ).encode("utf-8")
                status = 202
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return Handler


def request(base_url: str, method: str, path: str, *, body: bytes | None = None, headers: dict[str, str] | None = None):
    call = urllib.request.Request(base_url + path, data=body, headers=headers or {}, method=method)
    try:
        response = urllib.request.urlopen(call, timeout=2)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, dict(response.headers.items()), response.read()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--web-binary", required=True, type=Path)
    parser.add_argument("--web-root", required=True, type=Path)
    args = parser.parse_args()
    binary = args.web_binary.resolve(strict=True)
    web_root = args.web_root.resolve(strict=True)

    api_server = ThreadingHTTPServer(("127.0.0.1", 0), mock_handler("api"))
    ingest_server = ThreadingHTTPServer(("127.0.0.1", 0), mock_handler("ingest"))
    api_thread = Thread(target=api_server.serve_forever, daemon=True)
    ingest_thread = Thread(target=ingest_server.serve_forever, daemon=True)
    api_thread.start()
    ingest_thread.start()

    port = free_port()
    environment = os.environ.copy()
    environment.update(
        {
            "WEB_LISTEN": f"127.0.0.1:{port}",
            "WEB_ROOT": str(web_root),
            "API_UPSTREAM": f"http://127.0.0.1:{api_server.server_port}",
            "INGEST_UPSTREAM": f"http://127.0.0.1:{ingest_server.server_port}",
        }
    )
    process = subprocess.Popen(
        [str(binary)], cwd=str(binary.parent.parent), env=environment,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        live = None
        for _ in range(50):
            if process.poll() is not None:
                break
            try:
                live = request(base_url, "GET", "/health/live")
                break
            except (OSError, TimeoutError, urllib.error.URLError):
                time.sleep(0.1)
        require(process.poll() is None, "packaged tuba-web exited before becoming live")
        require(live is not None and live[0] == 200 and b"ok" in live[2], "packaged live endpoint failed")

        home = request(base_url, "GET", "/")
        spa = request(base_url, "GET", "/overview")
        runtime = request(base_url, "GET", "/config.js")
        require(home[0] == 200 and b'id="root"' in home[2], "packaged home page failed")
        require(spa[0] == 200 and b'id="root"' in spa[2], "packaged SPA fallback failed")
        require(
            runtime[0] == 200
            and runtime[1].get("Cache-Control") == "no-store"
            and b'"basePath":"/"' in runtime[2]
            and b"oidc" not in runtime[2],
            "packaged runtime configuration failed",
        )

        api = request(
            base_url, "GET", "/api/v1/me?expand=roles", headers={"Authorization": "Bearer api-proxy-canary"}
        )
        api_payload = json.loads(api[2])
        require(
            api[0] == 202
            and api_payload == {
                "role": "api",
                "method": "GET",
                "path": "/api/v1/me?expand=roles",
                "authorization": "Bearer api-proxy-canary",
                "body": "",
            },
            "API proxy did not preserve method, path, or Authorization",
        )

        ingest_body = b'{"event":"proxy-smoke"}'
        ingest = request(
            base_url,
            "POST",
            "/api/v1/ingest/events?source=smoke",
            body=ingest_body,
            headers={"Authorization": "Bearer ingest-proxy-canary", "Content-Type": "application/json"},
        )
        ingest_payload = json.loads(ingest[2])
        require(
            ingest[0] == 202
            and ingest_payload == {
                "role": "ingest",
                "method": "POST",
                "path": "/api/v1/ingest/events?source=smoke",
                "authorization": "Bearer ingest-proxy-canary",
                "body": ingest_body.decode(),
            },
            "ingest proxy did not preserve method, path, body, or Authorization",
        )
        denied = request(base_url, "POST", "/api/v1/internal/ingest/beat-events", body=b"{}")
        require(denied[0] == 404, "internal ingest route must remain unavailable through web gateway")
        ready = request(base_url, "GET", "/health/ready")
        require(ready[0] == 200, "packaged readiness did not pass with healthy upstreams")
        print("Packaged web proxy smoke passed: static/SPA/runtime config, API and ingest request fidelity, internal route denial, readiness.")
    except Exception:
        if process.poll() is None:
            process.terminate()
        output, _ = process.communicate(timeout=5)
        if output:
            print(output.decode("utf-8", errors="replace"), end="")
        raise
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        api_server.shutdown()
        ingest_server.shutdown()
        api_server.server_close()
        ingest_server.server_close()
        api_thread.join(timeout=2)
        ingest_thread.join(timeout=2)


if __name__ == "__main__":
    main()
