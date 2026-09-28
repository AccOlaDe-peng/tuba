#!/usr/bin/env python3
"""Loopback-only TCP fault proxy for isolated TUBA readiness drills."""

import argparse
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class TCPFaultProxy:
    def __init__(self, name, listen, target):
        self.name = name
        self.listen = listen
        self.target = target
        self.blocked = False
        self.lock = threading.Lock()
        self.connections = set()
        self.listener = None

    def serve(self):
        host, port = self.listen
        while True:
            with self.lock:
                if self.blocked:
                    listener = None
                else:
                    if self.listener is None:
                        try:
                            self.listener = socket.create_server((host, port), reuse_port=False)
                            self.listener.settimeout(0.5)
                        except OSError:
                            self.listener = None
                    listener = self.listener
            if listener is None:
                threading.Event().wait(0.1)
                continue
            try:
                incoming, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                continue
            with self.lock:
                blocked = self.blocked
            if blocked:
                incoming.close()
                continue
            try:
                outgoing = socket.create_connection(self.target, timeout=3)
            except OSError:
                incoming.close()
                continue
            with self.lock:
                if self.blocked:
                    incoming.close()
                    outgoing.close()
                    continue
                self.connections.update((incoming, outgoing))
            threading.Thread(
                target=self._pump,
                args=(incoming, outgoing),
                daemon=True,
            ).start()

    def _pump(self, left, right):
        def copy(source, destination):
            try:
                while True:
                    data = source.recv(65536)
                    if not data:
                        break
                    destination.sendall(data)
            except OSError:
                pass
            finally:
                for connection in (left, right):
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    connection.close()

        threading.Thread(target=copy, args=(left, right), daemon=True).start()
        copy(right, left)
        with self.lock:
            self.connections.discard(left)
            self.connections.discard(right)

    def set_blocked(self, blocked):
        with self.lock:
            self.blocked = blocked
            listener = self.listener if blocked else None
            if blocked:
                self.listener = None
            if blocked:
                active = tuple(self.connections)
                self.connections.clear()
            else:
                active = ()
        if listener is not None:
            listener.close()
        for connection in active:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()


def endpoint(value):
    host, separator, raw_port = value.rpartition(":")
    if not separator:
        raise argparse.ArgumentTypeError("endpoint must be host:port")
    try:
        port = int(raw_port)
        if not 1 <= port <= 65535:
            raise ValueError("port out of range")
        return host, port
    except ValueError as error:
        raise argparse.ArgumentTypeError("endpoint port must be an integer") from error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres-listen", type=endpoint, default=endpoint("127.0.0.1:15435"))
    parser.add_argument("--postgres-target", type=endpoint, default=endpoint("127.0.0.1:15434"))
    parser.add_argument("--kafka-listen", type=endpoint, default=endpoint("127.0.0.1:19095"))
    parser.add_argument("--kafka-target", type=endpoint, default=endpoint("127.0.0.1:19094"))
    parser.add_argument("--control", type=endpoint, default=endpoint("127.0.0.1:19096"))
    args = parser.parse_args()

    endpoints = (
        args.postgres_listen,
        args.postgres_target,
        args.kafka_listen,
        args.kafka_target,
        args.control,
    )
    if any(host not in ("127.0.0.1", "localhost", "::1") for host, _ in endpoints):
        raise SystemExit("all proxy endpoints must use loopback addresses")

    proxies = {
        "postgres": TCPFaultProxy("postgres", args.postgres_listen, args.postgres_target),
        "kafka": TCPFaultProxy("kafka", args.kafka_listen, args.kafka_target),
    }
    for proxy in proxies.values():
        threading.Thread(target=proxy.serve, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/status":
                self.send_error(404)
                return
            body = json.dumps({name: "blocked" if proxy.blocked else "open" for name, proxy in proxies.items()}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            parts = self.path.strip("/").split("/")
            if len(parts) != 3 or parts[0] != "targets" or parts[1] not in proxies or parts[2] not in ("open", "blocked"):
                self.send_error(404)
                return
            proxy = proxies[parts[1]]
            proxy.set_blocked(parts[2] == "blocked")
            self.send_response(204)
            self.end_headers()

        def log_message(self, _format, *_args):
            pass

    host, port = args.control
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("control listener must be loopback")
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
