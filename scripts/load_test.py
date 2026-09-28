"""Concurrent ingest load test for a local or staging TUBA environment."""

from __future__ import annotations

import argparse
import http.client
import json
import statistics
import threading
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone


def event(index: int, user_count: int) -> dict:
    minute = index // max(user_count, 1)
    user = f"load.user.{index % max(user_count, 1):04d}"
    outcome = "success" if index % 7 == 0 else "failure"
    return {
        "@timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event": {"id": f"load-{uuid.uuid4()}", "action": "logon", "outcome": outcome},
        "user": {"name": user},
        "source": {"ip": f"10.{minute % 255}.0.{index % 255}"},
    }


_local = threading.local()


def connection_for(url: str, timeout: float):
    parsed = urllib.parse.urlparse(url)
    key = (parsed.scheme, parsed.netloc)
    connection = getattr(_local, "connection", None)
    if connection is None or getattr(_local, "connection_key", None) != key:
        connection_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        connection = connection_class(parsed.hostname, parsed.port, timeout=timeout)
        _local.connection = connection
        _local.connection_key = key
    return connection, parsed.path


def send(url: str, api_key: str, context_id: str, payload: dict, position: str, timeout: float) -> tuple[int, float]:
    started = time.perf_counter()
    try:
        connection, path = connection_for(url, timeout)
        body = json.dumps(payload, separators=(",", ":")).encode()
        connection.request(
            "POST",
            path,
            body=body,
            headers={"Content-Type": "application/json", "X-API-Key": api_key, "X-Source-Position": position, "X-Source-Context": context_id},
        )
        response = connection.getresponse()
        response.read()
        return response.status, time.perf_counter() - started
    except (http.client.HTTPException, TimeoutError, OSError):
        connection = getattr(_local, "connection", None)
        if connection is not None:
            connection.close()
        _local.connection = None
        return 0, time.perf_counter() - started


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * quantile))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a TUBA ingest load test")
    parser.add_argument("--url", default="http://127.0.0.1:8080/api/v1/ingest/events")
    parser.add_argument("--api-key", required=True, help="API key issued for a registered source")
    parser.add_argument("--source-context", required=True, help="Immutable source_context_id returned during registration")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--users", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--max-error-rate", type=float, default=0.0)
    parser.add_argument("--max-p95-ms", type=float, default=1000.0)
    args = parser.parse_args()

    payloads = [event(index, args.users) for index in range(args.events)]
    started = time.perf_counter()
    results: list[tuple[int, float]] = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [
            executor.submit(send, args.url, args.api_key, args.source_context, payload, f"load-test-{uuid.uuid4()}", args.timeout)
            for payload in payloads
        ]
        for future in as_completed(futures):
            results.append(future.result())
    elapsed = time.perf_counter() - started

    statuses: dict[str, int] = {}
    latencies = [latency for _, latency in results]
    for status, _ in results:
        statuses[str(status)] = statuses.get(str(status), 0) + 1
    accepted = statuses.get("202", 0)
    backpressure = statuses.get("429", 0)
    errors = sum(count for status, count in statuses.items() if status not in {"202", "429"})
    error_rate = errors / max(len(results), 1)
    report = {
        "events": len(results),
        "concurrency": args.concurrency,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_events_per_second": round(len(results) / max(elapsed, 0.001), 2),
        "latency_ms": {
            "mean": round(statistics.fmean(latencies) * 1000, 2) if latencies else 0,
            "p50": round(percentile(latencies, 0.50) * 1000, 2),
            "p95": round(percentile(latencies, 0.95) * 1000, 2),
            "p99": round(percentile(latencies, 0.99) * 1000, 2),
            "max": round(max(latencies, default=0) * 1000, 2),
        },
        "statuses": statuses,
        "accepted_rate": round(accepted / max(len(results), 1), 6),
        "backpressure_rate": round(backpressure / max(len(results), 1), 6),
        "error_rate": round(error_rate, 6),
    }
    print(json.dumps(report, indent=2))
    if error_rate > args.max_error_rate or report["latency_ms"]["p95"] > args.max_p95_ms:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
