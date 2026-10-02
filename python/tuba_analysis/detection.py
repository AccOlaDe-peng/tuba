"""Deterministic authentication detection over a bounded event-time interval."""

from __future__ import annotations

import hashlib
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone


def parse_time(value: str) -> datetime:
    # Canonical implementation lives in the feature module; re-exported here
    # so existing detection callers keep working.
    from .features import parse_time as _parse_time

    return _parse_time(value)


def detect_failure_then_success(
    events: list[dict],
    organization_id: str,
    *,
    rule_id: str = "auth.failure-then-success",
    rule_version: str = "1.0.0",
    severity: str = "high",
    threshold: int = 5,
    lookback: timedelta = timedelta(minutes=30),
    window_seconds: int = 1800,
) -> list[dict]:
    """Return stable anomalies for failure bursts followed by a success."""
    if threshold < 1 or lookback <= timedelta(0) or window_seconds < 1:
        raise ValueError("threshold, lookback, and window must be positive")

    failures: dict[str, deque[tuple[datetime, str]]] = defaultdict(deque)
    output: dict[str, dict] = {}
    seen_ids: set[str] = set()
    ordered = sorted(events, key=lambda event: (parse_time(event["@timestamp"]), event["event"]["id"]))

    for item in ordered:
        if item.get("organization", {}).get("id") != organization_id:
            raise ValueError("cross-tenant event in analysis input")
        if item.get("ueba", {}).get("quality", {}).get("status") not in {"qualified", "partial"}:
            continue

        event_id = item["event"]["id"]
        if event_id in seen_ids:
            continue
        seen_ids.add(event_id)

        user_id = item.get("user", {}).get("id")
        if not user_id:
            continue
        occurred = parse_time(item["@timestamp"])
        prior = failures[user_id]
        while prior and prior[0][0] < occurred - lookback:
            prior.popleft()

        outcome = item["event"].get("outcome")
        if outcome == "failure":
            prior.append((occurred, event_id))
            continue
        if outcome != "success" or len(prior) < threshold:
            continue

        window_start = datetime.fromtimestamp(
            int(occurred.timestamp()) // window_seconds * window_seconds,
            tz=timezone.utc,
        )
        window_end = window_start + timedelta(seconds=window_seconds)
        key = f"{organization_id}|{user_id}|{window_start.isoformat()}|{rule_id}@{rule_version}"
        anomaly_id = "anom:" + hashlib.sha256(key.encode()).hexdigest()
        if anomaly_id in output:
            continue
        evidence = sorted([failure_id for _, failure_id in prior] + [event_id])
        output[anomaly_id] = {
            "_id": anomaly_id,
            "_source": {
                "@timestamp": occurred.isoformat().replace("+00:00", "Z"),
                "organization": {"id": organization_id},
                "entity": {"id": user_id, "type": "account"},
                "anomaly": {
                    "id": anomaly_id,
                    "type": rule_id,
                    "severity": severity,
                    "status": "open",
                    "score": 1.0,
                },
                "evidence": {"event_ids": evidence, "count": len(evidence)},
                "detection": {
                    "rule_id": rule_id,
                    "rule_version": rule_version,
                    "window": {
                        "start": window_start.isoformat().replace("+00:00", "Z"),
                        "end": window_end.isoformat().replace("+00:00", "Z"),
                    },
                },
                "explanation": {
                    "reason_codes": ["AUTH_FAILURE_BURST_THEN_SUCCESS"],
                    "summary": (
                        f"{len(prior)} failed logins followed by a successful login "
                        f"within {int(lookback.total_seconds() // 60)} minutes"
                    ),
                },
            },
        }
    return [output[key] for key in sorted(output)]
