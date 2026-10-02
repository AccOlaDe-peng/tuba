"""Authentication detection scenarios (F05), authoritative implementation.

Three first-phase scenarios per design baseline §6:

* ``detect_failure_then_success`` — failure burst followed by a success
  (deterministic rule, cold-start allowed);
* ``detect_failure_burst`` — short-window failure concentration
  (deterministic rule, cold-start allowed);
* ``detect_baseline_deviation`` — statistical deviation against the F04
  baseline model (z-score / 3-sigma over ``moments.v1`` statistics). A
  baseline that is not ``ready`` never scores: cold_start/training/retired
  yields zero findings with an explicit, queryable skip reason — the
  statistical scenario is never fabricated from an unready model.

Every finding carries the five mandatory elements (locked by tests and
asserted fail-closed at build time): a human-readable explanation, the
applied threshold, the contributing features, the rule/model version, and
input references (evidence event ids plus the bounded window). The output
shape is the existing anomaly document envelope (``_id``/``_source``) so the
analysis results topic / analysis-sink path consumes it unchanged.

F06: every finding carries a mandatory ``generation`` (default ``g1``); it is
part of the finding business key / anomaly-id derivation (contracts/ids.md),
so a rule upgrade on a new generation never overwrites the old generation's
findings. Revision/retracted lifecycle over those business keys lives in
``tuba_analysis/revisions.py``.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any

from .baseline import BaselineModel, BaselineStatus
from .features import compute_window_features, parse_time as _parse_time
from .revisions import DEFAULT_GENERATION

RULE_FAILURE_THEN_SUCCESS = "auth.failure-then-success"
RULE_FAILURE_BURST = "auth.failure-burst"
RULE_BASELINE_DEVIATION = "auth.baseline-deviation"
RULE_VERSION_V1 = "1.0.0"

DEFAULT_Z_THRESHOLD = 3.0


def parse_time(value: str) -> datetime:
    # Canonical implementation lives in the feature module; re-exported here
    # so existing detection callers keep working.
    return _parse_time(value)


def _format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _assert_five_elements(document: dict[str, Any], *, require_model: bool) -> None:
    """Fail-closed completeness check of one finding document."""
    explanation = document.get("explanation") or {}
    detection = document.get("detection") or {}
    features = document.get("features") or {}
    evidence = document.get("evidence") or {}
    if not explanation.get("summary") or not explanation.get("reason_codes"):
        raise ValueError("finding requires a human-readable explanation")
    if not detection.get("threshold"):
        raise ValueError("finding requires the applied threshold")
    if not features.get("feature_version") or not features.get("values"):
        raise ValueError("finding requires contributing features")
    if not detection.get("rule_id") or not detection.get("rule_version"):
        raise ValueError("finding requires the rule id and version")
    if require_model and not (detection.get("model") or {}).get("model_version"):
        raise ValueError("statistical finding requires the model version")
    if not evidence.get("event_ids"):
        raise ValueError("finding requires input references (event ids)")
    window = detection.get("window") or {}
    if not window.get("start") or not window.get("end"):
        raise ValueError("finding requires the bounded input window")


def _anomaly_document(
    *,
    anomaly_id: str,
    at: datetime,
    organization_id: str,
    entity_id: str,
    rule_id: str,
    rule_version: str,
    generation: str,
    severity: str,
    score: float,
    reason_codes: list[str],
    summary: str,
    threshold: dict[str, Any],
    feature_version: str,
    feature_values: dict[str, Any],
    evidence_ids: list[str],
    window_start: datetime,
    window_end: datetime,
    model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not generation:
        raise ValueError("finding requires a generation (business-key part)")
    detection: dict[str, Any] = {
        "rule_id": rule_id,
        "rule_version": rule_version,
        "threshold": threshold,
        "window": {"start": _format_time(window_start), "end": _format_time(window_end)},
    }
    if model is not None:
        detection["model"] = model
    document: dict[str, Any] = {
        "@timestamp": _format_time(at),
        "organization": {"id": organization_id},
        "entity": {"id": entity_id, "type": "account"},
        "anomaly": {
            "id": anomaly_id,
            "type": rule_id,
            "severity": severity,
            "status": "open",
            "score": score,
            "generation": generation,
        },
        "evidence": {"event_ids": sorted(evidence_ids), "count": len(evidence_ids)},
        "detection": detection,
        "features": {"feature_version": feature_version, "values": feature_values},
        "explanation": {"reason_codes": list(reason_codes), "summary": summary},
    }
    _assert_five_elements(document, require_model=model is not None)
    return document


def _eligible_events(events: list[dict], organization_id: str) -> list[dict]:
    """Tenant-checked, quality-filtered, deduplicated, deterministically ordered input."""
    seen_ids: set[str] = set()
    eligible: list[dict] = []
    for item in sorted(events, key=lambda event: (parse_time(event["@timestamp"]), event["event"]["id"])):
        if item.get("organization", {}).get("id") != organization_id:
            raise ValueError("cross-tenant event in analysis input")
        if item.get("ueba", {}).get("quality", {}).get("status") not in {"qualified", "partial"}:
            continue
        event_id = item["event"]["id"]
        if event_id in seen_ids:
            continue
        seen_ids.add(event_id)
        eligible.append(item)
    return eligible


def detect_failure_then_success(
    events: list[dict],
    organization_id: str,
    *,
    rule_id: str = RULE_FAILURE_THEN_SUCCESS,
    rule_version: str = RULE_VERSION_V1,
    generation: str = DEFAULT_GENERATION,
    severity: str = "high",
    threshold: int = 5,
    lookback: timedelta = timedelta(minutes=30),
    window_seconds: int = 1800,
) -> list[dict]:
    """Return stable anomalies for failure bursts followed by a success."""
    if threshold < 1 or lookback <= timedelta(0) or window_seconds < 1:
        raise ValueError("threshold, lookback, and window must be positive")

    failures: dict[str, deque[tuple[datetime, str]]] = defaultdict(deque)
    failure_events: dict[str, dict] = {}
    output: dict[str, dict] = {}

    for item in _eligible_events(events, organization_id):
        user_id = item.get("user", {}).get("id")
        if not user_id:
            continue
        event_id = item["event"]["id"]
        occurred = parse_time(item["@timestamp"])
        prior = failures[user_id]
        while prior and prior[0][0] < occurred - lookback:
            prior.popleft()

        outcome = item["event"].get("outcome")
        if outcome == "failure":
            prior.append((occurred, event_id))
            failure_events[event_id] = item
            continue
        if outcome != "success" or len(prior) < threshold:
            continue

        window_start = datetime.fromtimestamp(
            int(occurred.timestamp()) // window_seconds * window_seconds,
            tz=timezone.utc,
        )
        window_end = window_start + timedelta(seconds=window_seconds)
        key = f"{organization_id}|{user_id}|{window_start.isoformat()}|{rule_id}@{rule_version}|{generation}"
        anomaly_id = "anom:" + hashlib.sha256(key.encode()).hexdigest()
        if anomaly_id in output:
            continue
        contributing = [failure_events[failure_id] for _, failure_id in prior] + [item]
        evidence = sorted([failure_id for _, failure_id in prior] + [event_id])
        features = compute_window_features(contributing)
        document = _anomaly_document(
            anomaly_id=anomaly_id,
            at=occurred,
            organization_id=organization_id,
            entity_id=user_id,
            rule_id=rule_id,
            rule_version=rule_version,
            generation=generation,
            severity=severity,
            score=1.0,
            reason_codes=["AUTH_FAILURE_BURST_THEN_SUCCESS"],
            summary=(
                f"{len(prior)} failed logins followed by a successful login "
                f"within {int(lookback.total_seconds() // 60)} minutes"
            ),
            threshold={"failure_count": threshold, "lookback_seconds": int(lookback.total_seconds())},
            feature_version=features["feature_version"],
            feature_values=features["values"],
            evidence_ids=evidence,
            window_start=window_start,
            window_end=window_end,
        )
        output[anomaly_id] = {"_id": anomaly_id, "_source": document}
    return [output[key] for key in sorted(output)]


def detect_failure_burst(
    events: list[dict],
    organization_id: str,
    *,
    rule_id: str = RULE_FAILURE_BURST,
    rule_version: str = RULE_VERSION_V1,
    generation: str = DEFAULT_GENERATION,
    severity: str = "medium",
    threshold: int = 10,
    window_seconds: int = 300,
) -> list[dict]:
    """Short-window failure concentration rule (cold-start allowed).

    Per entity and per tumbling ``window_seconds`` window, at least
    ``threshold`` failures fire one finding. Deterministic and
    arrival-order independent; one finding per (entity, window, rule).
    """
    if threshold < 1 or window_seconds < 1:
        raise ValueError("threshold and window must be positive")

    buckets: dict[tuple[str, datetime], list[dict]] = defaultdict(list)
    for item in _eligible_events(events, organization_id):
        if item["event"].get("outcome") != "failure":
            continue
        user_id = item.get("user", {}).get("id")
        if not user_id:
            continue
        occurred = parse_time(item["@timestamp"])
        window_start = datetime.fromtimestamp(
            int(occurred.timestamp()) // window_seconds * window_seconds,
            tz=timezone.utc,
        )
        buckets[(user_id, window_start)].append(item)

    output: dict[str, dict] = {}
    for (user_id, window_start), bucket in sorted(buckets.items(), key=lambda entry: (entry[0][1], entry[0][0])):
        if len(bucket) < threshold:
            continue
        window_end = window_start + timedelta(seconds=window_seconds)
        key = f"{organization_id}|{user_id}|{window_start.isoformat()}|{rule_id}@{rule_version}|{generation}"
        anomaly_id = "anom:" + hashlib.sha256(key.encode()).hexdigest()
        features = compute_window_features(bucket)
        evidence = sorted(item["event"]["id"] for item in bucket)
        document = _anomaly_document(
            anomaly_id=anomaly_id,
            at=window_end,
            organization_id=organization_id,
            entity_id=user_id,
            rule_id=rule_id,
            rule_version=rule_version,
            generation=generation,
            severity=severity,
            score=1.0,
            reason_codes=["AUTH_FAILURE_BURST"],
            summary=(
                f"{len(bucket)} failed logins within {window_seconds // 60} minutes "
                f"(threshold {threshold})"
            ),
            threshold={"failure_count": threshold, "window_seconds": window_seconds},
            feature_version=features["feature_version"],
            feature_values=features["values"],
            evidence_ids=evidence,
            window_start=window_start,
            window_end=window_end,
        )
        output[anomaly_id] = {"_id": anomaly_id, "_source": document}
    return [output[key] for key in sorted(output)]


def detect_baseline_deviation(
    record: dict[str, Any],
    organization_id: str,
    model: BaselineModel,
    *,
    rule_id: str = RULE_BASELINE_DEVIATION,
    rule_version: str = RULE_VERSION_V1,
    generation: str = DEFAULT_GENERATION,
    severity: str = "medium",
    z_threshold: float = DEFAULT_Z_THRESHOLD,
) -> dict[str, Any]:
    """Statistical baseline detection over one closed window feature record.

    ``record`` is an F03 closed-window feature record (``entity_id``,
    ``feature_version``, ``window{start,end}``, ``values``, ``inputs``).
    Returns ``{"findings": [...], "baseline": {...}}`` — the baseline block
    always reports the model id/version/status and, when no scoring happened,
    the explicit reason. A baseline that is not ``ready`` (cold_start,
    training, retired) produces zero findings; the scenario is skipped, never
    fabricated. A feature-version mismatch or a trained feature missing from
    the record is a fail-closed contract error.
    """
    if z_threshold <= 0 or not math.isfinite(z_threshold):
        raise ValueError("z threshold must be a positive finite number")
    entity_id = record.get("entity_id")
    record_feature_version = record.get("feature_version")
    if not entity_id or not record_feature_version:
        raise ValueError("feature record requires entity id and feature version")
    window = record.get("window") or {}
    window_start = parse_time(window["start"])
    window_end = parse_time(window["end"])
    values = record.get("values") or {}
    inputs = record.get("inputs") or []

    baseline_info: dict[str, Any] = {
        "model_id": model.model_id,
        "model_version": model.version,
        "status": model.status.value,
        "scored": False,
        "reason": None,
    }
    if model.status != BaselineStatus.READY:
        baseline_info["reason"] = f"baseline_not_ready:{model.status.value}"
        return {"findings": [], "baseline": baseline_info}
    if model.feature_version != record_feature_version:
        raise ValueError(
            f"feature version mismatch: record {record_feature_version}, model {model.feature_version}"
        )

    stats = (model.statistics or {}).get("feature_stats") or {}
    if not stats:
        raise ValueError("ready baseline requires trained feature statistics")
    breaches: dict[str, dict[str, float]] = {}
    for name in sorted(stats):
        if name not in values:
            raise ValueError(f"feature record misses trained feature: {name}")
        mean = float(stats[name]["mean"])
        std = float(stats[name]["std"])
        observed = float(values[name])
        z = 0.0 if (std == 0.0 and observed == mean) else (math.inf if std == 0.0 else abs(observed - mean) / std)
        if z >= z_threshold:
            breaches[name] = {"observed": observed, "mean": mean, "std": std, "abs_z": z}

    baseline_info["scored"] = True
    if not breaches:
        return {"findings": [], "baseline": baseline_info}

    key = (
        f"{organization_id}|{entity_id}|{window_start.isoformat()}|{rule_id}@{rule_version}"
        f"|{generation}|{model.model_id}@{model.version}"
    )
    anomaly_id = "anom:" + hashlib.sha256(key.encode()).hexdigest()
    worst = max(breaches.values(), key=lambda entry: entry["abs_z"])
    summary = "; ".join(
        f"{name} observed {entry['observed']:g} vs baseline mean {entry['mean']:g} "
        f"(|z|={'inf' if math.isinf(entry['abs_z']) else format(entry['abs_z'], '.2f')})"
        for name, entry in sorted(breaches.items())
    )
    score = 1.0 if math.isinf(worst["abs_z"]) else min(1.0, worst["abs_z"] / (2 * z_threshold))
    document = _anomaly_document(
        anomaly_id=anomaly_id,
        at=window_end,
        organization_id=organization_id,
        entity_id=entity_id,
        rule_id=rule_id,
        rule_version=rule_version,
        generation=generation,
        severity=severity,
        score=score,
        reason_codes=["AUTH_BASELINE_DEVIATION"],
        summary=f"feature values deviate from baseline {model.model_id}@{model.version}: {summary}",
        threshold={"abs_z": z_threshold},
        feature_version=record_feature_version,
        feature_values={name: values[name] for name in sorted(breaches)},
        evidence_ids=inputs,
        window_start=window_start,
        window_end=window_end,
        model={
            "model_id": model.model_id,
            "model_version": model.version,
            "feature_version": model.feature_version,
            "algorithm": (model.statistics or {}).get("algorithm", ""),
            "baseline": {name: breaches[name] for name in sorted(breaches)},
        },
    )
    return {"findings": [{"_id": anomaly_id, "_source": document}], "baseline": baseline_info}
