"""Deterministic offline replay for analysis rules and scenarios."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from .detection import parse_time
from .registry import AnalysisRegistry, load_registry
from .worker import AuthenticationProcessor


def load_events(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path)
    if source.suffix == ".ndjson":
        return [
            json.loads(line)
            for line in source.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    document = json.loads(source.read_text(encoding="utf-8"))
    if isinstance(document, list):
        return document
    if isinstance(document, dict) and isinstance(document.get("events"), list):
        return document["events"]
    raise ValueError("replay input must be a JSON array or an object with events")


def deterministic_run_id(events: list[dict[str, Any]], registry_version: str) -> str:
    digest = hashlib.sha256()
    digest.update(registry_version.encode())
    for event in events:
        digest.update(
            json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(),
        )
    return "replay:" + digest.hexdigest()


def replay_events(
    events: list[dict[str, Any]],
    organization: str,
    namespace: str,
    registry: AnalysisRegistry,
) -> list[dict[str, Any]]:
    rule = registry.rule("auth.failure-then-success")
    run_id = deterministic_run_id(events, registry.version)
    if not events:
        return []
    generated_at = max(parse_time(event["@timestamp"]) for event in events)
    processor = AuthenticationProcessor(organization, namespace, rule)
    results: list[dict[str, Any]] = []
    for event in events:
        results.extend(
            processor.process(
                event,
                run_id,
                generated_at=generated_at,
                registry_version=registry.version,
            ),
        )
    return results


def business_projection(result: dict[str, Any]) -> dict[str, Any]:
    document = result["document"]
    source = {
        key: value
        for key, value in document.items()
        if key != "analysis"
    }
    return {
        "result_id": result["result_id"],
        "rule_id": result["rule_id"],
        "rule_version": result["rule_version"],
        "document": source,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay authentication events deterministically")
    parser.add_argument("--input", required=True, help="JSON or NDJSON event file")
    parser.add_argument("--organization", default="tenant_a")
    parser.add_argument("--namespace", default="tenant_a")
    parser.add_argument("--registry")
    parser.add_argument("--output")
    args = parser.parse_args()
    registry = load_registry(args.registry)
    results = replay_events(
        load_events(args.input),
        args.organization,
        args.namespace,
        registry,
    )
    output = json.dumps(results, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(output + "\n", encoding="utf-8")
    else:
        print(output)


if __name__ == "__main__":
    main()
