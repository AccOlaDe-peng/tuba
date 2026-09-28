"""Offline precision and recall evaluation for Golden Scenarios."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .registry import load_registry
from .replay import replay_events


def evaluate_scenario(
    scenario: dict[str, Any],
    organization: str,
    namespace: str,
    registry_path: str | None = None,
) -> dict[str, Any]:
    registry = load_registry(registry_path)
    results = replay_events(scenario.get("events", []), organization, namespace, registry)
    actual = {result["result_id"] for result in results}
    expected = set(scenario.get("expected_anomaly_ids", []))
    true_positive = len(actual & expected)
    false_positive = len(actual - expected)
    false_negative = len(expected - actual)
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 1.0
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 1.0
    return {
        "scenario": scenario.get("name", "unnamed"),
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "precision": precision,
        "recall": recall,
        "actual_anomaly_ids": sorted(actual),
        "expected_anomaly_ids": sorted(expected),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate analysis Golden Scenarios")
    parser.add_argument("scenarios", nargs="+", help="Scenario JSON files")
    parser.add_argument("--organization", default="tenant_a")
    parser.add_argument("--namespace", default="tenant_a")
    parser.add_argument("--registry")
    parser.add_argument("--min-precision", type=float, default=1.0)
    parser.add_argument("--min-recall", type=float, default=1.0)
    args = parser.parse_args()

    reports = []
    for scenario_path in args.scenarios:
        scenario = json.loads(Path(scenario_path).read_text(encoding="utf-8"))
        report = evaluate_scenario(scenario, args.organization, args.namespace, args.registry)
        reports.append(report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if report["precision"] < args.min_precision or report["recall"] < args.min_recall:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
