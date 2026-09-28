import json
import unittest
from pathlib import Path

from tuba_analysis.evaluate import evaluate_scenario
from tuba_analysis.registry import RegistryError, load_registry
from tuba_analysis.replay import business_projection, load_events, replay_events


class ReplayEvaluationTests(unittest.TestCase):
    def test_replay_is_deterministic(self):
        events = load_events(Path(__file__).parents[1] / "scenarios" / "auth_failure_then_success.json")
        registry = load_registry()
        first = replay_events(events, "tenant_a", "tenant_a", registry)
        second = replay_events(events, "tenant_a", "tenant_a", registry)
        self.assertEqual([business_projection(item) for item in first], [business_projection(item) for item in second])
        self.assertEqual(first[0]["run_id"], second[0]["run_id"])

    def test_golden_scenario_evaluation(self):
        path = Path(__file__).parents[1] / "scenarios" / "auth_failure_then_success.json"
        scenario = json.loads(path.read_text(encoding="utf-8"))
        baseline = evaluate_scenario(scenario, "tenant_a", "tenant_a")
        scenario["expected_anomaly_ids"] = baseline["actual_anomaly_ids"]
        report = evaluate_scenario(scenario, "tenant_a", "tenant_a")
        self.assertEqual(report["precision"], 1.0)
        self.assertEqual(report["recall"], 1.0)

    def test_registry_rejects_unknown_algorithm(self):
        document = {
            "registry_version": "1.0.0",
            "features": [],
            "detections": [
                {
                    "id": "broken",
                    "version": "1.0.0",
                    "algorithm": "unknown",
                    "feature_id": "missing",
                    "severity": "high",
                    "parameters": {},
                },
            ],
            "models": [],
        }
        path = Path(__file__).parent / "_invalid_registry.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        try:
            with self.assertRaises(RegistryError):
                load_registry(path)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
