import unittest

from tuba_analysis.registry import load_registry
from tuba_analysis.worker import AuthenticationProcessor


def event(event_id, minute, outcome):
    return {
        "@timestamp": f"2026-09-23T02:{minute:02d}:00.1234567Z",
        "organization": {"id": "tenant_a"},
        "user": {"id": "u1"},
        "event": {"id": event_id, "outcome": outcome},
        "ueba": {"quality": {"status": "qualified"}},
    }


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.rule = load_registry().rule("auth.failure-then-success")

    def test_result_envelope(self):
        processor = AuthenticationProcessor("tenant_a", "tenant_a", self.rule)
        for index in range(5):
            self.assertEqual(processor.process(event(f"f{index}", index, "failure"), "run-1"), [])
        result = processor.process(event("s1", 5, "success"), "run-1")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["contract_version"], "1.0.0")
        self.assertEqual(result[0]["organization_id"], "tenant_a")
        self.assertEqual(result[0]["run_id"], "run-1")
        self.assertIn("window", result[0]["document"]["detection"])

    def test_state_restore_and_late_reprocessing(self):
        processor = AuthenticationProcessor("tenant_a", "tenant_a", self.rule)
        for index in range(5):
            processor.process(event(f"f{index}", index, "failure"), "run-1")
        restored = AuthenticationProcessor.from_state(
            "tenant_a",
            "tenant_a",
            self.rule,
            processor.export_state(),
        )
        result = restored.process(event("s1", 5, "success"), "run-1")
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0]["document"]["analysis"]["reprocessed"])

        late = AuthenticationProcessor("tenant_a", "tenant_a", self.rule)
        late.process(event("s1", 20, "success"), "run-2")
        for index in range(5):
            late.process(event(f"late-f{index}", index, "failure"), "run-2")
        late_results = late.process(event("late-s1", 4, "success"), "run-2")
        self.assertEqual(len(late_results), 1)
        self.assertTrue(late_results[0]["document"]["analysis"]["reprocessed"])


if __name__ == "__main__":
    unittest.main()
