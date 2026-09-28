import unittest

from tuba_analysis.detection import detect_failure_then_success


def event(event_id, minute, outcome, organization="tenant_a", user="u1"):
    return {
        "@timestamp": f"2026-09-23T02:{minute:02d}:00Z",
        "organization": {"id": organization},
        "user": {"id": user},
        "event": {"id": event_id, "outcome": outcome},
        "ueba": {"quality": {"status": "qualified"}},
    }


class DetectionTests(unittest.TestCase):
    def test_five_failures_then_success(self):
        events = [event(f"f{i}", i, "failure") for i in range(5)] + [event("s1", 5, "success")]
        result = detect_failure_then_success(list(reversed(events)), "tenant_a")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_source"]["evidence"]["count"], 6)
        self.assertEqual(result, detect_failure_then_success(events + [events[0]], "tenant_a"))

    def test_tenant_and_window_boundaries(self):
        events = [event(f"f{i}", i, "failure") for i in range(4)] + [event("s1", 5, "success")]
        self.assertEqual(detect_failure_then_success(events, "tenant_a"), [])
        with self.assertRaises(ValueError):
            detect_failure_then_success(
                events + [event("foreign", 6, "failure", organization="tenant_b")],
                "tenant_a",
            )
        self.assertEqual(
            detect_failure_then_success(
                [event("f", 0, "failure"), event("s", 31, "success")],
                "tenant_a",
                threshold=1,
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
