import unittest
from datetime import timedelta

from tuba_analysis.baseline import BaselineModel, BaselineStatus
from tuba_analysis.detection import parse_time
from tuba_analysis.features import FeatureWindows


def event(event_id, minute, outcome="failure", user="u1"):
    return {
        "@timestamp": f"2026-09-23T02:{minute:02d}:00Z",
        "organization": {"id": "tenant_a"},
        "user": {"id": user},
        "event": {"id": event_id, "outcome": outcome},
    }


class FeatureWindowsTests(unittest.TestCase):
    def setUp(self):
        self.windows = FeatureWindows(lookback=timedelta(minutes=30), allowed_lateness=timedelta(minutes=5))

    def test_watermark_and_late_detection(self):
        late, _ = self.windows.observe(event("e1", 30), "u1", parse_time(event("e1", 30)["@timestamp"]))
        self.assertFalse(late)
        self.assertEqual(self.windows.watermark, parse_time("2026-09-23T02:25:00Z"))
        older = event("e0", 10)
        late, _ = self.windows.observe(older, "u1", parse_time(older["@timestamp"]))
        self.assertTrue(late)
        self.assertEqual(self.windows.watermark, parse_time("2026-09-23T02:25:00Z"))

    def test_dedup_and_retention(self):
        first = event("e1", 0)
        self.windows.observe(first, "u1", parse_time(first["@timestamp"]))
        self.windows.observe(first, "u1", parse_time(first["@timestamp"]))
        later = event("e2", 40)
        _, retained = self.windows.observe(later, "u1", parse_time(later["@timestamp"]))
        # e1 is 40 minutes behind max event time; retention is 35 minutes.
        self.assertEqual([item["event"]["id"] for item in retained], ["e2"])
        self.assertEqual(len(self.windows.history["u1"]), 1)

    def test_export_load_round_trip(self):
        for index in range(3):
            item = event(f"e{index}", index)
            self.windows.observe(item, "u1", parse_time(item["@timestamp"]))
        restored = FeatureWindows(lookback=timedelta(minutes=30), allowed_lateness=timedelta(minutes=5))
        restored.load(self.windows.export())
        self.assertEqual(restored.watermark, self.windows.watermark)
        self.assertEqual(restored.max_event_time, self.windows.max_event_time)
        self.assertEqual(
            [item["event"]["id"] for item in restored.history["u1"]],
            [item["event"]["id"] for item in self.windows.history["u1"]],
        )

    def test_invalid_bounds_rejected(self):
        with self.assertRaises(ValueError):
            FeatureWindows(lookback=timedelta(0), allowed_lateness=timedelta(minutes=5))


class BaselineModelTests(unittest.TestCase):
    def test_cold_start_is_valid_without_samples(self):
        model = BaselineModel("m1", "1.0.0", "f1", BaselineStatus.COLD_START)
        self.assertEqual(model.status, BaselineStatus.COLD_START)

    def test_ready_requires_samples_and_timestamp(self):
        with self.assertRaises(ValueError):
            BaselineModel("m1", "1.0.0", "f1", BaselineStatus.READY)
        with self.assertRaises(ValueError):
            BaselineModel("m1", "1.0.0", "f1", BaselineStatus.READY, sample_count=10)

    def test_unknown_status_rejected(self):
        with self.assertRaises(ValueError):
            BaselineModel("m1", "1.0.0", "f1", "bogus")


if __name__ == "__main__":
    unittest.main()
