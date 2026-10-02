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


def event_at(event_id, timestamp, user="u1"):
    return {
        "@timestamp": timestamp,
        "organization": {"id": "tenant_a"},
        "user": {"id": user},
        "event": {"id": event_id, "outcome": "failure"},
    }


class FeatureWindowsF02Tests(unittest.TestCase):
    def setUp(self):
        self.windows = FeatureWindows(
            lookback=timedelta(minutes=30),
            allowed_lateness=timedelta(minutes=10),
            idle_timeout=timedelta(minutes=5),
            future_time_limit=timedelta(minutes=5),
            max_events_per_entity=3,
            max_entities=2,
        )

    def observe(self, event_id, timestamp, entity="u1", **kwargs):
        item = event_at(event_id, timestamp, user=entity)
        return self.windows.observe(item, entity, parse_time(timestamp), **kwargs)

    def test_per_entity_watermark_independent(self):
        self.observe("a1", "2026-10-12T02:30:00Z", entity="ua")
        self.observe("b1", "2026-10-12T02:05:00Z", entity="ub")
        # Each entity's watermark trails its own max event time; the global
        # watermark is the minimum.
        self.assertEqual(self.windows.watermark, parse_time("2026-10-12T01:55:00Z"))
        # 02:12 is late for ua (watermark 02:20) but not for ub.
        late, _ = self.observe("a2", "2026-10-12T02:12:00Z", entity="ua")
        self.assertTrue(late)
        late, _ = self.observe("b2", "2026-10-12T02:12:00Z", entity="ub")
        self.assertFalse(late)

    def test_idle_partition_watermark_advances(self):
        self.observe(
            "a1",
            "2026-10-12T02:02:00Z",
            received_at=parse_time("2026-10-12T02:02:00Z"),
        )
        # Before the idle timeout nothing changes.
        self.windows.advance(parse_time("2026-10-12T02:06:00Z"))
        self.assertEqual(self.windows.watermark, parse_time("2026-10-12T01:52:00Z"))
        # After the idle timeout the watermark advances from processing
        # time, and genuinely old data is then flagged late.
        self.windows.advance(parse_time("2026-10-12T02:20:00Z"))
        self.assertEqual(self.windows.watermark, parse_time("2026-10-12T02:10:00Z"))
        late, _ = self.observe(
            "a2",
            "2026-10-12T02:05:00Z",
            received_at=parse_time("2026-10-12T02:21:00Z"),
        )
        self.assertTrue(late)

    def test_future_time_guard_rejects_and_counts(self):
        with self.assertRaises(ValueError):
            self.observe(
                "forged",
                "2026-10-12T08:00:00Z",
                received_at=parse_time("2026-10-12T02:00:00Z"),
            )
        self.assertEqual(self.windows.counters["future_rejected"], 1)
        # State is not polluted: the entity was never admitted.
        self.assertNotIn("u1", self.windows.entity_max)
        # Exactly at the limit is accepted.
        late, _ = self.observe(
            "ok",
            "2026-10-12T02:05:00Z",
            received_at=parse_time("2026-10-12T02:00:00Z"),
        )
        self.assertFalse(late)

    def test_dedup_key_repeat_delivery_counted_once(self):
        first = event_at("e1", "2026-10-12T02:00:00Z")
        at = parse_time("2026-10-12T02:00:00Z")
        self.windows.observe(first, "u1", at, dedup_key="att:1")
        self.windows.observe(first, "u1", at, dedup_key="att:1")
        self.windows.observe(first, "u1", at, dedup_key="att:2")
        self.assertEqual(len(self.windows.history["u1"]), 2)
        self.assertEqual(self.windows.counters["duplicates"], 1)

    def test_event_cap_evicts_oldest_counted(self):
        for index in range(4):
            self.observe(f"e{index}", f"2026-10-12T02:{index:02d}:00Z")
        self.assertEqual(
            [item["event"]["id"] for item in self.windows.history["u1"]],
            ["e1", "e2", "e3"],
        )
        self.assertEqual(self.windows.counters["evicted"], 1)

    def test_entity_cap_evicts_idle_shell_rejects_when_all_active(self):
        self.observe("a1", "2026-10-12T02:00:00Z", entity="ua")
        self.observe("b1", "2026-10-12T02:00:00Z", entity="ub")
        # Both entities hold history: a third is rejected fail-closed.
        with self.assertRaises(ValueError):
            self.observe("c1", "2026-10-12T02:00:00Z", entity="uc")
        self.assertEqual(self.windows.counters["capacity_rejected"], 1)
        # Drain ua beyond its retention, then it becomes an evictable shell.
        self.observe("a2", "2026-10-12T03:00:00Z", entity="ua")
        self.assertEqual([item["event"]["id"] for item in self.windows.history["ua"]], ["a2"])
        self.windows.history["ua"].clear()
        self.observe("c1", "2026-10-12T03:00:00Z", entity="uc")
        self.assertEqual(self.windows.counters["entities_pruned"], 1)
        self.assertIn("uc", self.windows.entity_max)
        self.assertIn("ub", self.windows.entity_max)

    def test_export_load_round_trip_preserves_f02_state(self):
        at = parse_time("2026-10-12T02:00:00Z")
        self.observe("a1", "2026-10-12T02:00:00Z", received_at=at)
        self.observe("a1", "2026-10-12T02:00:00Z", received_at=at)  # duplicate
        self.windows.advance(parse_time("2026-10-12T02:20:00Z"))
        restored = FeatureWindows(lookback=timedelta(minutes=30))
        restored.load(self.windows.export())
        self.assertEqual(restored.watermark, self.windows.watermark)
        self.assertEqual(restored.entity_max, self.windows.entity_max)
        self.assertEqual(restored.idle_floor, self.windows.idle_floor)
        self.assertEqual(dict(restored.counters), dict(self.windows.counters))
        # Dedup knowledge survives the restart.
        restored.observe(event_at("a1", "2026-10-12T02:00:00Z"), "u1", parse_time("2026-10-12T02:00:00Z"))
        self.assertEqual(restored.counters["duplicates"], self.windows.counters["duplicates"] + 1)

    def test_load_pre_f02_state_backward_compatible(self):
        legacy = {
            "watermark": "2026-09-23T02:25:00Z",
            "max_event_time": "2026-09-23T02:30:00Z",
            "history": {"u1": [event_at("e1", "2026-09-23T02:30:00Z")]},
        }
        restored = FeatureWindows(lookback=timedelta(minutes=30), allowed_lateness=timedelta(minutes=5))
        restored.load(legacy)
        self.assertEqual(restored.entity_max["u1"], parse_time("2026-09-23T02:30:00Z"))
        self.assertEqual(restored.watermark, parse_time("2026-09-23T02:25:00Z"))

    def test_invalid_bounds_rejected_fail_closed(self):
        for kwargs in (
            {"lookback": timedelta(minutes=30), "idle_timeout": timedelta(0)},
            {"lookback": timedelta(minutes=30), "future_time_limit": timedelta(0)},
            {"lookback": timedelta(minutes=30), "max_events_per_entity": 0},
            {"lookback": timedelta(minutes=30), "max_entities": 0},
        ):
            with self.assertRaises(ValueError):
                FeatureWindows(**kwargs)
