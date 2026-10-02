import math
import unittest
from datetime import datetime, timedelta, timezone

from tuba_analysis.baseline import BaselineModel, BaselineStatus
from tuba_analysis.detection import (
    detect_baseline_deviation,
    detect_failure_burst,
    detect_failure_then_success,
)


def event(event_id, minute, outcome, organization="tenant_a", user="u1", second=0):
    return {
        "@timestamp": f"2026-09-23T02:{minute:02d}:{second:02d}Z",
        "organization": {"id": organization},
        "user": {"id": user},
        "event": {"id": event_id, "outcome": outcome},
        "ueba": {"quality": {"status": "qualified"}},
    }


def assert_five_elements(test_case, source, *, require_model=False):
    explanation = source["explanation"]
    test_case.assertTrue(explanation["summary"])
    test_case.assertTrue(explanation["reason_codes"])
    test_case.assertTrue(source["detection"]["threshold"])
    test_case.assertTrue(source["features"]["feature_version"])
    test_case.assertTrue(source["features"]["values"])
    test_case.assertTrue(source["detection"]["rule_id"])
    test_case.assertTrue(source["detection"]["rule_version"])
    if require_model:
        test_case.assertTrue(source["detection"]["model"]["model_version"])
    test_case.assertTrue(source["evidence"]["event_ids"])
    test_case.assertTrue(source["detection"]["window"]["start"])
    test_case.assertTrue(source["detection"]["window"]["end"])


class FailureThenSuccessTests(unittest.TestCase):
    def test_five_failures_then_success(self):
        events = [event(f"f{i}", i, "failure") for i in range(5)] + [event("s1", 5, "success")]
        result = detect_failure_then_success(list(reversed(events)), "tenant_a")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["_source"]["evidence"]["count"], 6)
        self.assertEqual(result, detect_failure_then_success(events + [events[0]], "tenant_a"))

    def test_finding_has_five_elements(self):
        events = [event(f"f{i}", i, "failure") for i in range(5)] + [event("s1", 5, "success")]
        source = detect_failure_then_success(events, "tenant_a")[0]["_source"]
        assert_five_elements(self, source)
        self.assertEqual(source["detection"]["threshold"]["failure_count"], 5)
        self.assertEqual(source["detection"]["rule_version"], "1.0.0")
        self.assertEqual(source["features"]["values"]["auth.failure_then_success.count"], 1)
        self.assertEqual(
            source["explanation"]["reason_codes"], ["AUTH_FAILURE_BURST_THEN_SUCCESS"]
        )

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


class FailureBurstTests(unittest.TestCase):
    def test_burst_fires_at_threshold(self):
        events = [event(f"f{i}", 1, "failure", second=i) for i in range(10)]
        result = detect_failure_burst(events, "tenant_a", threshold=10, window_seconds=300)
        self.assertEqual(len(result), 1)
        source = result[0]["_source"]
        assert_five_elements(self, source)
        self.assertEqual(source["detection"]["threshold"]["failure_count"], 10)
        self.assertEqual(source["detection"]["rule_id"], "auth.failure-burst")
        self.assertEqual(source["features"]["values"]["auth.failure.count"], 10)
        self.assertEqual(source["evidence"]["count"], 10)
        self.assertEqual(
            source["detection"]["window"]["start"], "2026-09-23T02:00:00Z"
        )

    def test_below_threshold_and_split_windows_do_not_fire(self):
        events = [event(f"f{i}", 1, "failure", second=i) for i in range(9)]
        self.assertEqual(
            detect_failure_burst(events, "tenant_a", threshold=10, window_seconds=300), []
        )
        # 6 failures in each of two adjacent windows: 12 total, neither window fires.
        split = [event(f"a{i}", 4, "failure", second=i) for i in range(6)] + [
            event(f"b{i}", 6, "failure", second=i) for i in range(6)
        ]
        self.assertEqual(
            detect_failure_burst(split, "tenant_a", threshold=10, window_seconds=300), []
        )

    def test_deterministic_and_deduplicated(self):
        events = [event(f"f{i}", 1, "failure", second=i) for i in range(10)]
        baseline = detect_failure_burst(events, "tenant_a", threshold=10, window_seconds=300)
        replayed = detect_failure_burst(
            list(reversed(events)) + [events[0]], "tenant_a", threshold=10, window_seconds=300
        )
        self.assertEqual(baseline, replayed)

    def test_fail_closed_guards(self):
        with self.assertRaises(ValueError):
            detect_failure_burst([], "tenant_a", threshold=0)
        with self.assertRaises(ValueError):
            detect_failure_burst(
                [event("x", 1, "failure", organization="tenant_b")], "tenant_a"
            )
        quarantined = event("q", 1, "failure")
        quarantined["ueba"]["quality"]["status"] = "quarantined"
        self.assertEqual(
            detect_failure_burst([quarantined], "tenant_a", threshold=1), []
        )


def ready_model(statistics, *, status=BaselineStatus.READY, feature_version="1.0.0"):
    return BaselineModel(
        model_id="auth-baseline",
        version="1.0.0",
        feature_id="auth.window",
        feature_version=feature_version,
        generation="g1",
        status=status,
        sample_count=100,
        trained_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
        statistics=statistics,
    )


def feature_record(values, *, feature_version="1.0.0"):
    return {
        "entity_id": "ent:u1",
        "feature_version": feature_version,
        "window": {"start": "2026-09-23T02:00:00Z", "end": "2026-09-23T02:10:00Z"},
        "values": values,
        "inputs": ["e1", "e2", "e3"],
    }


STATS = {
    "algorithm": "moments.v1",
    "feature_stats": {
        "auth.failure.count": {"count": 100, "mean": 4.0, "std": 1.0, "min": 0.0, "max": 8.0},
        "auth.attempt.count": {"count": 100, "mean": 10.0, "std": 2.0, "min": 4.0, "max": 16.0},
    },
}


class BaselineDeviationTests(unittest.TestCase):
    def test_fires_at_3sigma(self):
        record = feature_record({"auth.failure.count": 7.0, "auth.attempt.count": 10.0})
        result = detect_baseline_deviation(record, "tenant_a", ready_model(STATS))
        self.assertTrue(result["baseline"]["scored"])
        self.assertEqual(len(result["findings"]), 1)
        source = result["findings"][0]["_source"]
        assert_five_elements(self, source, require_model=True)
        self.assertEqual(source["detection"]["threshold"]["abs_z"], 3.0)
        self.assertEqual(source["detection"]["model"]["model_id"], "auth-baseline")
        self.assertEqual(source["detection"]["model"]["model_version"], "1.0.0")
        self.assertEqual(
            source["features"]["values"], {"auth.failure.count": 7.0}
        )
        self.assertEqual(source["detection"]["model"]["baseline"]["auth.failure.count"]["abs_z"], 3.0)
        self.assertEqual(source["explanation"]["reason_codes"], ["AUTH_BASELINE_DEVIATION"])
        self.assertIn("auth.failure.count", source["explanation"]["summary"])
        self.assertEqual(source["evidence"]["event_ids"], ["e1", "e2", "e3"])

    def test_below_threshold_does_not_fire(self):
        record = feature_record({"auth.failure.count": 6.9, "auth.attempt.count": 10.0})
        result = detect_baseline_deviation(record, "tenant_a", ready_model(STATS))
        self.assertEqual(result["findings"], [])
        self.assertTrue(result["baseline"]["scored"])

    def test_zero_variance_semantics(self):
        stats = {
            "algorithm": "moments.v1",
            "feature_stats": {
                "auth.source_device.count": {"count": 100, "mean": 1.0, "std": 0.0, "min": 1.0, "max": 1.0},
            },
        }
        equal = detect_baseline_deviation(
            feature_record({"auth.source_device.count": 1.0}), "tenant_a", ready_model(stats)
        )
        self.assertEqual(equal["findings"], [])
        deviated = detect_baseline_deviation(
            feature_record({"auth.source_device.count": 3.0}), "tenant_a", ready_model(stats)
        )
        self.assertEqual(len(deviated["findings"]), 1)
        source = deviated["findings"][0]["_source"]
        self.assertEqual(
            source["detection"]["model"]["baseline"]["auth.source_device.count"]["abs_z"],
            math.inf,
        )
        self.assertEqual(source["anomaly"]["score"], 1.0)

    def test_cold_start_never_scores_and_says_so(self):
        model = BaselineModel(
            model_id="auth-baseline",
            version="1.0.0",
            feature_id="auth.window",
            feature_version="1.0.0",
            generation="g1",
            status=BaselineStatus.COLD_START,
            sample_count=12,
            complete_days=2,
        )
        record = feature_record({"auth.failure.count": 100.0})
        result = detect_baseline_deviation(record, "tenant_a", model)
        self.assertEqual(result["findings"], [])
        self.assertFalse(result["baseline"]["scored"])
        self.assertEqual(result["baseline"]["reason"], "baseline_not_ready:cold_start")
        self.assertEqual(result["baseline"]["model_version"], "1.0.0")

    def test_training_and_retired_never_score(self):
        for status in (BaselineStatus.TRAINING, BaselineStatus.RETIRED):
            model = ready_model(STATS, status=status)
            result = detect_baseline_deviation(
                feature_record({"auth.failure.count": 100.0, "auth.attempt.count": 10.0}),
                "tenant_a",
                model,
            )
            self.assertEqual(result["findings"], [])
            self.assertEqual(result["baseline"]["reason"], f"baseline_not_ready:{status.value}")

    def test_fail_closed_contract(self):
        with self.assertRaises(ValueError):
            detect_baseline_deviation(
                feature_record({"auth.failure.count": 1.0}, feature_version="2.0.0"),
                "tenant_a",
                ready_model(STATS),
            )
        with self.assertRaises(ValueError):
            detect_baseline_deviation(
                feature_record({"auth.attempt.count": 10.0}), "tenant_a", ready_model(STATS)
            )
        with self.assertRaises(ValueError):
            detect_baseline_deviation(
                feature_record({"auth.failure.count": 7.0, "auth.attempt.count": 10.0}),
                "tenant_a",
                ready_model({"algorithm": "moments.v1", "feature_stats": {}}),
            )
        with self.assertRaises(ValueError):
            detect_baseline_deviation(
                feature_record({"auth.failure.count": 7.0, "auth.attempt.count": 10.0}),
                "tenant_a",
                ready_model(STATS),
                z_threshold=0,
            )

    def test_deterministic_finding_id(self):
        record = feature_record({"auth.failure.count": 7.0, "auth.attempt.count": 10.0})
        first = detect_baseline_deviation(record, "tenant_a", ready_model(STATS))
        second = detect_baseline_deviation(dict(record), "tenant_a", ready_model(STATS))
        self.assertEqual(first, second)
        other_window = feature_record({"auth.failure.count": 7.0, "auth.attempt.count": 10.0})
        other_window["window"] = {
            "start": "2026-09-23T03:00:00Z",
            "end": "2026-09-23T03:10:00Z",
        }
        third = detect_baseline_deviation(other_window, "tenant_a", ready_model(STATS))
        self.assertNotEqual(first["findings"][0]["_id"], third["findings"][0]["_id"])


if __name__ == "__main__":
    unittest.main()


class GenerationBusinessKeyTests(unittest.TestCase):
    """F06: generation is a mandatory business-key part of every finding."""

    def burst_events(self, count=10):
        return [event(f"f{i}", i % 5, "failure", second=i) for i in range(count)]

    def test_finding_carries_generation_default_g1(self):
        for source in (
            detect_failure_burst(self.burst_events(), "tenant_a", threshold=10)[0]["_source"],
            detect_failure_then_success(
                [event(f"f{i}", i, "failure") for i in range(5)] + [event("s1", 5, "success")],
                "tenant_a",
            )[0]["_source"],
        ):
            self.assertEqual(source["anomaly"]["generation"], "g1")

    def test_generation_changes_business_key_same_window(self):
        g1 = detect_failure_burst(self.burst_events(), "tenant_a", threshold=10, generation="g1")
        g2 = detect_failure_burst(self.burst_events(), "tenant_a", threshold=10, generation="g2")
        self.assertNotEqual(g1[0]["_id"], g2[0]["_id"])  # new generation never overwrites old
        self.assertEqual(g1[0]["_source"]["anomaly"]["generation"], "g1")
        self.assertEqual(g2[0]["_source"]["anomaly"]["generation"], "g2")
        # Deterministic per generation.
        self.assertEqual(g1, detect_failure_burst(self.burst_events(), "tenant_a", threshold=10, generation="g1"))

    def test_statistical_finding_carries_generation(self):
        model = ready_model(
            {"algorithm": "moments.v1", "feature_stats": {"auth.failure.count": {"mean": 4.0, "std": 2.0, "count": 10, "min": 0.0, "max": 8.0}}}
        )
        record = feature_record({"auth.failure.count": 10.0})
        g1 = detect_baseline_deviation(record, "tenant_a", model, generation="g1")
        g2 = detect_baseline_deviation(record, "tenant_a", model, generation="g2")
        self.assertNotEqual(g1["findings"][0]["_id"], g2["findings"][0]["_id"])
        self.assertEqual(g2["findings"][0]["_source"]["anomaly"]["generation"], "g2")

    def test_empty_generation_fail_closed(self):
        with self.assertRaises(ValueError):
            detect_failure_burst(self.burst_events(), "tenant_a", threshold=10, generation="")
