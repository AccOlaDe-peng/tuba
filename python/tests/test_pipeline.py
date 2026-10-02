import unittest
from datetime import datetime, timedelta, timezone

from tuba_analysis.baseline import (
    BaselineModel,
    BaselineStatus,
    FeatureSample,
    ImmutableModelConflict,
    select_training_samples,
)
from tuba_analysis.pipeline import (
    AttributedAnalysisProcessor,
    AttributedContractError,
    TrainingScheduler,
    baseline_envelope,
    run_due_training,
    training_model_version,
)
from tuba_analysis.registry import RegistryError, load_registry

ORG = "tenant_a"
NS = "tenant_a"
ENTITY = "ent:" + "a" * 64
RECEIVED = datetime(2026, 10, 12, 1, 0, tzinfo=timezone.utc)

FEATURE_NAMES = [
    "auth.attempt.count",
    "auth.failure.count",
    "auth.failure.rate",
    "auth.source_device.count",
    "auth.source_ip.count",
    "auth.failure_then_success.count",
]


def attribution_id(sequence):
    return "att:" + ("%064x" % sequence)


def contribution(sequence, event_id, minute, outcome, **overrides):
    raw = {
        "schema_version": "1.0.0",
        "organization_id": ORG,
        "event_id": event_id,
        "event_time": f"2026-10-12T00:{minute:02d}:00Z",
        "domain": "authentication",
        "attribution_id": attribution_id(sequence),
        "role": "actor",
        "state": "resolved",
        "entity_id": ENTITY,
        "confidence": 1.0,
        "rule_version": "1.0.0",
        "partition_key": f"{ORG}:{ENTITY}",
        "evidence": {
            "identifiers": [],
            "adjudication": ["resolved_by_strong_identifier"],
            "event": {"outcome": outcome, "quality": "qualified"},
        },
    }
    raw.update(overrides)
    return raw


class FakeSampleStore:
    def __init__(self, samples=None):
        self.saved = []
        self._samples = list(samples or [])

    def save(self, organization, feature_id, sample):
        self.saved.append((organization, feature_id, sample))
        return "inserted"

    def training_samples(self, organization, feature_id, *, feature_version, generation, deadline):
        return select_training_samples(
            self._samples,
            deadline=deadline,
            feature_version=feature_version,
            generation=generation,
        )


class FakeModelStore:
    def __init__(self, seed=None):
        self.rows = {}
        self.seed = seed

    def latest(self, organization, model_id):
        candidates = [model for (mid, _), (_, _, model) in self.rows.items() if mid == model_id]
        if not candidates:
            return self.seed
        return max(candidates, key=lambda model: model.version)

    def publish(self, organization, model):
        digest = model.content_hash()
        row = self.rows.get((model.model_id, model.version))
        if row is not None:
            if row[0] == digest:
                return "idempotent"
            raise ImmutableModelConflict("same version different content")
        self.rows[(model.model_id, model.version)] = (digest, model.status.value, model)
        return "published"

    def record_cold_start(self, organization, model):
        digest = model.content_hash()
        row = self.rows.get((model.model_id, model.version))
        if row is None:
            self.rows[(model.model_id, model.version)] = (digest, model.status.value, model)
            return "published"
        if row[1] != BaselineStatus.COLD_START.value:
            raise ImmutableModelConflict("cold_start cannot overwrite a published model")
        if row[0] == digest:
            return "idempotent"
        self.rows[(model.model_id, model.version)] = (digest, model.status.value, model)
        return "refreshed"

    def retire(self, organization, model_id, version):
        digest, _, model = self.rows[(model_id, version)]
        self.rows[(model_id, version)] = (digest, BaselineStatus.RETIRED.value, model)
        return "retired"


def ready_model():
    stats = {}
    for name in FEATURE_NAMES:
        stats[name] = {"count": 100, "mean": 5.0, "std": 1.0, "min": 0.0, "max": 9.0}
    # Observed values of the golden window below: attempts 6, failures 5,
    # rate 5/6, devices 0, ips 0, failure-then-success 1. Pin every feature at
    # its observed value (z=0) except attempts, which deviates wildly.
    observed = {
        "auth.attempt.count": 6.0,
        "auth.failure.count": 5.0,
        "auth.failure.rate": 5.0 / 6.0,
        "auth.source_device.count": 0.0,
        "auth.source_ip.count": 0.0,
        "auth.failure_then_success.count": 1.0,
    }
    for name, value in observed.items():
        stats[name]["mean"] = 100.0 if name == "auth.attempt.count" else value
    return BaselineModel(
        model_id="auth.baseline",
        version="1.0.20261011000000",
        feature_id="authentication.failure_burst",
        feature_version="1.0.0",
        generation="g1",
        status=BaselineStatus.READY,
        sample_count=100,
        trained_at=datetime(2026, 10, 11, tzinfo=timezone.utc),
        statistics={"algorithm": "moments.v1", "feature_stats": stats},
    )


def new_processor(sample_store=None, model_store=None):
    return AttributedAnalysisProcessor(
        ORG,
        NS,
        load_registry(),
        sample_store=sample_store,
        model_store=model_store,
    )


def feed(processor, contributions, run_id="run-1"):
    envelopes = []
    for raw in contributions:
        envelopes.extend(processor.process(raw, run_id, received_at=RECEIVED))
    return envelopes


def five_failures_then_success(offset=0):
    raws = [contribution(offset + i, f"f{i}", i, "failure") for i in range(5)]
    raws.append(contribution(offset + 5, "s1", 5, "success"))
    return raws


class RegistryTests(unittest.TestCase):
    def test_three_scenarios_and_model_registered(self):
        registry = load_registry()
        self.assertEqual(registry.rule("auth.failure-then-success").algorithm, "failure_then_success")
        burst = registry.rule("auth.failure-burst")
        self.assertEqual(burst.algorithm, "failure_burst")
        self.assertEqual(burst.threshold, 10)
        self.assertEqual(burst.window_seconds, 300)
        deviation = registry.rule("auth.baseline-deviation")
        self.assertEqual(deviation.algorithm, "baseline_deviation")
        self.assertEqual(deviation.model_id, "auth.baseline")
        self.assertEqual(deviation.z_threshold, 3.0)
        model = registry.model("auth.baseline")
        self.assertEqual(model.feature_version, "1.0.0")
        self.assertEqual(model.generation, "g1")
        self.assertEqual(model.training_interval_seconds, 86400)
        self.assertEqual(model.min_samples, 100)
        self.assertEqual(model.min_complete_days, 14)

    def test_baseline_deviation_requires_known_model(self):
        import json
        import tempfile
        from pathlib import Path

        document = {
            "registry_version": "1.0.0",
            "features": [{"id": "f", "version": "1.0.0"}],
            "detections": [
                {
                    "id": "d",
                    "version": "1.0.0",
                    "algorithm": "baseline_deviation",
                    "feature_id": "f",
                    "severity": "low",
                    "parameters": {
                        "z_threshold": 3.0,
                        "lookback_seconds": 60,
                        "window_seconds": 60,
                        "allowed_lateness_seconds": 60,
                    },
                }
            ],
            "models": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(RegistryError):
                load_registry(path)


class PipelineTests(unittest.TestCase):
    def test_end_to_end_attributed_to_v2_three_scenarios(self):
        samples = FakeSampleStore()
        processor = new_processor(sample_store=samples, model_store=FakeModelStore(seed=ready_model()))
        raws = five_failures_then_success()
        # Ten failures inside one 5-minute burst window [00:10, 00:15).
        for index in range(10):
            raws.append(contribution(100 + index, f"b{index}", 10 + index // 2, "failure"))
        # Watermark must reach 00:10 to close the [00:00, 00:10) feature window.
        raws.append(contribution(200, "tail", 15, "failure"))

        envelopes = feed(processor, raws)
        anomalies = [e for e in envelopes if e["object_type"] == "anomaly"]
        features = [e for e in envelopes if e["object_type"] == "feature"]
        self.assertEqual(
            {e["rule_id"] for e in anomalies},
            {"auth.failure-then-success", "auth.failure-burst", "auth.baseline-deviation"},
        )
        for envelope in envelopes:
            self.assertEqual(envelope["contract_version"], "2.0.0")
            self.assertEqual(envelope["organization_id"], ORG)
            self.assertEqual(envelope["namespace"], NS)
            self.assertEqual(envelope["run_id"], "run-1")
            self.assertGreaterEqual(envelope["revision"], 1)
            self.assertEqual(envelope["generation"], "g1")
            self.assertEqual(envelope["date_key"], "2026-10-12")
            self.assertTrue(envelope["window_start"].startswith(envelope["date_key"]))
            self.assertIsNotNone(envelope["input_refs"])
        for envelope in anomalies:
            document = envelope["document"]
            # Five mandatory elements.
            self.assertTrue(document["explanation"]["summary"])
            self.assertTrue(document["explanation"]["reason_codes"])
            self.assertTrue(document["detection"]["threshold"])
            self.assertTrue(document["features"]["feature_version"])
            self.assertTrue(document["features"]["values"])
            self.assertEqual(document["detection"]["rule_id"], envelope["rule_id"])
            self.assertTrue(document["detection"]["rule_version"])
            self.assertTrue(document["evidence"]["event_ids"])
            self.assertTrue(document["detection"]["window"]["start"])
            self.assertTrue(document["detection"]["window"]["end"])
            self.assertEqual(envelope["operation"], "upsert")
        deviation = [e for e in anomalies if e["rule_id"] == "auth.baseline-deviation"]
        self.assertEqual(len(deviation), 1)
        self.assertEqual(deviation[0]["document"]["detection"]["model"]["model_version"], "1.0.20261011000000")
        # The closed window produced exactly one feature record and one
        # persisted training sample (F04 input).
        self.assertEqual(len(features), 1)
        self.assertEqual(features[0]["document"]["window"]["start"], "2026-10-12T00:00:00Z")
        self.assertEqual(len(samples.saved), 1)
        _, feature_id, sample = samples.saved[0]
        self.assertEqual(feature_id, "authentication.failure_burst")
        self.assertEqual(sample.generation, "g1")
        self.assertEqual(sample.revision, 1)

    def test_duplicate_attribution_never_double_emits(self):
        processor = new_processor()
        raws = five_failures_then_success()
        first = feed(processor, raws)
        self.assertEqual(len([e for e in first if e["object_type"] == "anomaly"]), 1)
        second = feed(processor, raws)
        self.assertEqual(second, [])

    def test_cold_start_baseline_never_scores(self):
        processor = new_processor(model_store=FakeModelStore())
        raws = five_failures_then_success()
        raws.append(contribution(200, "tail", 15, "failure"))
        envelopes = feed(processor, raws)
        self.assertEqual(
            {e["rule_id"] for e in envelopes if e["object_type"] == "anomaly"},
            {"auth.failure-then-success"},
        )
        self.assertEqual(len([e for e in envelopes if e["object_type"] == "feature"]), 1)
        baseline = processor.last_baseline
        self.assertIsNotNone(baseline)
        self.assertFalse(baseline["scored"])
        self.assertEqual(baseline["reason"], "baseline_not_ready:cold_start")

    def test_late_correction_increments_feature_and_finding_revision(self):
        processor = new_processor(model_store=FakeModelStore())
        raws = five_failures_then_success()
        raws.append(contribution(200, "tail", 15, "failure"))
        first = feed(processor, raws)
        first_features = [e for e in first if e["object_type"] == "feature"]
        self.assertEqual(len(first_features), 1)
        self.assertEqual(first_features[0]["revision"], 1)

        # A late failure inside the closed window [00:00, 00:10) — still
        # inside the retention boundary — recomputes the same window.
        second = feed(processor, [contribution(300, "late-f", 1, "failure")])
        second_features = [e for e in second if e["object_type"] == "feature"]
        self.assertEqual(len(second_features), 1)
        self.assertEqual(second_features[0]["object_id"], first_features[0]["object_id"])
        self.assertEqual(second_features[0]["revision"], 2)
        self.assertEqual(second_features[0]["document"]["values"]["auth.failure.count"], 6)
        # The corrected failure-then-success finding is the next revision of
        # the same business key (6 failures now precede the success).
        fts = [e for e in second if e["object_type"] == "anomaly" and e["rule_id"] == "auth.failure-then-success"]
        self.assertEqual(len(fts), 1)
        self.assertEqual(fts[0]["revision"], 2)
        self.assertIn("late-f", fts[0]["document"]["evidence"]["event_ids"])

    def test_unresolved_and_non_actor_contributions_are_counted_not_scored(self):
        processor = new_processor()
        unresolved = contribution(
            1,
            "u1",
            0,
            "failure",
            state="unresolved",
            reason="no_matching_entity",
            partition_key=f"{ORG}:unresolved:unresolved:no_matching_entity",
        )
        del unresolved["entity_id"]
        target = contribution(2, "t1", 1, "failure", role="target", entity_id="ent:" + "b" * 64,
                              partition_key=f"{ORG}:{'ent:' + 'b' * 64}")
        self.assertEqual(feed(processor, [unresolved, target]), [])
        self.assertEqual(processor.counters["skipped_unresolved"], 1)
        self.assertEqual(processor.counters["skipped_non_actor_role"], 1)

    def test_fail_closed_inputs(self):
        processor = new_processor()
        good = contribution(1, "e1", 0, "failure")
        cases = [
            {**good, "organization_id": "tenant_b"},
            {**good, "schema_version": "9.9.9"},
            {**good, "entity_id": "user-1"},
            {**good, "state": "unresolved", "reason": "no_matching_entity"},  # still carries entity_id
            {**good, "attribution_id": "x1"},
            {**good, "role": "nobody"},
            {**good, "evidence": {"event": {}}},
        ]
        for raw in cases:
            with self.assertRaises(AttributedContractError, msg=str(raw)):
                processor.process(raw, "run-1", received_at=RECEIVED)

    def test_state_round_trip_preserves_ledger_and_windows(self):
        processor = new_processor()
        first = feed(processor, five_failures_then_success())
        self.assertEqual(len([e for e in first if e["object_type"] == "anomaly"]), 1)
        restored = AttributedAnalysisProcessor.from_state(
            ORG, NS, load_registry(), processor.export_state()
        )
        self.assertEqual(feed(restored, five_failures_then_success()), [])
        with self.assertRaises(ValueError):
            AttributedAnalysisProcessor.from_state(ORG, NS, load_registry(), {"state_version": 99})


class TrainingSchedulerTests(unittest.TestCase):
    def test_cadence(self):
        registry = load_registry()
        scheduler = TrainingScheduler(registry.models.values())
        now = datetime(2026, 10, 12, 0, 30, tzinfo=timezone.utc)
        due = scheduler.due(now)
        self.assertEqual([model.id for model in due], ["auth.baseline"])
        scheduler.record("auth.baseline", now)
        self.assertEqual(scheduler.due(now + timedelta(hours=1)), [])
        self.assertEqual([m.id for m in scheduler.due(now + timedelta(days=1))], ["auth.baseline"])
        clone = TrainingScheduler(registry.models.values())
        clone.load(scheduler.export())
        self.assertEqual(clone.due(now + timedelta(hours=1)), [])

    def test_model_version_is_deterministic_semver(self):
        model = load_registry().model("auth.baseline")
        cutoff = datetime(2026, 10, 12, 0, 30, tzinfo=timezone.utc)
        version = training_model_version(model, cutoff)
        self.assertEqual(version, "1.0.20261012003000")
        self.assertRegex(version, r"^[0-9]+\.[0-9]+\.[0-9]+$")

    def _samples(self, per_day, days, start=datetime(2026, 9, 25, tzinfo=timezone.utc)):
        samples = []
        for day in range(days):
            for index in range(per_day):
                window_start = start + timedelta(days=day, hours=index)
                samples.append(
                    FeatureSample(
                        entity_id=ENTITY,
                        feature_version="1.0.0",
                        generation="g1",
                        window_start=window_start,
                        window_end=window_start + timedelta(minutes=10),
                        revision=1,
                        quality="qualified",
                        values={name: 1.0 for name in FEATURE_NAMES},
                        inputs=["evt:x"],
                    )
                )
        return samples

    def test_run_due_training_cold_start_no_fabricated_model(self):
        registry = load_registry()
        scheduler = TrainingScheduler(registry.models.values())
        model_store = FakeModelStore()
        now = datetime(2026, 10, 12, 0, 30, tzinfo=timezone.utc)
        envelopes = run_due_training(
            scheduler,
            FakeSampleStore(self._samples(per_day=3, days=2)),
            model_store,
            organization=ORG,
            namespace=NS,
            now=now,
            run_id="run-1",
        )
        self.assertEqual(envelopes, [])
        statuses = {status for _, status, _ in model_store.rows.values()}
        self.assertEqual(statuses, {"cold_start"})
        self.assertEqual(scheduler.due(now + timedelta(hours=1)), [])

    def test_run_due_training_publishes_immutable_baseline_object(self):
        registry = load_registry()
        model = registry.model("auth.baseline")
        model_store = FakeModelStore()
        now = datetime(2026, 10, 12, 0, 30, tzinfo=timezone.utc)
        # 15 complete UTC days x 7 samples = 105 samples, policy 14 days/100.
        samples = FakeSampleStore(self._samples(per_day=7, days=15))
        scheduler = TrainingScheduler(registry.models.values())
        envelopes = run_due_training(
            scheduler,
            samples,
            model_store,
            organization=ORG,
            namespace=NS,
            now=now,
            run_id="run-1",
        )
        self.assertEqual(len(envelopes), 1)
        envelope = envelopes[0]
        self.assertEqual(envelope["contract_version"], "2.0.0")
        self.assertEqual(envelope["object_type"], "baseline")
        self.assertEqual(envelope["revision"], 1)
        self.assertEqual(envelope["operation"], "upsert")
        self.assertEqual(envelope["generation"], "g1")
        self.assertEqual(envelope["rule_version"], "1.0.20261012003000")
        self.assertEqual(envelope["date_key"], "2026-09-25")
        self.assertTrue(envelope["window_start"].startswith("2026-09-25"))
        self.assertEqual(envelope["input_refs"], [])
        document = envelope["document"]
        self.assertEqual(document["status"], "ready")
        self.assertEqual(document["training_cutoff"], "2026-10-12T00:30:00Z")
        self.assertEqual(document["metrics"]["decision"]["reason"], "ok")
        self.assertEqual(document["sample_range"]["entities"], [ENTITY])

        # A replayed run at the same cutoff is idempotent and emits nothing.
        replay = TrainingScheduler(registry.models.values())
        again = run_due_training(
            replay,
            samples,
            model_store,
            organization=ORG,
            namespace=NS,
            now=now,
            run_id="run-2",
        )
        self.assertEqual(again, [])
        self.assertEqual(len(model_store.rows), 1)

    def test_baseline_envelope_only_for_fresh_ready_publication(self):
        model = load_registry().model("auth.baseline")
        cutoff = datetime(2026, 10, 12, tzinfo=timezone.utc)
        self.assertIsNone(
            baseline_envelope(
                {"status": "cold_start", "publish": "published", "model_version": "1.0.0"},
                model_definition=model,
                organization=ORG,
                namespace=NS,
                run_id="run-1",
                cutoff=cutoff,
            )
        )


if __name__ == "__main__":
    unittest.main()
