"""F04 baseline tests: pure-core unit tests plus PG integration tests.

The integration tests are gated on TUBA_TEST_DATABASE_URL and use a
throwaway f04_it_* organization that is deleted afterwards.
"""

from __future__ import annotations

import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from tuba_analysis.baseline import (
    BaselineModel,
    BaselineStatus,
    FeatureSample,
    ImmutableModelConflict,
    PostgresBaselineModelStore,
    PostgresFeatureSampleStore,
    RevisionConflict,
    TrainingPolicy,
    complete_days,
    evaluate_baseline,
    run_training_job,
    sample_range,
    select_training_samples,
    train_baseline,
    training_decision,
)

UTC = timezone.utc
DEADLINE = datetime(2026, 10, 15, 0, 0, tzinfo=UTC)


def make_sample(
    entity: str,
    day: int,
    hour: int = 0,
    *,
    revision: int = 1,
    quality: str = "qualified",
    feature_version: str = "1.0.0",
    generation: str = "g1",
    attempts: float = 4.0,
) -> FeatureSample:
    start = datetime(2026, 9, 1, tzinfo=UTC) + timedelta(days=day, hours=hour)
    return FeatureSample(
        entity_id=entity,
        feature_version=feature_version,
        generation=generation,
        window_start=start,
        window_end=start + timedelta(minutes=10),
        revision=revision,
        quality=quality,
        values={
            "auth.attempt.count": attempts,
            "auth.failure.count": attempts / 2,
            "auth.failure.rate": 0.5,
        },
        inputs=[f"evt-{entity}-{day}-{hour}"],
    )


class MemorySampleStore:
    def __init__(self, samples):
        self._samples = list(samples)

    def training_samples(self, organization, feature_id, *, feature_version, generation, deadline):
        return select_training_samples(
            self._samples, deadline=deadline, feature_version=feature_version, generation=generation
        )


class MemoryModelStore:
    def __init__(self):
        self.rows = {}

    def latest(self, organization, model_id):
        versions = [v for (o, m, v) in self.rows if o == organization and m == model_id]
        if not versions:
            return None
        return self.rows[(organization, model_id, sorted(versions)[-1])]

    def record_cold_start(self, organization, model):
        key = (organization, model.model_id, model.version)
        existing = self.rows.get(key)
        if existing is not None and existing.status != BaselineStatus.COLD_START:
            raise ImmutableModelConflict("cannot overwrite non-cold_start row")
        self.rows[key] = model
        return "published" if existing is None else "refreshed"

    def publish(self, organization, model):
        key = (organization, model.model_id, model.version)
        existing = self.rows.get(key)
        if existing is not None:
            if existing.content_hash() == model.content_hash():
                return "idempotent"
            raise ImmutableModelConflict("same version different content")
        self.rows[key] = model
        return "published"

    def retire(self, organization, model_id, version):
        model = self.rows[(organization, model_id, version)]
        object.__setattr__(model, "status", BaselineStatus.RETIRED)
        return "retired"


class SelectionTests(unittest.TestCase):
    def test_filters_version_generation_quality_and_deadline(self):
        samples = [
            make_sample("ent:a", 0),
            make_sample("ent:a", 1, feature_version="2.0.0"),  # wrong feature version
            make_sample("ent:a", 2, generation="g2"),  # wrong generation
            make_sample("ent:a", 3, quality="partial"),  # not qualified
            make_sample("ent:a", 44),  # window ends after the deadline
        ]
        selected = select_training_samples(
            samples, deadline=DEADLINE, feature_version="1.0.0", generation="g1"
        )
        self.assertEqual([s.window_start.day for s in selected], [1])

    def test_latest_revision_wins(self):
        old = make_sample("ent:a", 0, revision=1, attempts=4.0)
        corrected = make_sample("ent:a", 0, revision=2, attempts=9.0)
        selected = select_training_samples(
            [old, corrected], deadline=DEADLINE, feature_version="1.0.0", generation="g1"
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].revision, 2)
        self.assertEqual(selected[0].values["auth.attempt.count"], 9.0)

    def test_deadline_boundary_inclusive(self):
        edge = FeatureSample(
            entity_id="ent:a",
            feature_version="1.0.0",
            generation="g1",
            window_start=DEADLINE - timedelta(minutes=10),
            window_end=DEADLINE,
            revision=1,
            quality="qualified",
            values={"auth.attempt.count": 1.0},
            inputs=["e"],
        )
        selected = select_training_samples(
            [edge], deadline=DEADLINE, feature_version="1.0.0", generation="g1"
        )
        self.assertEqual(len(selected), 1)
        later = FeatureSample(
            entity_id="ent:a",
            feature_version="1.0.0",
            generation="g1",
            window_start=DEADLINE,
            window_end=DEADLINE + timedelta(minutes=10),
            revision=1,
            quality="qualified",
            values={"auth.attempt.count": 1.0},
            inputs=["e2"],
        )
        self.assertEqual(
            select_training_samples([later], deadline=DEADLINE, feature_version="1.0.0", generation="g1"),
            [],
        )

    def test_naive_deadline_rejected(self):
        with self.assertRaises(ValueError):
            select_training_samples(
                [], deadline=datetime(2026, 10, 15), feature_version="1.0.0", generation="g1"
            )


class PolicyTests(unittest.TestCase):
    def test_complete_days_excludes_partial_final_day(self):
        samples = [make_sample("ent:a", day) for day in range(15)]
        deadline = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
        self.assertEqual(complete_days(samples, deadline=deadline), 15)

    def test_insufficient_samples_is_cold_start(self):
        samples = [make_sample("ent:a", day) for day in range(30)]
        decision = training_decision(samples, deadline=DEADLINE)
        self.assertFalse(decision["trainable"])
        self.assertEqual(decision["reason"], "insufficient_samples")

    def test_insufficient_days_is_cold_start(self):
        samples = [make_sample("ent:a", 0, hour) for hour in range(0, 24, 2)] * 9
        decision = training_decision(samples, deadline=DEADLINE)
        self.assertFalse(decision["trainable"])
        self.assertEqual(decision["reason"], "insufficient_complete_days")
        self.assertGreaterEqual(decision["sample_count"], 100)

    def test_trainable_at_exact_minimum(self):
        samples = [make_sample("ent:a", day) for day in range(14)] + [
            make_sample("ent:b", day) for day in range(14)
        ] + [make_sample(f"ent:c{i}", 0) for i in range(72)]
        decision = training_decision(samples, deadline=DEADLINE)
        self.assertEqual(decision["sample_count"], 100)
        self.assertEqual(decision["complete_days"], 14)
        self.assertTrue(decision["trainable"])


class TrainingTests(unittest.TestCase):
    def setUp(self):
        self.samples = [
            make_sample("ent:a", 0, attempts=2.0),
            make_sample("ent:a", 1, attempts=4.0),
            make_sample("ent:b", 2, attempts=6.0),
        ]

    def test_train_statistics_deterministic(self):
        stats = train_baseline(self.samples)
        attempt = stats["feature_stats"]["auth.attempt.count"]
        self.assertEqual(attempt["count"], 3)
        self.assertAlmostEqual(attempt["mean"], 4.0)
        self.assertAlmostEqual(attempt["std"], (8.0 / 3.0) ** 0.5)
        self.assertEqual(attempt["min"], 2.0)
        self.assertEqual(attempt["max"], 6.0)
        self.assertEqual(stats, train_baseline(list(reversed(self.samples))))

    def test_train_empty_rejected(self):
        with self.assertRaises(ValueError):
            train_baseline([])

    def test_evaluate_metrics(self):
        stats = train_baseline(self.samples)
        metrics = evaluate_baseline(stats, self.samples)
        self.assertEqual(metrics["sample_count"], 3)
        self.assertEqual(metrics["entity_count"], 2)
        attempt = metrics["per_feature"]["auth.attempt.count"]
        self.assertAlmostEqual(attempt["max_abs_z"], 2.0 / (8.0 / 3.0) ** 0.5)
        self.assertEqual(attempt["within_3sigma_ratio"], 1.0)

    def test_evaluate_zero_variance(self):
        samples = [make_sample("ent:a", day, attempts=5.0) for day in range(3)]
        metrics = evaluate_baseline(train_baseline(samples), samples)
        self.assertEqual(metrics["per_feature"]["auth.attempt.count"]["max_abs_z"], 0.0)

    def test_sample_range(self):
        span = sample_range(self.samples)
        self.assertEqual(span["first_window_start"], "2026-09-01T00:00:00Z")
        self.assertEqual(span["last_window_end"], "2026-09-03T00:10:00Z")
        self.assertEqual(span["entities"], ["ent:a", "ent:b"])

    def test_model_content_hash_stable(self):
        model = BaselineModel(
            model_id="auth.baseline",
            version="1.0.0",
            feature_id="auth.features",
            status=BaselineStatus.READY,
            sample_count=3,
            trained_at=DEADLINE,
            feature_version="1.0.0",
            generation="g1",
            training_cutoff=DEADLINE,
            statistics=train_baseline(self.samples),
            sample_range=sample_range(self.samples),
        )
        self.assertEqual(model.content_hash(), model.content_hash())
        changed = BaselineModel(
            model_id="auth.baseline",
            version="1.0.0",
            feature_id="auth.features",
            status=BaselineStatus.READY,
            sample_count=4,
            trained_at=DEADLINE,
            feature_version="1.0.0",
            generation="g1",
            training_cutoff=DEADLINE,
            statistics=train_baseline(self.samples),
            sample_range=sample_range(self.samples),
        )
        self.assertNotEqual(model.content_hash(), changed.content_hash())


class TrainingJobTests(unittest.TestCase):
    def _policy(self):
        return TrainingPolicy(min_complete_days=2, min_samples=4)

    def test_insufficient_budget_publishes_cold_start(self):
        models = MemoryModelStore()
        result = run_training_job(
            MemorySampleStore([make_sample("ent:a", 0)]),
            models,
            organization="tenant_a",
            model_id="auth.baseline",
            model_version="1.0.0",
            feature_id="auth.features",
            feature_version="1.0.0",
            generation="g1",
            deadline=DEADLINE,
            policy=self._policy(),
        )
        self.assertEqual(result["status"], "cold_start")
        self.assertEqual(result["decision"]["reason"], "insufficient_samples")
        head = models.latest("tenant_a", "auth.baseline")
        self.assertEqual(head.status, BaselineStatus.COLD_START)
        self.assertEqual(head.sample_count, 1)

    def test_sufficient_budget_publishes_ready_model(self):
        samples = [make_sample("ent:a", day, attempts=float(day + 2)) for day in range(3)] + [
            make_sample("ent:b", 0, attempts=8.0)
        ]
        models = MemoryModelStore()
        result = run_training_job(
            MemorySampleStore(samples),
            models,
            organization="tenant_a",
            model_id="auth.baseline",
            model_version="1.0.0",
            feature_id="auth.features",
            feature_version="1.0.0",
            generation="g1",
            deadline=DEADLINE,
            policy=self._policy(),
            trained_at=DEADLINE,
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["decision"]["sample_count"], 4)
        self.assertEqual(result["sample_range"]["entities"], ["ent:a", "ent:b"])
        self.assertIn("per_feature", result["metrics"])
        head = models.latest("tenant_a", "auth.baseline")
        self.assertEqual(head.status, BaselineStatus.READY)
        self.assertEqual(head.training_cutoff, DEADLINE)

    def test_retrain_new_version_retires_old_with_audit_reference(self):
        samples = [make_sample("ent:a", day) for day in range(3)] + [make_sample("ent:b", 0)]
        models = MemoryModelStore()
        run_training_job(
            MemorySampleStore(samples), models,
            organization="tenant_a", model_id="auth.baseline", model_version="1.0.0",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, policy=self._policy(), trained_at=DEADLINE,
        )
        result = run_training_job(
            MemorySampleStore(samples + [make_sample("ent:b", 1)]), models,
            organization="tenant_a", model_id="auth.baseline", model_version="1.0.1",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, policy=self._policy(), trained_at=DEADLINE,
        )
        self.assertEqual(result["supersedes_model_version"], "1.0.0")
        old = models.rows[("tenant_a", "auth.baseline", "1.0.0")]
        self.assertEqual(old.status, BaselineStatus.RETIRED)
        new = models.rows[("tenant_a", "auth.baseline", "1.0.1")]
        self.assertEqual(new.status, BaselineStatus.READY)
        self.assertEqual(new.supersedes_model_version, "1.0.0")

    def test_same_version_different_content_rejected(self):
        samples = [make_sample("ent:a", day) for day in range(3)] + [make_sample("ent:b", 0)]
        models = MemoryModelStore()
        run_training_job(
            MemorySampleStore(samples), models,
            organization="tenant_a", model_id="auth.baseline", model_version="1.0.0",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, policy=self._policy(), trained_at=DEADLINE,
        )
        with self.assertRaises(ImmutableModelConflict):
            run_training_job(
                MemorySampleStore(samples + [make_sample("ent:b", 1)]), models,
                organization="tenant_a", model_id="auth.baseline", model_version="1.0.0",
                feature_id="auth.features", feature_version="1.0.0", generation="g1",
                deadline=DEADLINE, policy=self._policy(), trained_at=DEADLINE,
            )


@unittest.skipUnless(os.environ.get("TUBA_TEST_DATABASE_URL"), "TUBA_TEST_DATABASE_URL not set")
class PostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ["TUBA_TEST_DATABASE_URL"]

    def setUp(self):
        import psycopg

        self.org = f"f04_it_{uuid.uuid4().hex[:12]}"
        self.conn = psycopg.connect(self.dsn, autocommit=True)
        self.conn.execute(
            "INSERT INTO organizations(slug, name, namespace) VALUES (%s, %s, %s)",
            (self.org, self.org, self.org),
        )
        self.org_id = self.conn.execute(
            "SELECT id FROM organizations WHERE slug = %s", (self.org,)
        ).fetchone()[0]
        self.samples = PostgresFeatureSampleStore(self.dsn)
        self.models = PostgresBaselineModelStore(self.dsn)

    def tearDown(self):
        self.samples.close()
        self.models.close()
        self.conn.execute("DELETE FROM baseline_models WHERE organization_id = %s", (self.org_id,))
        self.conn.execute("DELETE FROM feature_samples WHERE organization_id = %s", (self.org_id,))
        self.conn.execute("DELETE FROM organizations WHERE id = %s", (self.org_id,))
        self.conn.close()

    def test_sample_persistence_revision_guard(self):
        sample = make_sample("ent:a", 0)
        self.assertEqual(self.samples.save(str(self.org_id), "auth.features", sample), "inserted")
        self.assertEqual(self.samples.save(str(self.org_id), "auth.features", sample), "idempotent")
        conflict = FeatureSample(
            entity_id="ent:a",
            feature_version="1.0.0",
            generation="g1",
            window_start=sample.window_start,
            window_end=sample.window_end,
            revision=1,
            quality="qualified",
            values={"auth.attempt.count": 123.0},
            inputs=["different"],
        )
        with self.assertRaises(RevisionConflict):
            self.samples.save(str(self.org_id), "auth.features", conflict)
        corrected = make_sample("ent:a", 0, revision=2, attempts=9.0)
        self.assertEqual(self.samples.save(str(self.org_id), "auth.features", corrected), "corrected")
        stale = make_sample("ent:a", 0, revision=1)
        self.assertEqual(self.samples.save(str(self.org_id), "auth.features", stale), "stale_revision")
        stored = self.samples.training_samples(
            str(self.org_id), "auth.features", feature_version="1.0.0", generation="g1", deadline=DEADLINE
        )
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].revision, 2)
        self.assertEqual(stored[0].values["auth.attempt.count"], 9.0)

    def test_training_samples_deadline_and_quality_filters(self):
        self.samples.save(str(self.org_id), "auth.features", make_sample("ent:a", 0))
        self.samples.save(str(self.org_id), "auth.features", make_sample("ent:a", 1, quality="partial"))
        self.samples.save(str(self.org_id), "auth.features", make_sample("ent:a", 44))
        early = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        selected = self.samples.training_samples(
            str(self.org_id), "auth.features", feature_version="1.0.0", generation="g1", deadline=early
        )
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].window_start.day, 1)

    def test_publish_immutable_and_cold_start_queryable(self):
        org = str(self.org_id)
        for day in range(16):
            self.samples.save(org, "auth.features", make_sample("ent:a", day))
        policy = TrainingPolicy(min_complete_days=14, min_samples=100)
        result = run_training_job(
            self.samples, self.models,
            organization=org, model_id="auth.baseline", model_version="1.0.0",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, policy=policy,
        )
        self.assertEqual(result["status"], "cold_start")
        self.assertEqual(result["decision"]["reason"], "insufficient_samples")
        status = self.models.status(org, "auth.baseline")
        self.assertEqual(status["status"], "cold_start")
        self.assertEqual(status["sample_count"], 16)
        # A later run with more persisted samples refreshes the queryable
        # cold_start state of the same (not yet published) version.
        self.samples.save(org, "auth.features", make_sample("ent:a", 16))
        refreshed = run_training_job(
            self.samples, self.models,
            organization=org, model_id="auth.baseline", model_version="1.0.0",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, policy=policy,
        )
        self.assertEqual(refreshed["publish"], "refreshed")
        status = self.models.status(org, "auth.baseline")
        self.assertEqual(status["status"], "cold_start")
        self.assertEqual(status["sample_count"], 17)

    def test_full_lifecycle_train_publish_retrain_immutable(self):
        org = str(self.org_id)
        for entity in ("ent:a", "ent:b", "ent:c", "ent:d", "ent:e"):
            for day in range(20):
                self.samples.save(org, "auth.features", make_sample(entity, day))
        result = run_training_job(
            self.samples, self.models,
            organization=org, model_id="auth.baseline", model_version="1.0.0",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, trained_at=DEADLINE,
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["decision"]["sample_count"], 100)
        self.assertEqual(result["decision"]["complete_days"], 20)
        self.assertEqual(result["sample_range"]["first_window_start"], "2026-09-01T00:00:00Z")
        loaded = self.models.load(org, "auth.baseline", "1.0.0")
        self.assertEqual(loaded.status, BaselineStatus.READY)
        self.assertEqual(loaded.sample_count, 100)
        self.assertEqual(loaded.training_cutoff, DEADLINE)
        self.assertIn("per_feature", loaded.metrics)
        self.assertIn("feature_stats", loaded.statistics)
        # Idempotent replay of the identical publication.
        again = run_training_job(
            self.samples, self.models,
            organization=org, model_id="auth.baseline", model_version="1.0.0",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, trained_at=DEADLINE,
        )
        self.assertEqual(again["publish"], "idempotent")
        # Same version with different content is rejected fail-closed.
        self.samples.save(org, "auth.features", make_sample("ent:f", 0))
        with self.assertRaises(ImmutableModelConflict):
            run_training_job(
                self.samples, self.models,
                organization=org, model_id="auth.baseline", model_version="1.0.0",
                feature_id="auth.features", feature_version="1.0.0", generation="g1",
                deadline=DEADLINE, trained_at=DEADLINE,
            )
        # Retraining after a correction produces a NEW immutable version; the
        # old version is retired but kept for audit.
        self.samples.save(org, "auth.features", make_sample("ent:a", 0, revision=2, attempts=11.0))
        retrained = run_training_job(
            self.samples, self.models,
            organization=org, model_id="auth.baseline", model_version="1.0.1",
            feature_id="auth.features", feature_version="1.0.0", generation="g1",
            deadline=DEADLINE, trained_at=DEADLINE,
        )
        self.assertEqual(retrained["status"], "ready")
        self.assertEqual(retrained["supersedes_model_version"], "1.0.0")
        old = self.models.load(org, "auth.baseline", "1.0.0")
        self.assertEqual(old.status, BaselineStatus.RETIRED)
        self.assertEqual(old.sample_count, 100)  # audit content preserved
        new = self.models.load(org, "auth.baseline", "1.0.1")
        self.assertEqual(new.status, BaselineStatus.READY)
        self.assertEqual(new.supersedes_model_version, "1.0.0")
        self.assertEqual(new.sample_count, 101)
        status = self.models.status(org, "auth.baseline")
        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["version"], "1.0.1")


if __name__ == "__main__":
    unittest.main()
