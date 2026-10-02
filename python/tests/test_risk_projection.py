"""R02: entity risk projection persistence path — the Python pipeline derives
immutable risk contributions from every finding frame (R01 RiskLedger) and
republishes the current entity risk as an analysis-result v2 ``entity_risk``
object with a pinned compute version and update time. The F07 analysis-sink
persists it through the E05 external-version facility; these tests lock the
producer semantics the sink relies on:

* projection emitted on every finding frame with fixed
  ``compute_version``/``updated_at`` and contribution references;
* deterministic projection revision (contribution-chain length) — strictly
  increasing, so an out-of-order/stale projection can never overwrite a newer
  one at the sink (external version);
* idempotent replay (same chain state -> same revision, same content);
* finding retracted -> reverse compensation -> projection risk rolls back
  (including to zero) at the next revision;
* authoritative state snapshot round-trip keeps revision continuity.
"""

import json
import unittest
from datetime import datetime, timedelta, timezone

from tuba_analysis.pipeline import (
    RISK_RULE_ID,
    AttributedAnalysisProcessor,
)
from tuba_analysis.risk import RISK_COMPUTE_VERSION, RiskEngineError, RiskLedger
from tuba_analysis.registry import load_registry
from tuba_analysis.worker import publish_results

ORG = "tenant_a"
NS = "tenant_a"
ENTITY = "ent:" + "a" * 64
RECEIVED = datetime(2026, 10, 12, 1, 0, tzinfo=timezone.utc)


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


def new_processor():
    return AttributedAnalysisProcessor(ORG, NS, load_registry())


def feed(processor, contributions, run_id="run-1"):
    envelopes = []
    for raw in contributions:
        envelopes.extend(processor.process(raw, run_id, received_at=RECEIVED))
    return envelopes


def five_failures_then_success(offset=0):
    raws = [contribution(offset + i, f"f{i}", i, "failure") for i in range(5)]
    raws.append(contribution(offset + 5, "s1", 5, "success"))
    return raws


def risk_envelopes(envelopes):
    return [e for e in envelopes if e["object_type"] == "entity_risk"]


class ProjectionEmissionTests(unittest.TestCase):
    def test_projection_emitted_with_pinned_compute_version_and_time(self):
        processor = new_processor()
        envelopes = feed(processor, five_failures_then_success())
        anomalies = [e for e in envelopes if e["object_type"] == "anomaly"]
        projections = risk_envelopes(envelopes)
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(len(projections), 1)

        projection = projections[0]
        self.assertEqual(projection["contract_version"], "2.0.0")
        self.assertEqual(projection["object_id"], ENTITY)
        self.assertEqual(projection["revision"], 1)
        self.assertEqual(projection["operation"], "upsert")
        self.assertEqual(projection["rule_id"], RISK_RULE_ID)
        self.assertEqual(projection["rule_version"], RISK_COMPUTE_VERSION)
        self.assertEqual(projection["date_key"], "2026-10-12")
        self.assertTrue(projection["window_start"].startswith(projection["date_key"]))

        document = projection["document"]
        # compute_version / updated_at are pinned by the producer.
        self.assertEqual(document["compute_version"], RISK_COMPUTE_VERSION)
        self.assertEqual(document["updated_at"], projection["window_start"])
        self.assertEqual(document["organization_id"], ORG)
        self.assertEqual(document["entity_id"], ENTITY)
        # Decay parameters ride along so readers can age the pinned score.
        self.assertEqual(document["decay"], {"half_life_days": 7.0, "zero_after_days": 30.0})
        # Age zero at the frame time: the score equals the finding score.
        self.assertEqual(document["risk_score"], anomalies[0]["document"]["anomaly"]["score"])
        # Explanation: contribution references of the live chain.
        self.assertEqual(len(document["contributions"]), 1)
        reference = document["contributions"][0]
        self.assertTrue(reference["contribution_id"].startswith("rc:"))
        self.assertEqual(reference["operation"], "positive")
        self.assertEqual(reference["finding_revision"], 1)

        # The projection cites the exact finding frame it was derived from.
        self.assertEqual(
            projection["input_refs"],
            [
                {
                    "object_type": "anomaly",
                    "object_id": anomalies[0]["object_id"],
                    "revision": 1,
                    "content_hash": projection["input_refs"][0]["content_hash"],
                }
            ],
        )
        self.assertRegex(projection["input_refs"][0]["content_hash"], r"^[a-f0-9]{64}$")

    def test_idempotent_replay_emits_no_new_projection(self):
        processor = new_processor()
        first = feed(processor, five_failures_then_success())
        self.assertEqual(len(risk_envelopes(first)), 1)
        self.assertEqual(feed(processor, five_failures_then_success()), [])

    def test_correction_advances_projection_revision_monotonically(self):
        processor = new_processor()
        raws = five_failures_then_success()
        raws.append(contribution(200, "tail", 15, "failure"))
        first = risk_envelopes(feed(processor, raws))
        self.assertEqual([e["revision"] for e in first], [1])

        # Late correction inside the retention boundary: finding revision 2,
        # projection revision moves forward and never backwards.
        second = risk_envelopes(feed(processor, [contribution(300, "late-f", 1, "failure")]))
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["revision"], 2)
        self.assertEqual(second[0]["object_id"], first[0]["object_id"])
        # The chain grew by the compensation contribution; the explanation
        # cites both, so the corrected projection cannot equal the old one.
        self.assertEqual(len(second[0]["document"]["contributions"]), 2)
        self.assertNotEqual(second[0]["document"], first[0]["document"])


class RetractionRollbackTests(unittest.TestCase):
    def _business_key(self, processor, rule_id):
        for entry in processor.risk.contributions_for(ENTITY):
            if entry.rule_id == rule_id:
                return entry.business_key
        self.fail(f"no contribution for rule {rule_id}")

    def test_retract_rolls_risk_back_to_zero(self):
        processor = new_processor()
        first = risk_envelopes(feed(processor, five_failures_then_success()))
        self.assertEqual(first[0]["document"]["risk_score"], 1.0)

        retracted = processor.retract_finding(
            self._business_key(processor, "auth.failure-then-success"),
            reason="input_withdrawn",
            run_id="run-1",
            at=RECEIVED + timedelta(minutes=1),
        )
        tombstone = [e for e in retracted if e["object_type"] == "anomaly"]
        projections = risk_envelopes(retracted)
        self.assertEqual(len(tombstone), 1)
        self.assertEqual(tombstone[0]["operation"], "retracted")
        self.assertEqual(tombstone[0]["revision"], 2)

        self.assertEqual(len(projections), 1)
        projection = projections[0]
        self.assertEqual(projection["revision"], 2)
        self.assertEqual(projection["document"]["risk_score"], 0.0)
        self.assertEqual(projection["document"]["compute_version"], RISK_COMPUTE_VERSION)
        self.assertEqual(
            projection["document"]["updated_at"], "2026-10-12T01:01:00Z"
        )
        operations = [c["operation"] for c in projection["document"]["contributions"]]
        self.assertEqual(operations, ["positive", "reversal"])
        # Score went strictly down; the old projection value is superseded by
        # a strictly greater revision, never overwritten in place.
        self.assertLess(
            projection["document"]["risk_score"], first[0]["document"]["risk_score"]
        )

    def test_retract_one_of_two_findings_leaves_residual_risk(self):
        processor = new_processor()
        raws = five_failures_then_success()
        for index in range(10):
            raws.append(contribution(100 + index, f"b{index}", 10 + index // 2, "failure"))
        projections = risk_envelopes(feed(processor, raws))
        # One projection per finding frame: failure-then-success then burst.
        self.assertEqual([e["revision"] for e in projections], [1, 2])
        self.assertEqual(projections[-1]["document"]["risk_score"], 2.0)

        retracted = risk_envelopes(
            processor.retract_finding(
                self._business_key(processor, "auth.failure-then-success"),
                reason="recompute_flip",
                run_id="run-1",
                at=RECEIVED + timedelta(minutes=1),
            )
        )
        self.assertEqual(len(retracted), 1)
        self.assertEqual(retracted[0]["revision"], 3)
        # The burst contribution remains live (decayed by one minute); the
        # retracted key nets to its decay asymmetry (older positive decays
        # marginally more than its newer reversal).
        decayed = 0.5 ** (1.0 / (7.0 * 1440.0))
        self.assertAlmostEqual(
            retracted[0]["document"]["risk_score"], decayed + (decayed - 1.0)
        )
        self.assertLess(retracted[0]["document"]["risk_score"], 2.0)

    def test_retract_unknown_key_is_fail_closed(self):
        processor = new_processor()
        with self.assertRaises(ValueError):
            processor.retract_finding(
                f"{ORG}|auth.failure-then-success|{ENTITY}|2026-10-12T00:00:00Z|g1",
                reason="input_withdrawn",
                run_id="run-1",
                at=RECEIVED,
            )


class StateSnapshotTests(unittest.TestCase):
    def test_round_trip_preserves_risk_and_revision_continuity(self):
        processor = new_processor()
        feed(processor, five_failures_then_success())
        restored = AttributedAnalysisProcessor.from_state(
            ORG, NS, load_registry(), processor.export_state()
        )
        # Idempotent replay after restore: no re-derived contributions.
        self.assertEqual(feed(restored, five_failures_then_success()), [])
        self.assertEqual(restored.risk.projection_revision(ENTITY), 1)
        self.assertEqual(
            restored.risk.entity_risk(ENTITY, RECEIVED)["risk_score"],
            processor.risk.entity_risk(ENTITY, RECEIVED)["risk_score"],
        )
        # The next projection continues the revision chain instead of
        # restarting at 1 (which the sink would rightfully reject as stale).
        retracted = risk_envelopes(
            restored.retract_finding(
                restored.risk.contributions_for(ENTITY)[0].business_key,
                reason="input_withdrawn",
                run_id="run-1",
                at=RECEIVED + timedelta(minutes=1),
            )
        )
        self.assertEqual(retracted[0]["revision"], 2)
        self.assertEqual(retracted[0]["document"]["risk_score"], 0.0)

    def test_risk_ledger_snapshot_fail_closed(self):
        processor = new_processor()
        feed(processor, five_failures_then_success())
        snapshot = processor.risk.export()

        with self.assertRaises(RiskEngineError):
            RiskLedger.load({"state_version": 99})
        with self.assertRaises(RiskEngineError):
            RiskLedger.load({"state_version": 1, "decay": snapshot["decay"]})

        tampered = json.loads(json.dumps(snapshot))
        tampered["contributions"][0]["contribution_id"] = "rc:" + "0" * 64
        with self.assertRaises(RiskEngineError):
            RiskLedger.load(tampered)

        replayed = json.loads(json.dumps(snapshot))
        replayed["contributions"].append(replayed["contributions"][0])
        with self.assertRaises(RiskEngineError):
            RiskLedger.load(replayed)

        bad_decay = json.loads(json.dumps(snapshot))
        bad_decay["decay"] = {"half_life_days": -1, "zero_after_days": 30}
        with self.assertRaises(RiskEngineError):
            RiskLedger.load(bad_decay)

    def test_old_processor_state_version_rejected(self):
        with self.assertRaises(ValueError):
            AttributedAnalysisProcessor.from_state(ORG, NS, load_registry(), {"state_version": 2})


class WorkerPublishTests(unittest.TestCase):
    def test_publish_results_keys_v2_objects_by_object_id(self):
        class FakeProducer:
            def __init__(self):
                self.produced = []

            def produce(self, topic, key=None, value=None):
                self.produced.append((topic, key, value))

            def flush(self, timeout):
                return 0

        producer = FakeProducer()
        v2 = {"object_id": ENTITY, "object_type": "entity_risk"}
        v1 = {"result_id": "legacy-1"}
        publish_results(producer, "topic", [v2, v1])
        self.assertEqual(producer.produced[0][1], ENTITY.encode())
        self.assertEqual(producer.produced[1][1], b"legacy-1")


if __name__ == "__main__":
    unittest.main()
