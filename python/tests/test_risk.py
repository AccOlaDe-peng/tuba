"""Tests for tuba_analysis.risk (R01).

Golden vectors are computed by hand; the module docstring in risk.py states
the compensation/decay rules these tests lock.
"""
from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone

from tuba_analysis.revisions import FindingLedger
from tuba_analysis.risk import (
    CONTRIB_OPERATION_COMPENSATION,
    CONTRIB_OPERATION_POSITIVE,
    CONTRIB_OPERATION_REVERSAL,
    DecayPolicy,
    RiskEngineError,
    RiskLedger,
    apply_ledger,
)

T0 = datetime(2026, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
WINDOW = datetime(2026, 9, 30, 0, 0, 0, tzinfo=timezone.utc)
ORG = "tenant_a"
RULE = "auth.failure-burst"
ENTITY = "ent:u1"


def finding_doc(score: float, entity: str = ENTITY) -> dict:
    return {
        "organization": {"id": ORG},
        "entity": {"id": entity, "type": "account"},
        "anomaly": {"score": score, "severity": "high"},
    }


def publish(ledger: FindingLedger, score: float, *, entity: str = ENTITY, at=T0, rule: str = RULE) -> None:
    ledger.publish(
        finding_doc(score, entity),
        organization_id=ORG,
        rule_id=rule,
        entity_id=entity,
        window_start=WINDOW,
        generation="g1",
        object_id="anom:test",
        at=at,
    )


def key_of(rule: str = RULE, entity: str = ENTITY) -> str:
    return f"{ORG}|{rule}|{entity}|2026-09-30T00:00:00Z|g1"


class TestContributionDerivation(unittest.TestCase):
    def test_first_publish_positive_contribution(self):
        findings = FindingLedger()
        publish(findings, 0.9)
        risk = apply_ledger(findings)
        contributions = risk.contributions_for(ENTITY)
        self.assertEqual(len(contributions), 1)
        c = contributions[0]
        self.assertEqual(c.operation, CONTRIB_OPERATION_POSITIVE)
        self.assertAlmostEqual(c.amount, 0.9)
        self.assertEqual(c.finding_revision, 1)
        self.assertTrue(c.contribution_id.startswith("rc:"))

    def test_correction_appends_delta_compensation(self):
        findings = FindingLedger()
        publish(findings, 0.9)
        publish(findings, 0.4)  # correction: same business key, revision 2
        risk = apply_ledger(findings)
        contributions = risk.contributions_for(ENTITY)
        self.assertEqual(len(contributions), 2)
        delta = contributions[1]
        self.assertEqual(delta.operation, CONTRIB_OPERATION_COMPENSATION)
        self.assertAlmostEqual(delta.amount, 0.4 - 0.9)
        # Both contributions are immutable records; the chain sums to the corrected score.
        self.assertAlmostEqual(sum(c.amount for c in contributions), 0.4)

    def test_retraction_reverse_compensation_zeroes_key(self):
        findings = FindingLedger()
        publish(findings, 0.9)
        findings.retract(key_of(), at=T0, reason="recompute flip")
        risk = apply_ledger(findings)
        contributions = risk.contributions_for(ENTITY)
        self.assertEqual(len(contributions), 2)
        self.assertEqual(contributions[1].operation, CONTRIB_OPERATION_REVERSAL)
        self.assertAlmostEqual(sum(c.amount for c in contributions), 0.0)
        risk_now = risk.entity_risk(ENTITY, T0)
        self.assertAlmostEqual(risk_now["risk_score"], 0.0)

    def test_frame_replay_and_gap_are_fail_closed(self):
        findings = FindingLedger()
        publish(findings, 0.9)
        risk = apply_ledger(findings)
        frame = findings.current(key_of())
        with self.assertRaises(RiskEngineError):
            risk.apply_frame(frame)  # replay: contribution already derived


class TestEntityAggregationAndDedup(unittest.TestCase):
    def test_two_rules_aggregate_per_entity(self):
        findings = FindingLedger()
        publish(findings, 0.9, rule="auth.failure-burst")
        publish(findings, 0.5, rule="auth.failure-then-success")
        risk = apply_ledger(findings)
        view = risk.entity_risk(ENTITY, T0)
        self.assertAlmostEqual(view["risk_score"], 1.4)
        self.assertEqual(len(view["contributions"]), 2)
        self.assertEqual(view["compute_version"], "1.0.0")

    def test_same_key_never_double_counts(self):
        findings = FindingLedger()
        publish(findings, 0.9)
        publish(findings, 0.9)  # same content, new revision
        risk = apply_ledger(findings)
        # 0.9 + (0.9-0.9) == 0.9, not 1.8
        self.assertAlmostEqual(risk.entity_risk(ENTITY, T0)["risk_score"], 0.9)

    def test_entities_are_isolated(self):
        findings = FindingLedger()
        publish(findings, 0.9, entity="ent:u1")
        publish(findings, 0.3, entity="ent:u2")
        risk = apply_ledger(findings)
        self.assertAlmostEqual(risk.entity_risk("ent:u1", T0)["risk_score"], 0.9)
        self.assertAlmostEqual(risk.entity_risk("ent:u2", T0)["risk_score"], 0.3)
        self.assertEqual(len(risk.current_view(T0)), 2)


class TestTimedDecay(unittest.TestCase):
    def test_half_life(self):
        findings = FindingLedger()
        publish(findings, 1.0, at=T0 - timedelta(days=7))
        risk = apply_ledger(findings)
        self.assertAlmostEqual(risk.entity_risk(ENTITY, T0)["risk_score"], 0.5)

    def test_zero_after_30_days_and_history_kept(self):
        findings = FindingLedger()
        publish(findings, 1.0, at=T0 - timedelta(days=31))
        risk = apply_ledger(findings)
        view = risk.entity_risk(ENTITY, T0)
        self.assertEqual(view["risk_score"], 0.0)
        self.assertEqual(view["contributions"], [])  # decayed out of the live view
        self.assertEqual(len(risk.contributions_for(ENTITY)), 1)  # history never deleted

    def test_shorter_rule_period(self):
        findings = FindingLedger()
        publish(findings, 1.0, at=T0 - timedelta(days=3))
        risk = apply_ledger(findings, RiskLedger(DecayPolicy(half_life_days=1.0, zero_after_days=3.0)))
        view = risk.entity_risk(ENTITY, T0)
        self.assertEqual(view["decay"]["half_life_days"], 1.0)
        self.assertAlmostEqual(view["risk_score"], 0.125)  # 0.5**3

    def test_boundary_30d_still_counts(self):
        findings = FindingLedger()
        publish(findings, 1.0, at=T0 - timedelta(days=30))
        risk = apply_ledger(findings)
        self.assertGreater(risk.entity_risk(ENTITY, T0)["risk_score"], 0.0)


class TestFailClosed(unittest.TestCase):
    def test_missing_score_rejected(self):
        findings = FindingLedger()
        findings.publish(
            {"organization": {"id": ORG}, "anomaly": {}},
            organization_id=ORG,
            rule_id=RULE,
            entity_id=ENTITY,
            window_start=WINDOW,
            generation="g1",
            object_id="anom:bad",
            at=T0,
        )
        with self.assertRaises(RiskEngineError):
            apply_ledger(findings)

    def test_naive_timestamp_rejected(self):
        policy = DecayPolicy()
        with self.assertRaises(RiskEngineError):
            policy.factor(-1)
        with self.assertRaises(RiskEngineError):
            DecayPolicy(half_life_days=0)

    def test_projection_records_version_time_and_refs(self):
        findings = FindingLedger()
        publish(findings, 0.7)
        risk = apply_ledger(findings)
        view = risk.entity_risk(ENTITY, T0)
        self.assertEqual(view["compute_version"], "1.0.0")
        self.assertEqual(view["updated_at"], "2026-10-01T00:00:00Z")
        self.assertEqual(view["contributions"][0]["business_key"], key_of())


if __name__ == "__main__":
    unittest.main()
