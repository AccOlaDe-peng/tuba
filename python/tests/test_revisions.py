"""F06 finding business-key revision / retracted / generation semantics.

Authoritative test suite for tuba_analysis/revisions.py. The Go mirror
internal/analysis/detection/revisions_test.go asserts the exact same golden
vector; both files cite each other.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tuba_analysis.revisions import (
    DEFAULT_GENERATION,
    OPERATION_RETRACTED,
    OPERATION_UPSERT,
    FindingLedger,
    Frame,
    RevisionConflict,
    StaleRevision,
    business_key,
    content_hash,
)

ORG = "tenant_a"
RULE = "auth.failure-burst"
ENTITY = "ent:u1"
WINDOW = datetime(2026, 10, 12, 0, 0, tzinfo=timezone.utc)
T0 = datetime(2026, 10, 12, 1, 0, tzinfo=timezone.utc)

# Golden vector, identical in the Go mirror:
KEY = "tenant_a|auth.failure-burst|ent:u1|2026-10-12T00:00:00Z|g1"
DOC_V1 = {"anomaly": {"score": 0.9}, "marker": "v1"}
DOC_V2 = {"anomaly": {"score": 0.95}, "marker": "v2"}
DOC_V1_ALT = {"anomaly": {"score": 0.91}, "marker": "v1-alt"}


def publish(ledger, document, *, at=T0, rule=RULE, entity=ENTITY, window=WINDOW, generation="g1", object_id="anom:x"):
    return ledger.publish(
        document,
        organization_id=ORG,
        rule_id=rule,
        entity_id=entity,
        window_start=window,
        generation=generation,
        object_id=object_id,
        at=at,
    )


class BusinessKeyTest(unittest.TestCase):
    def test_composition_locked(self):
        self.assertEqual(business_key(ORG, RULE, ENTITY, WINDOW, "g1"), KEY)

    def test_parts_are_mandatory(self):
        for kwargs in (
            {"organization_id": "", "rule_id": RULE, "entity_id": ENTITY, "generation": "g1"},
            {"organization_id": ORG, "rule_id": "", "entity_id": ENTITY, "generation": "g1"},
            {"organization_id": ORG, "rule_id": RULE, "entity_id": "", "generation": "g1"},
            {"organization_id": ORG, "rule_id": RULE, "entity_id": ENTITY, "generation": ""},
        ):
            with self.assertRaises(ValueError):
                business_key(window_start=WINDOW, **kwargs)
        with self.assertRaises(ValueError):
            business_key(ORG, RULE, ENTITY, datetime(2026, 10, 12), "g1")  # naive time
        with self.assertRaises(ValueError):
            business_key(ORG, "rule|x", ENTITY, WINDOW, "g1")  # separator injection

    def test_generation_isolates_keys(self):
        self.assertNotEqual(
            business_key(ORG, RULE, ENTITY, WINDOW, "g1"),
            business_key(ORG, RULE, ENTITY, WINDOW, "g2"),
        )


class RevisionLifecycleTest(unittest.TestCase):
    def test_correction_increments_revision_and_keeps_history(self):
        ledger = FindingLedger()
        first = publish(ledger, DOC_V1)
        self.assertEqual(first.revision, 1)
        second = publish(ledger, DOC_V2, at=T0 + timedelta(minutes=5))
        self.assertEqual(second.revision, 2)
        history = ledger.history(KEY)
        self.assertEqual([frame.revision for frame in history], [1, 2])
        # History is never deleted or rewritten; the current view shows only
        # the highest revision.
        self.assertEqual(history[0].document, DOC_V1)
        self.assertEqual(ledger.current(KEY).document, DOC_V2)

    def test_out_of_order_never_overwrites(self):
        ledger = FindingLedger()
        publish(ledger, DOC_V1)
        publish(ledger, DOC_V2, at=T0 + timedelta(minutes=5))
        stale = Frame(
            business_key=KEY,
            object_id="anom:x",
            revision=1,
            operation=OPERATION_UPSERT,
            generation="g1",
            content_hash=content_hash(DOC_V1),
            at=(T0).isoformat().replace("+00:00", "Z"),
            document=DOC_V1,
        )
        with self.assertRaises(StaleRevision) as caught:
            ledger.apply(stale)
        self.assertEqual((caught.exception.existing, caught.exception.incoming), (2, 1))
        self.assertEqual(ledger.current(KEY).document, DOC_V2)  # newer value intact
        self.assertEqual(len(ledger.history(KEY)), 2)  # stale frame not recorded

    def test_same_revision_idempotent_or_conflict(self):
        ledger = FindingLedger()
        first = publish(ledger, DOC_V1)
        replay = Frame(**{**first.to_dict()})
        self.assertFalse(ledger.apply(replay))  # at-least-once replay: no-op
        self.assertEqual(len(ledger.history(KEY)), 1)
        conflicting = Frame(**{**first.to_dict(), "content_hash": content_hash(DOC_V1_ALT), "document": DOC_V1_ALT})
        with self.assertRaises(RevisionConflict):
            ledger.apply(conflicting)
        self.assertEqual(ledger.current(KEY).document, DOC_V1)

    def test_retracted_marks_current_view_and_keeps_history(self):
        ledger = FindingLedger()
        publish(ledger, DOC_V1)
        tombstone = ledger.retract(KEY, at=T0 + timedelta(minutes=10), reason="input_withdrawn")
        self.assertEqual((tombstone.revision, tombstone.operation), (2, OPERATION_RETRACTED))
        # History fully queryable; current view marks the key retracted
        # (marked out of the alert view, never deleted).
        self.assertEqual(len(ledger.history(KEY)), 2)
        self.assertEqual(ledger.current(KEY).operation, OPERATION_RETRACTED)
        self.assertNotIn(KEY, ledger.current_view(alerts_only=True))
        self.assertIn(KEY, ledger.current_view())
        with self.assertRaises(ValueError):
            ledger.retract(KEY, at=T0, reason="again")  # already retracted
        with self.assertRaises(ValueError):
            ledger.retract("tenant_a|r|e|2026-01-01T00:00:00Z|g1", at=T0, reason="unknown key")
        # empty reason is rejected on a live key
        ledger2 = FindingLedger()
        publish(ledger2, DOC_V1)
        with self.assertRaises(ValueError):
            ledger2.retract(KEY, at=T0, reason="")

    def test_recompute_flip_publishes_retracted(self):
        ledger = FindingLedger()
        publish(ledger, DOC_V1)
        # Unchanged verdict: idempotent no-op.
        self.assertIsNone(
            ledger.recompute(
                DOC_V1, organization_id=ORG, rule_id=RULE, entity_id=ENTITY,
                window_start=WINDOW, generation="g1", object_id="anom:x",
                at=T0 + timedelta(minutes=1),
            )
        )
        self.assertEqual(ledger.current(KEY).revision, 1)
        # Corrected verdict: next revision.
        corrected = ledger.recompute(
            DOC_V2, organization_id=ORG, rule_id=RULE, entity_id=ENTITY,
            window_start=WINDOW, generation="g1", object_id="anom:x",
            at=T0 + timedelta(minutes=2),
        )
        self.assertEqual((corrected.revision, corrected.reason), (2, "recompute_correction"))
        # Flipped verdict (finding no longer holds): retracted tombstone.
        flipped = ledger.recompute(
            None, organization_id=ORG, rule_id=RULE, entity_id=ENTITY,
            window_start=WINDOW, generation="g1", object_id="anom:x",
            at=T0 + timedelta(minutes=3),
        )
        self.assertEqual((flipped.revision, flipped.operation, flipped.reason), (3, OPERATION_RETRACTED, "recompute_flip"))
        # Flipping an already-retracted key is a no-op.
        self.assertIsNone(
            ledger.recompute(
                None, organization_id=ORG, rule_id=RULE, entity_id=ENTITY,
                window_start=WINDOW, generation="g1", object_id="anom:x",
                at=T0 + timedelta(minutes=4),
            )
        )

    def test_generation_switch_no_double_alerts(self):
        ledger = FindingLedger()
        publish(ledger, DOC_V1, entity="ent:u1", object_id="anom:old1")
        publish(ledger, DOC_V1, entity="ent:u2", object_id="anom:old2")
        # Rule upgrade: the old generation is retired; the new generation
        # publishes the same (entity, window, rule) under its own key.
        retired = ledger.switch_generation(ORG, RULE, "g1", "g2", at=T0 + timedelta(hours=1))
        self.assertEqual(len(retired), 2)
        self.assertTrue(all(frame.operation == OPERATION_RETRACTED for frame in retired))
        self.assertTrue(all(frame.reason == "generation_retired:g1->g2" for frame in retired))
        new_key = business_key(ORG, RULE, "ent:u1", WINDOW, "g2")
        publish(ledger, DOC_V2, entity="ent:u1", generation="g2", object_id="anom:new1", at=T0 + timedelta(hours=1))
        # Red line: the online alert view never holds the same finding from
        # two generations; history keeps both generations fully.
        alerts = ledger.current_view(alerts_only=True)
        self.assertEqual(set(alerts), {new_key})
        self.assertEqual(len(ledger.history(KEY)), 2)
        self.assertEqual(ledger.current(KEY).operation, OPERATION_RETRACTED)
        # Old-generation findings were retired, not overwritten by the new
        # generation (distinct business keys).
        self.assertEqual(ledger.current(new_key).generation, "g2")
        self.assertEqual(ledger.current(KEY).generation, "g1")

    def test_generation_switch_validation(self):
        ledger = FindingLedger()
        with self.assertRaises(ValueError):
            ledger.switch_generation(ORG, RULE, "g1", "g1", at=T0)
        with self.assertRaises(ValueError):
            ledger.switch_generation(ORG, RULE, "", "g2", at=T0)

    def test_export_load_round_trip(self):
        ledger = FindingLedger()
        publish(ledger, DOC_V1)
        publish(ledger, DOC_V2, at=T0 + timedelta(minutes=5))
        ledger.retract(KEY, at=T0 + timedelta(minutes=10), reason="input_withdrawn")
        restored = FindingLedger.load(ledger.export())
        self.assertEqual(
            [frame.to_dict() for frame in restored.history(KEY)],
            [frame.to_dict() for frame in ledger.history(KEY)],
        )
        # Idempotent replay of the restored top frame is a no-op.
        self.assertFalse(restored.apply(Frame(**restored.current(KEY).to_dict())))
        with self.assertRaises(ValueError):
            FindingLedger.load('{"state_version": 999, "history": {}}')
        with self.assertRaises(ValueError):
            FindingLedger.load("not json")

    def test_default_generation(self):
        self.assertEqual(DEFAULT_GENERATION, "g1")


if __name__ == "__main__":
    unittest.main()
