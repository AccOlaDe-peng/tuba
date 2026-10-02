"""Finding business-key revision / retracted / generation semantics (F06).

Authoritative implementation (design baseline §6: "同一业务键修正时 revision
递增；结果失效发布 retracted，不删除历史。迟到但仍在保留边界内的数据重算
相同窗口；超界数据只能通过受控回填进入新 generation"). The Go package
``internal/analysis/detection`` (revisions.go) is the reference/diagnostic
mirror of the exact same semantics; both sides are locked by the same golden
vector (see tests/test_revisions.py and revisions_test.go, which cite each
other).

Business key composition (locked by tests, aligned with contracts/ids.md
``anomaly.id``: tenant, rule/version, entity key, stable window key,
generation)::

    organization_id | rule_id | entity_id | window_start(UTC ISO) | generation

Rules:

* **Correction** — re-publishing the same business key assigns the next
  monotonically increasing revision; the older revision stays in history and
  is never deleted or overwritten.
* **Retracted** — when a previously published result no longer holds (input
  withdrawn, recompute flip), a tombstone frame with ``operation=retracted``
  is appended at the next revision. History is retained; the current view
  marks the key as retracted instead of deleting it.
* **Out-of-order protection (E05 external-version mirror)** — ``apply``
  accepts only a strictly greater revision; an equal revision with identical
  content is an idempotent no-op (at-least-once replay safe); an equal
  revision with different content raises :class:`RevisionConflict`
  fail-closed; an older revision raises :class:`StaleRevision` and the stored
  newer value is never overwritten.
* **Generation isolation** — a rule upgrade switches to a new generation.
  Because the generation is part of the business key, findings of the new
  generation never overwrite the old generation's. ``switch_generation``
  retires the old generation by retracting every currently-live key of the
  old generation, so the alert (current, non-retracted) view never holds the
  same (entity, window, rule) finding from two generations at once — no
  double online alerts. Old-generation frames remain fully queryable in
  history.
* **Late recompute linkage** — ``recompute`` re-evaluates one business key:
  an unchanged verdict is an idempotent no-op, a changed verdict publishes
  the next revision, and a flipped verdict (no finding anymore) publishes a
  ``retracted`` tombstone. Data beyond the retention boundary may only enter
  through controlled backfill into a new generation (see features.py), which
  is exactly the generation switch path above.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

DEFAULT_GENERATION = "g1"

OPERATION_UPSERT = "upsert"
OPERATION_RETRACTED = "retracted"

_SNAPSHOT_VERSION = 1


class RevisionConflict(ValueError):
    """Same revision, different content — fail-closed (mirrors E05 PROJECTION_REVISION_CONFLICT)."""


class StaleRevision(ValueError):
    """Older revision than the stored one — the stored newer value is never overwritten."""

    def __init__(self, business_key: str, existing: int, incoming: int):
        super().__init__(
            f"stale revision for {business_key}: stored revision {existing}, incoming {incoming}"
        )
        self.business_key = business_key
        self.existing = existing
        self.incoming = incoming


def _format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def business_key(
    organization_id: str,
    rule_id: str,
    entity_id: str,
    window_start: datetime,
    generation: str,
) -> str:
    """Canonical finding business key; every part is mandatory (fail-closed)."""
    if not organization_id or not rule_id or not entity_id:
        raise ValueError("business key requires organization id, rule id, and entity id")
    if not generation:
        raise ValueError("business key requires a generation")
    if not isinstance(window_start, datetime) or window_start.tzinfo is None:
        raise ValueError("business key requires a timezone-aware window start")
    for part in (organization_id, rule_id, entity_id, generation):
        if "|" in part:
            raise ValueError("business key parts must not contain the separator")
    return "|".join([organization_id, rule_id, entity_id, _format_time(window_start), generation])


def content_hash(document: dict[str, Any] | None) -> str:
    """Deterministic sha256 over the canonical JSON of a finding document."""
    canonical = json.dumps(document or {}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Frame:
    """One immutable revision frame of a finding business key."""

    business_key: str
    object_id: str
    revision: int
    operation: str  # OPERATION_UPSERT | OPERATION_RETRACTED
    generation: str
    content_hash: str
    at: str  # UTC ISO timestamp of the frame
    reason: str = ""
    document: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "business_key": self.business_key,
            "object_id": self.object_id,
            "revision": self.revision,
            "operation": self.operation,
            "generation": self.generation,
            "content_hash": self.content_hash,
            "at": self.at,
            "reason": self.reason,
            "document": self.document,
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "Frame":
        return Frame(
            business_key=str(payload["business_key"]),
            object_id=str(payload["object_id"]),
            revision=int(payload["revision"]),
            operation=str(payload["operation"]),
            generation=str(payload["generation"]),
            content_hash=str(payload["content_hash"]),
            at=str(payload["at"]),
            reason=str(payload.get("reason", "")),
            document=payload.get("document"),
        )


@dataclass
class FindingLedger:
    """Revision ledger over finding business keys.

    Producer side: ``publish`` / ``retract`` / ``recompute`` /
    ``switch_generation``. Consumer side: ``apply`` mirrors the E05
    external-version projection semantics — only the highest revision is
    ever current, history is append-only, and stale writes never overwrite.
    """

    _history: dict[str, list[Frame]] = field(default_factory=dict)

    # ------------------------------------------------------------------ apply
    def apply(self, frame: Frame) -> bool:
        """Consume one frame in external-version order.

        Returns True when the frame became the new current state, False for
        an idempotent replay. Raises :class:`RevisionConflict` (same revision,
        different content) or :class:`StaleRevision` (older revision)
        fail-closed; the stored newer value is never overwritten.
        """
        self._validate_frame(frame)
        frames = self._history.setdefault(frame.business_key, [])
        current = frames[-1] if frames else None
        if current is None or frame.revision > current.revision:
            frames.append(frame)
            return True
        if frame.revision == current.revision:
            if frame.content_hash == current.content_hash and frame.operation == current.operation:
                return False
            raise RevisionConflict(
                f"revision {frame.revision} of {frame.business_key} already exists with different content"
            )
        raise StaleRevision(frame.business_key, current.revision, frame.revision)

    # --------------------------------------------------------------- producer
    def publish(
        self,
        document: dict[str, Any],
        *,
        organization_id: str,
        rule_id: str,
        entity_id: str,
        window_start: datetime,
        generation: str,
        object_id: str,
        at: datetime,
        reason: str = "",
    ) -> Frame:
        """Publish (or correct) one finding: the next revision of its business key."""
        key = business_key(organization_id, rule_id, entity_id, window_start, generation)
        frame = Frame(
            business_key=key,
            object_id=self._require_object_id(object_id),
            revision=self._next_revision(key),
            operation=OPERATION_UPSERT,
            generation=generation,
            content_hash=content_hash(document),
            at=_format_time(at),
            reason=reason,
            document=document,
        )
        self.apply(frame)
        return frame

    def retract(self, key: str, *, at: datetime, reason: str) -> Frame:
        """Tombstone one business key at the next revision; history is retained."""
        if not reason:
            raise ValueError("retraction requires a reason")
        current = self.current(key)
        if current is None:
            raise ValueError(f"cannot retract an unknown business key: {key}")
        if current.operation == OPERATION_RETRACTED:
            raise ValueError(f"business key is already retracted: {key}")
        frame = Frame(
            business_key=key,
            object_id=current.object_id,
            revision=current.revision + 1,
            operation=OPERATION_RETRACTED,
            generation=current.generation,
            content_hash=content_hash(current.document),
            at=_format_time(at),
            reason=reason,
            document=current.document,
        )
        self.apply(frame)
        return frame

    def recompute(
        self,
        document: dict[str, Any] | None,
        *,
        organization_id: str,
        rule_id: str,
        entity_id: str,
        window_start: datetime,
        generation: str,
        object_id: str,
        at: datetime,
    ) -> Frame | None:
        """Late recompute of the same window (inside the retention boundary).

        * unchanged verdict -> idempotent no-op (returns None);
        * changed verdict   -> next revision (correction);
        * flipped verdict (``document`` is None) -> ``retracted`` tombstone
          (no-op when the key is unknown or already retracted).
        """
        key = business_key(organization_id, rule_id, entity_id, window_start, generation)
        current = self.current(key)
        if document is None:
            if current is None or current.operation == OPERATION_RETRACTED:
                return None
            return self.retract(key, at=at, reason="recompute_flip")
        if current is not None and current.operation == OPERATION_UPSERT:
            if current.content_hash == content_hash(document):
                return None
        return self.publish(
            document,
            organization_id=organization_id,
            rule_id=rule_id,
            entity_id=entity_id,
            window_start=window_start,
            generation=generation,
            object_id=object_id,
            at=at,
            reason="recompute_correction" if current is not None else "",
        )

    def switch_generation(
        self,
        organization_id: str,
        rule_id: str,
        old_generation: str,
        new_generation: str,
        *,
        at: datetime,
    ) -> list[Frame]:
        """Retire one rule generation: retract every live old-generation key.

        The new generation publishes under its own business keys, so old and
        new never overwrite each other; after the switch the alert view
        contains no old-generation finding — no double online alerts.
        """
        if not organization_id or not rule_id or not old_generation or not new_generation:
            raise ValueError("generation switch requires organization, rule, and both generations")
        if old_generation == new_generation:
            raise ValueError("generation switch requires distinct generations")
        reason = f"generation_retired:{old_generation}->{new_generation}"
        retired: list[Frame] = []
        prefix = f"{organization_id}|{rule_id}|"
        suffix = f"|{old_generation}"
        for key in sorted(self._history):
            if not (key.startswith(prefix) and key.endswith(suffix)):
                continue
            current = self.current(key)
            if current is not None and current.operation == OPERATION_UPSERT:
                retired.append(self.retract(key, at=at, reason=reason))
        return retired

    # ----------------------------------------------------------------- views
    def current(self, key: str) -> Frame | None:
        frames = self._history.get(key)
        return frames[-1] if frames else None

    def current_view(self, *, alerts_only: bool = False) -> dict[str, Frame]:
        """Highest-revision frame per key. ``alerts_only=True`` is the online
        alert view: retracted keys are marked out, never deleted."""
        view = {key: frames[-1] for key, frames in self._history.items() if frames}
        if alerts_only:
            view = {key: frame for key, frame in view.items() if frame.operation == OPERATION_UPSERT}
        return view

    def history(self, key: str) -> list[Frame]:
        return list(self._history.get(key, []))

    # ------------------------------------------------------------- snapshot
    def export(self) -> str:
        payload = {
            "state_version": _SNAPSHOT_VERSION,
            "history": {
                key: [frame.to_dict() for frame in frames]
                for key, frames in sorted(self._history.items())
            },
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def load(cls, raw: str) -> "FindingLedger":
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid ledger snapshot: {error}") from error
        if payload.get("state_version") != _SNAPSHOT_VERSION:
            raise ValueError("unsupported ledger snapshot version")
        ledger = cls()
        history = payload.get("history")
        if not isinstance(history, dict):
            raise ValueError("ledger snapshot requires a history object")
        for key in sorted(history):
            frames = history[key]
            if not isinstance(frames, list):
                raise ValueError("ledger snapshot history must hold frame lists")
            for item in frames:
                frame = Frame.from_dict(item)
                if frame.business_key != key:
                    raise ValueError("ledger snapshot frame filed under the wrong key")
                ledger.apply(frame)
        return ledger

    # -------------------------------------------------------------- internal
    def _next_revision(self, key: str) -> int:
        current = self.current(key)
        return 1 if current is None else current.revision + 1

    @staticmethod
    def _require_object_id(object_id: str) -> str:
        if not object_id:
            raise ValueError("finding object id is required")
        return object_id

    @staticmethod
    def _validate_frame(frame: Frame) -> None:
        if frame.revision < 1:
            raise ValueError("frame revision must be >= 1")
        if frame.operation not in {OPERATION_UPSERT, OPERATION_RETRACTED}:
            raise ValueError(f"unknown frame operation: {frame.operation}")
        if not frame.business_key or not frame.object_id or not frame.generation:
            raise ValueError("frame requires business key, object id, and generation")
        if not frame.at:
            raise ValueError("frame requires a timestamp")
