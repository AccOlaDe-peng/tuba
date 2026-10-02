"""Immutable risk contributions, revision compensation, entity aggregation,
correlation dedup and timed decay (R01).

Authoritative implementation (design baseline §6: "风险由不可变贡献组成。
异常新增/修订/撤回分别产生正贡献、差额补偿或反向补偿；当前风险是未过期
贡献的确定性聚合。默认半衰期 7 日、贡献 30 日后归零，规则可声明更短周期。
风险投影记录计算版本、更新时间和贡献引用。").

The engine consumes finding frames from the F06 FindingLedger (same
business-key/revision/retracted semantics). Every ledger frame produces at
most one **immutable** contribution record; corrections never rewrite an
existing contribution, they append a compensating one:

* **upsert, first revision** — positive contribution ``+score``.
* **upsert, later revision (correction)** — delta compensation
  ``new_score - previous_live_score`` (差额补偿). The previous live score is
  the amount the key currently contributes to risk, so after compensation the
  key's effective amount equals the corrected score.
* **retracted** — reverse compensation ``-previous_live_score`` (反向补偿),
  driving the key's effective amount to zero. History is never deleted.

Correlation dedup is inherent in the compensation chain: one finding business
key can never count twice, because every new frame compensates the key's own
previous live amount rather than adding an independent contribution.

Timed decay is applied **at read time** over the immutable contributions —
decay never rewrites history::

    decayed(amount, age_days) = amount * 0.5 ** (age_days / half_life_days)   age_days <= zero_after_days
                              = 0                                           otherwise

Defaults: half-life 7 days, zero after 30 days; a rule may declare a shorter
period via ``DecayPolicy``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .revisions import Frame, FindingLedger, OPERATION_RETRACTED, OPERATION_UPSERT

CONTRIB_OPERATION_POSITIVE = "positive"
CONTRIB_OPERATION_COMPENSATION = "compensation"
CONTRIB_OPERATION_REVERSAL = "reversal"

RISK_COMPUTE_VERSION = "1.0.0"

DEFAULT_HALF_LIFE_DAYS = 7.0
DEFAULT_ZERO_AFTER_DAYS = 30.0

_SNAPSHOT_VERSION = 1


class RiskEngineError(ValueError):
    """Fail-closed risk engine error (bad frame, bad score, bad policy)."""


def _utc_day(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise RiskEngineError("contribution timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class DecayPolicy:
    half_life_days: float = DEFAULT_HALF_LIFE_DAYS
    zero_after_days: float = DEFAULT_ZERO_AFTER_DAYS

    def __post_init__(self) -> None:
        if self.half_life_days <= 0:
            raise RiskEngineError("half_life_days must be positive")
        if self.zero_after_days <= 0:
            raise RiskEngineError("zero_after_days must be positive")
        if self.zero_after_days < self.half_life_days:
            raise RiskEngineError("zero_after_days must be >= half_life_days")

    def factor(self, age_days: float) -> float:
        if age_days < 0:
            raise RiskEngineError(f"contribution age is negative: {age_days}")
        if age_days > self.zero_after_days:
            return 0.0
        return 0.5 ** (age_days / self.half_life_days)


@dataclass(frozen=True)
class RiskContribution:
    """One immutable risk contribution derived from a finding frame."""

    contribution_id: str
    organization_id: str
    entity_id: str
    business_key: str
    rule_id: str
    generation: str
    finding_revision: int
    operation: str  # positive | compensation | reversal
    amount: float
    occurred_at: datetime

    def reference(self) -> dict[str, Any]:
        return {
            "contribution_id": self.contribution_id,
            "business_key": self.business_key,
            "finding_revision": self.finding_revision,
            "operation": self.operation,
            "amount": self.amount,
            "occurred_at": _format_time(self.occurred_at),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.reference(),
            "organization_id": self.organization_id,
            "entity_id": self.entity_id,
            "rule_id": self.rule_id,
            "generation": self.generation,
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> "RiskContribution":
        try:
            return RiskContribution(
                contribution_id=str(payload["contribution_id"]),
                organization_id=str(payload["organization_id"]),
                entity_id=str(payload["entity_id"]),
                business_key=str(payload["business_key"]),
                rule_id=str(payload["rule_id"]),
                generation=str(payload["generation"]),
                finding_revision=int(payload["finding_revision"]),
                operation=str(payload["operation"]),
                amount=float(payload["amount"]),
                occurred_at=_parse_frame_time(str(payload["occurred_at"])),
            )
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, RiskEngineError):
                raise
            raise RiskEngineError(f"invalid risk contribution snapshot record: {exc}") from exc


def _format_time(value: datetime) -> str:
    return _utc_day(value).isoformat().replace("+00:00", "Z")


def _parse_business_key(key: str) -> tuple[str, str, str]:
    parts = key.split("|")
    if len(parts) != 5:
        raise RiskEngineError(f"finding business key has {len(parts)} parts, want 5: {key!r}")
    return parts[0], parts[1], parts[2]  # organization_id, rule_id, entity_id


def _parse_frame_time(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RiskEngineError(f"finding frame timestamp is not ISO: {value!r}") from exc
    return _utc_day(parsed)


def contribution_id_for(
    organization_id: str, business_key: str, finding_revision: int, operation: str
) -> str:
    digest = hashlib.sha256(
        json.dumps(
            [organization_id, business_key, finding_revision, operation],
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return "rc:" + digest


def _score_of(frame: Frame) -> float:
    score = frame.document.get("anomaly", {}).get("score") if frame.document else None
    if not isinstance(score, (int, float)) or isinstance(score, bool):
        raise RiskEngineError(
            f"finding frame {frame.business_key} rev {frame.revision} has no numeric anomaly.score"
        )
    return float(score)


class RiskLedger:
    """Derives immutable risk contributions from finding frames and aggregates
    the current per-entity risk with timed decay.

    Producer side: ``apply_frame`` (one FindingLedger frame at a time, in
    external-version order — the engine validates ordering per key and is
    fail-closed on gaps or duplicates beyond idempotent replay).
    Consumer side: ``entity_risk(entity_id, at)`` /
    ``current_view(at)``.
    """

    def __init__(self, decay: DecayPolicy | None = None) -> None:
        self._decay = decay or DecayPolicy()
        # business_key -> live effective amount contributed by that key
        self._live_amount: dict[str, float] = {}
        # business_key -> last applied revision (ordering guard)
        self._applied_revision: dict[str, int] = {}
        # entity_id -> organization_id (consistency guard)
        self._entity_org: dict[str, str] = {}
        self._contributions: list[RiskContribution] = []

    # ------------------------------------------------------------- producer
    def apply_frame(self, frame: Frame) -> RiskContribution:
        """Derive one immutable contribution from a finding frame.

        Frames must arrive in monotonically increasing revision order per
        business key (the FindingLedger external-version order); replaying an
        already-applied frame raises fail-closed because the contribution is
        already recorded (contributions are immutable).
        """
        key = frame.business_key
        last = self._applied_revision.get(key, 0)
        if frame.revision <= last:
            raise RiskEngineError(
                f"finding frame {key} rev {frame.revision} already applied (last {last}); "
                "contributions are immutable and must not be re-derived"
            )
        if frame.revision != last + 1:
            raise RiskEngineError(
                f"finding frame {key} rev {frame.revision} skips revision after {last}"
            )

        previous_live = self._live_amount.get(key, 0.0)
        if frame.operation == OPERATION_UPSERT:
            score = _score_of(frame)
            amount = score - previous_live
            if frame.revision == 1:
                operation = CONTRIB_OPERATION_POSITIVE
            else:
                operation = CONTRIB_OPERATION_COMPENSATION
            self._live_amount[key] = score
        elif frame.operation == OPERATION_RETRACTED:
            amount = -previous_live
            operation = CONTRIB_OPERATION_REVERSAL
            self._live_amount[key] = 0.0
        else:
            raise RiskEngineError(f"unknown finding operation {frame.operation!r}")

        org, rule_id, entity = _parse_business_key(key)
        existing_org = self._entity_org.setdefault(entity, org)
        if existing_org != org:
            raise RiskEngineError(
                f"entity {entity} contributed under two organizations: {existing_org} vs {org}"
            )

        contribution = RiskContribution(
            contribution_id=contribution_id_for(org, key, frame.revision, operation),
            organization_id=org,
            entity_id=entity,
            business_key=key,
            rule_id=rule_id,
            generation=frame.generation,
            finding_revision=frame.revision,
            operation=operation,
            amount=amount,
            occurred_at=_parse_frame_time(frame.at),
        )
        self._contributions.append(contribution)
        self._applied_revision[key] = frame.revision
        return contribution

    # ------------------------------------------------------------- consumer
    def contributions_for(self, entity_id: str) -> list[RiskContribution]:
        return [c for c in self._contributions if c.entity_id == entity_id]

    def _decayed_sum(self, entity_id: str, at: datetime) -> tuple[float, list[RiskContribution]]:
        at = _utc_day(at)
        total = 0.0
        live: list[RiskContribution] = []
        for contribution in self.contributions_for(entity_id):
            age_days = (at - contribution.occurred_at).total_seconds() / 86400.0
            factor = self._decay.factor(age_days)
            if factor == 0.0:
                continue
            total += contribution.amount * factor
            live.append(contribution)
        # Decay is applied per contribution age, so an older positive
        # contribution decays slightly more than its newer reversal; the
        # summed total can drift marginally below zero even though the key's
        # effective amount is exactly zero. Risk is floored at zero.
        return max(0.0, total), live

    def entity_risk(self, entity_id: str, at: datetime) -> dict[str, Any]:
        """Current risk projection for one entity at time ``at``."""
        at = _utc_day(at)
        total, live = self._decayed_sum(entity_id, at)
        return {
            "organization_id": self._entity_org.get(entity_id),
            "entity_id": entity_id,
            "risk_score": total,
            "compute_version": RISK_COMPUTE_VERSION,
            "updated_at": _format_time(at),
            "decay": {
                "half_life_days": self._decay.half_life_days,
                "zero_after_days": self._decay.zero_after_days,
            },
            "contributions": [c.reference() for c in live],
        }

    def projection_revision(self, entity_id: str) -> int:
        """Deterministic external revision of the entity risk projection (R02).

        The projection is re-derived and republished only when the entity's
        immutable contribution chain grows; every appended contribution moves
        the projection exactly one revision forward. Same chain state -> same
        revision and same content (idempotent replay); a strictly greater
        revision always carries the newer chain, so the E05/F07 external
        version semantics in the sink reject any stale projection write.
        """
        return len(self.contributions_for(entity_id))

    # ------------------------------------------------------------- snapshot
    def export(self) -> dict[str, Any]:
        return {
            "state_version": _SNAPSHOT_VERSION,
            "decay": {
                "half_life_days": self._decay.half_life_days,
                "zero_after_days": self._decay.zero_after_days,
            },
            "contributions": [c.to_dict() for c in self._contributions],
        }

    @classmethod
    def load(cls, payload: dict[str, Any] | None) -> "RiskLedger":
        if not payload:
            return cls()
        if not isinstance(payload, dict):
            raise RiskEngineError("risk ledger snapshot must be an object")
        if payload.get("state_version") != _SNAPSHOT_VERSION:
            raise RiskEngineError("unsupported risk ledger snapshot version")
        decay_raw = payload.get("decay")
        if not isinstance(decay_raw, dict):
            raise RiskEngineError("risk ledger snapshot requires a decay object")
        try:
            decay = DecayPolicy(
                half_life_days=float(decay_raw["half_life_days"]),
                zero_after_days=float(decay_raw["zero_after_days"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, RiskEngineError):
                raise
            raise RiskEngineError(f"invalid risk ledger decay snapshot: {exc}") from exc
        records = payload.get("contributions")
        if not isinstance(records, list):
            raise RiskEngineError("risk ledger snapshot requires a contributions list")
        ledger = cls(decay)
        for record in records:
            if not isinstance(record, dict):
                raise RiskEngineError("risk ledger snapshot contribution must be an object")
            contribution = RiskContribution.from_dict(record)
            ledger._append_loaded(contribution)
        return ledger

    def _append_loaded(self, contribution: RiskContribution) -> None:
        """Rebuild internal state from one snapshotted contribution, checking
        the chain invariants fail-closed (the snapshot is authoritative state;
        corruption must never be silently accepted)."""
        if contribution.operation not in {
            CONTRIB_OPERATION_POSITIVE,
            CONTRIB_OPERATION_COMPENSATION,
            CONTRIB_OPERATION_REVERSAL,
        }:
            raise RiskEngineError(f"unknown contribution operation {contribution.operation!r}")
        expected_id = contribution_id_for(
            contribution.organization_id,
            contribution.business_key,
            contribution.finding_revision,
            contribution.operation,
        )
        if contribution.contribution_id != expected_id:
            raise RiskEngineError("risk ledger snapshot contribution id does not match its content")
        key = contribution.business_key
        last = self._applied_revision.get(key, 0)
        if contribution.finding_revision <= last:
            raise RiskEngineError(
                f"risk ledger snapshot replays revision {contribution.finding_revision} of {key} (last {last})"
            )
        self._applied_revision[key] = contribution.finding_revision
        existing_org = self._entity_org.setdefault(contribution.entity_id, contribution.organization_id)
        if existing_org != contribution.organization_id:
            raise RiskEngineError(
                f"entity {contribution.entity_id} contributed under two organizations in snapshot"
            )
        self._contributions.append(contribution)
        if contribution.operation == CONTRIB_OPERATION_REVERSAL:
            self._live_amount[key] = 0.0
        else:
            self._live_amount[key] = self._live_amount.get(key, 0.0) + contribution.amount

    def current_view(self, at: datetime) -> dict[str, dict[str, Any]]:
        """Current risk projection for every entity with live contributions."""
        at = _utc_day(at)
        entities = sorted(
            {c.entity_id for c in self._contributions if self._decay.factor(
                (at - c.occurred_at).total_seconds() / 86400.0) > 0.0}
        )
        return {entity: self.entity_risk(entity, at) for entity in entities}


def apply_ledger(ledger: FindingLedger, risk: RiskLedger | None = None) -> RiskLedger:
    """Drive a RiskLedger from every frame a FindingLedger has accepted, in
    per-key revision order. Convenience for tests and replay wiring."""
    risk = risk or RiskLedger()
    frames: list[Frame] = []
    for key_frames in ledger._history.values():  # noqa: SLF001 - same-module family
        frames.extend(key_frames)
    frames.sort(key=lambda f: (f.at, f.business_key, f.revision))  # at is UTC ISO; lexicographic order == chronological
    for frame in frames:
        risk.apply_frame(frame)
    return risk
