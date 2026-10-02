"""Unified authoritative analysis scheduling pipeline (F08).

This module is the single production scheduling entry of the analysis plane
(design baseline §6: "正式调度入口只有 tuba-analysis-worker"). It consumes the
entity-keyed attributed stream (``tuba.attributed.events.v1``, E04), drives
the bounded event-time windows (F02 FeatureWindows), computes window features
(F03 ``compute_window_features``), runs all three first-phase detection
scenarios registered in the analysis registry (F05: failure-then-success,
failure-burst, baseline-deviation), derives immutable risk contributions and
republishes the entity risk projection with a pinned compute version and
update time (R01/R02), schedules the baseline training job (F04
``run_training_job``) and emits every derived object as an analysis-result v2
envelope (C04/F07) with FindingLedger revision/generation semantics (F06).

No double emission: the Go detection/feature/baseline packages are
diagnostic/reference mirrors with no worker or topic wiring (asserted by
internal/analysis/assembly_test.go); only this pipeline produces analysis
results.

Input contract note: the attributed-event v1 schema is closed at the top
level, so the authentication event semantics the scenarios need (outcome,
quality, host, source ip) ride inside the open ``evidence`` object as
``evidence.event`` — the E04 entity-worker embeds exactly this summary
(``EventSummary`` in internal/entityworker). A resolved actor contribution
without an outcome is rejected fail-closed (DLQ), never guessed.

v1 compatibility: this pipeline emits v2 envelopes only. Any remaining v1
producers stay queryable through the F07 analysis-sink migration path
(legacy write + ``MigrateLegacy``), which is the compatibility design for the
migration window.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from .baseline import (
    BaselineModel,
    BaselineStatus,
    FeatureSample,
    PostgresBaselineModelStore,
    FeatureSampleStore,
    RevisionConflict,
    TrainingPolicy,
    run_training_job,
)
from .detection import (
    RULE_BASELINE_DEVIATION,
    RULE_FAILURE_BURST,
    RULE_FAILURE_THEN_SUCCESS,
    detect_baseline_deviation,
    detect_failure_burst,
    detect_failure_then_success,
    parse_time,
)
from .features import FeatureWindows
from .registry import AnalysisRegistry, ModelDefinition, RuleDefinition
from .revisions import FindingLedger, Frame
from .risk import RISK_COMPUTE_VERSION, RiskLedger

ATTRIBUTED_SCHEMA_VERSION = "1.0.0"
CONTRACT_VERSION_V2 = "2.0.0"

# The entity_risk projection object (R02): its state owner is the RiskLedger,
# so the envelope rule identity is the risk compute itself, and the rule
# version is pinned to the risk compute version.
RISK_RULE_ID = "risk.entity"
OBJECT_TYPE_ENTITY_RISK = "entity_risk"

ROLE_ACTOR = "actor"
STATE_RESOLVED = "resolved"
STATE_UNRESOLVED = "unresolved"
STATE_AMBIGUOUS = "ambiguous"

# How long a loaded ready baseline model stays cached before re-reading the
# model store (publication of a new immutable version must become visible).
MODEL_CACHE_TTL = timedelta(seconds=300)

# Feature window size used for closed-window feature records, baseline
# sampling and the statistical scenario comes from the baseline-deviation
# rule registration.


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_hash(payload: Any) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class AttributedContractError(ValueError):
    """Fail-closed rejection of one attributed contribution (DLQ path)."""


class AttributedContribution:
    """One validated attributed-event contribution (contracts/events/attributed-event/1)."""

    def __init__(self, raw: dict[str, Any], organization: str):
        if not isinstance(raw, dict):
            raise AttributedContractError("attributed contribution must be a JSON object")
        if raw.get("schema_version") != ATTRIBUTED_SCHEMA_VERSION:
            raise AttributedContractError("unsupported attributed schema version")
        if raw.get("organization_id") != organization:
            raise AttributedContractError("cross-tenant attributed contribution")
        self.organization_id = organization
        self.event_id = str(raw.get("event_id") or "")
        if not self.event_id:
            raise AttributedContractError("attributed contribution requires event_id")
        event_time = raw.get("event_time")
        if not isinstance(event_time, str) or not event_time:
            raise AttributedContractError("attributed contribution requires event_time")
        self.event_time = parse_time(event_time)
        self.attribution_id = str(raw.get("attribution_id") or "")
        if not self.attribution_id.startswith("att:"):
            raise AttributedContractError("attribution_id must carry the att: prefix")
        self.role = str(raw.get("role") or "")
        if self.role not in {"actor", "target", "group", "host", "source_device", "destination_device"}:
            raise AttributedContractError(f"unknown attribution role: {self.role!r}")
        self.state = str(raw.get("state") or "")
        if self.state not in {STATE_RESOLVED, STATE_UNRESOLVED, STATE_AMBIGUOUS}:
            raise AttributedContractError(f"unknown attribution state: {self.state!r}")
        self.entity_id = str(raw.get("entity_id") or "")
        if self.state == STATE_RESOLVED:
            if not self.entity_id.startswith("ent:"):
                raise AttributedContractError("resolved attribution requires an ent: entity id")
        else:
            if self.entity_id:
                raise AttributedContractError("unresolved/ambiguous attribution must not carry an entity id")
            if not raw.get("reason"):
                raise AttributedContractError("unresolved/ambiguous attribution requires a reason")
        if not raw.get("partition_key"):
            raise AttributedContractError("attributed contribution requires partition_key")
        evidence = raw.get("evidence")
        if not isinstance(evidence, dict):
            raise AttributedContractError("attributed contribution requires the evidence object")
        summary = evidence.get("event") or {}
        if not isinstance(summary, dict):
            raise AttributedContractError("evidence.event must be an object when present")
        self.outcome = str(summary.get("outcome") or "")
        self.quality = str(summary.get("quality") or "qualified")
        self.host_id = str(summary.get("host_id") or "")
        self.source_ip = str(summary.get("source_ip") or "")
        if self.scenario_eligible and not self.outcome:
            # The auth scenarios cannot run without the event outcome; guessing
            # is forbidden (fail-closed into the DLQ path).
            raise AttributedContractError("resolved actor contribution carries no event outcome")

    @property
    def scenario_eligible(self) -> bool:
        """Only resolved actor contributions feed the account-centric auth scenarios."""
        return self.state == STATE_RESOLVED and self.role == ROLE_ACTOR

    def detection_event(self) -> dict[str, Any]:
        """Minimal UIM-shaped detection input reconstructed from the contribution."""
        event: dict[str, Any] = {
            "@timestamp": _format_time(self.event_time),
            "organization": {"id": self.organization_id},
            "user": {"id": self.entity_id},
            "event": {"id": self.event_id, "outcome": self.outcome},
            "ueba": {
                "quality": {"status": self.quality},
                "schema": {"version": f"attributed-event/{ATTRIBUTED_SCHEMA_VERSION}"},
            },
        }
        if self.host_id:
            event["host"] = {"id": self.host_id}
        if self.source_ip:
            event["source"] = {"ip": self.source_ip}
        return event


class AttributedAnalysisProcessor:
    """Bounded per-entity scheduling core: windows -> features -> scenarios -> v2.

    One instance per input partition. All state (feature windows, finding
    ledger, closed-window revision tracking, counters) exports/loads as one
    versioned snapshot persisted through the runtime store, so recovery is
    authoritative from the PG processor state.
    """

    state_version = 3

    def __init__(
        self,
        organization: str,
        namespace: str,
        registry: AnalysisRegistry,
        *,
        sample_store: FeatureSampleStore | None = None,
        model_store: PostgresBaselineModelStore | None = None,
    ):
        self.organization = organization
        self.namespace = namespace
        self.registry = registry
        self.rule_fts = registry.rule(RULE_FAILURE_THEN_SUCCESS)
        self.rule_burst = registry.rule(RULE_FAILURE_BURST)
        self.rule_deviation = registry.rule(RULE_BASELINE_DEVIATION)
        self.model_definition = registry.model(self.rule_deviation.model_id or "")
        lookback = max(
            rule.lookback_seconds
            for rule in (self.rule_fts, self.rule_burst, self.rule_deviation)
        )
        lateness = max(
            rule.allowed_lateness_seconds
            for rule in (self.rule_fts, self.rule_burst, self.rule_deviation)
        )
        self.windows = FeatureWindows(
            lookback=timedelta(seconds=lookback),
            allowed_lateness=timedelta(seconds=lateness),
        )
        self.ledger = FindingLedger()
        self.risk = RiskLedger()
        # Closed-window bookkeeping: (entity, window start) -> revision/hash of
        # the last emitted feature record. A late correction inside the
        # retention boundary recomputes the same window with revision+1.
        self.window_frames: dict[str, dict[str, Any]] = {}
        self.counters: dict[str, int] = defaultdict(int)
        self.sample_store = sample_store
        self.model_store = model_store
        self._model_cache: tuple[datetime, BaselineModel] | None = None
        self.last_baseline: dict[str, Any] | None = None

    # ------------------------------------------------------------------ rules
    @property
    def watermark(self) -> datetime | None:
        return self.windows.watermark

    @property
    def feature_window_seconds(self) -> int:
        return self.rule_deviation.window_seconds

    def _retention(self) -> timedelta:
        return self.windows.lookback + self.windows.allowed_lateness

    def _current_model(self, at: datetime) -> BaselineModel:
        """Latest published baseline model; cold_start placeholder when absent."""
        if self._model_cache is not None and at - self._model_cache[0] < MODEL_CACHE_TTL:
            return self._model_cache[1]
        model: BaselineModel | None = None
        if self.model_store is not None:
            model = self.model_store.latest(self.organization, self.model_definition.id)
        if model is None:
            model = BaselineModel(
                model_id=self.model_definition.id,
                version="0.0.0",
                feature_id=self.model_definition.feature_id,
                feature_version=self.model_definition.feature_version,
                generation=self.model_definition.generation,
                status=BaselineStatus.COLD_START,
            )
        self._model_cache = (at, model)
        return model

    # -------------------------------------------------------------- envelopes
    def _envelope(
        self,
        *,
        object_type: str,
        object_id: str,
        revision: int,
        operation: str,
        generation: str,
        rule_id: str,
        rule_version: str,
        run_id: str,
        window_start: datetime,
        input_refs: list[dict[str, Any]],
        document: dict[str, Any],
    ) -> dict[str, Any]:
        if revision < 1 or not object_id or not generation:
            raise ValueError("v2 envelope requires object id, generation and revision >= 1")
        window_start = window_start.astimezone(timezone.utc)
        return {
            "contract_version": CONTRACT_VERSION_V2,
            "object_type": object_type,
            "object_id": object_id,
            "revision": revision,
            "operation": operation,
            "generation": generation,
            "organization_id": self.organization,
            "namespace": self.namespace,
            "rule_id": rule_id,
            "rule_version": rule_version,
            "run_id": run_id,
            "window_start": _format_time(window_start),
            "date_key": window_start.strftime("%Y-%m-%d"),
            "input_refs": input_refs,
            "document": document,
        }

    def _finding_envelopes(
        self,
        anomalies: Iterable[dict[str, Any]],
        rule: RuleDefinition,
        run_id: str,
        at: datetime,
    ) -> list[dict[str, Any]]:
        """Publish findings through the ledger; emit only changed frames (F06)."""
        envelopes: list[dict[str, Any]] = []
        for anomaly in anomalies:
            document = anomaly["_source"]
            window_start = parse_time(document["detection"]["window"]["start"])
            entity_id = document["entity"]["id"]
            frame = self.ledger.recompute(
                document,
                organization_id=self.organization,
                rule_id=rule.id,
                entity_id=entity_id,
                window_start=window_start,
                generation=document["anomaly"]["generation"],
                object_id=anomaly["_id"],
                at=at,
            )
            if frame is None:
                continue
            envelopes.extend(self._frame_envelopes(frame, rule.id, rule.version, run_id))
        return envelopes

    def _frame_envelopes(
        self,
        frame: Frame,
        rule_id: str,
        rule_version: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """One ledger frame -> anomaly envelope + entity_risk projection (R02).

        Every frame the FindingLedger emits — upsert, correction or retracted
        tombstone — is applied to the RiskLedger (immutable contribution /
        compensation / reversal, R01) and the entity's current risk projection
        is republished at the next deterministic projection revision. A
        retracted finding therefore always lowers the projected risk (to zero
        when it was the key's only live contribution); the sink's external
        version semantics keep the newer projection from ever being
        overwritten by a stale one.
        """
        document = frame.document or {}
        window_start = parse_time(document["detection"]["window"]["start"])
        entity_id = document["entity"]["id"]
        input_refs = [
            {"object_type": "event", "object_id": event_id}
            for event_id in document["evidence"]["event_ids"]
        ]
        anomaly_envelope = self._envelope(
            object_type="anomaly",
            object_id=frame.object_id,
            revision=frame.revision,
            operation=frame.operation,
            generation=frame.generation,
            rule_id=rule_id,
            rule_version=rule_version,
            run_id=run_id,
            window_start=window_start,
            input_refs=input_refs,
            document=document,
        )
        self.counters["findings_emitted"] += 1
        self.risk.apply_frame(frame)
        self.counters["risk_contributions_applied"] += 1
        return [anomaly_envelope, self._entity_risk_envelope(frame, entity_id, run_id)]

    def _entity_risk_envelope(self, frame: Frame, entity_id: str, run_id: str) -> dict[str, Any]:
        """Current entity risk projection as an entity_risk v2 object (R02).

        Revision = the length of the entity's immutable contribution chain
        (state owner: RiskLedger). The projection is computed and pinned at
        the triggering frame's time: ``updated_at`` and the decayed score are
        deterministic for a given chain state, identical content replays
        idempotently at the same revision, and decay parameters ride along so
        readers can age the score without a rewrite.
        """
        frame_time = parse_time(frame.at)
        projection = self.risk.entity_risk(entity_id, frame_time)
        return self._envelope(
            object_type=OBJECT_TYPE_ENTITY_RISK,
            object_id=entity_id,
            revision=self.risk.projection_revision(entity_id),
            operation="upsert",
            generation=frame.generation,
            rule_id=RISK_RULE_ID,
            rule_version=RISK_COMPUTE_VERSION,
            run_id=run_id,
            window_start=frame_time,
            input_refs=[
                {
                    "object_type": "anomaly",
                    "object_id": frame.object_id,
                    "revision": frame.revision,
                    "content_hash": frame.content_hash,
                }
            ],
            document=projection,
        )

    def retract_finding(self, business_key: str, *, reason: str, run_id: str, at: datetime) -> list[dict[str, Any]]:
        """Withdraw one published finding: retracted tombstone + risk rollback.

        The FindingLedger appends the tombstone at the next revision; the
        RiskLedger derives the reverse compensation and the entity risk
        projection is republished with the lowered (possibly zero) score.
        """
        frame = self.ledger.retract(business_key, at=at, reason=reason)
        document = frame.document or {}
        rule_id = document.get("detection", {}).get("rule_id") or RISK_RULE_ID
        rule_version = document.get("detection", {}).get("rule_version") or RISK_COMPUTE_VERSION
        return self._frame_envelopes(frame, rule_id, rule_version, run_id)

    # ------------------------------------------------------------- processing
    def process(
        self,
        raw: dict[str, Any],
        run_id: str,
        *,
        received_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        contribution = AttributedContribution(raw, self.organization)
        at = received_at or datetime.now(timezone.utc)
        if not contribution.scenario_eligible:
            # Unresolved/ambiguous contributions and non-actor roles stay
            # consumable event-level input for future scenarios; they are
            # counted, never silently dropped, and never fed to the account
            # scenarios under a wrong entity.
            key = "skipped_unresolved" if contribution.state != STATE_RESOLVED else "skipped_non_actor_role"
            self.counters[key] += 1
            return []
        self.counters["contributions_processed"] += 1
        event = contribution.detection_event()
        entity_id = contribution.entity_id
        _is_late, retained = self.windows.observe(
            event,
            entity_id,
            contribution.event_time,
            received_at=at,
            dedup_key=contribution.attribution_id,
        )

        envelopes: list[dict[str, Any]] = []
        fts = detect_failure_then_success(
            retained,
            self.organization,
            rule_id=self.rule_fts.id,
            rule_version=self.rule_fts.version,
            generation=self.rule_fts.generation,
            severity=self.rule_fts.severity,
            threshold=self.rule_fts.threshold or 1,
            lookback=timedelta(seconds=self.rule_fts.lookback_seconds),
            window_seconds=self.rule_fts.window_seconds,
        )
        envelopes.extend(self._finding_envelopes(fts, self.rule_fts, run_id, at))
        burst = detect_failure_burst(
            retained,
            self.organization,
            rule_id=self.rule_burst.id,
            rule_version=self.rule_burst.version,
            generation=self.rule_burst.generation,
            severity=self.rule_burst.severity,
            threshold=self.rule_burst.threshold or 1,
            window_seconds=self.rule_burst.window_seconds,
        )
        envelopes.extend(self._finding_envelopes(burst, self.rule_burst, run_id, at))
        envelopes.extend(self._drain_closed(entity_id=entity_id, run_id=run_id, at=at))
        return envelopes

    def advance(self, now: datetime, run_id: str) -> list[dict[str, Any]]:
        """Idle advance: close windows whose watermark passed, emit their results."""
        self.windows.advance(now)
        envelopes: list[dict[str, Any]] = []
        for entity_id in sorted(self.windows.entity_max):
            envelopes.extend(self._drain_closed(entity_id=entity_id, run_id=run_id, at=now))
        self._prune_window_frames()
        return envelopes

    def _drain_closed(self, *, entity_id: str, run_id: str, at: datetime) -> list[dict[str, Any]]:
        """Closed-window path: feature record -> sample -> statistical scenario."""
        envelopes: list[dict[str, Any]] = []
        seconds = self.feature_window_seconds
        for record in self.windows.closed_windows(entity_id, seconds):
            window_start = parse_time(record["window"]["start"])
            frame_key = f"{entity_id}|{_format_time(window_start)}"
            digest = _canonical_hash({"values": record["values"], "inputs": record["inputs"]})
            tracked = self.window_frames.get(frame_key)
            if tracked is not None and tracked["hash"] == digest:
                continue
            revision = 1 if tracked is None else tracked["revision"] + 1
            self.window_frames[frame_key] = {"hash": digest, "revision": revision}

            if self.sample_store is not None:
                sample = FeatureSample.from_record(
                    record,
                    generation=self.model_definition.generation,
                    revision=revision,
                )
                # 崩溃/状态重置后内存中的窗口 revision 可能落后于 PG 已存样本：
                # 同键异内容按 F06 语义必须推进 revision 而非崩溃循环。
                try:
                    self.sample_store.save(self.organization, self.model_definition.feature_id, sample)
                except RevisionConflict:
                    stored = self.sample_store.stored_revision(
                        self.organization, self.model_definition.feature_id, sample
                    )
                    revision = stored + 1
                    self.window_frames[frame_key] = {"hash": digest, "revision": revision}
                    sample = FeatureSample.from_record(
                        record,
                        generation=self.model_definition.generation,
                        revision=revision,
                    )
                    self.sample_store.save(self.organization, self.model_definition.feature_id, sample)
            feature_object_id = "feat:" + _canonical_hash(
                {
                    "organization": self.organization,
                    "feature_id": self.model_definition.feature_id,
                    "feature_version": record["feature_version"],
                    "generation": self.model_definition.generation,
                    "entity_id": entity_id,
                    "window_start": _format_time(window_start),
                }
            )
            envelopes.append(
                self._envelope(
                    object_type="feature",
                    object_id=feature_object_id,
                    revision=revision,
                    operation="upsert",
                    generation=self.model_definition.generation,
                    rule_id=self.model_definition.feature_id,
                    rule_version=record["feature_version"],
                    run_id=run_id,
                    window_start=window_start,
                    input_refs=[{"object_type": "event", "object_id": eid} for eid in record["inputs"]],
                    document=record,
                )
            )
            self.counters["feature_records_emitted"] += 1

            model = self._current_model(at)
            result = detect_baseline_deviation(
                record,
                self.organization,
                model,
                rule_id=self.rule_deviation.id,
                rule_version=self.rule_deviation.version,
                generation=self.rule_deviation.generation,
                severity=self.rule_deviation.severity,
                z_threshold=self.rule_deviation.z_threshold or 3.0,
            )
            self.last_baseline = result["baseline"]
            if not result["baseline"]["scored"]:
                self.counters["baseline_skipped_not_ready"] += 1
            envelopes.extend(self._finding_envelopes(result["findings"], self.rule_deviation, run_id, at))
        return envelopes

    def _prune_window_frames(self) -> None:
        watermark = self.windows.watermark
        if watermark is None:
            return
        cutoff = watermark - self._retention()
        for key in sorted(self.window_frames):
            _, start = key.rsplit("|", 1)
            if parse_time(start) < cutoff:
                del self.window_frames[key]

    # ------------------------------------------------------------------ state
    def export_state(self) -> dict[str, Any]:
        return {
            "state_version": self.state_version,
            "windows": self.windows.export(),
            "ledger": self.ledger.export(),
            "risk": self.risk.export(),
            "window_frames": dict(self.window_frames),
            "counters": dict(self.counters),
        }

    @classmethod
    def from_state(
        cls,
        organization: str,
        namespace: str,
        registry: AnalysisRegistry,
        state: dict[str, Any] | None,
        *,
        sample_store: FeatureSampleStore | None = None,
        model_store: PostgresBaselineModelStore | None = None,
    ) -> "AttributedAnalysisProcessor":
        processor = cls(
            organization,
            namespace,
            registry,
            sample_store=sample_store,
            model_store=model_store,
        )
        if not state:
            return processor
        if state.get("state_version") != cls.state_version:
            raise ValueError("unsupported processor state version")
        processor.windows.load(state.get("windows") or {})
        processor.ledger = FindingLedger.load(str(state.get("ledger") or ""))
        processor.risk = RiskLedger.load(state.get("risk"))
        frames = state.get("window_frames") or {}
        if not isinstance(frames, dict):
            raise ValueError("processor state window_frames must be an object")
        processor.window_frames = dict(frames)
        processor.counters = defaultdict(int, state.get("counters", {}))
        return processor


def baseline_envelope(
    outcome: dict[str, Any],
    *,
    model_definition: ModelDefinition,
    organization: str,
    namespace: str,
    run_id: str,
    cutoff: datetime,
) -> dict[str, Any] | None:
    """v2 baseline object for a freshly published ready model (else None)."""
    if outcome.get("status") != BaselineStatus.READY.value or outcome.get("publish") != "published":
        return None
    sample_range = outcome.get("sample_range") or {}
    window_start = parse_time(sample_range["first_window_start"])
    model_version = str(outcome["model_version"])
    document = {
        "model_id": model_definition.id,
        "model_version": model_version,
        "feature_id": model_definition.feature_id,
        "feature_version": model_definition.feature_version,
        "generation": model_definition.generation,
        "status": BaselineStatus.READY.value,
        "training_cutoff": _format_time(cutoff),
        "sample_range": sample_range,
        "metrics": outcome.get("metrics") or {},
        "supersedes_model_version": outcome.get("supersedes_model_version"),
    }
    window_start = window_start.astimezone(timezone.utc)
    return {
        "contract_version": CONTRACT_VERSION_V2,
        "object_type": "baseline",
        "object_id": "base:" + _canonical_hash(
            {"organization": organization, "model_id": model_definition.id, "model_version": model_version}
        ),
        "revision": 1,
        "operation": "upsert",
        "generation": model_definition.generation,
        "organization_id": organization,
        "namespace": namespace,
        "rule_id": model_definition.id,
        "rule_version": model_version,
        "run_id": run_id,
        "window_start": _format_time(window_start),
        "date_key": window_start.strftime("%Y-%m-%d"),
        "input_refs": [],
        "document": document,
    }


class TrainingScheduler:
    """Periodic cadence of the baseline training job (design baseline §6).

    One tick per model whose last completed run is older than the registered
    ``training_interval_seconds`` (default daily). The training cutoff is the
    tick time itself; the published model version is derived deterministically
    from it (``<registry major>.<minor>.<UTC YYYYMMDDHHMMSS>``), so retraining
    after corrections produces a new immutable version and an identical cutoff
    replays idempotently.
    """

    def __init__(self, models: Iterable[ModelDefinition]):
        registered = list(models)
        self.models = {model.id: model for model in registered}
        if len(self.models) != len(registered):
            raise ValueError("duplicate model in training scheduler")
        self.last_run: dict[str, str] = {}

    def due(self, now: datetime) -> list[ModelDefinition]:
        ready: list[ModelDefinition] = []
        for model_id in sorted(self.models):
            model = self.models[model_id]
            last = self.last_run.get(model_id)
            if last is None:
                ready.append(model)
                continue
            if (now - parse_time(last)).total_seconds() >= model.training_interval_seconds:
                ready.append(model)
        return ready

    def record(self, model_id: str, now: datetime) -> None:
        if model_id not in self.models:
            raise ValueError(f"unknown training model: {model_id}")
        self.last_run[model_id] = _format_time(now) or ""

    def export(self) -> dict[str, Any]:
        return {"last_run": dict(self.last_run)}

    def load(self, state: dict[str, Any] | None) -> None:
        last_run = (state or {}).get("last_run") or {}
        if not isinstance(last_run, dict):
            raise ValueError("training scheduler state last_run must be an object")
        self.last_run = {str(key): str(value) for key, value in last_run.items()}


def training_model_version(model: ModelDefinition, cutoff: datetime) -> str:
    """Deterministic immutable version of one training run (semver)."""
    parts = model.version.split(".")
    stamp = cutoff.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{parts[0]}.{parts[1]}.{stamp}"


def run_due_training(
    scheduler: TrainingScheduler,
    sample_store: FeatureSampleStore,
    model_store: PostgresBaselineModelStore,
    *,
    organization: str,
    namespace: str,
    now: datetime,
    run_id: str,
) -> list[dict[str, Any]]:
    """Run every due training job; emit baseline objects for new ready versions."""
    envelopes: list[dict[str, Any]] = []
    for model in scheduler.due(now):
        outcome = run_training_job(
            sample_store,
            model_store,
            organization=organization,
            model_id=model.id,
            model_version=training_model_version(model, now),
            feature_id=model.feature_id,
            feature_version=model.feature_version,
            generation=model.generation,
            deadline=now,
            policy=TrainingPolicy(
                min_complete_days=model.min_complete_days,
                min_samples=model.min_samples,
            ),
            trained_at=now,
        )
        scheduler.record(model.id, now)
        envelope = baseline_envelope(
            outcome,
            model_definition=model,
            organization=organization,
            namespace=namespace,
            run_id=run_id,
            cutoff=now,
        )
        if envelope is not None:
            envelopes.append(envelope)
    return envelopes


# Re-exported for the worker entrypoint (single import surface).
__all__ = [
    "AttributedAnalysisProcessor",
    "AttributedContribution",
    "AttributedContractError",
    "TrainingScheduler",
    "baseline_envelope",
    "run_due_training",
    "training_model_version",
]
