"""Baseline module: training jobs, deadlines, minimum samples, cold_start,
evaluation, and immutable model publication (F04).

This is the middle layer of the analysis module DAG (feature <- baseline <-
detection) and the authoritative production implementation of baseline
training (design baseline §6; the Go side in internal/analysis/baseline is
the reference/diagnostic mirror of the same pure core, with no worker or
database wiring).

Contract implemented here:

* baselines are trained per tenant/entity feature set, feature version, and
  generation; a training run requires at least ``min_complete_days`` (14)
  complete UTC days of samples and at least ``min_samples`` (100) persisted
  feature samples;
* training reads ONLY persisted feature samples (``feature_samples`` rows,
  written when a window closes) — never Raw/standard events or Kafka; it
  never fabricates the 14-day observation window from expired event indexes;
  when the persisted sample budget cannot satisfy the policy the lineage
  stays ``cold_start`` and only deterministic rules that explicitly allow
  cold start may run (the detection layer reads the status, it is never
  silently treated as ready);
* a training run only reads samples that are closed, quality-qualified, of
  the same feature version/generation, and whose window ended at or before
  the training deadline watermark;
* publication is immutable: a (organization, model_id, model_version) row is
  insert-only for content; re-publishing the same version with different
  content is rejected fail-closed. A publication carries the training
  deadline, the sample range, evaluation metrics, and the content hash.
  Revision/withdrawal retraining publishes a NEW version; the superseded
  version is retired (kept for audit, referenced via
  ``supersedes_model_version``);
* ``cold_start`` is an explicit, queryable status row of the model lineage,
  recording how many samples/complete days were available at the deadline.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Iterable, Protocol

import psycopg
from psycopg.types.json import Jsonb

from .features import parse_time

DEFAULT_MIN_COMPLETE_DAYS = 14
DEFAULT_MIN_SAMPLES = 100

QUALITY_QUALIFIED = "qualified"
QUALITY_PARTIAL = "partial"


class BaselineError(ValueError):
    """Base class for fail-closed baseline errors."""


class ImmutableModelConflict(BaselineError):
    """Same model version re-published with different content."""


class RevisionConflict(BaselineError):
    """Same sample business key and revision with different content."""


class BaselineStatus(str, Enum):
    """Lifecycle states of one baseline model version."""

    COLD_START = "cold_start"  # declared inputs, not enough samples to score
    TRAINING = "training"
    READY = "ready"  # immutable and scorable
    RETIRED = "retired"  # kept for audit, must not score new data


@dataclass(frozen=True)
class BaselineModel:
    """One immutable baseline version over one feature version."""

    model_id: str
    version: str
    feature_id: str
    status: BaselineStatus
    sample_count: int = 0
    trained_at: datetime | None = None
    feature_version: str = ""
    generation: str = ""
    complete_days: int = 0
    training_cutoff: datetime | None = None
    sample_range: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    statistics: dict[str, Any] = field(default_factory=dict)
    supersedes_model_version: str | None = None

    def __post_init__(self) -> None:
        if not self.model_id or not self.version or not self.feature_id:
            raise ValueError("baseline model id, version, and feature id are required")
        if not isinstance(self.status, BaselineStatus):
            raise ValueError(f"unknown baseline status: {self.status!r}")
        if self.status == BaselineStatus.READY and (self.sample_count < 1 or self.trained_at is None):
            raise ValueError("ready baseline requires samples and a training timestamp")
        if self.sample_count < 0 or self.complete_days < 0:
            raise ValueError("baseline sample and day counts must be non-negative")

    def content_hash(self) -> str:
        """Stable SHA-256 of the immutable publication content."""
        return _canonical_hash(
            {
                "model_id": self.model_id,
                "version": self.version,
                "feature_id": self.feature_id,
                "feature_version": self.feature_version,
                "generation": self.generation,
                "statistics": self.statistics,
                "sample_count": self.sample_count,
                "complete_days": self.complete_days,
                "sample_range": self.sample_range,
                "training_cutoff": _format_time(self.training_cutoff),
            }
        )


@dataclass(frozen=True)
class TrainingPolicy:
    """Minimum-sample policy; a run that cannot meet it stays cold_start."""

    min_complete_days: int = DEFAULT_MIN_COMPLETE_DAYS
    min_samples: int = DEFAULT_MIN_SAMPLES

    def __post_init__(self) -> None:
        if self.min_complete_days < 1 or self.min_samples < 1:
            raise ValueError("training policy bounds must be positive")


@dataclass(frozen=True)
class FeatureSample:
    """One persisted, closed, windowed feature record (training input)."""

    entity_id: str
    feature_version: str
    generation: str
    window_start: datetime
    window_end: datetime
    revision: int
    quality: str
    values: dict[str, Any]
    inputs: list[str]

    def __post_init__(self) -> None:
        if not self.entity_id or not self.feature_version or not self.generation:
            raise ValueError("sample entity id, feature version, and generation are required")
        if self.revision < 1:
            raise ValueError("sample revision must be >= 1")
        if self.quality not in (QUALITY_QUALIFIED, QUALITY_PARTIAL):
            raise ValueError(f"unknown sample quality: {self.quality!r}")
        if self.window_end <= self.window_start:
            raise ValueError("sample window end must be after its start")

    @classmethod
    def from_record(
        cls,
        record: dict[str, Any],
        *,
        generation: str,
        revision: int = 1,
        quality: str = QUALITY_QUALIFIED,
    ) -> "FeatureSample":
        """Build a sample from an F03 ``closed_windows`` feature record."""
        window = record.get("window") or {}
        return cls(
            entity_id=record["entity_id"],
            feature_version=record["feature_version"],
            generation=generation,
            window_start=parse_time(window["start"]),
            window_end=parse_time(window["end"]),
            revision=revision,
            quality=quality,
            values=dict(record["values"]),
            inputs=list(record["inputs"]),
        )

    def content_hash(self) -> str:
        return _canonical_hash({"values": self.values, "inputs": self.inputs})


def _canonical_hash(payload: Any) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def select_training_samples(
    samples: Iterable[FeatureSample],
    *,
    deadline: datetime,
    feature_version: str,
    generation: str,
) -> list[FeatureSample]:
    """The training input set of one run, fail-closed.

    Only samples of the same feature version/generation that are
    quality-qualified and whose window ended at or before the deadline are
    eligible. The latest revision of each (entity, window) business key wins.
    The result is sorted deterministically by (window_start, entity_id).
    """
    if deadline.tzinfo is None:
        raise ValueError("training deadline requires timezone")
    latest: dict[tuple[str, datetime], FeatureSample] = {}
    for sample in samples:
        if sample.feature_version != feature_version or sample.generation != generation:
            continue
        if sample.quality != QUALITY_QUALIFIED:
            continue
        if sample.window_end > deadline:
            continue
        key = (sample.entity_id, sample.window_start)
        existing = latest.get(key)
        if existing is None or sample.revision > existing.revision:
            latest[key] = sample
    return sorted(latest.values(), key=lambda item: (item.window_start, item.entity_id))


def complete_days(samples: Iterable[FeatureSample], *, deadline: datetime) -> int:
    """Distinct UTC calendar days with samples that fully end before the deadline.

    A day counts only when its entire [00:00Z, next 00:00Z) span is at or
    before the training deadline — the final partial day never counts, which
    is what makes the 14-day observation window "complete days".
    """
    days = {sample.window_start.astimezone(timezone.utc).date() for sample in samples}
    count = 0
    for day in days:
        day_end = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(days=1)
        if day_end <= deadline:
            count += 1
    return count


def training_decision(
    samples: list[FeatureSample],
    *,
    deadline: datetime,
    policy: TrainingPolicy = TrainingPolicy(),
) -> dict[str, Any]:
    """Decide trainable vs cold_start with an explicit, auditable reason."""
    days = complete_days(samples, deadline=deadline)
    if len(samples) < policy.min_samples:
        return {
            "trainable": False,
            "reason": "insufficient_samples",
            "sample_count": len(samples),
            "complete_days": days,
            "required_samples": policy.min_samples,
            "required_complete_days": policy.min_complete_days,
        }
    if days < policy.min_complete_days:
        return {
            "trainable": False,
            "reason": "insufficient_complete_days",
            "sample_count": len(samples),
            "complete_days": days,
            "required_samples": policy.min_samples,
            "required_complete_days": policy.min_complete_days,
        }
    return {
        "trainable": True,
        "reason": "ok",
        "sample_count": len(samples),
        "complete_days": days,
        "required_samples": policy.min_samples,
        "required_complete_days": policy.min_complete_days,
    }


def train_baseline(samples: list[FeatureSample]) -> dict[str, Any]:
    """Deterministic per-feature statistics (population moments)."""
    if not samples:
        raise ValueError("training requires at least one sample")
    series: dict[str, list[float]] = {}
    for sample in samples:
        for name, value in sample.values.items():
            series.setdefault(name, []).append(float(value))
    stats: dict[str, Any] = {}
    for name in sorted(series):
        values = series[name]
        mean = math.fsum(values) / len(values)
        variance = math.fsum((value - mean) ** 2 for value in values) / len(values)
        stats[name] = {
            "count": len(values),
            "mean": mean,
            "std": math.sqrt(variance),
            "min": min(values),
            "max": max(values),
        }
    return {"algorithm": "moments.v1", "feature_stats": stats}


def evaluate_baseline(statistics: dict[str, Any], samples: list[FeatureSample]) -> dict[str, Any]:
    """Evaluation metrics published with the model version.

    Deterministic in-sample fit metrics: for every feature the maximum
    absolute z-score of the training samples and the share of samples within
    3 standard deviations (a degenerate zero-variance feature scores any
    equal value as z=0 and any deviation as infinite).
    """
    stats = statistics.get("feature_stats", {})
    if not stats:
        raise ValueError("evaluation requires trained feature statistics")
    if not samples:
        raise ValueError("evaluation requires at least one sample")
    per_feature: dict[str, Any] = {}
    for name in sorted(stats):
        entry = stats[name]
        mean = float(entry["mean"])
        std = float(entry["std"])
        observed = [float(sample.values[name]) for sample in samples if name in sample.values]
        if not observed:
            raise ValueError(f"evaluation sample misses feature: {name}")
        if std == 0.0:
            zscores = [0.0 if value == mean else math.inf for value in observed]
        else:
            zscores = [abs(value - mean) / std for value in observed]
        within = sum(1 for z in zscores if z <= 3.0)
        per_feature[name] = {
            "max_abs_z": max(zscores),
            "within_3sigma_ratio": within / len(zscores),
        }
    return {
        "algorithm": statistics.get("algorithm", ""),
        "sample_count": len(samples),
        "entity_count": len({sample.entity_id for sample in samples}),
        "per_feature": per_feature,
    }


def sample_range(samples: list[FeatureSample]) -> dict[str, Any]:
    if not samples:
        raise ValueError("sample range requires at least one sample")
    return {
        "first_window_start": _format_time(min(sample.window_start for sample in samples)),
        "last_window_end": _format_time(max(sample.window_end for sample in samples)),
        "entities": sorted({sample.entity_id for sample in samples}),
    }


class FeatureSampleStore(Protocol):
    def save(self, organization: str, feature_id: str, sample: FeatureSample) -> str: ...

    def training_samples(
        self,
        organization: str,
        feature_id: str,
        *,
        feature_version: str,
        generation: str,
        deadline: datetime,
    ) -> list[FeatureSample]: ...


class PostgresFeatureSampleStore:
    """Window feature records persisted for baseline training (F04).

    Rows are written when a window closes. A late correction of the same
    business key (entity, window, feature version, generation) rewrites the
    row only with a strictly higher revision; the same revision with
    different content is a fail-closed conflict, and an older revision is a
    no-op (at-least-once replay safe).
    """

    def __init__(self, dsn: str):
        self.connection = psycopg.connect(dsn, autocommit=True)

    def close(self) -> None:
        self.connection.close()

    def stored_revision(self, organization: str, feature_id: str, sample: FeatureSample) -> int:
        """Highest stored revision for the sample's business key, 0 if none."""
        row = self.connection.execute(
            """
            SELECT revision FROM feature_samples
            WHERE organization_id = %s AND feature_id = %s AND feature_version = %s
              AND generation = %s AND entity_id = %s AND window_start = %s
            ORDER BY revision DESC LIMIT 1
            """,
            (
                organization,
                feature_id,
                sample.feature_version,
                sample.generation,
                sample.entity_id,
                sample.window_start,
            ),
        ).fetchone()
        return int(row[0]) if row else 0

    def save(self, organization: str, feature_id: str, sample: FeatureSample) -> str:
        digest = sample.content_hash()
        with self.connection.transaction():
            row = self.connection.execute(
                """
                SELECT revision, content_sha256 FROM feature_samples
                WHERE organization_id = %s AND feature_id = %s AND feature_version = %s
                  AND generation = %s AND entity_id = %s AND window_start = %s
                FOR UPDATE
                """,
                (
                    organization,
                    feature_id,
                    sample.feature_version,
                    sample.generation,
                    sample.entity_id,
                    sample.window_start,
                ),
            ).fetchone()
            if row is not None:
                stored_revision, stored_hash = row
                if sample.revision < stored_revision:
                    return "stale_revision"
                if sample.revision == stored_revision:
                    if stored_hash == digest:
                        return "idempotent"
                    raise RevisionConflict(
                        f"feature sample {sample.entity_id}@{_format_time(sample.window_start)} "
                        f"revision {sample.revision} already stored with different content"
                    )
                self.connection.execute(
                    """
                    UPDATE feature_samples
                    SET revision = %s, window_end = %s, quality = %s, values = %s, inputs = %s,
                        content_sha256 = %s, closed_at = %s
                    WHERE organization_id = %s AND feature_id = %s AND feature_version = %s
                      AND generation = %s AND entity_id = %s AND window_start = %s
                    """,
                    (
                        sample.revision,
                        sample.window_end,
                        sample.quality,
                        Jsonb(sample.values),
                        Jsonb(sample.inputs),
                        digest,
                        datetime.now(timezone.utc),
                        organization,
                        feature_id,
                        sample.feature_version,
                        sample.generation,
                        sample.entity_id,
                        sample.window_start,
                    ),
                )
                return "corrected"
            self.connection.execute(
                """
                INSERT INTO feature_samples (
                  organization_id, feature_id, feature_version, generation, entity_id,
                  window_start, window_end, revision, quality, values, inputs,
                  content_sha256, closed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    organization,
                    feature_id,
                    sample.feature_version,
                    sample.generation,
                    sample.entity_id,
                    sample.window_start,
                    sample.window_end,
                    sample.revision,
                    sample.quality,
                    Jsonb(sample.values),
                    Jsonb(sample.inputs),
                    digest,
                    datetime.now(timezone.utc),
                ),
            )
            return "inserted"

    def training_samples(
        self,
        organization: str,
        feature_id: str,
        *,
        feature_version: str,
        generation: str,
        deadline: datetime,
    ) -> list[FeatureSample]:
        """Closed, qualified samples of one version/generation before the deadline."""
        rows = self.connection.execute(
            """
            SELECT entity_id, window_start, window_end, revision, quality, values, inputs
            FROM feature_samples
            WHERE organization_id = %s AND feature_id = %s AND feature_version = %s
              AND generation = %s AND quality = 'qualified' AND window_end <= %s
            ORDER BY window_start, entity_id
            """,
            (organization, feature_id, feature_version, generation, deadline),
        ).fetchall()
        return [
            FeatureSample(
                entity_id=row[0],
                feature_version=feature_version,
                generation=generation,
                window_start=row[1],
                window_end=row[2],
                revision=row[3],
                quality=row[4],
                values=dict(row[5]),
                inputs=list(row[6]),
            )
            for row in rows
        ]


class PostgresBaselineModelStore:
    """Immutable baseline model versions (F04 publication).

    A version row is insert-only for content: publishing the same version
    with identical content is idempotent, with different content is a
    fail-closed conflict. The only mutation ever applied is the lifecycle
    transition to retired, keeping the old version for audit and linking the
    new version via ``supersedes_model_version``.
    """

    def __init__(self, dsn: str):
        self.connection = psycopg.connect(dsn, autocommit=True)

    def close(self) -> None:
        self.connection.close()

    def publish(self, organization: str, model: BaselineModel) -> str:
        digest = model.content_hash()
        with self.connection.transaction():
            row = self.connection.execute(
                """
                SELECT content_sha256, status FROM baseline_models
                WHERE organization_id = %s AND model_id = %s AND model_version = %s
                FOR UPDATE
                """,
                (organization, model.model_id, model.version),
            ).fetchone()
            if row is not None:
                stored_hash, _stored_status = row
                if stored_hash == digest:
                    return "idempotent"
                raise ImmutableModelConflict(
                    f"baseline model {model.model_id} version {model.version} "
                    "already published with different content"
                )
            self.connection.execute(
                """
                INSERT INTO baseline_models (
                  organization_id, model_id, model_version, feature_id, feature_version,
                  generation, status, sample_count, complete_days, trained_at,
                  training_cutoff, sample_range, metrics, statistics, content_sha256,
                  supersedes_model_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    organization,
                    model.model_id,
                    model.version,
                    model.feature_id,
                    model.feature_version,
                    model.generation,
                    model.status.value,
                    model.sample_count,
                    model.complete_days,
                    model.trained_at,
                    model.training_cutoff,
                    Jsonb(model.sample_range),
                    Jsonb(model.metrics),
                    Jsonb(model.statistics),
                    digest,
                    model.supersedes_model_version,
                ),
            )
            return "published"

    def record_cold_start(self, organization: str, model: BaselineModel) -> str:
        """Record/refresh the queryable cold_start status of a lineage.

        cold_start is pre-model state, not a published model: repeated
        training runs that still cannot meet the policy refresh the same
        version row (counts, cutoff, decision) so the lineage status stays
        current and queryable. A row in any other lifecycle state is never
        overwritten by a cold_start decision.
        """
        if model.status != BaselineStatus.COLD_START:
            raise BaselineError("record_cold_start requires a cold_start model")
        digest = model.content_hash()
        with self.connection.transaction():
            row = self.connection.execute(
                """
                SELECT status, content_sha256 FROM baseline_models
                WHERE organization_id = %s AND model_id = %s AND model_version = %s
                FOR UPDATE
                """,
                (organization, model.model_id, model.version),
            ).fetchone()
            if row is not None:
                stored_status, stored_hash = row
                if stored_status != BaselineStatus.COLD_START.value:
                    raise ImmutableModelConflict(
                        f"baseline model {model.model_id} version {model.version} "
                        f"is {stored_status}; cold_start cannot overwrite it"
                    )
                if stored_hash == digest:
                    return "idempotent"
                self.connection.execute(
                    """
                    UPDATE baseline_models
                    SET sample_count = %s, complete_days = %s, training_cutoff = %s,
                        metrics = %s, content_sha256 = %s, updated_at = now()
                    WHERE organization_id = %s AND model_id = %s AND model_version = %s
                    """,
                    (
                        model.sample_count,
                        model.complete_days,
                        model.training_cutoff,
                        Jsonb(model.metrics),
                        digest,
                        organization,
                        model.model_id,
                        model.version,
                    ),
                )
                return "refreshed"
            self.connection.execute(
                """
                INSERT INTO baseline_models (
                  organization_id, model_id, model_version, feature_id, feature_version,
                  generation, status, sample_count, complete_days, training_cutoff,
                  metrics, content_sha256, supersedes_model_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    organization,
                    model.model_id,
                    model.version,
                    model.feature_id,
                    model.feature_version,
                    model.generation,
                    BaselineStatus.COLD_START.value,
                    model.sample_count,
                    model.complete_days,
                    model.training_cutoff,
                    Jsonb(model.metrics),
                    digest,
                    model.supersedes_model_version,
                ),
            )
            return "published"

    def retire(self, organization: str, model_id: str, version: str) -> str:
        with self.connection.transaction():
            row = self.connection.execute(
                """
                SELECT status FROM baseline_models
                WHERE organization_id = %s AND model_id = %s AND model_version = %s
                FOR UPDATE
                """,
                (organization, model_id, version),
            ).fetchone()
            if row is None:
                raise BaselineError(f"unknown baseline model version: {model_id} {version}")
            if row[0] == BaselineStatus.RETIRED.value:
                return "idempotent"
            self.connection.execute(
                """
                UPDATE baseline_models SET status = 'retired', updated_at = now()
                WHERE organization_id = %s AND model_id = %s AND model_version = %s
                """,
                (organization, model_id, version),
            )
            return "retired"

    def load(self, organization: str, model_id: str, version: str) -> BaselineModel | None:
        row = self.connection.execute(
            """
            SELECT feature_id, feature_version, generation, status, sample_count,
                   complete_days, trained_at, training_cutoff, sample_range, metrics,
                   statistics, supersedes_model_version
            FROM baseline_models
            WHERE organization_id = %s AND model_id = %s AND model_version = %s
            """,
            (organization, model_id, version),
        ).fetchone()
        if row is None:
            return None
        return BaselineModel(
            model_id=model_id,
            version=version,
            feature_id=row[0],
            feature_version=row[1],
            generation=row[2],
            status=BaselineStatus(row[3]),
            sample_count=row[4],
            complete_days=row[5],
            trained_at=row[6],
            training_cutoff=row[7],
            sample_range=dict(row[8]),
            metrics=dict(row[9]),
            statistics=dict(row[10]),
            supersedes_model_version=row[11],
        )

    def latest(self, organization: str, model_id: str) -> BaselineModel | None:
        row = self.connection.execute(
            """
            SELECT model_version FROM baseline_models
            WHERE organization_id = %s AND model_id = %s
            ORDER BY created_at DESC, model_version DESC
            LIMIT 1
            """,
            (organization, model_id),
        ).fetchone()
        if row is None:
            return None
        return self.load(organization, model_id, row[0])

    def status(self, organization: str, model_id: str) -> dict[str, Any] | None:
        """Explicit, queryable lineage status — cold_start included."""
        model = self.latest(organization, model_id)
        if model is None:
            return None
        return {
            "model_id": model.model_id,
            "version": model.version,
            "status": model.status.value,
            "sample_count": model.sample_count,
            "complete_days": model.complete_days,
            "trained_at": _format_time(model.trained_at),
            "training_cutoff": _format_time(model.training_cutoff),
            "supersedes_model_version": model.supersedes_model_version,
        }


def run_training_job(
    sample_store: FeatureSampleStore,
    model_store: PostgresBaselineModelStore,
    *,
    organization: str,
    model_id: str,
    model_version: str,
    feature_id: str,
    feature_version: str,
    generation: str,
    deadline: datetime,
    policy: TrainingPolicy = TrainingPolicy(),
    trained_at: datetime | None = None,
) -> dict[str, Any]:
    """One baseline training run: select -> decide -> train -> evaluate -> publish.

    The deadline is the training cutoff watermark; only persisted samples of
    the same feature version/generation whose closed, qualified windows ended
    at or before it are read. An insufficient budget publishes an explicit
    cold_start status row (never a fabricated model); a trainable budget
    publishes a new immutable ready version and retires the superseded
    lineage head, which is kept for audit via ``supersedes_model_version``.
    """
    samples = sample_store.training_samples(
        organization,
        feature_id,
        feature_version=feature_version,
        generation=generation,
        deadline=deadline,
    )
    decision = training_decision(samples, deadline=deadline, policy=policy)
    previous = model_store.latest(organization, model_id)
    supersedes = previous.version if previous is not None and previous.version != model_version else None
    if not decision["trainable"]:
        model = BaselineModel(
            model_id=model_id,
            version=model_version,
            feature_id=feature_id,
            feature_version=feature_version,
            generation=generation,
            status=BaselineStatus.COLD_START,
            sample_count=decision["sample_count"],
            complete_days=decision["complete_days"],
            training_cutoff=deadline,
            metrics={"decision": decision},
            supersedes_model_version=supersedes,
        )
        outcome = model_store.record_cold_start(organization, model)
        return {
            "status": BaselineStatus.COLD_START.value,
            "publish": outcome,
            "decision": decision,
            "model_version": model_version,
        }
    statistics = train_baseline(samples)
    metrics = evaluate_baseline(statistics, samples)
    metrics["decision"] = decision
    model = BaselineModel(
        model_id=model_id,
        version=model_version,
        feature_id=feature_id,
        feature_version=feature_version,
        generation=generation,
        status=BaselineStatus.READY,
        sample_count=len(samples),
        complete_days=decision["complete_days"],
        trained_at=trained_at or datetime.now(timezone.utc),
        training_cutoff=deadline,
        sample_range=sample_range(samples),
        metrics=metrics,
        statistics=statistics,
        supersedes_model_version=supersedes,
    )
    outcome = model_store.publish(organization, model)
    if supersedes is not None:
        model_store.retire(organization, model_id, supersedes)
    return {
        "status": BaselineStatus.READY.value,
        "publish": outcome,
        "decision": decision,
        "model_version": model_version,
        "sample_range": model.sample_range,
        "metrics": metrics,
        "supersedes_model_version": supersedes,
    }
