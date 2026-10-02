"""Feature module: bounded per-entity event-time windows with watermarking.

This is the feature layer of the analysis module DAG (feature <- baseline <-
detection). Detection code consumes retained windows from here; concrete
feature computations are F03 scope.

F02 semantics contract — shared with the Go window engine in
internal/analysis/feature/engine.go, which consumes the entity-keyed
attributed stream and aggregates into tumbling windows. Both sides implement
the same rules; they differ only in state layout (this module retains the
bounded per-entity event history those windows are computed from):

* per-entity event-time watermark = max observed event time for that entity
  minus allowed lateness; the global watermark is the minimum over entities;
* a contribution behind its entity watermark is late: it is still recorded
  while inside the retention boundary and downstream windows are recomputed
  (the Go side reports ``late_accepted``); beyond the boundary it can only
  enter through controlled backfill;
* an entity partition with no input for ``idle_timeout`` advances its
  watermark from processing time (``now - allowed_lateness``) so its windows
  still close instead of waiting forever;
* an event time further than ``future_time_limit`` ahead of the receive time
  (clock skew / forged input) is rejected fail-closed — never applied to
  state — and counted;
* deduplication is by a stable contribution key (``event.id`` on this stream,
  ``attribution_id`` on the attributed stream): a repeat delivery is counted
  exactly once;
* all state is bounded: per-entity history by ``lookback + allowed_lateness``
  and ``max_events_per_entity`` (oldest evicted first, counted), entity count
  by ``max_entities`` (idle shells evicted first, active state never dropped;
  if nothing is evictable the input is rejected fail-closed). Nothing is
  dropped silently — every rejection/eviction lands in ``counters``, and
  retention-pruned (out-of-bounds) events are handed to the optional
  ``backfill_sink`` for the controlled backfill path (design baseline §6:
  out-of-bounds data may only enter a new generation through controlled
  backfill; scheduling the backfill job is F08/replay scope).

F03 concrete features: ``compute_window_features`` is the authoritative
production implementation of the first-phase authentication feature set
(design baseline §6). The Go side (internal/analysis/feature/compute.go) is
the diagnostic/reference implementation of the identical definitions until
F08 unifies scheduling; both are pinned to the same golden test vector, so
the same window and the same logical input yield the same values. The Go
side has no worker wiring, so there is no double-emission risk.
"""

from __future__ import annotations

from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

# Immutable feature version stamp of the first-phase authentication set.
# Any change to the definitions in compute_window_features requires a new
# version.
FEATURE_VERSION_AUTH_V1 = "1.0.0"

FEATURE_AUTH_ATTEMPT_COUNT = "auth.attempt.count"
FEATURE_AUTH_FAILURE_COUNT = "auth.failure.count"
FEATURE_AUTH_FAILURE_RATE = "auth.failure.rate"
FEATURE_AUTH_SOURCE_DEVICE_COUNT = "auth.source_device.count"
FEATURE_AUTH_SOURCE_IP_COUNT = "auth.source_ip.count"
FEATURE_AUTH_FAILURE_THEN_SUCCESS_COUNT = "auth.failure_then_success.count"


def compute_window_features(
    events: list[dict[str, Any]],
    *,
    feature_version: str = FEATURE_VERSION_AUTH_V1,
) -> dict[str, Any]:
    """Compute the first-phase authentication features of one bounded window.

    Deterministic and order-independent: events are re-sorted by
    (event time, event id) before the failure-then-success sequence feature
    is evaluated. Returns the computed values plus the feature version stamp
    and the sorted contributing event ids as input references (for F04
    baseline sampling and F05 detection input).

    Definitions (identical to the Go reference implementation):
    ``auth.attempt.count`` counts all events; ``auth.failure.count`` counts
    outcome "failure"; ``auth.failure.rate`` is failures / attempts (0.0 when
    empty); the distinct-count features count non-empty host.id / source.ip
    values; ``auth.failure_then_success.count`` counts successes that follow
    at least one failure since the previous success.
    """
    ordered = sorted(events, key=lambda item: (parse_time(item["@timestamp"]), item["event"]["id"]))
    failures = 0
    failure_then_success = 0
    failures_since_success = 0
    devices: set[str] = set()
    ips: set[str] = set()
    for item in ordered:
        outcome = item.get("event", {}).get("outcome")
        if outcome == "failure":
            failures += 1
            failures_since_success += 1
        elif outcome == "success":
            if failures_since_success >= 1:
                failure_then_success += 1
            failures_since_success = 0
        device = item.get("host", {}).get("id")
        ip = item.get("source", {}).get("ip")
        if device:
            devices.add(device)
        if ip:
            ips.add(ip)
    attempts = len(ordered)
    return {
        "feature_version": feature_version,
        "values": {
            FEATURE_AUTH_ATTEMPT_COUNT: attempts,
            FEATURE_AUTH_FAILURE_COUNT: failures,
            FEATURE_AUTH_FAILURE_RATE: failures / attempts if attempts else 0.0,
            FEATURE_AUTH_SOURCE_DEVICE_COUNT: len(devices),
            FEATURE_AUTH_SOURCE_IP_COUNT: len(ips),
            FEATURE_AUTH_FAILURE_THEN_SUCCESS_COUNT: failure_then_success,
        },
        "inputs": sorted(item["event"]["id"] for item in ordered),
    }


def _window_start(event_time: datetime, window_seconds: int) -> datetime:
    return datetime.fromtimestamp(
        int(event_time.timestamp()) // window_seconds * window_seconds,
        tz=timezone.utc,
    )


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event timestamp requires timezone")
    return parsed.astimezone(timezone.utc)


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class FeatureWindows:
    """Bounded, deduplicated per-entity event history plus watermark state.

    Retention per entity is ``lookback + allowed_lateness`` behind that
    entity's maximum observed event time; the entity watermark trails that
    maximum by ``allowed_lateness`` and also advances once the partition has
    been idle for ``idle_timeout``.
    """

    def __init__(
        self,
        *,
        lookback: timedelta,
        allowed_lateness: timedelta = timedelta(minutes=10),
        idle_timeout: timedelta = timedelta(minutes=5),
        future_time_limit: timedelta = timedelta(minutes=5),
        max_events_per_entity: int = 10000,
        max_entities: int = 100000,
        backfill_sink: Callable[[dict[str, Any]], None] | None = None,
    ):
        for name, value in (
            ("lookback", lookback),
            ("allowed_lateness", allowed_lateness),
            ("idle_timeout", idle_timeout),
            ("future_time_limit", future_time_limit),
        ):
            if value <= timedelta(0):
                raise ValueError(f"{name} must be positive")
        if max_events_per_entity <= 0 or max_entities <= 0:
            raise ValueError("state bounds must be positive")
        self.lookback = lookback
        self.allowed_lateness = allowed_lateness
        self.idle_timeout = idle_timeout
        self.future_time_limit = future_time_limit
        self.max_events_per_entity = max_events_per_entity
        self.max_entities = max_entities
        # Controlled backfill path for out-of-bounds (retention-pruned)
        # events; None keeps count-only behavior. A raising sink propagates
        # fail-closed (the caller's DLQ path handles the input).
        self.backfill_sink = backfill_sink
        self.history: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self.entity_max: dict[str, datetime] = {}
        self.last_receive: dict[str, datetime] = {}
        self.idle_floor: dict[str, datetime] = {}
        self.counters: dict[str, int] = defaultdict(int)

    def _entity_watermark(self, entity_id: str) -> datetime:
        watermark = self.entity_max[entity_id] - self.allowed_lateness
        floor = self.idle_floor.get(entity_id)
        if floor is not None and floor > watermark:
            watermark = floor
        return watermark

    @property
    def max_event_time(self) -> datetime | None:
        if not self.entity_max:
            return None
        return max(self.entity_max.values())

    @property
    def watermark(self) -> datetime | None:
        """Global watermark: the minimum per-entity watermark."""
        if not self.entity_max:
            return None
        return min(self._entity_watermark(entity) for entity in self.entity_max)

    def _refresh_idle(self, entity_id: str, now: datetime) -> None:
        last = self.last_receive.get(entity_id)
        if last is None:
            return
        if now - last >= self.idle_timeout:
            floor = now - self.allowed_lateness
            current = self.idle_floor.get(entity_id)
            if current is None or floor > current:
                self.idle_floor[entity_id] = floor

    def _admit_entity(self, entity_id: str) -> None:
        if entity_id in self.entity_max:
            return
        if len(self.entity_max) < self.max_entities:
            return
        # Budget exhausted: evict the stalest idle shell (no retained
        # history) — active entity state is never dropped. If nothing is
        # evictable, reject fail-closed.
        shells = [
            (self.last_receive.get(other, datetime.min.replace(tzinfo=timezone.utc)), other)
            for other in self.entity_max
            if not self.history.get(other)
        ]
        if not shells:
            self.counters["capacity_rejected"] += 1
            raise ValueError("entity state budget exhausted")
        _, victim = min(shells)
        for mapping in (self.entity_max, self.last_receive, self.idle_floor):
            mapping.pop(victim, None)
        self.history.pop(victim, None)
        self.counters["entities_pruned"] += 1

    def advance(self, now: datetime) -> None:
        """Advance idle entity partitions' watermarks from processing time."""
        for entity_id in list(self.entity_max):
            self._refresh_idle(entity_id, now)

    def window_events(
        self,
        entity_id: str,
        window_start: datetime,
        window_seconds: int,
    ) -> list[dict[str, Any]]:
        """Retained events of one entity inside [window_start, +window_seconds).

        F03 late correction uses this: after a late (still retained) event is
        observed, the caller recomputes the same window from the full event
        set and emits the same business key with a higher revision.
        """
        if window_seconds < 1:
            raise ValueError("window_seconds must be positive")
        end = window_start + timedelta(seconds=window_seconds)
        return [
            item
            for item in self.history.get(entity_id, ())
            if window_start <= parse_time(item["@timestamp"]) < end
        ]

    def closed_windows(
        self,
        entity_id: str,
        window_seconds: int,
        *,
        feature_version: str = FEATURE_VERSION_AUTH_V1,
    ) -> list[dict[str, Any]]:
        """Feature records of the entity's windows the watermark has closed.

        A window closes once the entity watermark reaches its end (the same
        rule as the Go engine's ClosedWindow drainage). Returned records are
        sorted by window start and each carries the feature version stamp and
        input references.
        """
        if window_seconds < 1:
            raise ValueError("window_seconds must be positive")
        if entity_id not in self.entity_max:
            return []
        watermark = self._entity_watermark(entity_id)
        grouped: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
        for item in self.history.get(entity_id, ()):
            grouped[_window_start(parse_time(item["@timestamp"]), window_seconds)].append(item)
        closed = []
        for start in sorted(grouped):
            end = start + timedelta(seconds=window_seconds)
            if watermark < end:
                continue
            record = compute_window_features(grouped[start], feature_version=feature_version)
            record["entity_id"] = entity_id
            record["window"] = {"start": _format_time(start), "end": _format_time(end)}
            closed.append(record)
        return closed

    def observe(
        self,
        event: dict[str, Any],
        entity_id: str,
        event_time: datetime,
        *,
        received_at: datetime | None = None,
        dedup_key: str | None = None,
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Record one event and return (is_late, retained window for the entity).

        ``received_at`` enables the future-time guard and idle tracking;
        ``dedup_key`` overrides the default ``event.id`` deduplication key.
        """
        if received_at is not None and event_time > received_at + self.future_time_limit:
            self.counters["future_rejected"] += 1
            raise ValueError("event time exceeds future time limit")
        self._admit_entity(entity_id)
        if received_at is not None:
            self._refresh_idle(entity_id, received_at)

        previous_max = self.entity_max.get(entity_id)
        self.entity_max[entity_id] = max(previous_max or event_time, event_time)
        if received_at is not None:
            self.last_receive[entity_id] = received_at
        is_late = event_time < self._entity_watermark(entity_id)
        if is_late:
            self.counters["late_accepted"] += 1

        events = self.history[entity_id]
        key = dedup_key if dedup_key is not None else event.get("event", {}).get("id")
        if key is None:
            raise ValueError("dedup key is required")

        def item_key(item: dict[str, Any]) -> Any:
            if dedup_key is not None:
                return item.get("_dedup_key")
            return item.get("event", {}).get("id")

        if any(item_key(item) == key for item in events):
            self.counters["duplicates"] += 1
        else:
            record = deepcopy(event)
            if dedup_key is not None:
                record["_dedup_key"] = key
            events.append(record)
            self.counters["added"] += 1

        ordered = sorted(events, key=lambda item: (parse_time(item["@timestamp"]), item["event"]["id"]))
        retention_start = self.entity_max[entity_id] - (self.lookback + self.allowed_lateness)
        retained: deque[dict[str, Any]] = deque()
        for item in ordered:
            if parse_time(item["@timestamp"]) >= retention_start:
                retained.append(item)
            else:
                self.counters["retention_pruned"] += 1
                # Out-of-bounds data is not dropped: it is recorded for the
                # controlled backfill path (F08/replay schedules the job).
                if self.backfill_sink is not None:
                    self.backfill_sink(
                        {
                            "entity_id": entity_id,
                            "event": item,
                            "reason": "beyond_retention",
                            "retention_start": _format_time(retention_start),
                        }
                    )
        while len(retained) > self.max_events_per_entity:
            retained.popleft()
            self.counters["evicted"] += 1
        self.history[entity_id] = retained
        return is_late, list(retained)

    def export(self) -> dict[str, Any]:
        return {
            "watermark": _format_time(self.watermark),
            "max_event_time": _format_time(self.max_event_time),
            "history": {entity: list(events) for entity, events in self.history.items() if events},
            "entity_max": {entity: _format_time(value) for entity, value in self.entity_max.items()},
            "last_receive": {entity: _format_time(value) for entity, value in self.last_receive.items()},
            "idle_floor": {entity: _format_time(value) for entity, value in self.idle_floor.items()},
            "counters": dict(self.counters),
        }

    def load(self, state: dict[str, Any]) -> None:
        def load_map(key: str) -> dict[str, datetime]:
            return {
                entity: parse_time(value)
                for entity, value in state.get(key, {}).items()
                if value
            }

        self.history = defaultdict(deque)
        for entity_id, events in state.get("history", {}).items():
            self.history[entity_id] = deque(events)
        self.entity_max = load_map("entity_max")
        self.last_receive = load_map("last_receive")
        self.idle_floor = load_map("idle_floor")
        self.counters = defaultdict(int, state.get("counters", {}))
        # Backward compatibility with pre-F02 states: derive per-entity max
        # event times from the retained history when absent.
        for entity_id, events in self.history.items():
            if entity_id not in self.entity_max and events:
                self.entity_max[entity_id] = max(parse_time(item["@timestamp"]) for item in events)
