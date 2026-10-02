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
  dropped silently — every rejection/eviction lands in ``counters``.
"""

from __future__ import annotations

from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any


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
