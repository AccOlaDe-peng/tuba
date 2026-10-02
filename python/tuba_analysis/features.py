"""Feature module: bounded per-entity event-time windows with watermarking.

This is the feature layer of the analysis module DAG (feature <- baseline <-
detection). Detection code consumes retained windows from here; concrete
feature computations are F03 scope.
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


class FeatureWindows:
    """Bounded, deduplicated per-entity event history plus watermark state.

    Retention is ``lookback + allowed_lateness`` behind the maximum observed
    event time; the watermark trails that maximum by ``allowed_lateness``.
    """

    def __init__(self, *, lookback: timedelta, allowed_lateness: timedelta):
        if lookback <= timedelta(0) or allowed_lateness <= timedelta(0):
            raise ValueError("lookback and allowed lateness must be positive")
        self.lookback = lookback
        self.allowed_lateness = allowed_lateness
        self.history: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self.watermark: datetime | None = None
        self.max_event_time: datetime | None = None

    def observe(self, event: dict[str, Any], entity_id: str, event_time: datetime) -> tuple[bool, list[dict[str, Any]]]:
        """Record one event and return (is_late, retained window for the entity)."""
        is_late = self.watermark is not None and event_time < self.watermark
        self.max_event_time = max(self.max_event_time or event_time, event_time)
        self.watermark = self.max_event_time - self.allowed_lateness

        events = self.history[entity_id]
        event_id = event.get("event", {}).get("id")
        if not any(item.get("event", {}).get("id") == event_id for item in events):
            events.append(deepcopy(event))
        ordered = sorted(events, key=lambda item: (parse_time(item["@timestamp"]), item["event"]["id"]))
        retention_start = self.max_event_time - (self.lookback + self.allowed_lateness)
        retained = deque(item for item in ordered if parse_time(item["@timestamp"]) >= retention_start)
        self.history[entity_id] = retained
        return is_late, list(retained)

    def export(self) -> dict[str, Any]:
        def fmt(value: datetime | None) -> str | None:
            return value.isoformat().replace("+00:00", "Z") if value else None

        return {
            "watermark": fmt(self.watermark),
            "max_event_time": fmt(self.max_event_time),
            "history": {entity: list(events) for entity, events in self.history.items() if events},
        }

    def load(self, state: dict[str, Any]) -> None:
        watermark = state.get("watermark")
        max_event_time = state.get("max_event_time")
        self.watermark = parse_time(watermark) if watermark else None
        self.max_event_time = parse_time(max_event_time) if max_event_time else None
        for entity_id, events in state.get("history", {}).items():
            self.history[entity_id] = deque(events)
