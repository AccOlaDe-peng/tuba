"""Baseline module: versioned baseline model framework.

This is the middle layer of the analysis module DAG (feature <- baseline <-
detection). Training jobs, minimum samples, evaluation, and immutable model
publication are F04 scope; this module carries the lifecycle contract only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


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

    def __post_init__(self) -> None:
        if not self.model_id or not self.version or not self.feature_id:
            raise ValueError("baseline model id, version, and feature id are required")
        if not isinstance(self.status, BaselineStatus):
            raise ValueError(f"unknown baseline status: {self.status!r}")
        if self.status == BaselineStatus.READY and (self.sample_count < 1 or self.trained_at is None):
            raise ValueError("ready baseline requires samples and a training timestamp")
        if self.sample_count < 0:
            raise ValueError("baseline sample count must be non-negative")
