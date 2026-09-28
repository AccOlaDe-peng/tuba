"""Durable analysis run, checkpoint, and processor state persistence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import psycopg
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Checkpoint:
    topic: str
    partition: int
    offset: int
    watermark: datetime | None


class RuntimeStore(Protocol):
    def start_run(
        self,
        run_id: str,
        worker_type: str,
        worker_instance: str,
        consumer_group: str,
        registry_version: str,
    ) -> None: ...

    def heartbeat(self, run_id: str, processed_events: int, emitted_results: int) -> None: ...

    def finish_run(self, run_id: str, status: str, last_error: str | None = None) -> None: ...

    def load_processor_state(
        self,
        consumer_group: str,
        topic: str,
        partition: int,
    ) -> dict[str, Any] | None: ...

    def save_processor_state(
        self,
        consumer_group: str,
        topic: str,
        partition: int,
        state: dict[str, Any],
        watermark: datetime | None,
    ) -> None: ...

    def save_checkpoint(
        self,
        consumer_group: str,
        run_id: str,
        checkpoint: Checkpoint,
    ) -> None: ...

    def close(self) -> None: ...


class MemoryRuntimeStore:
    """In-memory runtime store used by unit tests and replay."""

    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.states: dict[tuple[str, str, int], tuple[dict[str, Any], datetime | None]] = {}
        self.checkpoints: dict[tuple[str, str, int], Checkpoint] = {}

    def start_run(
        self,
        run_id: str,
        worker_type: str,
        worker_instance: str,
        consumer_group: str,
        registry_version: str,
    ) -> None:
        self.runs[run_id] = {
            "worker_type": worker_type,
            "worker_instance": worker_instance,
            "consumer_group": consumer_group,
            "registry_version": registry_version,
            "status": "running",
            "processed_events": 0,
            "emitted_results": 0,
        }

    def heartbeat(self, run_id: str, processed_events: int, emitted_results: int) -> None:
        run = self.runs[run_id]
        run["processed_events"] = processed_events
        run["emitted_results"] = emitted_results

    def finish_run(self, run_id: str, status: str, last_error: str | None = None) -> None:
        run = self.runs[run_id]
        run["status"] = status
        run["last_error"] = last_error

    def load_processor_state(
        self,
        consumer_group: str,
        topic: str,
        partition: int,
    ) -> dict[str, Any] | None:
        value = self.states.get((consumer_group, topic, partition))
        return value[0] if value else None

    def save_processor_state(
        self,
        consumer_group: str,
        topic: str,
        partition: int,
        state: dict[str, Any],
        watermark: datetime | None,
    ) -> None:
        self.states[(consumer_group, topic, partition)] = (state, watermark)

    def save_checkpoint(
        self,
        consumer_group: str,
        run_id: str,
        checkpoint: Checkpoint,
    ) -> None:
        self.checkpoints[(consumer_group, checkpoint.topic, checkpoint.partition)] = checkpoint

    def close(self) -> None:
        return None


class PostgresRuntimeStore:
    """PostgreSQL-backed state store from migration 00005."""

    def __init__(self, dsn: str) -> None:
        self.connection = psycopg.connect(dsn, autocommit=True)

    def start_run(
        self,
        run_id: str,
        worker_type: str,
        worker_instance: str,
        consumer_group: str,
        registry_version: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO analysis_runs(
              run_id,worker_type,worker_instance,consumer_group,registry_version,status
            ) VALUES (%s,%s,%s,%s,%s,'running')
            ON CONFLICT(run_id) DO UPDATE SET
              worker_instance=excluded.worker_instance,
              registry_version=excluded.registry_version,
              status='running',
              last_heartbeat_at=now(),
              stopped_at=NULL
            """,
            (run_id, worker_type, worker_instance, consumer_group, registry_version),
        )

    def heartbeat(self, run_id: str, processed_events: int, emitted_results: int) -> None:
        self.connection.execute(
            """
            UPDATE analysis_runs
            SET processed_events=%s,emitted_results=%s,last_heartbeat_at=now()
            WHERE run_id=%s
            """,
            (processed_events, emitted_results, run_id),
        )

    def finish_run(self, run_id: str, status: str, last_error: str | None = None) -> None:
        self.connection.execute(
            """
            UPDATE analysis_runs
            SET status=%s,last_error=%s,last_heartbeat_at=now(),stopped_at=now()
            WHERE run_id=%s
            """,
            (status, last_error, run_id),
        )

    def load_processor_state(
        self,
        consumer_group: str,
        topic: str,
        partition: int,
    ) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT state FROM analysis_processor_states
            WHERE consumer_group=%s AND topic=%s AND partition=%s
            """,
            (consumer_group, topic, partition),
        ).fetchone()
        return row[0] if row else None

    def save_processor_state(
        self,
        consumer_group: str,
        topic: str,
        partition: int,
        state: dict[str, Any],
        watermark: datetime | None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO analysis_processor_states(
              consumer_group,topic,partition,state,watermark,updated_at
            ) VALUES (%s,%s,%s,%s,%s,now())
            ON CONFLICT(consumer_group,topic,partition) DO UPDATE SET
              state=excluded.state,
              watermark=excluded.watermark,
              updated_at=now()
            """,
            (consumer_group, topic, partition, Jsonb(state), watermark),
        )

    def save_checkpoint(
        self,
        consumer_group: str,
        run_id: str,
        checkpoint: Checkpoint,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO analysis_checkpoints(
              consumer_group,topic,partition,"offset",watermark,run_id,updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,now())
            ON CONFLICT(consumer_group,topic,partition) DO UPDATE SET
              "offset"=excluded."offset",
              watermark=excluded.watermark,
              run_id=excluded.run_id,
              updated_at=now()
            """,
            (
                consumer_group,
                checkpoint.topic,
                checkpoint.partition,
                checkpoint.offset,
                checkpoint.watermark,
                run_id,
            ),
        )

    def close(self) -> None:
        self.connection.close()
