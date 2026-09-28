"""Durable event-time Kafka worker for authentication detections."""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from .detection import detect_failure_then_success, parse_time
from .metrics import Metrics, start_http_server
from .registry import AnalysisRegistry, RuleDefinition, load_registry
from .runtime import Checkpoint, PostgresRuntimeStore, RuntimeStore


def _format_time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class AuthenticationProcessor:
    """Bounded per-user event-time state with deterministic, replayable results."""

    state_version = 1

    def __init__(self, organization: str, namespace: str, rule: RuleDefinition):
        self.organization = organization
        self.namespace = namespace
        self.rule = rule
        self.history: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self.watermark: datetime | None = None
        self.max_event_time: datetime | None = None

    def process(
        self,
        event: dict[str, Any],
        run_id: str,
        *,
        generated_at: datetime | None = None,
        registry_version: str = "1.0.0",
    ) -> list[dict[str, Any]]:
        if event.get("organization", {}).get("id") != self.organization:
            raise ValueError("cross-tenant event in analysis input")
        user_id = event.get("user", {}).get("id")
        if not user_id:
            raise ValueError("event user.id is required")
        event_time = parse_time(event["@timestamp"])
        event_id = event.get("event", {}).get("id")
        if not event_id:
            raise ValueError("event id is required")

        previous_watermark = self.watermark
        is_late = previous_watermark is not None and event_time < previous_watermark
        self.max_event_time = max(self.max_event_time or event_time, event_time)
        self.watermark = self.max_event_time - timedelta(seconds=self.rule.allowed_lateness_seconds)

        events = self.history[user_id]
        if not any(item.get("event", {}).get("id") == event_id for item in events):
            events.append(deepcopy(event))
        ordered = sorted(events, key=lambda item: (parse_time(item["@timestamp"]), item["event"]["id"]))
        retention_start = self.max_event_time - timedelta(
            seconds=self.rule.lookback_seconds + self.rule.allowed_lateness_seconds,
        )
        retained = deque(item for item in ordered if parse_time(item["@timestamp"]) >= retention_start)
        self.history[user_id] = retained

        anomalies = detect_failure_then_success(
            list(retained),
            self.organization,
            rule_id=self.rule.id,
            rule_version=self.rule.version,
            severity=self.rule.severity,
            threshold=self.rule.threshold,
            lookback=timedelta(seconds=self.rule.lookback_seconds),
            window_seconds=self.rule.window_seconds,
        )
        generated = generated_at or datetime.now(timezone.utc)
        output: list[dict[str, Any]] = []
        for anomaly in anomalies:
            anomaly_time = parse_time(anomaly["_source"]["@timestamp"])
            if not is_late and anomaly_time != event_time:
                continue
            document = deepcopy(anomaly["_source"])
            document["analysis"] = {
                "run_id": run_id,
                "generated_at": _format_time(generated),
                "registry_version": registry_version,
                "watermark": _format_time(self.watermark),
                "reprocessed": is_late,
                "input_contract_version": event.get("ueba", {}).get("schema", {}).get("version", ""),
            }
            output.append(
                {
                    "contract_version": "1.0.0",
                    "result_type": "anomaly",
                    "result_id": anomaly["_id"],
                    "organization_id": self.organization,
                    "namespace": self.namespace,
                    "rule_id": self.rule.id,
                    "rule_version": self.rule.version,
                    "run_id": run_id,
                    "document": document,
                },
            )
        return output

    def export_state(self) -> dict[str, Any]:
        return {
            "state_version": self.state_version,
            "watermark": _format_time(self.watermark),
            "max_event_time": _format_time(self.max_event_time),
            "history": {
                user_id: list(events)
                for user_id, events in self.history.items()
                if events
            },
        }

    @classmethod
    def from_state(
        cls,
        organization: str,
        namespace: str,
        rule: RuleDefinition,
        state: dict[str, Any] | None,
    ) -> AuthenticationProcessor:
        processor = cls(organization, namespace, rule)
        if not state:
            return processor
        if state.get("state_version") != cls.state_version:
            raise ValueError("unsupported processor state version")
        watermark = state.get("watermark")
        max_event_time = state.get("max_event_time")
        processor.watermark = parse_time(watermark) if watermark else None
        processor.max_event_time = parse_time(max_event_time) if max_event_time else None
        for user_id, events in state.get("history", {}).items():
            processor.history[user_id] = deque(events)
        return processor


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def dead_letter_message(
    message: Any,
    stage: str,
    code: str,
    detail: str,
) -> bytes:
    payload = {
        "source": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "key": message.key().decode(errors="replace") if message.key() else "",
        },
        "failure": {
            "stage": stage,
            "code": code,
            "message": detail[:512],
            "retryable": False,
        },
        "payload": message.value().decode(errors="replace"),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def publish_results(
    producer: Producer,
    result_topic: str,
    results: list[dict[str, Any]],
) -> None:
    for result in results:
        producer.produce(
            result_topic,
            key=result["result_id"].encode(),
            value=json.dumps(result, separators=(",", ":")).encode(),
        )
    producer.flush(10)


def publish_dead_letter(
    producer: Producer,
    dead_letter_topic: str,
    message: Any,
    stage: str,
    code: str,
    detail: str,
) -> None:
    producer.produce(
        dead_letter_topic,
        key=message.key(),
        value=dead_letter_message(message, stage, code, detail),
    )
    producer.flush(10)


def main() -> None:
    brokers = required("KAFKA_BROKERS")
    source_topic = os.getenv("KAFKA_EVENTS_TOPIC", "tuba.events.authentication.v1")
    result_topic = os.getenv("KAFKA_ANALYSIS_RESULTS_TOPIC", "tuba.analysis.results.v1")
    dead_letter_topic = os.getenv("KAFKA_DLQ_TOPIC", "tuba.indexing.dlq.v1")
    organization = required("TUBA_ORGANIZATION_ID")
    namespace = required("TUBA_NAMESPACE")
    group_id = os.getenv("KAFKA_ANALYSIS_GROUP", f"tuba-analysis-auth-{namespace}")
    registry = load_registry()
    rule = registry.rule("auth.failure-then-success")
    run_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
    store: RuntimeStore = PostgresRuntimeStore(required("DATABASE_URL"))
    store.start_run(run_id, "authentication", run_id, group_id, registry.version)
    metrics = Metrics()
    start_http_server(os.getenv("METRICS_LISTEN", "127.0.0.1:9090"), metrics)

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": False,
        },
    )
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
        },
    )
    consumer.subscribe([source_topic])
    metrics.set_ready(True)
    processors: dict[int, AuthenticationProcessor] = {}
    processed = 0
    emitted = 0
    last_heartbeat = 0.0
    shutdown_error: str | None = None
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                producer.poll(0)
                now = time.monotonic()
                if now - last_heartbeat >= 5:
                    store.heartbeat(run_id, processed, emitted)
                    last_heartbeat = now
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            partition = message.partition()
            processor = processors.get(partition)
            if processor is None:
                state = store.load_processor_state(group_id, source_topic, partition)
                processor = AuthenticationProcessor.from_state(organization, namespace, rule, state)
                processors[partition] = processor

            try:
                event = json.loads(message.value())
                results = processor.process(event, run_id, registry_version=registry.version)
            except (json.JSONDecodeError, ValueError) as error:
                publish_dead_letter(
                    producer,
                    dead_letter_topic,
                    message,
                    "analysis",
                    "ANALYSIS_INPUT_REJECTED",
                    str(error),
                )
                store.save_processor_state(
                    group_id,
                    source_topic,
                    partition,
                    processor.export_state(),
                    processor.watermark,
                )
                store.save_checkpoint(
                    group_id,
                    run_id,
                    Checkpoint(source_topic, partition, message.offset() + 1, processor.watermark),
                )
                consumer.commit(message=message, asynchronous=False)
                processed += 1
                metrics.inc("tuba_analysis_dlq_total")
                metrics.inc("tuba_analysis_processed_events_total")
                continue

            publish_results(producer, result_topic, results)
            emitted += len(results)
            store.save_processor_state(
                group_id,
                source_topic,
                partition,
                processor.export_state(),
                processor.watermark,
            )
            store.save_checkpoint(
                group_id,
                run_id,
                Checkpoint(source_topic, partition, message.offset() + 1, processor.watermark),
            )
            consumer.commit(message=message, asynchronous=False)
            processed += 1
            metrics.inc("tuba_analysis_processed_events_total")
            metrics.inc("tuba_analysis_emitted_results_total", len(results))
            metrics.set_watermark(partition, processor.watermark.timestamp() if processor.watermark else None)
    except KeyboardInterrupt:
        pass
    except Exception as error:
        shutdown_error = str(error)
        raise
    finally:
        metrics.set_ready(False)
        store.finish_run(run_id, "failed" if shutdown_error else "stopped", shutdown_error)
        producer.flush(10)
        consumer.close()
        store.close()


if __name__ == "__main__":
    main()
