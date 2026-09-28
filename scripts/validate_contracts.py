#!/usr/bin/env python3
"""Validate M0 contracts without requiring project dependencies."""

from __future__ import annotations

import json
import ipaddress
import re
from datetime import datetime
from pathlib import Path, PurePosixPath


ROOT = Path(__file__).resolve().parents[1]
SUPPORTED_SCHEMA_KEYS = {
    "$schema", "$id", "title", "description", "type", "additionalProperties", "required", "properties",
    "items", "contains", "const", "enum", "minLength", "maxLength", "pattern", "minItems", "maxItems",
    "minimum", "maximum", "format",
}


def validate_schema(schema: dict, path: str) -> None:
    unknown = schema.keys() - SUPPORTED_SCHEMA_KEYS
    if unknown:
        raise ValueError(f"{path}: unsupported JSON Schema keywords {sorted(unknown)}")
    allowed_types = {"object", "array", "string", "number", "integer", "boolean", "null"}
    schema_type = schema.get("type")
    types = schema_type if isinstance(schema_type, list) else [schema_type]
    if schema_type is not None and any(value not in allowed_types for value in types):
        raise ValueError(f"{path}: unsupported JSON Schema type {schema_type!r}")
    if schema.get("format") not in (None, "date-time", "ip"):
        raise ValueError(f"{path}: unsupported JSON Schema format {schema['format']!r}")
    if pattern := schema.get("pattern"):
        re.compile(pattern)
    properties = schema.get("properties", {})
    missing = set(schema.get("required", [])) - properties.keys()
    if missing:
        raise ValueError(f"{path}: required properties have no schema {sorted(missing)}")
    for name, child in properties.items():
        validate_schema(child, f"{path}.properties.{name}")
    if "items" in schema:
        validate_schema(schema["items"], f"{path}.items")
    if "contains" in schema:
        validate_schema(schema["contains"], f"{path}.contains")


def validate_release_manifest(manifest: dict) -> None:
    required_kinds = {"dip", "uim", "routing", "es_mapping", "entity_rules", "data_model", "analysis_rules"}
    assets = manifest.get("assets", [])
    by_id: dict[str, dict] = {}
    kinds: set[str] = set()
    for asset in assets:
        asset_id = asset["asset_id"]
        if asset_id in by_id:
            raise ValueError(f"duplicate release asset id: {asset_id}")
        by_id[asset_id] = asset
        if asset["kind"] in kinds:
            raise ValueError(f"release contains more than one {asset['kind']} asset")
        kinds.add(asset["kind"])
        path = PurePosixPath(asset["path"])
        if path.is_absolute() or ".." in path.parts or "\\" in asset["path"]:
            raise ValueError(f"release asset path must be relative and stay in the bundle: {asset['path']}")
        if len(asset["dependencies"]) != len(set(asset["dependencies"])):
            raise ValueError(f"duplicate dependency in release asset: {asset_id}")
    if kinds != required_kinds:
        raise ValueError(f"release asset kinds differ from required set: missing={sorted(required_kinds-kinds)} extra={sorted(kinds-required_kinds)}")
    for asset in assets:
        for dependency in asset["dependencies"]:
            if dependency not in by_id:
                raise ValueError(f"release asset {asset['asset_id']} references missing dependency {dependency}")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(asset_id: str) -> None:
        if asset_id in visiting:
            raise ValueError(f"release asset dependency cycle includes {asset_id}")
        if asset_id in visited:
            return
        visiting.add(asset_id)
        for dependency in by_id[asset_id]["dependencies"]:
            visit(dependency)
        visiting.remove(asset_id)
        visited.add(asset_id)

    for asset_id in by_id:
        visit(asset_id)


def validate_topic_catalog(catalog: dict) -> None:
    """Keep Kafka delivery, ACL, and single-node assumptions machine-checkable."""
    if catalog.get("version") != 1:
        raise ValueError("topics.v1.json: unsupported catalog version")
    defaults = catalog.get("defaults", {})
    expected_defaults = {
        "partitions": 1,
        "replication_factor": 1,
        "min_insync_replicas": 1,
        "consumer_commit": "manual",
        "producer_acks": "all",
        "producer_delivery": "at_least_once",
        "kafka_transactions": False,
    }
    for key, expected in expected_defaults.items():
        if defaults.get(key) != expected:
            raise ValueError(f"topics.v1.json: defaults.{key} must be {expected!r}")
    if defaults.get("retention_status") != "approved_50gib_single_node_A03_20260928":
        raise ValueError("topics.v1.json: default retention must reference the accepted 50 GiB single-node A03 boundary")
    profiles = catalog.get("runtime_profiles")
    if not isinstance(profiles, list) or not profiles:
        raise ValueError("topics.v1.json: runtime_profiles must declare an installed profile")
    profile_by_name = {profile.get("profile"): profile for profile in profiles if isinstance(profile, dict)}
    if len(profile_by_name) != len(profiles) or "zeek_validation_single_node" not in profile_by_name:
        raise ValueError("topics.v1.json: runtime profile names must be unique and include Zeek validation")
    validation_profile = profile_by_name["zeek_validation_single_node"]
    expected_profile = {
        "namespace": "zeek_validation_20260927_001",
        "partitions": 1,
        "replication_factor": 1,
        "min_insync_replicas": 1,
        "retention": "24h",
        "max_message_bytes": 2097152,
        "producer_acks": "all",
        "producer_delivery": "at_least_once",
        "consumer_commit": "manual",
        "kafka_transactions": False,
    }
    for key, expected in expected_profile.items():
        if validation_profile.get(key) != expected:
            raise ValueError(f"topics.v1.json: Zeek validation profile {key} must be {expected!r}")
    if validation_profile.get("retention_status") != "bounded_validation_profile_within_A03_budget":
        raise ValueError("topics.v1.json: validation retention must remain within the accepted A03 budget")
    expected_service_principals = {
        "source-adapter": "tuba-zeek-source-adapter",
        "ingest": "tuba-zeek-ingest",
        "raw-indexer": "tuba-zeek-raw-indexer",
        "normalizer": "tuba-zeek-normalizer",
        "quarantine-indexer": "tuba-zeek-quarantine-indexer",
        "standard-indexer": "tuba-zeek-standard-indexer",
    }
    if validation_profile.get("service_principals") != expected_service_principals:
        raise ValueError("topics.v1.json: validation service principal map differs from the deployed role policy")

    topics = catalog.get("topics")
    if not isinstance(topics, list) or not topics:
        raise ValueError("topics.v1.json: topics must be a non-empty array")
    by_name: dict[str, dict] = {}
    for topic in topics:
        name = topic.get("name")
        if not isinstance(name, str) or not name or name in by_name:
            raise ValueError(f"topics.v1.json: missing or duplicate topic template {name!r}")
        by_name[name] = topic
        physical_name = topic.get("physical_name")
        if not isinstance(physical_name, str) or not physical_name.strip():
            raise ValueError(f"topics.v1.json: {name} must declare a physical_name template")
        partitions = topic.get("partitions", defaults["partitions"])
        replicas = topic.get("replication_factor", defaults["replication_factor"])
        min_isr = topic.get("min_insync_replicas", defaults["min_insync_replicas"])
        if not all(isinstance(value, int) and not isinstance(value, bool) and value > 0 for value in (partitions, replicas, min_isr)):
            raise ValueError(f"topics.v1.json: {name} has invalid partition/replica settings")
        if min_isr > replicas:
            raise ValueError(f"topics.v1.json: {name} min_insync_replicas exceeds replication_factor")
        if topic.get("producer_acks", defaults["producer_acks"]) != "all":
            raise ValueError(f"topics.v1.json: {name} must require producer acks=all")
        if topic.get("producer_delivery", defaults["producer_delivery"]) != "at_least_once":
            raise ValueError(f"topics.v1.json: {name} has unsupported delivery semantics")
        if topic.get("kafka_transactions", defaults["kafka_transactions"]) is not False:
            raise ValueError(f"topics.v1.json: {name} cannot claim Kafka transaction support")
        if topic.get("consumer_commit", defaults["consumer_commit"]) != "manual":
            raise ValueError(f"topics.v1.json: {name} must use manual commits")
        if not isinstance(topic.get("key"), str) or not topic["key"].strip():
            raise ValueError(f"topics.v1.json: {name} requires a message key")
        if not re.fullmatch(r"[1-9][0-9]*(?:s|m|h|d)", str(topic.get("retention", ""))):
            raise ValueError(f"topics.v1.json: {name} retention must be an explicit duration")
        if topic.get("retention") != "24h":
            raise ValueError(f"topics.v1.json: {name} exceeds the accepted 50 GiB single-node Kafka retention budget")
        if not isinstance(topic.get("max_message_bytes"), int) or isinstance(topic["max_message_bytes"], bool) or topic["max_message_bytes"] <= 0:
            raise ValueError(f"topics.v1.json: {name} max_message_bytes must be positive")
        if topic["max_message_bytes"] > validation_profile["max_message_bytes"]:
            raise ValueError(f"topics.v1.json: {name} exceeds the installed validation profile message limit")
        producers = topic.get("producers")
        consumers = topic.get("consumers")
        acl = topic.get("acl")
        if not isinstance(producers, list) or not producers or not isinstance(consumers, list) or not consumers:
            raise ValueError(f"topics.v1.json: {name} must declare producers and consumers")
        if not isinstance(acl, dict):
            raise ValueError(f"topics.v1.json: {name} must declare ACL roles")
        for producer in producers:
            # Beat credentials are provisioned per source/topic; the contract names their shared role.
            acl_principal = "bound_source_credential" if name == "tuba.source.{source_context_id}.v1" and producer in {"filebeat", "winlogbeat"} else producer
            if not {"WRITE", "DESCRIBE"}.issubset(set(acl.get(acl_principal, []))):
                raise ValueError(f"topics.v1.json: {name} producer {producer} lacks WRITE/DESCRIBE ACL")
        for consumer in consumers:
            if not isinstance(consumer, dict) or not isinstance(consumer.get("consumer_group"), str) or not consumer["consumer_group"].strip():
                raise ValueError(f"topics.v1.json: {name} has invalid consumer group formula")
        for principal, permissions in acl.items():
            if not isinstance(permissions, list) or not permissions or set(permissions) - {"READ", "WRITE", "DESCRIBE"}:
                raise ValueError(f"topics.v1.json: {name} has invalid ACL for {principal}")

    required = {
        "tuba.raw.events.v1", "tuba.source.{source_context_id}.v1",
        "tuba.source-adapter.dlq.v1", "tuba.events.{domain}.v1",
        "tuba.quarantine.v1", "tuba.attributed.events.v1",
        "tuba.analysis.results.v1", "tuba.analysis.results.v2", "tuba.indexing.dlq.v1",
    }
    missing = required - by_name.keys()
    if missing:
        raise ValueError(f"topics.v1.json: missing required topic templates {sorted(missing)}")
    expected_physical_names = {
        "tuba.raw.events.v1": "tuba.collector.{namespace}.raw.v1",
        "tuba.source.{source_context_id}.v1": "tuba.source.{source_context_id}.v1",
        "tuba.source-adapter.dlq.v1": "tuba.collector.{namespace}.source-adapter.dlq.v1",
        "tuba.events.{domain}.v1": "tuba.collector.{namespace}.events.{domain}.v1",
        "tuba.quarantine.v1": "tuba.collector.{namespace}.quarantine.v1",
        "tuba.attributed.events.v1": "tuba.collector.{namespace}.attributed.events.v1",
        "tuba.analysis.results.v1": "tuba.collector.{namespace}.analysis.results.v1",
        "tuba.analysis.results.v2": "tuba.collector.{namespace}.analysis.results.v2",
        "tuba.indexing.dlq.v1": "tuba.collector.{namespace}.dlq.v1",
    }
    resolved_names = set()
    for logical_name, template in expected_physical_names.items():
        if by_name[logical_name]["physical_name"] != template:
            raise ValueError(f"topics.v1.json: {logical_name} physical_name must be {template!r}")
        resolved_names.add(template.format(namespace="ns_validation", domain="network", source_context_id="ctx_validation"))
    if len(resolved_names) != len(expected_physical_names):
        raise ValueError("topics.v1.json: physical name templates collide when resolved")
    domains = {"authentication", "session", "iam", "directory", "network", "dns", "web", "tls"}
    if set(by_name["tuba.events.{domain}.v1"].get("domains", [])) != domains:
        raise ValueError("topics.v1.json: domain event topic must cover the canonical eight domains")

    source = by_name["tuba.source.{source_context_id}.v1"]
    source_acl = source["acl"]
    expected_group = "tuba-source-adapter-<first-16-hex-of-sha256-topic>[-<consumer_group_suffix>]"
    if source["consumers"] != [{"consumer_group": expected_group}]:
        raise ValueError("topics.v1.json: source adapter group formula must match the application implementation")
    if source.get("retention_status") != "approved_50gib_single_node_A03_20260928":
        raise ValueError("topics.v1.json: source retention must reference the accepted A03 boundary")
    if source.get("partitions", defaults["partitions"]) != 1 or source.get("replication_factor", defaults["replication_factor"]) != 1:
        raise ValueError("topics.v1.json: source topics require one partition and one replica in the current profile")
    if source_acl.get("bound_source_credential") != ["WRITE", "DESCRIBE"]:
        raise ValueError("topics.v1.json: source credentials require exact-topic WRITE/DESCRIBE only")
    if not {"READ", "DESCRIBE"}.issubset(set(source_acl.get("tuba-source-adapter", []))):
        raise ValueError("topics.v1.json: source adapter lacks topic READ/DESCRIBE ACL")
    if "source_adapter_group" not in source_acl or source_acl["source_adapter_group"] != ["READ"]:
        raise ValueError("topics.v1.json: source adapter consumer-group READ ACL must be explicit")
    if any("*" in principal for principal in source_acl):
        raise ValueError("topics.v1.json: wildcard source ACL principals are prohibited")
    if by_name["tuba.analysis.results.v1"].get("status") != "legacy_v1_read_only_during_migration":
        raise ValueError("topics.v1.json: analysis result v1 must remain explicitly migration-only")
    if by_name["tuba.analysis.results.v2"].get("contract") != "events/analysis-result/2/schema.json":
        raise ValueError("topics.v1.json: analysis result v2 must reference its canonical schema")


def validate(instance: object, schema: dict, path: str = "$") -> None:
    expected = schema.get("type")
    if isinstance(expected, list):
        allowed = expected
    elif expected:
        allowed = [expected]
    else:
        allowed = []
    type_checks = {
        "object": lambda value: isinstance(value, dict),
        "array": lambda value: isinstance(value, list),
        "string": lambda value: isinstance(value, str),
        "number": lambda value: isinstance(value, (int, float)) and not isinstance(value, bool),
        "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
        "boolean": lambda value: isinstance(value, bool),
        "null": lambda value: value is None,
    }
    if allowed and not any(type_checks[name](instance) for name in allowed):
        raise ValueError(f"{path}: expected {allowed}")
    if "const" in schema and instance != schema["const"]:
        raise ValueError(f"{path}: expected constant {schema['const']!r}")
    if "enum" in schema and instance not in schema["enum"]:
        raise ValueError(f"{path}: value is outside enum")
    if isinstance(instance, str):
        if len(instance) < schema.get("minLength", 0) or len(instance) > schema.get("maxLength", 1 << 30):
            raise ValueError(f"{path}: string length is invalid")
        if pattern := schema.get("pattern"):
            if re.fullmatch(pattern, instance) is None:
                raise ValueError(f"{path}: value does not match pattern")
        if schema.get("format") == "date-time":
            try:
                parsed = datetime.fromisoformat(instance.replace("Z", "+00:00"))
                if "T" not in instance or parsed.tzinfo is None:
                    raise ValueError("date-time must include T and an explicit timezone")
            except ValueError as error:
                raise ValueError(f"{path}: expected date-time") from error
        if schema.get("format") == "ip":
            try:
                ipaddress.ip_address(instance)
            except ValueError as error:
                raise ValueError(f"{path}: expected IP address") from error
    if isinstance(instance, dict):
        for name in schema.get("required", []):
            if name not in instance:
                raise ValueError(f"{path}: missing required property {name}")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            unknown = instance.keys() - properties.keys()
            if unknown:
                raise ValueError(f"{path}: unknown properties {sorted(unknown)}")
        for name, value in instance.items():
            if name in properties:
                validate(value, properties[name], f"{path}.{name}")
    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0) or len(instance) > schema.get("maxItems", 1 << 30):
            raise ValueError(f"{path}: array length is invalid")
        if "items" in schema:
            for index, value in enumerate(instance):
                validate(value, schema["items"], f"{path}[{index}]")
        if "contains" in schema:
            matched = False
            for index, value in enumerate(instance):
                try:
                    validate(value, schema["contains"], f"{path}[{index}]")
                    matched = True
                    break
                except ValueError:
                    continue
            if not matched:
                raise ValueError(f"{path}: no array item matches contains")
    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if instance < schema.get("minimum", float("-inf")) or instance > schema.get("maximum", float("inf")):
            raise ValueError(f"{path}: number is outside the allowed range")


def main() -> None:
    topic_catalog_path = ROOT / "contracts" / "events" / "topics.v1.json"
    validate_topic_catalog(json.loads(topic_catalog_path.read_text()))
    schemas = list((ROOT / "contracts" / "events").glob("*/*/schema.json"))
    if len(schemas) < 8:
        raise SystemExit("expected all versioned event schemas, including analysis-result v2")
    identifiers: set[str] = set()
    for path in schemas:
        data = json.loads(path.read_text())
        validate_schema(data, str(path.relative_to(ROOT)))
        if data.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            raise SystemExit(f"{path}: unsupported JSON Schema dialect")
        identifier = data.get("$id")
        if not identifier or identifier in identifiers:
            raise SystemExit(f"{path}: missing or duplicate $id")
        identifiers.add(identifier)
        if data.get("type") != "object" or not data.get("required"):
            raise SystemExit(f"{path}: root object and required fields are mandatory")
    examples = list((ROOT / "contracts" / "examples").glob("*.json"))
    if not examples:
        raise SystemExit("at least one contract example is required")
    example_schemas = {
        "authentication.valid.json": ROOT / "contracts/events/authentication/1/schema.json",
        "analysis-result.valid.json": ROOT / "contracts/events/analysis-result/1/schema.json",
        "analysis-result-v2.valid.json": ROOT / "contracts/events/analysis-result/2/schema.json",
        "raw.valid.json": ROOT / "contracts/events/raw/1/schema.json",
        "beat-ingress.valid.json": ROOT / "contracts/events/beat-ingress/1/schema.json",
        "standard-event.valid.json": ROOT / "contracts/events/standard-event/1/schema.json",
    }
    for path in examples:
        data = json.loads(path.read_text())
        schema_path = example_schemas.get(path.name)
        if schema_path:
            validate(data, json.loads(schema_path.read_text()))
        if path.name == "release-manifest.valid.json":
            validate(data, json.loads((ROOT / "contracts/releases/1/manifest.schema.json").read_text()))
            validate_release_manifest(data)
    for name, schema_path in example_schemas.items():
        if not (ROOT / "contracts" / "examples" / name).is_file():
            raise SystemExit(f"missing canonical example: {name}")
    case_path = ROOT / "contracts" / "uim" / "validation-cases.v1.json"
    cases = json.loads(case_path.read_text())
    if cases.get("version") != 1 or cases.get("schema_version") != "1.0.0" or not cases.get("cases"):
        raise SystemExit("UIM validation case contract has an unsupported or empty version")
    case_ids: set[str] = set()
    for case in cases["cases"]:
        identifier = case.get("id")
        if not identifier or identifier in case_ids:
            raise SystemExit("UIM validation case IDs must be present and unique")
        case_ids.add(identifier)
        if not all(case.get("vendor", {}).get(key) for key in ("name", "product", "dataset")):
            raise SystemExit(f"UIM validation case {identifier}: trusted vendor identity is incomplete")
        if not isinstance(case.get("payload"), dict):
            raise SystemExit(f"UIM validation case {identifier}: payload must be an object")
        expected = case.get("expected", {})
        if expected.get("result") == "quarantine" and not expected.get("code"):
            raise SystemExit(f"UIM validation case {identifier}: quarantine code is required")
        if expected.get("result") == "standard_event" and not all(expected.get(key) for key in ("route", "outcome")):
            raise SystemExit(f"UIM validation case {identifier}: standard event route/outcome are required")
        if expected.get("result") not in ("quarantine", "standard_event"):
            raise SystemExit(f"UIM validation case {identifier}: unsupported expected result")
    openapi_path = ROOT / "contracts" / "api" / "openapi.yaml"
    openapi = openapi_path.read_text()
    for marker in ("openapi: 3.1.0", "paths:", "components:"):
        if marker not in openapi:
            raise SystemExit(f"openapi.yaml: missing {marker}")
    beat_schema_ref = "../events/beat-ingress/1/schema.json"
    if beat_schema_ref not in openapi:
        raise SystemExit("openapi.yaml: BeatIngressEvent must reference the canonical Beat ingress schema")
    if not (openapi_path.parent / beat_schema_ref).is_file():
        raise SystemExit("openapi.yaml: BeatIngressEvent schema reference does not resolve")
    print(f"validated topic catalog, {len(schemas)} schemas, {len(examples)} examples, {len(case_ids)} UIM cases and OpenAPI baseline")


if __name__ == "__main__":
    main()
