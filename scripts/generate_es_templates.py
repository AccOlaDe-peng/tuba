#!/usr/bin/env python3
"""Generate deterministic Elasticsearch templates from canonical event schemas."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "elasticsearch" / "generated-v1"
COMMON_FIELDS = {"@timestamp", "organization", "event", "vendor", "ueba"}
DOMAIN_FIELDS = {
    "authentication": ["user", "group", "host", "source", "destination"],
    "session": ["user", "host", "source", "destination"],
    "iam": ["user", "group", "host"],
    "directory": ["user", "group", "host"],
    "network": ["source", "destination", "network"],
    "dns": ["source", "destination", "network", "dns"],
    "web": ["source", "destination", "network", "url", "http"],
    "tls": ["source", "destination", "network", "tls"],
}


def map_schema(schema: dict, path: str = "") -> dict:
    kind = schema.get("type")
    if kind == "array":
        return map_schema(schema.get("items", {"type": "keyword"}), path)
    if schema.get("format") == "date-time":
        return {"type": "date"}
    if schema.get("format") == "ip":
        return {"type": "ip"}
    if kind == "object":
        if schema.get("additionalProperties") is True and path == "vendor.payload":
            return {"type": "flattened"}
        properties = schema.get("properties", {})
        return {"dynamic": "strict", "properties": {
            name: map_schema(child, f"{path}.{name}" if path else name)
            for name, child in properties.items()
        }}
    if kind == "integer":
        return {"type": "long" if path in {"event.duration", "network.bytes", "network.packets"} else "integer"}
    if kind == "number":
        return {"type": "double"}
    if kind == "boolean":
        return {"type": "boolean"}
    return {"type": "keyword"}


def component(name: str, properties: dict, version: str) -> dict:
    return {
        "template": {"mappings": {"dynamic": "strict", "properties": properties}},
        "_meta": {"managed_by": "tuba", "version": version},
    }


def build_assets() -> dict[str, dict]:
    uim = json.loads((ROOT / "contracts/events/standard-event/1/schema.json").read_text(encoding="utf-8"))
    raw = json.loads((ROOT / "contracts/events/raw/1/schema.json").read_text(encoding="utf-8"))
    quarantine = json.loads((ROOT / "contracts/events/quarantine/1/schema.json").read_text(encoding="utf-8"))
    uim_fields = uim["properties"]
    assets: dict[str, dict] = {}
    common = {name: map_schema(uim_fields[name], name) for name in sorted(COMMON_FIELDS)}
    assets["component-uim-common-v1.json"] = component("tuba-uim-common-v1", common, "1")
    for domain, field_names in DOMAIN_FIELDS.items():
        properties = {name: map_schema(uim_fields[name], name) for name in field_names}
        component_name = f"tuba-uim-{domain}-v1"
        assets[f"component-uim-{domain}-v1.json"] = component(component_name, properties, "1")
        assets[f"index-uim-{domain}-v1.json"] = {
            "index_patterns": [f"tuba-v1-uim-{domain}-*-g1-*"],
            "priority": 300,
            "composed_of": ["tuba-uim-common-v1", component_name],
            "template": {"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
            "_meta": {"managed_by": "tuba", "schema": "standard-event/1", "domain": domain, "version": 1},
        }

    raw_properties = {name: map_schema(child, name) for name, child in raw["properties"].items()}
    raw_properties["payload"] = {"type": "flattened"}
    assets["component-raw-v1.json"] = component("tuba-raw-v1", raw_properties, "1")
    assets["index-raw-v1.json"] = {
        "index_patterns": ["tuba-v1-raw-*-g1-*"], "priority": 300,
        "composed_of": ["tuba-raw-v1"],
        "template": {"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
        "_meta": {"managed_by": "tuba", "schema": "raw/1", "version": 1},
    }

    quarantine_properties = {name: map_schema(child, name) for name, child in quarantine["properties"].items()}
    assets["component-quarantine-v1.json"] = component("tuba-quarantine-v1", quarantine_properties, "1")
    assets["index-quarantine-v1.json"] = {
        "index_patterns": ["tuba-v1-quarantine-*-g1-*"], "priority": 300,
        "composed_of": ["tuba-quarantine-v1"],
        "template": {"settings": {"number_of_shards": 1, "number_of_replicas": 0}},
        "_meta": {"managed_by": "tuba", "schema": "quarantine/1", "version": 1},
    }
    assets["retention-policy-v1.json"] = {
        "version": 1,
        "defaults_are_capacity_targets_not_legal_retention": True,
        "indices": {
            "raw": {"days": 30, "date_field": "received_at"},
            "standard_events": {"days": 90, "date_field": "@timestamp"},
            "quarantine": {"days": 30, "date_field": "occurred_at"},
            "attributions_relations": {"days": 90, "date_field": "event_time_or_valid_from"},
            "features": {"days": 180, "date_field": "window_start"},
            "anomalies_risk_events": {"days": 365, "date_field": "window_start_or_event_time"},
            "current_state_and_baselines": {"days": None, "delete": "lifecycle_or_reference_checked_job_only"},
        },
        "delete_guards": ["partition_is_closed", "no_active_job_lease", "backup_policy_allows_delete", "audit_delete", "never_recreate_expired_partition_on_retry"],
    }
    return assets


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="write generated JSON assets")
    args = parser.parse_args()
    assets = build_assets()
    if args.write:
        OUTPUT.mkdir(parents=True, exist_ok=True)
        for filename, asset in assets.items():
            (OUTPUT / filename).write_text(json.dumps(asset, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {len(assets)} Elasticsearch assets to {OUTPUT.relative_to(ROOT)}")
        return 0
    missing = [name for name in assets if not (OUTPUT / name).is_file()]
    changed = [name for name, asset in assets.items() if (OUTPUT / name).is_file() and json.loads((OUTPUT / name).read_text(encoding="utf-8")) != asset]
    if missing or changed:
        raise SystemExit(f"Elasticsearch assets are stale; missing={missing}, changed={changed}; run with --write")
    print(f"Elasticsearch templates are current ({len(assets)} assets)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
