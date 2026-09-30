#!/usr/bin/env python3
"""Validate the OpenAPI document as a contract rather than as text.

`validate_contracts.py` deliberately depends on nothing but the standard
library, which left its OpenAPI check as a string search: it looked for a few
markers and for the Beat schema reference appearing somewhere in the file. That
cannot tell a live document from one whose `$ref` targets were renamed, whose
paths were emptied, or whose referenced schema no longer validates at all.

This is the real gate, kept as a separate script so the dependency-free baseline
stays usable in minimal environments. It parses the YAML, resolves every
reference — internal JSON pointers into the parsed document, and external files
— and re-checks each referenced JSON Schema with the same rules the baseline
enforces. A reference that no longer resolves, or a schema that no longer
validates, fails here.

Requires PyYAML.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # environment guard, not a code path
    raise SystemExit(
        "validate_openapi.py requires PyYAML (pip install pyyaml). "
        "The dependency-free baseline is scripts/validate_contracts.py."
    )

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from validate_contracts import validate_schema  # noqa: E402

SPEC = ROOT / "contracts" / "api" / "openapi.yaml"
BEAT_SCHEMA_REF = "../events/beat-ingress/1/schema.json"
HTTP_METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")


def resolve_pointer(document: dict, pointer: str, origin: str):
    """Resolve a local pointer such as `#/components/schemas/Foo`."""
    if not pointer.startswith("#/"):
        raise ValueError(f"{origin}: unsupported reference {pointer!r}")
    node = document
    for raw in pointer[2:].split("/"):
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(node, dict) and token in node:
            node = node[token]
        elif isinstance(node, list) and token.isdigit() and int(token) < len(node):
            node = node[int(token)]
        else:
            raise ValueError(f"{origin}: reference {pointer!r} does not resolve")
    return node


def collect_references(node, origin: str, found: list) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "$ref" and isinstance(value, str):
                found.append((value, origin))
            else:
                collect_references(value, origin, found)
    elif isinstance(node, list):
        for value in node:
            collect_references(value, origin, found)


def check_references(document: dict) -> tuple:
    references = []
    collect_references(document, "openapi.yaml", references)
    problems = []
    internal = external = 0
    for target, origin in references:
        if target.startswith("#/"):
            internal += 1
            try:
                resolve_pointer(document, target, origin)
            except ValueError as error:
                problems.append(str(error))
            continue
        external += 1
        path = (SPEC.parent / target).resolve()
        if not path.is_file():
            problems.append(f"{origin}: reference {target!r} points at a missing file")
            continue
        if path.suffix != ".json":
            continue
        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
        except ValueError as error:
            problems.append(f"{origin}: {target}: invalid JSON: {error}")
            continue
        try:
            validate_schema(schema, target)
        except (ValueError, KeyError, TypeError) as error:
            problems.append(f"{origin}: {target}: {error}")
    return internal, external, problems


def check_paths(document: dict) -> list:
    problems = []
    for name, item in document["paths"].items():
        if not isinstance(item, dict):
            problems.append(f"paths.{name}: not a mapping")
            continue
        operations = [key for key in item if key.lower() in HTTP_METHODS]
        if not operations:
            problems.append(f"paths.{name}: declares no HTTP method")
        for method in operations:
            operation = item[method]
            if not isinstance(operation, dict) or not operation.get("responses"):
                problems.append(f"paths.{name}.{method}: declares no responses")
    return problems


def main() -> int:
    if not SPEC.is_file():
        print(f"openapi.yaml: not found at {SPEC}", file=sys.stderr)
        return 1
    document = yaml.safe_load(SPEC.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        print("openapi.yaml: not a mapping", file=sys.stderr)
        return 1
    if str(document.get("openapi")) != "3.1.0":
        print(f"openapi.yaml: expected openapi: 3.1.0, found {document.get('openapi')!r}", file=sys.stderr)
        return 1
    problems = []
    for key in ("paths", "components"):
        if not isinstance(document.get(key), dict) or not document[key]:
            problems.append(f"{key} must be a non-empty mapping")

    internal = external = 0
    if not problems:
        internal, external, reference_problems = check_references(document)
        problems.extend(reference_problems)
        problems.extend(check_paths(document))
        # The one reference that carries the ingress contract has to keep
        # pointing at the canonical schema, not merely mention it somewhere.
        beat = document["components"].get("schemas", {}).get("BeatIngressEvent")
        if not isinstance(beat, dict) or beat.get("$ref") != BEAT_SCHEMA_REF:
            problems.append(f"components.schemas.BeatIngressEvent must reference {BEAT_SCHEMA_REF}")

    if problems:
        for problem in problems:
            print(f"openapi.yaml: {problem}", file=sys.stderr)
        return 1
    print(
        f"validated openapi.yaml: {len(document['paths'])} paths, "
        f"{len(document['components'])} component groups, "
        f"{internal} internal and {external} external references resolved"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
