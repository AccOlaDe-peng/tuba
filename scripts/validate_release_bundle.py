#!/usr/bin/env python3
"""Verify every referenced asset in a TUBA semantic release bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath

from validate_contracts import ROOT, validate, validate_release_manifest


def canonical_manifest_bytes(manifest: dict) -> bytes:
    # Manifest schema v1 contains strings, arrays and objects only; this stable
    # UTF-8 JSON encoding is its canonical byte representation for bundle IDs.
    return json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("bundle_root", type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    schema = json.loads((ROOT / "contracts/releases/1/manifest.schema.json").read_text(encoding="utf-8"))
    validate(manifest, schema)
    validate_release_manifest(manifest)

    root = args.bundle_root.resolve(strict=True)
    for asset in manifest["assets"]:
        relative = PurePosixPath(asset["path"])
        target = (root / Path(*relative.parts)).resolve(strict=True)
        if root not in target.parents or not target.is_file():
            raise SystemExit(f"asset is not a regular file within the release root: {asset['asset_id']}")
        digest = sha256_file(target)
        if digest != asset["sha256"]:
            raise SystemExit(f"SHA-256 mismatch for release asset {asset['asset_id']}")

    print("release bundle verified; manifest_sha256=" + hashlib.sha256(canonical_manifest_bytes(manifest)).hexdigest())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
