# TUBA semantic release manifest v1

The manifest is the immutable dependency graph stored in `release_bundles.manifest`. Each asset is an ASCII relative path inside a separately delivered release bundle and has a SHA-256 digest over its exact file bytes. The database `release_bundles.sha256` is the SHA-256 of the RFC 8785 canonical JSON bytes for the complete manifest. Schema v1 restricts manifest strings to ASCII and numbers are absent, so the canonical encoding is deterministic. The manifest does not contain its own digest.

The release must contain exactly one top-level asset of each required kind: DIP, UIM, routing registry, Elasticsearch mappings/templates, entity rules, data model and analysis rules. Asset IDs are unique; dependencies reference IDs in the same manifest, and the dependency graph must be acyclic. Every path must remain inside the extracted release root. Verification happens before a release can move from `draft` to `validated`; activation and rollback are separate audited operations.

`compatibility` pins the raw/UIM/result contract majors and the output generation. A compatibility change that cannot be consumed by the deployed services creates a new release and generation; an existing release is never edited in place. This manifest is distinct from `deploy/components/manifest.v1.json`, which only pins upstream collector binaries.
