# Windows Security ingestion release 1.0.0

This bundle records the Winlogbeat Security-channel profile currently implemented by `internal/uim.normalizeWindows` and `deploy/components/windows-security-winlogbeat.example.yml`. It covers the 24 Event IDs in the collector profile, preserves the full Beat event in Raw evidence, maps the four supported UIM routes, and pins the generated Elasticsearch templates used by those routes.

`analysis/rules.json` is intentionally an ingestion-only declaration with no detection rules. This bundle does not claim that Windows-specific detections are implemented or activated.

Validate the manifest and every asset hash from the repository root:

```powershell
python scripts/validate_release_bundle.py releases/windows-security-1.0.0/manifest.json releases/windows-security-1.0.0
```

The repository validator confirms structure, dependency graph, containment, and hashes. It does not publish the release to a TUBA runtime. Runtime release publication/validation/activation APIs are still required before sources can bind to this release.
