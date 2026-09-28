# Elasticsearch M1 assets

`generated-v1/` contains deterministic component/index templates generated from the canonical Raw, Quarantine and UIM JSON Schemas by `scripts/generate_es_templates.py`. `make contracts` checks that committed assets still match the schema source. Templates use 1 primary shard/0 replicas for the current single-node profile, strict known-field mappings, and no alias declaration; indexers create tenant/generation aliases when they create physical UTC-date indices. `retention-policy-v1.json` records target days and guarded partition-deletion rules; it is not installed as ILM because deletion must first check leases, backup policy and audit requirements.

The legacy authentication data stream uses `logs-ueba.authentication-<namespace>`, which conflicts with the target authentication read alias of the same name. The standard indexer must not be started for that namespace until the legacy stream has been snapshotted, consumers have been switched, and the old data stream has been explicitly retired. The generic event API path must never delete or rename an existing stream automatically. Generated template index patterns use the separate `tuba-v1-*` physical namespace, so installing templates alone does not modify legacy indices.

The legacy authentication writer uses the `logs-ueba.authentication-<namespace>` data stream. The target raw and UIM indexers use deterministic physical indices `tuba-v1-raw-<namespace>-g1-<UTC-day>` and `tuba-v1-uim-<domain>-<namespace>-g1-<UTC-day>`, then expose read aliases named `logs-ueba.raw-<namespace>` and `logs-ueba.<domain>-<namespace>`. Stable event IDs therefore retry against the same physical date index. These target indices are created by the Go sinks with explicit bounded mappings; install/query API keys must include the `tuba-v1-*` patterns. Do not run the legacy authentication indexer in parallel with the target standard indexer for a namespace.

Anomalies are written by the analysis sink to `ueba-anomalies-<namespace>` with their stable `anomaly.id` as `_id`. The anomaly mapping is strict and indexes only the approved tenant, entity, detection window, evidence, explanation and analysis-run fields. The query API role reads authentication evidence and anomalies; the analysis sink role can only create anomaly documents.

Apply the assets with an administrative bootstrap key:

```bash
ES_URL=https://elasticsearch.example ES_API_KEY=... ./scripts/apply_elasticsearch_assets.sh
```

The installer validates that `generated-v1/` matches the canonical schemas, then idempotently PUTs the generated component/index templates and the current anomaly mapping. It does not install the legacy authentication Data Stream template or any ILM delete policy; legacy assets stay available for explicit migration work, and retention remains unset until A03 approves a capacity and recovery budget.

Runtime API keys must be generated for one namespace and one service at a time by `../scripts/manage_elasticsearch_api_keys.py`. The old global query/indexer role descriptors were removed because their wildcards crossed namespace and data-plane responsibility boundaries. The bootstrap key is only for controlled asset/key initialization and must never be mounted into runtime services. The separate legacy authentication Data Stream template is retained only for the old writer and is not the target UIM index contract.
