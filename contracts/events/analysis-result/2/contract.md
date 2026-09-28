# Analysis result v2

This contract is the versioned sink boundary for entity, feature, anomaly, and risk projections. `object_id` is stable for the business object, `revision` is assigned monotonically by that object's state owner, and `generation` identifies the active model or rule generation. `operation=retracted` is a tombstone for a previously published object; it must be retained and applied in revision order.

`window_start` is the event-time start of the object's stable detection window. `date_key` is the UTC calendar date derived from `window_start`; it is immutable across retries and revisions. Producers must not derive it from processing time or `run_id`. `input_refs` identifies the exact upstream object IDs and optional revisions/content hashes used to derive the result.

## Compatibility and migration from v1

`analysis-result/1/schema.json` remains the immutable legacy contract. During migration, consumers may read both versions; producers switch to v2 only after the v2 sink is deployed. A v1 `result_type=feature|anomaly|risk` maps to `object_type=feature|anomaly|risk_event`; `result_id` maps to `object_id`, with `revision=1` and `operation=upsert`. Because v1 has no trustworthy generation or input references, the migration adapter must set `generation=legacy-v1` and `input_refs=[]`; it must not infer either from `rule_version` or `run_id`. These object types align with the persisted `analysis_object_revisions` contract.

The v1 document's explicit `window_start` is required to derive v2 `window_start` and `date_key`. If absent or invalid, retain the v1 message and route it to a migration quarantine; never substitute `generated_at`, `@timestamp`, or current time. V1 output remains queryable through its existing index/alias until its retention window expires. New writers and the v2 topic/index use only this schema.
