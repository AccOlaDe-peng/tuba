# Stable identity contract v1

All components use the UTF-8 byte length followed by `:` and the field bytes, in order, before hashing. The prefix and hash algorithm are part of the identity version; callers must not concatenate raw values themselves.

| Identifier | Canonical input | Format / owner |
| --- | --- | --- |
| `raw_event_id` | `raw-v1`, organization ID, source instance ID, vendor dataset, source epoch, source position | `raw:` + lowercase SHA-256 hex; ingest |
| `event.id` | `event-v1`, raw event ID, semantic discriminator, child record key | `evt:` + lowercase SHA-256 hex; DIP; raw ID already binds tenant/source/dataset/epoch/position |
| `entity.id` | tenant, entity type, authority, canonical key | `ent:` + lowercase SHA-256 hex; entity worker |
| `attribution.id` | event ID, entity snapshot, role mapping version | `att:` + lowercase SHA-256 hex; entity worker |
| `relation.id` | tenant, from entity, relation type, to entity, event ID, resolution snapshot | `rel:` + lowercase SHA-256 hex; entity worker |
| `feature.id` | tenant, entity, feature/version, window start/end, generation | `feat:` + lowercase SHA-256 hex; analysis worker |
| `anomaly.id` | tenant, rule/version, entity-or-event key, stable window key, generation | `ano:` + lowercase SHA-256 hex; analysis worker |
| `risk_event.id` | tenant, contribution type, anomaly ID, anomaly revision, policy version | `risk:` + lowercase SHA-256 hex; risk processor |
| `quarantine.id` | raw event ID, release ID, failure stage, reason code | `qua:` + lowercase SHA-256 hex; UIM gateway |

The hash provides compact encoding, not identity correctness. Strong/weak identity scope and canonicalization are defined by the versioned source/entity contract. A content hash is not an event identity. Changes to these inputs require an explicit identity version and replay generation.
