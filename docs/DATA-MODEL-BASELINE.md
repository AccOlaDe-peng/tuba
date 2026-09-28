# PostgreSQL data model baseline

Status: reviewable single-node model for migrations `00007`–`00011`; this describes the current schema, not a claim that every control-plane API or worker is delivered.

## Ownership and tenant boundaries

| Concept | Authoritative tables | Scope and invariant |
| --- | --- | --- |
| Source and credentials | `source_instances`, credential tables, `source_contexts`, `source_context_credentials` | `source_instances.organization_id` references `organizations`; source identity is unique within an organization. Context snapshots bind tenant, namespace, vendor/dataset, release and epoch immutably. Credential rotation may overlap without changing the epoch. API resolvers must select enabled sources by credential digest and organization scope. |
| Release and assets | `release_bundles` | Bundle ID and version are immutable. The canonical manifest and its SHA-256 live together in `manifest`/`sha256`; the manifest enumerates content-addressed DIP/UIM, route, mapping, entity and analysis assets. A separate asset table is unnecessary while an asset cannot be independently activated or shared. `source_instances.release_id` is a restrictive FK. |
| Jobs and attempts | `processing_jobs`, `processing_job_attempts` | Job is tenant scoped by `organization_id` and `namespace`; the UUID identifies its attempts. Attempt rows are subordinate operational history and must be read through their parent job after tenant authorization. |
| Leases | `processing_jobs` lease columns; `processor_outbox` fencing columns in `00008` | Lease owner, expiry and fencing token are persisted with the unit being claimed. Every renewal, completion and release must match owner plus fencing token. Lease fields are not a separate source of truth. |
| Consumer state | `processor_checkpoints`, `processor_inbox`, `processor_outbox` | Checkpoint identity is consumer group + topic + partition. Inbox uniqueness is consumer group + message ID and transport position; outbox order is producer + aggregate key + sequence. These are service-internal global coordination keys, not user query tables. Payloads and aggregate keys must carry organization identity for tenant-derived work; APIs do not expose these tables directly. |
| Entity timeline | `entities`, `entity_attributions`, `entity_relations` | Every table is scoped by `organization_id`. Composite FKs prevent attribution/relation references crossing organizations. Strong active identity uniqueness is tenant + entity type + authority + canonical key. Validity uses half-open `[valid_from, valid_to)` intervals. |
| Result revisions | `analysis_object_revisions` | Organization + object type + object ID + generation + revision is the primary identity. `operation` is `upsert` or `retracted`; revision is monotonic within the object generation and payload hash detects same-revision conflicts. `partition_date` is the immutable sink date key. |
| Ingest receipts | `ingest_receipts` | Keyed by organization slug + source instance + raw event ID; payload hash prevents a reused source position from silently changing content. This table is not an event-body store; Kafka/Raw remains the evidence path. |

## Migration ownership

- `00007_ingestion_and_processing.sql`: initial source/release, receipt, job/attempt, checkpoint/inbox/outbox, entity/attribution/relation, and result revision tables.
- `00008_worker_leases.sql`: outbox lease owner, expiry, fencing token and retry scheduling.
- `00009_source_credentials.sql`: unique credential digest constraint.
- `00010_collector_contexts.sql`: immutable source-context snapshot and receipt/ACK state; backfills existing sources.
- `00011_collector_management.sql`: enrollment, Collector identity, desired state, heartbeat and append-only configuration versions.
- `00012_source_context_scope.sql`: database trigger validates the immutable context's tenant/namespace/vendor/epoch/release tuple against its source instance at insertion.

Migrations are append-only. A deployed schema correction uses a new migration. DDL runs under a dedicated migration identity; application connections use DML-only roles. Migration files do not prove that their API/worker paths are active; those are tracked independently in the TODO.

## Explicit gaps

- Release manifest create/validate/stage/activate APIs and content-addressed artifact storage are not complete. The current JSONB manifest is a contract boundary, not a binary registry implementation.
- Job and release authorization/API surfaces are incomplete. Runtime processing workers do not yet register all business handlers.
- Inbox/outbox/checkpoint cleanup windows and result revision retention need A03 capacity/RPO decisions before production defaults are final.
- Current tenant boundaries are enforced by organization FKs and composite entity FKs, plus service-level authorization. PostgreSQL row-level security is not enabled; no direct user-facing query role may bypass the API authorization layer.

## Migration validation

On 2026-09-27, migrations `00001`–`00012` were applied in order and rolled back in reverse order using an isolated PostgreSQL 18.3 WASM instance (PGlite 0.5.8) with `pgcrypto` enabled. A seeded tenant/source context matching its source instance was accepted; a context with a mismatched namespace was rejected by migration `00012`. The disposable database ran outside the repository and no shared/248 database was modified. The host's deployed PostgreSQL is 14.23, so a staging run on that exact major version remains a deployment preflight, not a reason to experiment on the shared database.
