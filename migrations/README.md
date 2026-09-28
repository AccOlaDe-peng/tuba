# PostgreSQL migrations

M2 control-plane schema starts at `00001_control_plane.sql` and covers organizations, external identities, tenant memberships, roles, cases, anomaly links, immutable audit events, and idempotency records. `00002_seed_local.sql` creates only the local `tenant_a` organization and built-in roles.

M4 analysis runtime schema starts at `00005_analysis_runtime.sql` and adds worker runs, Kafka checkpoints, recoverable partition state, and analyst feedback. `00006_analysis_feedback_context.sql` adds rule/entity/evidence context for false-negative and manual feedback.

`00007_ingestion_and_processing.sql` adds the initial source/release registry, leased processing jobs, inbox/outbox/checkpoints, Account/Device entity attribution and relation tables, and revision tracking. Runtime integration and authorization APIs are still pending; this migration only establishes the storage model.

`00008_worker_leases.sql` adds lease fencing and retry scheduling fields for the control worker's outbox publisher and processing-job handler framework. Outbox delivery is at least once; consumers must deduplicate by business identity or revision. The current executable has no business job handlers registered.

`00009_source_credentials.sql` makes the stored source credential digest unique. Raw ingest resolves API keys to an enabled source record and uses only registry-owned tenant, namespace, dataset, epoch, release, and rate limit values.

`00010_collector_contexts.sql` snapshots immutable source contexts, allows overlapping credential rotation without changing `source_epoch`, and records trusted receipt metadata plus Kafka acknowledgment state. It also backfills a context and credential row for existing sources; event payloads are not duplicated in PostgreSQL receipts.

`00011_collector_management.sql` adds one-time enrollment tokens, tenant-scoped Collector identities with credential digests, latest heartbeat/desired-version state, and append-only versioned Collector configurations. The agent credential is returned only during enrollment; revocation sets the agent state to `disabled`. This migration provides the server-side registry only: Collector-side enrollment, polling, config application, and binary rollout are not yet wired end to end.

`00012_source_context_scope.sql` enforces that a newly created immutable source context exactly snapshots the owning source instance's tenant slug, namespace, vendor tuple, epoch, and release. This closes a database integrity gap where a malformed context row could otherwise claim another tenant while referencing a valid source instance.

Migrations use goose annotations. Shared and production environments must run them with a dedicated DDL identity. Application credentials receive DML permissions only. Applied migrations are immutable; corrections require a new numbered file.

For a local database with `psql` installed:

```bash
DATABASE_URL='postgres://tuba:tuba-local-only@127.0.0.1:5432/tuba?sslmode=disable' ./scripts/apply_postgres_migrations.sh
```
