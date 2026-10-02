# PostgreSQL migrations

M2 control-plane schema starts at `00001_control_plane.sql` and covers organizations, external identities, tenant memberships, roles, cases, anomaly links, immutable audit events, and idempotency records. `00002_seed_local.sql` creates only the local `tenant_a` organization and built-in roles.

M4 analysis runtime schema starts at `00005_analysis_runtime.sql` and adds worker runs, Kafka checkpoints, recoverable partition state, and analyst feedback. `00006_analysis_feedback_context.sql` adds rule/entity/evidence context for false-negative and manual feedback.

`00007_ingestion_and_processing.sql` adds the initial source/release registry, leased processing jobs, inbox/outbox/checkpoints, Account/Device entity attribution and relation tables, and revision tracking.

`00008_worker_leases.sql` adds lease fencing and retry scheduling fields for the control worker's outbox publisher and processing-job handler framework. Outbox delivery is at least once; consumers must deduplicate by business identity or revision. The current executable has no business job handlers registered.

`00009_source_credentials.sql` makes the stored source credential digest unique. Raw ingest resolves API keys to an enabled source record and uses only registry-owned tenant, namespace, dataset, epoch, release, and rate limit values.

`00010_collector_contexts.sql` snapshots immutable source contexts, allows overlapping credential rotation without changing `source_epoch`, and records trusted receipt metadata plus Kafka acknowledgment state. It also backfills a context and credential row for existing sources; event payloads are not duplicated in PostgreSQL receipts.

`00011_collector_management.sql` adds one-time enrollment tokens, tenant-scoped Collector identities with credential digests, latest heartbeat/desired-version state, and append-only versioned Collector configurations. The agent credential is returned only during enrollment; revocation sets the agent state to `disabled`. This migration provides the server-side registry only: Collector-side enrollment, polling, config application, and binary rollout are not yet wired end to end.

`00012_source_context_scope.sql` enforces that a newly created immutable source context exactly snapshots the owning source instance's tenant slug, namespace, vendor tuple, epoch, and release. This closes a database integrity gap where a malformed context row could otherwise claim another tenant while referencing a valid source instance.

`00016_identity_spaces.sql` adds the identity space registry (organization-scoped, normalized name unique per tenant, stable `is:`-prefixed ID) and a composite FK so `entities.authority` can only reference a registered space of the same tenant. Entity rows are therefore impossible without prior space registration.
`00018_baseline_training.sql` adds the persisted feature-sample store (windowed feature records written at window close, revision-guarded corrections) and the immutable baseline model version store (insert-only content, lifecycle status transitions only, cold_start queryable, superseding references for audit). Baseline training reads only these samples — never Raw/standard events or Kafka retention.

`00013_release_publishing.sql` adds a global platform-publisher grant table and publisher-scoped idempotency records. These grants are separate from tenant memberships; release API permissions are resolved from this table after validating the caller's tenant membership. Release transition APIs write state and audit records in one transaction.

Migrations use goose annotations. Shared and production environments must run them with a dedicated DDL identity. Application credentials receive DML permissions only. Applied migrations are immutable; corrections require a new numbered file.

For a local database with `psql` installed:

```bash
DATABASE_URL='postgres://tuba:tuba-local-only@127.0.0.1:5432/tuba?sslmode=disable' ./scripts/apply_postgres_migrations.sh
```
