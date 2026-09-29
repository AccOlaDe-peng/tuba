# Release publishing migration runbook

## 248 deployment record (2026-09-29)

The 248 TUBA database is hosted by ADMS PostgreSQL 14.23 and uses the legacy ledger `public.schema_migrations(filename, applied_at)`. It does not use `public.tuba_schema_migrations(version, checksum)`. Before execution, the ledger ended at `00011_collector_management.sql`; migration 00012's source-context consistency precondition found zero mismatches.

With the user's explicit instruction to execute the migration, and after validating the full backup below, migrations 00012 and 00013 were applied in separate transactions while holding advisory lock `74190024001`. The existing database identity `tuba` owns the relevant tables and successfully performed the scoped DDL; no role grants, role attributes, or `pg_hba.conf` settings were changed.

Backup: `/opt/tuba/backups/pre-release-publishing-20260929.dump`  
Format: PostgreSQL custom dump; `pg_restore --list` succeeded  
Size: 151,504,146 bytes  
SHA-256: `e43e88d7a55911c2af16e0a61ced7bcfbd395db0b31f5cdfe309dcecf0b3c935`

Readback confirmed both filenames in `public.schema_migrations`, both `platform_release_publishers` and `platform_release_idempotency` tables, and the `source_context_scope_trg` and `audit_events_append_only` triggers. The publisher bootstrap audit was also recorded after deployment. Windows Security publication is still pending a fresh operator login; the previous browser access token was rejected as invalid, and no Windows Security release state was created.

## Applying future migrations on 248

Do not run `docs/dba/00013-release-publishing.psql` against 248: that generic artifact targets `public.tuba_schema_migrations`, which is absent on this host. First verify the ledger and current schema, take and validate a fresh backup, inspect every migration's preconditions, and apply each migration and its legacy filename ledger row in one transaction. Hold advisory lock `74190024001` for the entire migration batch. Never infer permission from the application role name; verify table ownership/required DDL rights first, and do not change shared PostgreSQL authentication to make migrations work.

For a new database using the current migration runner, follow `scripts/apply_postgres_migrations.sh` and `docs/dba/00013-release-publishing.psql` with the dedicated migration connection. Do not copy the 248 legacy-ledger procedure to a different database without checking its ledger schema.
