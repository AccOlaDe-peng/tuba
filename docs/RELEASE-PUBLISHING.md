# TUBA semantic release publishing

This is the release control-plane contract for DIP/UIM, routing, Elasticsearch templates, entity rules, data model, and analysis rules. It is separate from the binary/package release lifecycle.

## Authorization boundary

Release permissions are global because a semantic release is shared by tenants. Tenant roles (including `tenant_admin`) do not grant release access. Only an active row in `platform_release_publishers`, looked up by the API's verified OIDC issuer and subject, yields `release:read` and `release:manage`. Token role claims cannot create this grant. A current publisher can grant or revoke another publisher through the platform endpoint; those changes are audited. Self-revocation is rejected so an operator cannot accidentally remove their own access.

The first publisher is bootstrapped once by an operator after migration 00013, using `cmd/tuba-bootstrap-release-publisher`. It refuses to run once any publisher row exists. `--approved-by` is mandatory and recorded in the global audit trail; because there is no prior publisher to act as the first publisher, this one-time bootstrap has a null actor and explicit approval metadata. Thereafter all grants use the API and identify the acting publisher.

## Bundle storage and immutable content

The API receives a manifest, not an archive. An operator first copies the release bundle to `${TUBA_RELEASE_ROOT}/<release_id>` on the API host. Set this directory to read-only for the API service account (directories `0550`, files `0440` is a suitable Linux baseline); only the deployment operator should be able to replace it. The folder name must exactly match manifest `release_id`. The API resolves symlinks and rejects any asset that escapes the release root, is not a regular file, has an unsafe path, is absent, or fails its SHA-256 check. Hashes are checked at validation, staging, and activation, so a changed bundle cannot advance.

Manifest schema v1 requires one each of DIP, UIM, routing, Elasticsearch mappings/templates, entity rules, data model, and analysis rules, with a valid acyclic dependency graph and compatibility declaration. The stored digest is SHA-256 over deterministic, key-sorted JSON bytes. A release ID is immutable: same ID and same content is a no-op; different content conflicts. `Idempotency-Key` is scoped to the authenticated publisher; reuse with different content conflicts.

## Publishing sequence

1. Build the bundle and run `python scripts/validate_release_bundle.py <manifest> <bundle-root>` before delivery.
2. Copy the complete directory to the configured API release root, preserving paths. Do not make the service account able to modify it.
3. Register the manifest with `POST /api/v1/releases` and an `Idempotency-Key` of at least 16 characters. It creates a `draft` record and immutable manifest hash.
4. `POST /api/v1/releases/{id}/validate` verifies the server-side bundle and atomically records `draft -> validated` with an audit event.
5. `POST /api/v1/releases/{id}/stage` rechecks all assets and records `validated -> staged` with an audit event.
6. `POST /api/v1/releases/{id}/activate` rechecks all assets and records `staged -> active` with an audit event. No source is changed implicitly.
7. Use `GET /api/v1/releases/{id}/audit` to inspect actor, action, request ID, timestamps, and state/hash details. Use the ordinary tenant source-management API to explicitly bind sources to a `staged` or `active` release.

All state updates and their audit rows commit in one PostgreSQL transaction and use a compare-and-set on the old state. Failed validation leaves the record in its prior state. Release records and bundles are never overwritten by the API. Existing source references prevent database deletion; retiring content is a separate future operation and does not erase historical records.

## Operator bootstrap

After applying migrations through `00013_release_publishing.sql` and setting the API's `TUBA_RELEASE_ROOT`, run once with the runtime DML database identity:

```powershell
$env:OIDC_ISSUER = 'https://<configured-issuer>/realms/tuba'
$env:DATABASE_URL = '<protected runtime DSN>'
go run ./cmd/tuba-bootstrap-release-publisher --subject '<exact OIDC sub>' --approved-by '<change or explicit authorization reference>'
```

Never put the DSN or access token in release manifests or command arguments. The command does not create or change Keycloak users; it grants only the exact supplied OIDC subject a TUBA platform publishing role.

## API summary

| Operation | Permission | Result |
|---|---|---|
| `GET /api/v1/releases` and `GET /api/v1/releases/{id}` | `release:read` | Immutable manifest and state |
| `POST /api/v1/releases` | `release:manage` | Draft registration |
| `POST /api/v1/releases/{id}/validate` | `release:manage` | Hash/dependency check, draft to validated |
| `POST /api/v1/releases/{id}/stage` | `release:manage` | Recheck, validated to staged |
| `POST /api/v1/releases/{id}/activate` | `release:manage` | Recheck, staged to active |
| `GET /api/v1/releases/{id}/audit` | `release:read` | Append-only release audit |
| `PUT/DELETE /api/v1/platform/release-publishers/{subject}` | `release:manage` | Grant/revoke global publisher, audited |

