param(
    [Parameter(Mandatory)][string]$Container,
    [string]$DatabaseUser = "tuba"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$suffix = [Guid]::NewGuid().ToString("N").Substring(0, 8)
$tempRoot = Join-Path $env:TEMP "tuba-pg-migration-verify-$suffix"
$profiles = @("normal", "drift", "failure", "runtime")
$databaseNames = @("o02_main_$suffix", "o02_concurrent_$suffix", "o02_failure_$suffix", "o03_runtime_$suffix")
$containerPaths = @("/tmp/tuba-pg-verify-$suffix")
$runnerPath = "/tmp/tuba-pg-runner-$suffix.sh"
$runnerLocal = Join-Path $env:TEMP "tuba-pg-runner-$suffix.sh"

function Invoke-Docker {
    param([Parameter(Mandatory)][string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker command failed: docker $($Arguments -join ' ')"
    }
}

function Invoke-Migrations {
    param([string]$Database, [string]$Path)
    $output = & docker exec -e "DATABASE_MIGRATION_URL=postgresql://$DatabaseUser@/$Database" $Container bash $runnerPath $Path
    $exitCode = $LASTEXITCODE
    $runnerStatus = $output | Where-Object { $_ -match '^RUNNER_EXIT=(\d+)$' } | Select-Object -Last 1
    if ($runnerStatus -match '^RUNNER_EXIT=(\d+)$') { $exitCode = [int]$Matches[1] }
    $output | ForEach-Object { Write-Host $_ }
    return $exitCode
}

try {
    & docker exec $Container bash -lc "command -v psql >/dev/null && command -v sha256sum >/dev/null"
    if ($LASTEXITCODE -ne 0) {
        throw "Container '$Container' must provide psql and sha256sum."
    }

    $runnerBody = @'
#!/usr/bin/env bash
set +e
cd "$1"
./scripts/apply_postgres_migrations.sh
status=$?
echo "RUNNER_EXIT=$status"
exit "$status"
'@
    [IO.File]::WriteAllText($runnerLocal, $runnerBody.Replace("`r`n", "`n") + "`n", [Text.UTF8Encoding]::new($false))
    Invoke-Docker -Arguments @("cp", $runnerLocal, "${Container}:$runnerPath")

    foreach ($profile in $profiles) {
        $profileRoot = Join-Path $tempRoot $profile
        New-Item -ItemType Directory -Force -Path (Join-Path $profileRoot "scripts"), (Join-Path $profileRoot "migrations") | Out-Null
        Copy-Item -LiteralPath (Join-Path $PSScriptRoot "apply_postgres_migrations.sh") -Destination (Join-Path $profileRoot "scripts/apply_postgres_migrations.sh")
        Copy-Item -Path (Join-Path $root "migrations/*.sql") -Destination (Join-Path $profileRoot "migrations")
    }
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot "provision_postgres_runtime_role.sh") -Destination (Join-Path $tempRoot "runtime/scripts/provision_postgres_runtime_role.sh")

    $driftMigration = Get-ChildItem (Join-Path $tempRoot "drift/migrations") -File | Sort-Object Name | Select-Object -First 1
    [IO.File]::AppendAllText($driftMigration.FullName, "`n-- checksum drift verification`n", [Text.UTF8Encoding]::new($false))
    $normalFirst = Get-ChildItem (Join-Path $tempRoot "normal/migrations") -File | Sort-Object Name | Select-Object -First 1
    if ((Get-FileHash $normalFirst.FullName -Algorithm SHA256).Hash -eq (Get-FileHash $driftMigration.FullName -Algorithm SHA256).Hash) {
        throw "Checksum drift probe did not change the copied migration file."
    }

    $failureMigration = Get-ChildItem (Join-Path $tempRoot "failure/migrations") -File | Sort-Object Name | Select-Object -Last 1
    $failureSQL = [IO.File]::ReadAllText($failureMigration.FullName)
    $failureSQL = $failureSQL.Replace("-- +goose Down", "SELECT * FROM tuba_intentionally_missing_for_rollback_check;`n-- +goose Down")
    if ($failureSQL -notmatch "tuba_intentionally_missing_for_rollback_check") {
        throw "Could not add a failure probe to $($failureMigration.Name)."
    }
    [IO.File]::WriteAllText($failureMigration.FullName, $failureSQL, [Text.UTF8Encoding]::new($false))

    $basePath = $containerPaths[0]
    Invoke-Docker -Arguments @("exec", $Container, "mkdir", "-p", $basePath)
    foreach ($profile in $profiles) {
        $destination = if ($profile -eq "normal") { "$basePath/normal" } else { "$basePath/$profile" }
        Invoke-Docker -Arguments @("exec", $Container, "mkdir", "-p", $destination)
        Invoke-Docker -Arguments @("cp", "$(Join-Path $tempRoot $profile)/.", "${Container}:$destination")
    }

    foreach ($database in $databaseNames) {
        Invoke-Docker -Arguments @("exec", $Container, "createdb", "-U", $DatabaseUser, $database)
    }

    if ((Invoke-Migrations $databaseNames[0] "$basePath/normal") -ne 0) {
        throw "Initial migration failed for disposable database $($databaseNames[0])."
    }
    if ((Invoke-Migrations $databaseNames[0] "$basePath/normal") -ne 0) {
        throw "Repeat migration failed for disposable database $($databaseNames[0])."
    }
    if ((Invoke-Migrations $databaseNames[3] "$basePath/runtime") -ne 0) {
        throw "Initial migration failed for runtime-role disposable database $($databaseNames[3])."
    }
    $runtimeDatabaseName = $databaseNames[3]
    $runtimeOnlyDatabaseURL = "postgresql://${DatabaseUser}@/$runtimeDatabaseName"
    & docker exec $Container bash -lc "cd $basePath/runtime && env -u DATABASE_MIGRATION_URL DATABASE_URL='$runtimeOnlyDatabaseURL' ./scripts/apply_postgres_migrations.sh" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        throw "Migration runner unexpectedly accepted runtime DATABASE_URL without DATABASE_MIGRATION_URL."
    }
    Write-Host "Migration runner rejected runtime-only DATABASE_URL as expected."
    $mainVersionCount = & docker exec $Container psql -U $DatabaseUser -d $databaseNames[0] -Atc "SELECT count(*) FROM public.tuba_schema_migrations"
    Write-Host "Migration ledger after first and repeat runs: $($mainVersionCount.Trim()) versions."

    $driftExit = Invoke-Migrations $databaseNames[0] "$basePath/drift"
    if ($driftExit -eq 0) {
        throw "Changed migration checksum was not rejected in $($databaseNames[0])."
    }

    $concurrentScript = Join-Path $tempRoot "concurrent.sh"
    $concurrentBody = @'
#!/usr/bin/env bash
set -uo pipefail
database="$1"
path="$2"
user="$3"
cd "$path"
DATABASE_MIGRATION_URL="postgresql://${user}@/${database}" ./scripts/apply_postgres_migrations.sh >/tmp/tuba-pg-a.log 2>&1 &
first=$!
DATABASE_MIGRATION_URL="postgresql://${user}@/${database}" ./scripts/apply_postgres_migrations.sh >/tmp/tuba-pg-b.log 2>&1 &
second=$!
wait "$first"
first_status=$?
wait "$second"
second_status=$?
if ((first_status != 0 || second_status != 0)); then
  echo "concurrent migration exit codes: $first_status $second_status"
  tail -n 12 /tmp/tuba-pg-a.log /tmp/tuba-pg-b.log
  exit 1
fi
cat /tmp/tuba-pg-a.log /tmp/tuba-pg-b.log
'@
    [IO.File]::WriteAllText($concurrentScript, $concurrentBody.Replace("`r`n", "`n") + "`n", [Text.UTF8Encoding]::new($false))
    Invoke-Docker -Arguments @("cp", $concurrentScript, "${Container}:/tmp/tuba-pg-concurrent-$suffix.sh")
    Invoke-Docker -Arguments @("exec", $Container, "bash", "/tmp/tuba-pg-concurrent-$suffix.sh", $databaseNames[1], "$basePath/normal", $DatabaseUser)

    $failureExit = Invoke-Migrations $databaseNames[2] "$basePath/failure"
    if ($failureExit -eq 0) {
        throw "Intentional migration error unexpectedly succeeded in $($databaseNames[2])."
    }
    $lastVersion = [IO.Path]::GetFileNameWithoutExtension($failureMigration.Name)
    $recordCount = & docker exec $Container psql -U $DatabaseUser -d $databaseNames[2] -Atc "SELECT count(*) FROM public.tuba_schema_migrations WHERE version='$lastVersion'"
    if ($LASTEXITCODE -ne 0 -or $recordCount.Trim() -ne "0") {
        throw "Failed migration $lastVersion was recorded instead of rolling back."
    }
    $triggerAbsent = & docker exec $Container psql -U $DatabaseUser -d $databaseNames[2] -Atc "SELECT to_regprocedure('enforce_source_context_scope()') IS NULL"
    if ($LASTEXITCODE -ne 0 -or $triggerAbsent.Trim() -ne "t") {
        throw "DDL from failed migration $lastVersion was not rolled back."
    }

    $expectedCount = @(Get-ChildItem (Join-Path $root "migrations") -File | Where-Object { $_.Name -match '^\d{5}_[a-z0-9_]+\.sql$' }).Count
    foreach ($database in @($databaseNames[0], $databaseNames[1])) {
        $appliedCount = & docker exec $Container psql -U $DatabaseUser -d $database -Atc "SELECT count(*) FROM public.tuba_schema_migrations"
        if ($LASTEXITCODE -ne 0 -or [int]$appliedCount.Trim() -ne $expectedCount) {
            throw "Expected $expectedCount applied migrations in $database; found $appliedCount."
        }
    }

    $runtimeUser = "o03_runtime_$suffix"
    $runtimePassword = "TUBA-runtime-validation-$suffix"
    $migrationURL = "postgresql://${DatabaseUser}@/$runtimeDatabaseName"
    $provisionPath = "$basePath/runtime/scripts/provision_postgres_runtime_role.sh"
    foreach ($attempt in 1..2) {
        $provisionOutput = & docker exec `
            -e "DATABASE_MIGRATION_URL=$migrationURL" `
            -e "TUBA_MIGRATION_DB_USER=$DatabaseUser" `
            -e "TUBA_RUNTIME_DB_USER=$runtimeUser" `
            -e "TUBA_RUNTIME_DB_PASSWORD=$runtimePassword" `
            $Container bash $provisionPath 2>&1
        if ($LASTEXITCODE -ne 0) {
            $provisionOutput | ForEach-Object { Write-Host $_ }
            throw "Runtime role provisioning attempt $attempt failed."
        }
    }
    $roleFlags = & docker exec $Container psql -U $DatabaseUser -d $databaseNames[3] -Atc "SELECT rolsuper || ':' || rolcreatedb || ':' || rolcreaterole || ':' || rolreplication || ':' || rolbypassrls FROM pg_roles WHERE rolname='$runtimeUser'"
    if ($LASTEXITCODE -ne 0 -or $roleFlags.Trim() -ne "false:false:false:false:false") {
        throw "Runtime role has elevated privileges: $roleFlags"
    }

    $probeTable = "o03_runtime_probe_$suffix"
    $probeState = & docker exec $Container psql -U $DatabaseUser -d $databaseNames[3] -X -v ON_ERROR_STOP=1 -Atc "CREATE TABLE public.$probeTable (id bigserial primary key, value text not null); SELECT current_database() || ':' || current_user || ':' || current_role || ':' || coalesce((SELECT defaclacl::text FROM pg_default_acl WHERE defaclrole=(SELECT oid FROM pg_roles WHERE rolname='$DatabaseUser') AND defaclnamespace='public'::regnamespace AND defaclobjtype='r'), 'NO_DEFAULT_ACL') || ':' || tableowner || ':' || coalesce(relacl::text, 'NULL') || ':' || has_table_privilege('$runtimeUser', 'public.$probeTable', 'SELECT') || ':' || has_table_privilege('$runtimeUser', 'public.$probeTable', 'INSERT') || ':' || has_table_privilege('$runtimeUser', 'public.$probeTable', 'UPDATE') || ':' || has_table_privilege('$runtimeUser', 'public.$probeTable', 'DELETE') FROM pg_tables JOIN pg_class ON pg_class.relname=pg_tables.tablename WHERE pg_tables.schemaname='public' AND pg_tables.tablename='$probeTable'"
    $probeLine = [string]($probeState | Select-Object -Last 1)
    $probeFields = $probeLine.Trim().Split(':')
    Write-Host "Runtime DML probe database, creator role, default ACL, table owner/ACL and privileges: $($probeLine.Trim())"
    if ($LASTEXITCODE -ne 0 -or $probeFields.Count -ne 10 -or $probeFields[0] -ne $databaseNames[3] -or $probeFields[1] -ne $DatabaseUser -or $probeFields[2] -ne $DatabaseUser -or $probeFields[3] -notmatch [regex]::Escape($runtimeUser) -or $probeFields[4] -ne $DatabaseUser -or $probeFields[5] -notmatch [regex]::Escape($runtimeUser) -or ($probeFields[6..9] -join ':') -ne "true:true:true:true") {
        throw "Runtime role lacks expected DML rights on a newly created table."
    }
    $runtimeExec = @("exec", "-e", "PGPASSWORD=$runtimePassword", $Container, "psql", "-h", "127.0.0.1", "-U", $runtimeUser, "-d", $databaseNames[3], "-v", "ON_ERROR_STOP=1", "-q")
    & docker @runtimeExec -c "INSERT INTO public.$probeTable(value) VALUES ('dml-ok')"
    if ($LASTEXITCODE -ne 0) { throw "Runtime role could not insert into a newly migrated table." }
    & docker @runtimeExec -c "UPDATE public.$probeTable SET value='dml-updated' WHERE value='dml-ok'"
    if ($LASTEXITCODE -ne 0) { throw "Runtime role could not update a newly migrated table." }
    & docker @runtimeExec -c "SELECT value FROM public.$probeTable WHERE value='dml-updated'"
    if ($LASTEXITCODE -ne 0) { throw "Runtime role could not select from a newly migrated table." }
    & docker @runtimeExec -c "DELETE FROM public.$probeTable WHERE value='dml-updated'"
    if ($LASTEXITCODE -ne 0) { throw "Runtime role could not delete from a newly migrated table." }
    & docker @runtimeExec -c "CREATE TABLE public.o03_runtime_forbidden_$suffix (id integer)" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { throw "Runtime role unexpectedly created a table." }
    & docker @runtimeExec -c "SELECT count(*) FROM public.tuba_schema_migrations" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { throw "Runtime role unexpectedly read the migration ledger." }

    Write-Host "PostgreSQL acceptance passed: migration first/repeat/concurrent, checksum drift rejection, failed migration rollback, and idempotent DML-only runtime role."
}
finally {
    foreach ($database in $databaseNames) {
        & docker exec $Container dropdb -U $DatabaseUser --if-exists $database 2>$null | Out-Null
    }
    & docker exec $Container rm -rf $containerPaths[0] $runnerPath "/tmp/tuba-pg-concurrent-$suffix.sh" 2>$null | Out-Null
    Remove-Item -LiteralPath $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $runnerLocal -Force -ErrorAction SilentlyContinue
}
