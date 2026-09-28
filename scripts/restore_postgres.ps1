param(
    [Parameter(Mandatory = $true)]
    [string]$BackupFile,
    [string]$PostgresContainer = "product-postgres-1",
    [string]$Database = "tuba"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root

$resolved = (Resolve-Path -LiteralPath $BackupFile).Path
$containerPath = "/tmp/$(Split-Path -Leaf $resolved)"
docker cp $resolved "${PostgresContainer}:$containerPath"
if ($LASTEXITCODE -ne 0) {
    throw "docker cp failed"
}
docker compose exec -T postgres pg_restore -U tuba -d $Database --clean --if-exists --no-owner $containerPath
if ($LASTEXITCODE -ne 0) {
    throw "pg_restore failed"
}
docker compose exec -T postgres rm -f $containerPath
Write-Host "Restored $resolved"
