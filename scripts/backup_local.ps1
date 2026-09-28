param(
    [string]$OutputDirectory = "output\backups",
    [string]$PostgresContainer = "product-postgres-1",
    [string]$ElasticsearchURL = "http://127.0.0.1:9200",
    [string]$SnapshotRepository = "tuba-backups"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root

$output = Join-Path $root $OutputDirectory
New-Item -ItemType Directory -Path $output -Force | Out-Null
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")

$dumpName = "postgres-$stamp.dump"
$containerPath = "/tmp/$dumpName"
docker compose exec -T postgres pg_dump -U tuba -d tuba --format=custom --file=$containerPath
if ($LASTEXITCODE -ne 0) {
    throw "pg_dump failed"
}
docker cp "${PostgresContainer}:$containerPath" (Join-Path $output $dumpName)
if ($LASTEXITCODE -ne 0) {
    throw "docker cp failed"
}
docker compose exec -T postgres rm -f $containerPath

$repositoryBody = @{
    type = "fs"
    settings = @{
        location = "/usr/share/elasticsearch/backups"
        compress = $true
    }
} | ConvertTo-Json -Depth 5 -Compress
try {
    Invoke-RestMethod -Method Put -Uri "$ElasticsearchURL/_snapshot/$SnapshotRepository" `
        -ContentType "application/json" -Body $repositoryBody | Out-Null
} catch {
    if ($_.Exception.Response.StatusCode.value__ -ne 400) {
        throw
    }
}
$snapshotName = "tuba-$stamp"
Invoke-RestMethod -Method Put `
    -Uri "$ElasticsearchURL/_snapshot/$SnapshotRepository/$snapshotName?wait_for_completion=true" `
    -TimeoutSec 300 | Out-Null

[PSCustomObject]@{
    PostgresBackup = (Join-Path $output $dumpName)
    Snapshot = $snapshotName
    Repository = $SnapshotRepository
} | Format-List
