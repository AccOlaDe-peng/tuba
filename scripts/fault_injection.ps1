param(
    [int]$Events = 50,
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root

function Get-IndexerLag {
    $output = docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:29092 `
        --group tuba-raw-indexer-tenant_a --describe
    $lag = 0
    foreach ($line in $output) {
        if ($line -match '^\S+\s+\S+\s+\d+\s+\S+\s+\S+\s+(\S+)\s+') {
            $value = $matches[1]
            if ($value -match '^\d+$') {
                $lag += [int]$value
            }
        }
    }
    return $lag
}

if ([string]::IsNullOrWhiteSpace($env:TUBA_SOURCE_API_KEY)) {
    throw "Set TUBA_SOURCE_API_KEY to a key issued for a registered source"
}
if ([string]::IsNullOrWhiteSpace($env:TUBA_SOURCE_CONTEXT_ID)) {
    throw "Set TUBA_SOURCE_CONTEXT_ID to the immutable context ID returned during source registration"
}
$before = Get-IndexerLag
docker compose stop elasticsearch
if ($LASTEXITCODE -ne 0) {
    throw "failed to stop Elasticsearch"
}

$headers = @{"X-API-Key" = $env:TUBA_SOURCE_API_KEY; "X-Source-Context" = $env:TUBA_SOURCE_CONTEXT_ID; "Content-Type" = "application/json"}
for ($index = 0; $index -lt $Events; $index++) {
    $payload = @{
        "@timestamp" = (Get-Date).ToUniversalTime().ToString("o")
        event = @{id = "fault-$([guid]::NewGuid())"; action = "logon"; outcome = "failure"}
        user = @{name = "fault.user"}
    } | ConvertTo-Json -Depth 8 -Compress
    Invoke-RestMethod -Method Post `
        -Uri "http://127.0.0.1:8080/api/v1/ingest/events" `
        -Headers ($headers + @{"X-Source-Position" = "fault-$index-$([guid]::NewGuid())"}) -Body $payload | Out-Null
}

Start-Sleep -Seconds 2
$during = Get-IndexerLag
docker compose start elasticsearch
if ($LASTEXITCODE -ne 0) {
    throw "failed to start Elasticsearch"
}
& (Join-Path $PSScriptRoot "start_local.ps1") -SkipDependencies
if ($LASTEXITCODE -ne 0) {
    throw "failed to restart application processes"
}

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
do {
    Start-Sleep -Seconds 5
    $lag = Get-IndexerLag
} while ($lag -gt 0 -and (Get-Date) -lt $deadline)

$result = [PSCustomObject]@{
    EventsSubmitted = $Events
    LagBefore = $before
    LagDuringOutage = $during
    LagAfterRecovery = $lag
    Recovered = ($lag -eq 0)
}
$result | Format-List
if (-not $result.Recovered -or $during -le 0) {
    throw "fault recovery test failed"
}
