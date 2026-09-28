param(
    [string]$ElasticsearchURL = "http://127.0.0.1:9200",
    [string]$Repository = "tuba-backups",
    [string]$SnapshotPrefix = "tuba"
)

$ErrorActionPreference = "Stop"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$snapshot = "$SnapshotPrefix-$stamp"
$body = @{
    type = "fs"
    settings = @{
        location = "/usr/share/elasticsearch/backups"
        compress = $true
    }
} | ConvertTo-Json -Depth 5 -Compress

try {
    Invoke-RestMethod -Method Put -Uri "$ElasticsearchURL/_snapshot/$Repository" `
        -ContentType "application/json" -Body $body | Out-Null
} catch {
    if ($_.Exception.Response.StatusCode.value__ -ne 400) {
        throw
    }
}
$response = Invoke-RestMethod -Method Put `
    -Uri "$ElasticsearchURL/_snapshot/$Repository/$snapshot?wait_for_completion=true" `
    -TimeoutSec 300
[PSCustomObject]@{
    Snapshot = $snapshot
    Accepted = $response.accepted
    Repository = $Repository
} | Format-List
