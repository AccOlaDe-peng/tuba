param(
    [Parameter(Mandatory = $true)]
    [string]$Snapshot,
    [string]$ElasticsearchURL = "http://127.0.0.1:9200",
    [string]$Repository = "tuba-backups"
)

$ErrorActionPreference = "Stop"
$body = @{
    include_global_state = $false
    indices = "logs-ueba.*,tuba-v1-*,ueba-anomalies-*"
} | ConvertTo-Json -Compress

$response = Invoke-RestMethod -Method Post `
    -Uri "$ElasticsearchURL/_snapshot/$Repository/$Snapshot/_restore?wait_for_completion=true" `
    -ContentType "application/json" -Body $body -TimeoutSec 600
$response | ConvertTo-Json -Depth 8
