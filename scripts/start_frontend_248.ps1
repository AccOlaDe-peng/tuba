param(
    [int]$Port = 5173,
    [string]$Gateway = 'http://10.6.68.248:8088'
)

$ErrorActionPreference = 'Stop'
$previousGateway = $env:TUBA_DEV_GATEWAY
Push-Location (Join-Path $PSScriptRoot '..\web')
try {
    $env:TUBA_DEV_GATEWAY = $Gateway
    Write-Host "Frontend http://127.0.0.1:$Port connects to $Gateway (shared environment)."
    & node node_modules/vite/bin/vite.js --host 127.0.0.1 --port $Port --strictPort
} finally {
    $env:TUBA_DEV_GATEWAY = $previousGateway
    Pop-Location
}
