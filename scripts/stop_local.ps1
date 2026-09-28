$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$pidFile = Join-Path $root ".runtime\pids.json"

if (-not (Test-Path -LiteralPath $pidFile)) {
    Write-Host "no local PID file found"
    exit 0
}

$processes = Get-Content -Raw -LiteralPath $pidFile | ConvertFrom-Json
foreach ($process in @($processes | Sort-Object pid -Descending)) {
    $running = Get-Process -Id $process.pid -ErrorAction SilentlyContinue
    if ($running) {
        Stop-Process -Id $process.pid -Force
        Write-Host "stopped $($process.name) pid=$($process.pid)"
    }
}
Remove-Item -LiteralPath $pidFile -Force
