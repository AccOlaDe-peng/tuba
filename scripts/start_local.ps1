param(
    [switch]$SkipDependencies
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root

if (-not (Test-Path -LiteralPath ".env")) {
    Copy-Item -LiteralPath ".env.example" -Destination ".env"
}

Get-Content -LiteralPath ".env" | ForEach-Object {
    if ($_ -match "^([A-Z0-9_]+)=(.*)$") {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process")
    }
}

$nodeRoot = Join-Path $env:LOCALAPPDATA "Programs\node-v24.14.1\node-v24.14.1-win-x64"
if (Test-Path -LiteralPath (Join-Path $nodeRoot "node.exe")) {
    $env:Path = "$nodeRoot;$env:Path"
}

if (-not $SkipDependencies) {
    & (Join-Path $PSScriptRoot "bootstrap_local.ps1")
}

$runtime = Join-Path $root ".runtime"
$logs = Join-Path $runtime "logs"
New-Item -ItemType Directory -Path $logs -Force | Out-Null
$pidFile = Join-Path $runtime "pids.json"

$processes = @(
    @{name = "ingest"; file = "go"; args = @("run", "./cmd/tuba-ingest"); env = @{HTTP_LISTEN = "127.0.0.1:8080"}; probePort = 8080},
    @{name = "normalizer"; file = "go"; args = @("run", "./cmd/tuba-normalizer"); env = @{}; probePort = 0},
    @{name = "raw-indexer"; file = "go"; args = @("run", "./cmd/tuba-raw-indexer"); env = @{RAW_INDEXER_METRICS_LISTEN = "127.0.0.1:19095"}; probePort = 19095},
    @{name = "quarantine-indexer"; file = "go"; args = @("run", "./cmd/tuba-quarantine-indexer"); env = @{}; probePort = 0},
    @{name = "standard-indexer"; file = "go"; args = @("run", "./cmd/tuba-standard-indexer"); env = @{}; probePort = 0},
    @{name = "control-worker"; file = "go"; args = @("run", "./cmd/tuba-control-worker"); env = @{}; probePort = 0},
    @{name = "analysis-sink"; file = "go"; args = @("run", "./cmd/tuba-analysis-sink"); env = @{ANALYSIS_SINK_METRICS_LISTEN = "127.0.0.1:19094"}; probePort = 19094},
    @{name = "api"; file = "go"; args = @("run", "./cmd/tuba-api"); env = @{API_LISTEN = "127.0.0.1:8788"}; probePort = 8788},
    @{name = "analysis-worker"; file = "uv"; args = @("run", "--project", "python", "tuba-analysis-worker"); env = @{METRICS_LISTEN = "127.0.0.1:9093"}; probePort = 9093},
    @{name = "web"; file = "corepack"; args = @("pnpm@12.6.0", "--dir", "web", "dev", "--host", "127.0.0.1"); env = @{}; probePort = 5173}
)

$started = @()
foreach ($item in $processes) {
    $listener = if ($item.probePort -gt 0) { Get-NetTCPConnection -State Listen -LocalAddress 127.0.0.1 -LocalPort $item.probePort -ErrorAction SilentlyContinue | Select-Object -First 1 } else { $null }
    if ($listener) {
        $started += [PSCustomObject]@{
            name = $item.name
            pid = $listener.OwningProcess
            stdout = Join-Path $logs "$($item.name).out.log"
            stderr = Join-Path $logs "$($item.name).err.log"
        }
        Write-Host "$($item.name) already running pid=$($listener.OwningProcess)"
        continue
    }
    foreach ($entry in $item.env.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, "Process")
    }
    $stdout = Join-Path $logs "$($item.name).out.log"
    $stderr = Join-Path $logs "$($item.name).err.log"
    $command = Get-Command $item.file -ErrorAction Stop
    $process = Start-Process -FilePath $command.Source `
        -ArgumentList $item.args `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr `
        -PassThru
    $actualPID = $process.Id
    if ($item.probePort -gt 0) {
        for ($attempt = 0; $attempt -lt 10; $attempt++) {
            Start-Sleep -Milliseconds 500
            $listener = Get-NetTCPConnection -State Listen -LocalAddress 127.0.0.1 -LocalPort $item.probePort -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($listener) {
                $actualPID = $listener.OwningProcess
                break
            }
        }
    } else {
        Start-Sleep -Milliseconds 500
        $process.Refresh()
        if ($process.HasExited) { throw "$($item.name) exited during startup; inspect $stderr" }
    }
    $started += [PSCustomObject]@{
        name = $item.name
        pid = $actualPID
        stdout = $stdout
        stderr = $stderr
    }
    Write-Host "started $($item.name) pid=$actualPID"
}

$started | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $pidFile
Start-Sleep -Seconds 3
Get-NetTCPConnection -State Listen -LocalAddress 127.0.0.1 -LocalPort 5173,8080,8788,9093,9094,9095 -ErrorAction SilentlyContinue |
    Sort-Object LocalPort |
    Select-Object LocalAddress,LocalPort,OwningProcess |
    Format-Table -AutoSize
