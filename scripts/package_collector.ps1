param([ValidatePattern('^[A-Za-z0-9._-]+$')][string]$Tag = "dev")

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$stage = Join-Path $root "dist\collector\tuba-collector-$Tag"
$linuxBin = Join-Path $stage "bin\linux-amd64"
$windowsBin = Join-Path $stage "bin\windows-amd64"
$configDir = Join-Path $stage "config"
if (Test-Path -LiteralPath $stage) { throw "Package staging path already exists: $stage" }
New-Item -ItemType Directory -Force -Path $linuxBin,$windowsBin,$configDir | Out-Null

Push-Location $root
$previousGOOS = $env:GOOS
$previousGOARCH = $env:GOARCH
$previousCGO = $env:CGO_ENABLED
try {
    $env:CGO_ENABLED = "0"
    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    go build -trimpath -ldflags="-s -w" -o (Join-Path $linuxBin "tuba-collector") ./cmd/tuba-collector
    if ($LASTEXITCODE -ne 0) { throw "Linux Collector build failed" }

    $env:GOOS = "windows"
    $env:GOARCH = "amd64"
    go build -trimpath -ldflags="-s -w" -o (Join-Path $windowsBin "tuba-collector.exe") ./cmd/tuba-collector
    if ($LASTEXITCODE -ne 0) { throw "Windows Collector build failed" }
} finally {
    if ($null -eq $previousGOOS) { Remove-Item Env:GOOS -ErrorAction SilentlyContinue } else { $env:GOOS = $previousGOOS }
    if ($null -eq $previousGOARCH) { Remove-Item Env:GOARCH -ErrorAction SilentlyContinue } else { $env:GOARCH = $previousGOARCH }
    if ($null -eq $previousCGO) { Remove-Item Env:CGO_ENABLED -ErrorAction SilentlyContinue } else { $env:CGO_ENABLED = $previousCGO }
    Pop-Location
}

Copy-Item -LiteralPath (Join-Path $root "deploy\collector\tuba-collector.example.json") -Destination (Join-Path $configDir "collector.json")
Copy-Item -LiteralPath (Join-Path $root "deploy\collector\tuba-collector.ps1") -Destination $stage
Copy-Item -LiteralPath (Join-Path $root "deploy\collector\tuba-collector.sh") -Destination $stage
Copy-Item -LiteralPath (Join-Path $root "deploy\collector\README.md") -Destination $stage
$archive = "$stage.zip"
Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $archive
Write-Host "Created unified Collector bundle: $archive"
