$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$binary = Join-Path $root "bin\windows-amd64\tuba-collector.exe"
if (-not (Test-Path -LiteralPath $binary)) { throw "Windows Collector binary is missing from this bundle" }
& $binary @args --config (Join-Path $root "config\collector.json")
exit $LASTEXITCODE
