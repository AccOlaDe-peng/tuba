param(
    [string]$InstallRoot = (Join-Path $env:ProgramFiles 'TUBA'),
    [string]$DataRoot = (Join-Path $env:ProgramData 'TUBA')
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'lib\tuba_release_windows.ps1')

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Run this rollback from an elevated PowerShell session.'
}

$currentPath = Join-Path $InstallRoot 'current'
$previousPath = Join-Path $InstallRoot 'previous'
$manifestPath = Join-Path $DataRoot 'config\tuba-services.json'
if (-not (Test-TubaJunction $currentPath) -or -not (Test-TubaJunction $previousPath)) {
    throw 'Rollback requires both current and previous to be directory junctions.'
}
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw "Launcher manifest not found: $manifestPath"
}

$currentLauncher = Join-Path $currentPath 'bin\tuba-launcher.exe'
$previousLauncher = Join-Path $previousPath 'bin\tuba-launcher.exe'
if (-not (Test-Path -LiteralPath $currentLauncher -PathType Leaf) -or
    -not (Test-Path -LiteralPath $previousLauncher -PathType Leaf)) {
    throw 'Current or previous release does not contain tuba-launcher.exe.'
}

& $currentLauncher stop --manifest $manifestPath --timeout 30s
if ($LASTEXITCODE -ne 0) { throw 'Current TUBA processes did not stop; rollback was not started.' }

try {
    Switch-TubaCurrentAndPrevious -CurrentPath $currentPath -PreviousPath $previousPath
} catch {
    & $currentLauncher start --manifest $manifestPath
    throw
}

$rollbackLauncher = Join-Path $currentPath 'bin\tuba-launcher.exe'
& $rollbackLauncher start --manifest $manifestPath
if ($LASTEXITCODE -ne 0) {
    throw "Previous release is active at $currentPath but could not start. Inspect the Launcher log before retrying."
}

Write-Output "Rollback completed. Current points to the previous release; the former release is retained at $previousPath."

