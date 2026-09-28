$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'lib\tuba_release_windows.ps1')

$tempBase = [IO.Path]::GetFullPath($env:TEMP).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$root = [IO.Path]::GetFullPath((Join-Path $env:TEMP ("tuba-release-test-" + [guid]::NewGuid().ToString('N'))))
if (-not $root.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase) -or
    [IO.Path]::GetFileName($root) -notmatch '^tuba-release-test-[0-9a-f]{32}$') {
    throw 'Refusing to create test files outside the uniquely named temporary directory.'
}

$releaseRoot = Join-Path $root 'releases'
$current = Join-Path $root 'current'
$previous = Join-Path $root 'previous'
$v1 = Join-Path $releaseRoot 'v1'
$v2 = Join-Path $releaseRoot 'v2'
New-Item -ItemType Directory -Path $v1 -Force | Out-Null
New-Item -ItemType Directory -Path $v2 -Force | Out-Null
Set-Content -LiteralPath (Join-Path $v1 'version.txt') -Value 'v1'
Set-Content -LiteralPath (Join-Path $v2 'version.txt') -Value 'v2'

$manifestCurrent = Join-Path $root 'Program Files\TUBA\current'
$manifestRelease = Join-Path $root 'Program Files\TUBA\releases\v3'
$manifestForValidation = [pscustomobject]@{
    services = @([pscustomobject]@{
        command = Join-Path $manifestCurrent 'bin\tuba-api.exe'
        working_dir = $manifestCurrent
        environment = @{ DATABASE_URL = '${DATABASE_URL}' }
    })
}
$manifestForValidation = Set-TubaManifestReleasePaths -Manifest $manifestForValidation -CurrentPath $manifestCurrent -ReleasePath $manifestRelease
if ($manifestForValidation.services[0].command -ne (Join-Path $manifestRelease 'bin\tuba-api.exe') -or
    $manifestForValidation.services[0].working_dir -ne $manifestRelease -or
    $manifestForValidation.services[0].environment.DATABASE_URL -ne '${DATABASE_URL}') {
    throw 'Validation manifest did not replace only the current release paths.'
}

try {
    Set-TubaCurrentRelease -CurrentPath $current -PreviousPath $previous -ReleasePath $v1
    if ((Get-Content -LiteralPath (Join-Path $current 'version.txt') -Raw).Trim() -ne 'v1') {
        throw 'Initial activation did not point current at v1.'
    }
    if (Test-Path -LiteralPath $previous) { throw 'Initial activation unexpectedly created previous.' }

    Set-TubaCurrentRelease -CurrentPath $current -PreviousPath $previous -ReleasePath $v2
    if ((Get-Content -LiteralPath (Join-Path $current 'version.txt') -Raw).Trim() -ne 'v2') {
        throw 'Upgrade did not point current at v2.'
    }
    if ((Get-Content -LiteralPath (Join-Path $previous 'version.txt') -Raw).Trim() -ne 'v1') {
        throw 'Upgrade did not retain v1 as the previous release.'
    }

    Switch-TubaCurrentAndPrevious -CurrentPath $current -PreviousPath $previous
    if ((Get-Content -LiteralPath (Join-Path $current 'version.txt') -Raw).Trim() -ne 'v1' -or
        (Get-Content -LiteralPath (Join-Path $previous 'version.txt') -Raw).Trim() -ne 'v2') {
        throw 'Rollback did not exchange current and previous releases.'
    }

    $failedAsExpected = $false
    try {
        Set-TubaCurrentRelease -CurrentPath $current -PreviousPath $previous -ReleasePath (Join-Path $releaseRoot 'missing')
    } catch {
        $failedAsExpected = $true
    }
    if (-not $failedAsExpected) { throw 'Activation accepted a missing release directory.' }
    if ((Get-Content -LiteralPath (Join-Path $current 'version.txt') -Raw).Trim() -ne 'v1' -or
        (Get-Content -LiteralPath (Join-Path $previous 'version.txt') -Raw).Trim() -ne 'v2') {
        throw 'Rejected activation modified current or previous release links.'
    }

    Write-Output 'Windows release activation checks passed: initial install, upgrade, rollback, retained failed release, and failed preflight preservation.'
} finally {
    foreach ($link in @($current, $previous)) {
        if (Test-Path -LiteralPath $link) {
            if (-not (Test-TubaJunction $link)) { throw "Refusing to remove a non-junction test path: $link" }
            Remove-Item -LiteralPath $link -Force
        }
    }
    if (Test-Path -LiteralPath $root) {
        $resolvedRoot = [IO.Path]::GetFullPath($root)
        if (-not $resolvedRoot.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase) -or
            [IO.Path]::GetFileName($resolvedRoot) -notmatch '^tuba-release-test-[0-9a-f]{32}$') {
            throw 'Refusing to remove a path outside the uniquely named temporary directory.'
        }
        Remove-Item -LiteralPath $resolvedRoot -Recurse -Force
    }
}

