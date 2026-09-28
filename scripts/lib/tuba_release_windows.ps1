function Test-TubaJunction([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $item = Get-Item -LiteralPath $Path -Force
    return (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -and
        ($item.Attributes -band [IO.FileAttributes]::Directory) -ne 0)
}

function Set-TubaManifestReleasePaths {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]$Manifest,
        [Parameter(Mandatory = $true)][string]$CurrentPath,
        [Parameter(Mandatory = $true)][string]$ReleasePath
    )

    $currentPrefix = [IO.Path]::GetFullPath($CurrentPath).TrimEnd([IO.Path]::DirectorySeparatorChar)
    $releasePrefix = [IO.Path]::GetFullPath($ReleasePath).TrimEnd([IO.Path]::DirectorySeparatorChar)
    foreach ($service in $Manifest.services) {
        foreach ($field in @('command', 'working_dir')) {
            $property = $service.PSObject.Properties[$field]
            if ($null -eq $property -or -not $property.Value) { continue }
            $configuredPath = [string]$property.Value
            if ($configuredPath.Equals($currentPrefix, [StringComparison]::OrdinalIgnoreCase)) {
                $null = ($property.Value = $releasePrefix)
            } elseif ($configuredPath.StartsWith($currentPrefix + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
                $null = ($property.Value = (Join-Path $releasePrefix $configuredPath.Substring($currentPrefix.Length + 1)))
            }
        }
    }
    return $Manifest
}

function Set-TubaCurrentRelease {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$CurrentPath,
        [Parameter(Mandatory = $true)][string]$PreviousPath,
        [Parameter(Mandatory = $true)][string]$ReleasePath
    )

    $currentFull = [IO.Path]::GetFullPath($CurrentPath)
    $previousFull = [IO.Path]::GetFullPath($PreviousPath)
    $releaseFull = [IO.Path]::GetFullPath($ReleasePath)
    if ([IO.Path]::GetDirectoryName($currentFull) -ne [IO.Path]::GetDirectoryName($previousFull)) {
        throw 'Current and previous release links must share a parent directory.'
    }
    if (-not (Test-Path -LiteralPath $releaseFull -PathType Container)) {
        throw "Release directory does not exist: $releaseFull"
    }
    $hasCurrent = Test-Path -LiteralPath $currentFull
    $hasPrevious = Test-Path -LiteralPath $previousFull
    if ($hasCurrent -and -not (Test-TubaJunction $currentFull)) {
        throw "Refusing to replace a current path that is not a directory junction: $currentFull"
    }
    if ($hasPrevious -and -not (Test-TubaJunction $previousFull)) {
        throw "Refusing to replace a previous path that is not a directory junction: $previousFull"
    }
    if (-not $hasCurrent -and $hasPrevious) {
        throw "Current is missing while a previous release link exists; restore it before installing another release."
    }

    $nextPath = "$currentFull.next-$([guid]::NewGuid().ToString('N'))"
    New-Item -ItemType Junction -Path $nextPath -Target $releaseFull | Out-Null
    $movedCurrent = $false
    try {
        if (Test-Path -LiteralPath $previousFull) {
            Remove-Item -LiteralPath $previousFull -Force
        }
        if (Test-Path -LiteralPath $currentFull) {
            Move-Item -LiteralPath $currentFull -Destination $previousFull
            $movedCurrent = $true
        }
        Move-Item -LiteralPath $nextPath -Destination $currentFull
    } catch {
        if (Test-Path -LiteralPath $nextPath) { Remove-Item -LiteralPath $nextPath -Force }
        if ($movedCurrent -and (Test-Path -LiteralPath $previousFull) -and -not (Test-Path -LiteralPath $currentFull)) {
            Move-Item -LiteralPath $previousFull -Destination $currentFull
        }
        throw
    }
}

function Switch-TubaCurrentAndPrevious {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)][string]$CurrentPath,
        [Parameter(Mandatory = $true)][string]$PreviousPath
    )

    $currentFull = [IO.Path]::GetFullPath($CurrentPath)
    $previousFull = [IO.Path]::GetFullPath($PreviousPath)
    if ([IO.Path]::GetDirectoryName($currentFull) -ne [IO.Path]::GetDirectoryName($previousFull)) {
        throw 'Current and previous release links must share a parent directory.'
    }
    if (-not (Test-TubaJunction $currentFull) -or -not (Test-TubaJunction $previousFull)) {
        throw 'Rollback requires current and previous to be directory junctions.'
    }

    $failedPath = Join-Path ([IO.Path]::GetDirectoryName($currentFull)) ('.rollback-current-' + [guid]::NewGuid().ToString('N'))
    Move-Item -LiteralPath $currentFull -Destination $failedPath
    try {
        Move-Item -LiteralPath $previousFull -Destination $currentFull
        Move-Item -LiteralPath $failedPath -Destination $previousFull
    } catch {
        if ((Test-Path -LiteralPath $currentFull) -and (Test-Path -LiteralPath $failedPath) -and
            -not (Test-Path -LiteralPath $previousFull)) {
            Move-Item -LiteralPath $currentFull -Destination $previousFull
        }
        if (-not (Test-Path -LiteralPath $currentFull) -and (Test-Path -LiteralPath $failedPath)) {
            Move-Item -LiteralPath $failedPath -Destination $currentFull
        }
        throw
    }
}

