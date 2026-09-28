param(
    [Parameter(Mandatory = $true)][string]$Version,
    [Parameter(Mandatory = $true)][string]$ArchivePath,
    [Parameter(Mandatory = $true)][string]$RunAs,
    [switch]$CreateLocalAccount
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "lib\tuba_release_windows.ps1")
if ($Version -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$') { throw "Invalid version identifier." }
if (-not (Test-Path -LiteralPath $ArchivePath -PathType Leaf)) { throw "Package archive not found." }
$ArchivePath = (Resolve-Path -LiteralPath $ArchivePath).Path
$hashPath = "$ArchivePath.sha256"
if (-not (Test-Path -LiteralPath $hashPath -PathType Leaf)) { throw "Package .sha256 file not found." }
$expectedHash = ((Get-Content -Raw -LiteralPath $hashPath).Trim() -split '\s+')[0]
$actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $ArchivePath).Hash
if ($actualHash -ne $expectedHash) { throw "Package SHA-256 mismatch." }

$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$principalContext = New-Object System.Security.Principal.WindowsPrincipal($identity)
if (-not $principalContext.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this installer from an elevated PowerShell session."
}
if ($CreateLocalAccount) {
    if ($RunAs.Contains('\')) { throw "-CreateLocalAccount requires a local account name, not a domain account." }
    if (-not (Get-LocalUser -Name $RunAs -ErrorAction SilentlyContinue)) {
        $servicePassword = Read-Host "Set a password for the dedicated TUBA account $RunAs" -AsSecureString
        New-LocalUser -Name $RunAs -Password $servicePassword -AccountNeverExpires -PasswordNeverExpires | Out-Null
    }
}
$principal = New-Object System.Security.Principal.NTAccount($RunAs)
$null = $principal.Translate([System.Security.Principal.SecurityIdentifier])

$installRoot = Join-Path $env:ProgramFiles "TUBA"
$releaseRoot = Join-Path $installRoot "releases"
$releasePath = Join-Path $releaseRoot $Version
$currentPath = Join-Path $installRoot "current"
$previousPath = Join-Path $installRoot "previous"
$dataRoot = Join-Path $env:ProgramData "TUBA"
$configRoot = Join-Path $dataRoot "config"
$stateRoot = Join-Path $dataRoot "state"
$logRoot = Join-Path $dataRoot "logs"
if (Test-Path -LiteralPath $releasePath) { throw "Release already exists: $releasePath" }

function Set-PrivateDirectoryAcl([string]$Path) {
    New-Item -ItemType Directory -Path $Path -Force | Out-Null
    & icacls.exe $Path /inheritance:r /grant:r `
        "${RunAs}:(OI)(CI)F" "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Could not restrict directory ACL: $Path" }
}

$stage = Join-Path $releaseRoot (".stage-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $releaseRoot -Force | Out-Null
New-Item -ItemType Directory -Path $stage | Out-Null
try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead($ArchivePath)
    try {
        $stagePrefix = [IO.Path]::GetFullPath($stage).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
        foreach ($entry in $zip.Entries) {
            $name = $entry.FullName.Replace('\', '/')
            $unixMode = ($entry.ExternalAttributes -shr 16) -band 0xF000
            if ($name.StartsWith('/') -or $name -match '(^|/)\.\.(/|$)' -or $name -match '^[A-Za-z]:' -or $unixMode -eq 0xA000) {
                throw "Package contains an unsafe path."
            }
            $destination = [IO.Path]::GetFullPath((Join-Path $stage ($name.Replace('/', [IO.Path]::DirectorySeparatorChar))))
            if (-not $destination.StartsWith($stagePrefix, [StringComparison]::OrdinalIgnoreCase) -and $destination -ne $stagePrefix.TrimEnd([IO.Path]::DirectorySeparatorChar)) {
                throw "Package entry resolves outside the staging directory."
            }
        }
    } finally {
        $zip.Dispose()
    }
    Expand-Archive -LiteralPath $ArchivePath -DestinationPath $stage -Force
    if (-not (Test-Path -LiteralPath (Join-Path $stage 'bin\tuba-launcher.exe') -PathType Leaf) -or
        -not (Test-Path -LiteralPath (Join-Path $stage 'bin\tuba-api.exe') -PathType Leaf)) {
        throw "Package is missing required TUBA binaries."
    }
    Move-Item -LiteralPath $stage -Destination $releasePath
} finally {
    if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
}

Set-PrivateDirectoryAcl $dataRoot
Set-PrivateDirectoryAcl $configRoot
Set-PrivateDirectoryAcl $stateRoot
Set-PrivateDirectoryAcl (Join-Path $stateRoot 'launcher')
Set-PrivateDirectoryAcl $logRoot

# Apply read/execute permissions before stopping the current release. A failed
# ACL update must leave the active release running.
& icacls.exe $releasePath /inheritance:r /grant:r `
    "${RunAs}:(OI)(CI)RX" "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F" /T /C | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Could not restrict release tree ACL." }

$manifestPath = Join-Path $configRoot 'tuba-services.json'
$environmentPath = Join-Path $configRoot 'tuba.env'
$sourceAdapterConfigPath = Join-Path $configRoot 'source-adapter.json'
$migratedManifestPath = $null
if (-not (Test-Path -LiteralPath $manifestPath)) {
    Copy-Item -LiteralPath (Join-Path $releasePath 'deploy\launcher\tuba-services.windows.example.json') -Destination $manifestPath
}
if (-not (Test-Path -LiteralPath $environmentPath)) {
    Copy-Item -LiteralPath (Join-Path $releasePath 'deploy\launcher\tuba.env.example') -Destination $environmentPath
}
if (-not (Test-Path -LiteralPath $sourceAdapterConfigPath)) {
    Copy-Item -LiteralPath (Join-Path $releasePath 'deploy\launcher\source-adapter.example.json') -Destination $sourceAdapterConfigPath
}

& icacls.exe $manifestPath /inheritance:r /grant:r "${RunAs}:R" "SYSTEM:F" "BUILTIN\Administrators:F" | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Could not restrict launcher manifest ACL." }
& icacls.exe $sourceAdapterConfigPath /inheritance:r /grant:r "${RunAs}:R" "SYSTEM:F" "BUILTIN\Administrators:F" | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Could not restrict source adapter config ACL." }
& icacls.exe $environmentPath /inheritance:r /grant:r "${RunAs}:F" "SYSTEM:F" "BUILTIN\Administrators:F" | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Could not restrict environment file ACL." }

if (Test-Path -LiteralPath $currentPath) {
    if (-not (Test-TubaJunction $currentPath)) {
        throw "Refusing to replace current path because it is not a directory junction."
    }
    $newLauncher = Join-Path $releasePath 'bin\tuba-launcher.exe'
    $migratedManifestPath = Join-Path $configRoot ('.tuba-services.' + $Version + '.candidate.json')
    if (Test-Path -LiteralPath $migratedManifestPath) { throw "Manifest migration candidate already exists: $migratedManifestPath" }
    & $newLauncher merge-manifest `
        --manifest $manifestPath `
        --previous-defaults (Join-Path $currentPath 'deploy\launcher\tuba-services.windows.example.json') `
        --defaults (Join-Path $releasePath 'deploy\launcher\tuba-services.windows.example.json') `
        --output $migratedManifestPath `
        --current-path $currentPath `
        --release-path $releasePath
    if ($LASTEXITCODE -ne 0) { throw "The new release could not migrate and validate the launcher manifest; the current release was not stopped." }
    & icacls.exe $migratedManifestPath /inheritance:r /grant:r "${RunAs}:R" "SYSTEM:F" "BUILTIN\Administrators:F" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Remove-Item -LiteralPath $migratedManifestPath -Force
        throw "Could not restrict candidate manifest ACL."
    }
    $validationManifestPath = Join-Path $configRoot ('.validate-' + [guid]::NewGuid().ToString('N') + '.json')
    try {
        $validationManifest = Get-Content -Raw -LiteralPath $migratedManifestPath | ConvertFrom-Json
        $validationManifest = Set-TubaManifestReleasePaths -Manifest $validationManifest -CurrentPath $currentPath -ReleasePath $releasePath
        $utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
        [IO.File]::WriteAllText($validationManifestPath, ($validationManifest | ConvertTo-Json -Depth 100), $utf8WithoutBom)
        & icacls.exe $validationManifestPath /inheritance:r /grant:r "${RunAs}:R" "SYSTEM:F" "BUILTIN\Administrators:F" | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Could not restrict temporary validation manifest ACL." }
        & $newLauncher validate --manifest $validationManifestPath
        if ($LASTEXITCODE -ne 0) {
            Remove-Item -LiteralPath $migratedManifestPath -Force
            throw "The new release failed manifest or environment validation; the current release was not stopped."
        }
    } catch {
        if (Test-Path -LiteralPath $migratedManifestPath) { Remove-Item -LiteralPath $migratedManifestPath -Force }
        throw
    } finally {
        if (Test-Path -LiteralPath $validationManifestPath) { Remove-Item -LiteralPath $validationManifestPath -Force }
    }
    $oldLauncher = Join-Path $currentPath 'bin\tuba-launcher.exe'
    if ((Test-Path -LiteralPath $oldLauncher) -and (Test-Path -LiteralPath $manifestPath)) {
        & $oldLauncher stop --manifest $manifestPath --timeout 30s
        if ($LASTEXITCODE -ne 0) {
            Remove-Item -LiteralPath $migratedManifestPath -Force
            throw "The current TUBA processes did not stop; release was installed but not activated."
        }
    }
}

try {
    Set-TubaCurrentRelease -CurrentPath $currentPath -PreviousPath $previousPath -ReleasePath $releasePath
    if ($migratedManifestPath) {
        $manifestBackupPath = "$manifestPath.pre-$Version"
        if (Test-Path -LiteralPath $manifestBackupPath) { throw "Refusing to overwrite manifest backup: $manifestBackupPath" }
        [IO.File]::Replace($migratedManifestPath, $manifestPath, $manifestBackupPath, $true)
    }
} catch {
    if ($migratedManifestPath -and (Test-Path -LiteralPath $migratedManifestPath)) {
        Remove-Item -LiteralPath $migratedManifestPath -Force
    }
    if ((Test-Path -LiteralPath $currentPath) -and (Test-Path -LiteralPath $previousPath)) {
        try { Switch-TubaCurrentAndPrevious -CurrentPath $currentPath -PreviousPath $previousPath } catch { Write-Warning "Could not restore the previous release link: $_" }
    }
    if (Test-Path -LiteralPath $currentPath) {
        $restoredLauncher = Join-Path $currentPath 'bin\tuba-launcher.exe'
        if ((Test-Path -LiteralPath $restoredLauncher) -and (Test-Path -LiteralPath $manifestPath)) {
            & $restoredLauncher start --manifest $manifestPath
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Previous release was restored but could not be started. Use $currentPath and $manifestPath to recover manually."
            }
        }
    }
    throw
}
Write-Output "Installed TUBA $Version under $releasePath."
if (Test-Path -LiteralPath $previousPath) { Write-Output "Previous release link retained at $previousPath for rollback." }
Write-Output "Review $manifestPath and replace placeholders in $environmentPath."
Write-Output "Run the Launcher as $RunAs; no Windows Service is registered."
