param(
    [string]$ArchivePath,
    [string]$CleanupRoot
)

$ErrorActionPreference = 'Stop'
$tempBase = [IO.Path]::GetFullPath($env:TEMP).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar

function Remove-VerifiedTemporaryRoot([string]$Path) {
    $resolvedRoot = [IO.Path]::GetFullPath($Path)
    if (-not $resolvedRoot.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetFileName($resolvedRoot) -notmatch '^tuba-package-verify-[0-9a-f]{32}$') {
        throw 'Refusing to remove a path outside a unique TUBA package verification directory.'
    }
    if (-not (Test-Path -LiteralPath $resolvedRoot)) { return }
    $cleanupUser = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    Get-ChildItem -LiteralPath $resolvedRoot -File -Recurse -Force | ForEach-Object {
        & icacls.exe $_.FullName /grant:r "${cleanupUser}:F" /C /Q | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "Could not restore temporary file permissions for cleanup: $($_.Name)" }
    }
    Remove-Item -LiteralPath $resolvedRoot -Recurse -Force
}

function Invoke-LocalWebRequest([string]$Uri, [string]$Method = 'GET') {
    $request = [Net.HttpWebRequest]::Create($Uri)
    $request.Method = $Method
    $request.Timeout = 2000
    try {
        $response = [Net.HttpWebResponse]$request.GetResponse()
    } catch [Net.WebException] {
        if ($null -eq $_.Exception.Response) { throw }
        $response = [Net.HttpWebResponse]$_.Exception.Response
    }
    try {
        $reader = New-Object IO.StreamReader($response.GetResponseStream())
        try { $body = $reader.ReadToEnd() } finally { $reader.Dispose() }
        return [pscustomobject]@{
            StatusCode = [int]$response.StatusCode
            CacheControl = [string]$response.Headers['Cache-Control']
            Body = $body
        }
    } finally {
        $response.Dispose()
    }
}

function Get-FreeLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([Net.IPEndPoint]$listener.LocalEndpoint).Port } finally { $listener.Stop() }
}


if ($CleanupRoot) {
    Remove-VerifiedTemporaryRoot $CleanupRoot
    Write-Output 'Verified temporary package directory removed.'
    return
}
if (-not $ArchivePath) { throw 'Provide ArchivePath or a verified CleanupRoot.' }

$archive = (Resolve-Path -LiteralPath $ArchivePath).Path
if (-not (Test-Path -LiteralPath $archive -PathType Leaf) -or [IO.Path]::GetExtension($archive) -ne '.zip') {
    throw 'ArchivePath must name an existing Windows package .zip file.'
}
$sidecar = "$archive.sha256"
if (-not (Test-Path -LiteralPath $sidecar -PathType Leaf)) { throw 'Package SHA-256 sidecar is missing.' }
$expectedHash = ((Get-Content -Raw -LiteralPath $sidecar).Trim() -split '\s+')[0]
$actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash
if ($actualHash -ne $expectedHash) { throw 'Package SHA-256 does not match its sidecar.' }

$root = [IO.Path]::GetFullPath((Join-Path $env:TEMP ("tuba-package-verify-" + [guid]::NewGuid().ToString('N'))))
if (-not $root.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase) -or
    [IO.Path]::GetFileName($root) -notmatch '^tuba-package-verify-[0-9a-f]{32}$') {
    throw 'Refusing to create verification files outside the unique temporary directory.'
}

try {
    $packageRoot = Join-Path $root 'package'
    New-Item -ItemType Directory -Path $packageRoot -Force | Out-Null
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [IO.Compression.ZipFile]::OpenRead($archive)
    try {
        foreach ($entry in $zip.Entries) {
            $name = $entry.FullName.Replace('\', '/')
            $unixMode = ($entry.ExternalAttributes -shr 16) -band 0xF000
            if ($name.StartsWith('/') -or $name -match '(^|/)\.\.(/|$)' -or $name -match '^[A-Za-z]:' -or $unixMode -eq 0xA000) {
                throw 'Package contains an unsafe archive path.'
            }
        }
    } finally {
        $zip.Dispose()
    }
    Expand-Archive -LiteralPath $archive -DestinationPath $packageRoot -Force

    $launcher = Join-Path $packageRoot 'bin\tuba-launcher.exe'
    $manifestPath = Join-Path $packageRoot 'deploy\launcher\tuba-services.windows.example.json'
    $environmentPath = Join-Path $packageRoot 'deploy\launcher\tuba.env.example'
    $webIndex = Join-Path $packageRoot 'web\index.html'
    $webConfig = Join-Path $packageRoot 'web\config.js'
    foreach ($required in @($launcher, $manifestPath, $environmentPath, $webIndex, $webConfig)) {
        if (-not (Test-Path -LiteralPath $required -PathType Leaf)) { throw "Package is missing required file: $required" }
    }
    if (@(Get-ChildItem -LiteralPath (Join-Path $packageRoot 'web') -File -Recurse | Where-Object { $_.Extension -eq '.map' }).Count -gt 0) {
        throw 'Windows package must not include frontend source maps.'
    }

    $verifiedEnvironment = Join-Path $root 'tuba.env'
    Copy-Item -LiteralPath $environmentPath -Destination $verifiedEnvironment
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $verifiedEnvironment /inheritance:r /grant:r "${currentUser}:R" 'SYSTEM:F' 'BUILTIN\Administrators:F' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not restrict the temporary verification environment file ACL.' }

    $manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    $manifest.state_dir = Join-Path $root 'state'
    $manifest.log_dir = Join-Path $root 'logs'
    $manifest.environment_file = $verifiedEnvironment
    foreach ($service in $manifest.services) {
        $binaryName = [IO.Path]::GetFileName([string]$service.command)
        $service.command = Join-Path (Join-Path $packageRoot 'bin') $binaryName
        $service.working_dir = Join-Path $packageRoot 'bin'
    }
    $utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($manifestPath, ($manifest | ConvertTo-Json -Depth 100), $utf8WithoutBom)

    & $launcher validate --manifest $manifestPath
    if ($LASTEXITCODE -ne 0) { throw 'Packaged Windows Launcher rejected its full service manifest.' }

    $ingestService = $manifest.services | Where-Object { $_.name -eq 'ingest' } | Select-Object -First 1
    if ($null -eq $ingestService) { throw 'Packaged manifest is missing the ingest service.' }
    $ingestService.environment.HTTP_LISTEN = ':8080'
    $preflightManifestPath = Join-Path $root 'listener-preflight.json'
    [IO.File]::WriteAllText($preflightManifestPath, ($manifest | ConvertTo-Json -Depth 100), $utf8WithoutBom)
    $rejectOutput = & $launcher validate --manifest $preflightManifestPath 2>&1
    $rejectExitCode = $LASTEXITCODE
    if ($rejectExitCode -eq 0) { throw 'Packaged Launcher accepted a wildcard listener without explicit opt-in.' }

    $ingestService.environment | Add-Member -NotePropertyName TUBA_ALLOW_NON_LOOPBACK_LISTEN -NotePropertyValue 'true' -Force
    [IO.File]::WriteAllText($preflightManifestPath, ($manifest | ConvertTo-Json -Depth 100), $utf8WithoutBom)
    $allowOutput = & $launcher validate --manifest $preflightManifestPath 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Packaged Launcher rejected an explicitly enabled wildcard listener: $($allowOutput -join ' ')" }

    $rawIndexerService = $manifest.services | Where-Object { $_.name -eq 'raw-indexer' } | Select-Object -First 1
    if ($null -eq $rawIndexerService) { throw 'Packaged manifest is missing the raw-indexer service.' }
    $rawIndexerService.environment | Add-Member -NotePropertyName RAW_INDEXER_METRICS_LISTEN -NotePropertyValue ':19095' -Force
    [void]$rawIndexerService.environment.PSObject.Properties.Remove('TUBA_ALLOW_NON_LOOPBACK_LISTEN')
    [IO.File]::WriteAllText($preflightManifestPath, ($manifest | ConvertTo-Json -Depth 100), $utf8WithoutBom)
    $rejectMetricsOutput = & $launcher validate --manifest $preflightManifestPath 2>&1
    $rejectMetricsExitCode = $LASTEXITCODE
    if ($rejectMetricsExitCode -eq 0) { throw 'Packaged Launcher accepted a wildcard metrics listener without explicit opt-in.' }

    $rawIndexerService.environment | Add-Member -NotePropertyName TUBA_ALLOW_NON_LOOPBACK_LISTEN -NotePropertyValue 'true' -Force
    [IO.File]::WriteAllText($preflightManifestPath, ($manifest | ConvertTo-Json -Depth 100), $utf8WithoutBom)
    $allowMetricsOutput = & $launcher validate --manifest $preflightManifestPath 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Packaged Launcher rejected an explicitly enabled wildcard metrics listener: $($allowMetricsOutput -join ' ')" }

    $webPort = Get-FreeLoopbackPort
    $apiPort = Get-FreeLoopbackPort
    $ingestPort = Get-FreeLoopbackPort
    $webEnvironmentNames = @('WEB_LISTEN', 'WEB_ROOT', 'API_UPSTREAM', 'INGEST_UPSTREAM')
    $oldWebEnvironment = @{}
    foreach ($name in $webEnvironmentNames) { $oldWebEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
    $webProcess = $null
    try {
        $env:WEB_LISTEN = "127.0.0.1:$webPort"
        $env:WEB_ROOT = Join-Path $packageRoot 'web'
        $env:API_UPSTREAM = "http://127.0.0.1:$apiPort"
        $env:INGEST_UPSTREAM = "http://127.0.0.1:$ingestPort"
        $webProcess = Start-Process -FilePath (Join-Path $packageRoot 'bin\tuba-web.exe') -WorkingDirectory $packageRoot -PassThru -WindowStyle Hidden
        $baseUrl = "http://127.0.0.1:$webPort"
        $live = $null
        for ($attempt = 0; $attempt -lt 40; $attempt++) {
            if ($webProcess.HasExited) { throw "Packaged tuba-web exited unexpectedly with code $($webProcess.ExitCode)." }
            try { $live = Invoke-LocalWebRequest "$baseUrl/health/live"; break } catch { Start-Sleep -Milliseconds 100 }
        }
        if ($null -eq $live -or $live.StatusCode -ne 200 -or $live.Body.Trim() -ne 'ok') { throw 'Packaged tuba-web did not become live.' }
        $homeResponse = Invoke-LocalWebRequest "$baseUrl/"
        if ($homeResponse.StatusCode -ne 200 -or $homeResponse.Body -notmatch 'id="root"') { throw 'Packaged tuba-web did not serve the built application shell.' }
        $spa = Invoke-LocalWebRequest "$baseUrl/overview"
        if ($spa.StatusCode -ne 200 -or $spa.Body -notmatch 'id="root"') { throw 'Packaged tuba-web SPA fallback failed.' }
        $runtime = Invoke-LocalWebRequest "$baseUrl/config.js"
        if ($runtime.StatusCode -ne 200 -or $runtime.CacheControl -ne 'no-store' -or
            $runtime.Body -notmatch '"basePath":"/"' -or $runtime.Body -match 'oidc' ) {
            throw 'Packaged tuba-web runtime configuration was not served as expected.'
        }
        $internal = Invoke-LocalWebRequest "$baseUrl/api/v1/internal/ingest/beat-events" 'POST'
        if ($internal.StatusCode -ne 404) { throw "Packaged tuba-web exposed internal ingest route (status=$($internal.StatusCode))." }
        $ready = Invoke-LocalWebRequest "$baseUrl/health/ready"
        if ($ready.StatusCode -ne 503) { throw "Packaged tuba-web readiness should fail while upstreams are absent (status=$($ready.StatusCode))." }
    } finally {
        if ($webProcess -and -not $webProcess.HasExited) {
            Stop-Process -Id $webProcess.Id -Force
            $webProcess.WaitForExit()
        }
        foreach ($name in $webEnvironmentNames) {
            [Environment]::SetEnvironmentVariable($name, $oldWebEnvironment[$name], 'Process')
        }
    }
    & python (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\verify_packaged_web_proxy.py') `
        --web-binary (Join-Path $packageRoot 'bin\tuba-web.exe') `
        --web-root (Join-Path $packageRoot 'web')
    if ($LASTEXITCODE -ne 0) { throw 'Packaged tuba-web API/ingest reverse proxy acceptance failed.' }
    Write-Output "Windows package acceptance passed: SHA-256, archive paths, manifest services=$($manifest.services.Count), HTTP and metrics wildcard preflight, static web runtime, and API/ingest proxy fidelity."
} finally {
    Remove-VerifiedTemporaryRoot $root
}
