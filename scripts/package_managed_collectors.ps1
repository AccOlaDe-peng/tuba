param(
    [ValidatePattern('^[A-Za-z0-9._-]+$')][string]$Tag = "dev",
    [string]$ArtifactCache = "",
    [string]$SignKeyFile = "",
    [string]$SignKeyID = "",
    [int]$StateFormatVersion = 1,
    [int]$StateFormatMin = 1,
    [switch]$Offline
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $root "deploy\components\manifest.v1.json"
$elasticLicensePath = Join-Path $root "deploy\components\ELASTIC-LICENSE-2.0.txt"
$distributionRoot = Join-Path $root "dist\managed-collectors"
$outputPath = Join-Path $distributionRoot "tuba-managed-collectors-$Tag.zip"
$stage = Join-Path $distributionRoot ("staging-" + [guid]::NewGuid().ToString("N"))
$resolvedDistributionRoot = [System.IO.Path]::GetFullPath($distributionRoot).TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
$resolvedStage = [System.IO.Path]::GetFullPath($stage)
if (-not $resolvedStage.StartsWith($resolvedDistributionRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to use staging path outside distribution directory"
}

if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) { throw "Component manifest is missing" }
if (-not (Test-Path -LiteralPath $elasticLicensePath -PathType Leaf)) { throw "Elastic License 2.0 text is missing: $elasticLicensePath" }
if (($SignKeyFile -eq "") -ne ($SignKeyID -eq "")) { throw "-SignKeyFile and -SignKeyID must be given together" }
if ($SignKeyFile -ne "") {
    if (-not (Test-Path -LiteralPath $SignKeyFile -PathType Leaf)) { throw "Signing key file is missing: $SignKeyFile" }
    if ($StateFormatVersion -lt 1 -or $StateFormatMin -lt 1 -or $StateFormatMin -gt $StateFormatVersion) {
        throw "Invalid state format range"
    }
}
if (Test-Path -LiteralPath $outputPath) { throw "Output already exists: $outputPath" }
if ($ArtifactCache -eq "") { $ArtifactCache = Join-Path $root "dist\component-cache" }
$ArtifactCache = [System.IO.Path]::GetFullPath($ArtifactCache)

$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
$null = New-Item -ItemType Directory -Force -Path $distributionRoot,$stage,$ArtifactCache
$manifest.artifacts | ForEach-Object {
    $artifact = $_
    $fileName = [System.IO.Path]::GetFileName(([uri]$artifact.url).AbsolutePath)
    $archive = Join-Path $ArtifactCache $fileName
    $extractPath = Join-Path $stage ("extract-" + [guid]::NewGuid().ToString("N"))
    $componentPath = Join-Path $stage (Join-Path $artifact.component (Join-Path $artifact.version ("$($artifact.os)-$($artifact.architecture)")))
    $null = New-Item -ItemType Directory -Force -Path $extractPath,$componentPath
    try {
        if (-not (Test-Path -LiteralPath $archive -PathType Leaf)) {
            if ($Offline) { throw "Artifact is not present in offline cache: $fileName" }
            Invoke-WebRequest -Uri $artifact.url -OutFile $archive
        }
        $actualHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA512).Hash.ToLowerInvariant()
        if ($actualHash -ne $artifact.sha512.ToLowerInvariant()) {
            throw "SHA-512 mismatch for $fileName; expected artifact was not packaged"
        }
        if ($artifact.format -eq "zip") {
            Expand-Archive -LiteralPath $archive -DestinationPath $extractPath
        } elseif ($artifact.format -eq "tar.gz") {
            & (Join-Path $env:SystemRoot "System32\tar.exe") -xzf $archive -C $extractPath
            if ($LASTEXITCODE -ne 0) { throw "Could not extract $fileName" }
        } else {
            throw "Unsupported archive format: $($artifact.format)"
        }
        $roots = @(Get-ChildItem -LiteralPath $extractPath -Directory)
        if ($roots.Count -ne 1) { throw "Unexpected archive layout for $fileName" }
        foreach ($required in @("LICENSE.txt", "NOTICE.txt")) {
            if (-not (Test-Path -LiteralPath (Join-Path $roots[0].FullName $required) -PathType Leaf)) {
                throw "Upstream $required is missing inside $fileName; redistribution requires license and notice files"
            }
        }
        Copy-Item -LiteralPath $roots[0].FullName -Destination $componentPath -Recurse
        if ($SignKeyFile -ne "") {
            # Sign the staged component directory for the managed supervisor
            # (internal/component manifest format). The private key stays on
            # the operator machine; only the signed manifest.json ships.
            & go run ./cmd/tuba-component sign --package $componentPath --component $artifact.component --version $artifact.version `
                --os $artifact.os --arch $artifact.architecture --format $StateFormatVersion --format-min $StateFormatMin `
                --key-file $SignKeyFile --key-id $SignKeyID
            if ($LASTEXITCODE -ne 0) { throw "Signing failed for $($artifact.component) $($artifact.version)" }
        }
    } finally {
        if (Test-Path -LiteralPath $extractPath) { Remove-Item -LiteralPath $extractPath -Recurse -Force }
    }
}

Copy-Item -LiteralPath $manifestPath -Destination (Join-Path $stage "manifest.v1.json")
Copy-Item -LiteralPath $elasticLicensePath -Destination (Join-Path $stage "ELASTIC-LICENSE-2.0.txt")
$readme = @"
TUBA managed collection components $Tag

This archive contains the pinned Filebeat and Winlogbeat distributions with their upstream license and notice files. It does not contain the TUBA Management Agent or production-ready source configuration. Do not run a Beat directly from this archive until COL-03/08 configuration and topic bindings are installed.

Component versions and SHA-512 values are in manifest.v1.json. The packaging script verifies each archive before extraction and confirms each upstream package retains its LICENSE.txt and NOTICE.txt. The upstream distributions are governed by the Elastic License 2.0; a copy of the license text is included at the root of this archive as ELASTIC-LICENSE-2.0.txt. When built with -SignKeyFile/-SignKeyID, each component directory additionally carries a manifest.json signed for the TUBA managed component supervisor (tuba-component verify).
"@
Set-Content -LiteralPath (Join-Path $stage "README.txt") -Value $readme -Encoding utf8
$null = New-Item -ItemType Directory -Force -Path $distributionRoot
Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $outputPath
Remove-Item -LiteralPath $stage -Recurse -Force
Write-Host "Created managed collector component bundle: $outputPath"
