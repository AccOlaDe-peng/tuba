param(
    [Parameter(Mandatory = $true)][string]$Version,
    [ValidateSet("windows", "linux")][string]$GOOS = "windows",
    [ValidateSet("amd64", "arm64")][string]$GOARCH = "amd64",
    [string]$OutputDirectory = "dist"
)

$ErrorActionPreference = "Stop"
if ($Version -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$') { throw "Invalid version identifier." }
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root
$webSource = Join-Path $root "web"
Push-Location -LiteralPath $webSource
try {
    & pnpm run build
    if ($LASTEXITCODE -ne 0) { throw "Frontend build failed." }
} finally {
    Pop-Location
}
$outputPath = if ([IO.Path]::IsPathRooted($OutputDirectory)) { $OutputDirectory } else { Join-Path $root $OutputDirectory }
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$stage = Join-Path $outputPath (".tuba-package-" + [guid]::NewGuid().ToString("N"))
$binDirectory = Join-Path $stage "bin"
$launcherDirectory = Join-Path $stage "deploy\launcher"
$contractDirectory = Join-Path $stage "contracts\events"
$staticDirectory = Join-Path $stage "web"
New-Item -ItemType Directory -Path $binDirectory,$launcherDirectory,$contractDirectory,$staticDirectory -Force | Out-Null

$commands = @(
    "tuba-launcher",
    "tuba-agent",
    "tuba-component",
    "tuba-web",
    "tuba-api",
    "tuba-ingest",
    "tuba-normalizer",
    "tuba-raw-indexer",
    "tuba-quarantine-indexer",
    "tuba-standard-indexer",
    "tuba-control-worker",
    "tuba-analysis-sink",
    "tuba-source-adapter",
    "tuba-source-topic-admin",
    "tuba-topic-admin",
    "tuba-kafka-security-admin"
)
$oldGOOS = $env:GOOS
$oldGOARCH = $env:GOARCH
try {
    $env:GOOS = $GOOS
    $env:GOARCH = $GOARCH
    foreach ($name in $commands) {
        $extension = if ($GOOS -eq "windows") { ".exe" } else { "" }
        $destination = Join-Path $binDirectory ($name + $extension)
        & go build -trimpath -o $destination ("./cmd/" + $name)
        if ($LASTEXITCODE -ne 0) { throw "Build failed for $name ($GOOS/$GOARCH)." }
    }
} finally {
    $env:GOOS = $oldGOOS
    $env:GOARCH = $oldGOARCH
}

$manifestName = if ($GOOS -eq "windows") { "tuba-services.windows.example.json" } else { "tuba-services.linux.example.json" }
Copy-Item -LiteralPath (Join-Path $root "deploy\launcher\$manifestName") -Destination (Join-Path $launcherDirectory $manifestName)
Copy-Item -LiteralPath (Join-Path $root "deploy\launcher\tuba.env.example") -Destination (Join-Path $launcherDirectory "tuba.env.example")
Copy-Item -LiteralPath (Join-Path $root "deploy\components\source-adapter.example.json") -Destination (Join-Path $launcherDirectory "source-adapter.example.json")
Copy-Item -LiteralPath (Join-Path $root "contracts\events\topics.v1.json") -Destination (Join-Path $contractDirectory "topics.v1.json")
Copy-Item -Path (Join-Path $webSource "dist\*") -Destination $staticDirectory -Recurse -Force

$packageName = "tuba-$Version-$GOOS-$GOARCH"
try {
    & (Join-Path $PSScriptRoot "verify_tuba_package_stage.ps1") -StageDirectory $stage -GOOS $GOOS -Commands $commands
    if ($GOOS -eq "windows") {
        $archive = Join-Path $outputPath ($packageName + ".zip")
        Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $archive -CompressionLevel Optimal -Force
    } else {
        $archive = Join-Path $outputPath ($packageName + ".tar.gz")
        & tar.exe -czf $archive -C $stage bin deploy contracts web
        if ($LASTEXITCODE -ne 0) { throw "Could not create Linux package archive." }
    }
    $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash.ToLowerInvariant()
    Set-Content -LiteralPath ($archive + ".sha256") -Encoding Ascii -NoNewline `
        -Value ("$hash  " + [IO.Path]::GetFileName($archive))
    Write-Output "Package: $archive"
    Write-Output "SHA-256: $hash"
} finally {
    if (Test-Path -LiteralPath $stage) { Remove-Item -LiteralPath $stage -Recurse -Force }
}
