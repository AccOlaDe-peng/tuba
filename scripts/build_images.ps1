param(
    [string]$Registry = "registry.example.com/tuba",
    [string]$Tag = "1.0.0",
    [switch]$Push
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root

$goTargets = @{
	"tuba-collector" = "tuba-collector"
    "tuba-ingest" = "tuba-ingest"
    "tuba-raw-indexer" = "tuba-raw-indexer"
    "tuba-normalizer" = "tuba-normalizer"
    "tuba-standard-indexer" = "tuba-standard-indexer"
    "tuba-quarantine-indexer" = "tuba-quarantine-indexer"
    "tuba-control-worker" = "tuba-control-worker"
    "tuba-analysis-sink" = "tuba-analysis-sink"
    "tuba-api" = "tuba-api"
}
foreach ($service in $goTargets.Keys) {
    $image = "$Registry/$($goTargets[$service]):${Tag}"
    docker build -f deploy/docker/go.Dockerfile --build-arg "TARGET=$service" -t $image .
    if ($LASTEXITCODE -ne 0) {
        throw "failed to build $image"
    }
    if ($Push) {
        docker push $image
    }
}

$analysisImage = "$Registry/tuba-analysis:${Tag}"
docker build -f deploy/docker/Dockerfile.analysis -t $analysisImage .
if ($LASTEXITCODE -ne 0) {
    throw "failed to build $analysisImage"
}

$webImage = "$Registry/tuba-web:${Tag}"
docker build `
    -f deploy/docker/Dockerfile.web `
    -t $webImage .
if ($LASTEXITCODE -ne 0) {
    throw "failed to build $webImage"
}

if ($Push) {
    foreach ($image in @($analysisImage, $webImage)) {
        docker push $image
    }
}

Write-Host "Images built for ${Registry}:${Tag}"
