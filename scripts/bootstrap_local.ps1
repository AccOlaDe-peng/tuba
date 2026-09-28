$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root

if (-not (Test-Path -LiteralPath ".env")) {
    Copy-Item -LiteralPath ".env.example" -Destination ".env"
}

docker compose --profile identity up -d

$services = @{
    "product-kafka-1" = "healthy"
    "product-postgres-1" = "healthy"
    "product-elasticsearch-1" = "healthy"
    "product-keycloak-1" = "healthy"
}

$deadline = (Get-Date).AddMinutes(3)
do {
    $pending = @()
    foreach ($container in $services.Keys) {
        $state = docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $container 2>$null
        if ($state -ne $services[$container]) {
            $pending += "$container=$state"
        }
    }
    if ($pending.Count -eq 0) {
        break
    }
    Start-Sleep -Seconds 3
} while ((Get-Date) -lt $deadline)

if ($pending.Count -gt 0) {
    throw "Dependencies did not become healthy: $($pending -join ', ')"
}

$files = Get-ChildItem -LiteralPath "migrations" -File -Filter "*.sql" |
    Where-Object { $_.Name -match "^[0-9]" } |
    Sort-Object Name

docker compose exec -T postgres psql -U tuba -d tuba -v ON_ERROR_STOP=1 --quiet -c "CREATE TABLE IF NOT EXISTS schema_migrations(filename text PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now())" | Out-Null
$schemaReady = docker compose exec -T postgres psql -U tuba -d tuba -Atc "select to_regclass('public.organizations') is not null"
if ($schemaReady.Trim() -eq "t") {
    foreach ($file in ($files | Where-Object { $_.Name -match '^0000[1-6]_' })) {
        docker compose exec -T postgres psql -U tuba -d tuba -Atc "INSERT INTO schema_migrations(filename) VALUES ('$($file.Name)') ON CONFLICT DO NOTHING" | Out-Null
    }
    Write-Host "Existing PostgreSQL schema detected; legacy migrations baselined and new migrations will be applied."
}
foreach ($file in $files) {
    $applied = docker compose exec -T postgres psql -U tuba -d tuba -Atc "SELECT 1 FROM schema_migrations WHERE filename='$($file.Name)'"
    if ($applied.Trim() -eq "1") {
        continue
    }
    $sql = (Get-Content -Raw -LiteralPath $file.FullName) -split "(?m)^-- \+goose Down\r?$" |
        Select-Object -First 1
    $sql = $sql -replace "(?m)^-- \+goose Up\r?$", ""
    $sql | docker compose exec -T postgres psql -U tuba -d tuba -v ON_ERROR_STOP=1 --quiet
    if ($LASTEXITCODE -ne 0) {
        throw "Failed applying $($file.Name)"
    }
    docker compose exec -T postgres psql -U tuba -d tuba -v ON_ERROR_STOP=1 --quiet -c "INSERT INTO schema_migrations(filename) VALUES ('$($file.Name)')" | Out-Null
    Write-Host "Applied $($file.Name)"
}

$baseURL = "http://127.0.0.1:9200"
$assets = @(
    @("_ilm/policy/tuba-authentication-v1", "elasticsearch\ilm-authentication-v1.json"),
    @("_component_template/tuba-authentication-v1", "elasticsearch\component-template-authentication-v1.json"),
    @("_index_template/tuba-authentication-v1", "elasticsearch\index-template-authentication-v1.json"),
    @("_ilm/policy/tuba-anomaly-v1", "elasticsearch\ilm-anomaly-v1.json"),
    @("_component_template/tuba-anomaly-v1", "elasticsearch\component-template-anomaly-v1.json"),
    @("_index_template/tuba-anomaly-v1", "elasticsearch\index-template-anomaly-v1.json"),
    @("ueba-anomalies-*/_mapping", "elasticsearch\anomaly-runtime-mapping-v1.json")
)

foreach ($asset in $assets) {
    & curl.exe --fail --silent --show-error -X PUT `
        -H "Authorization: ApiKey local-dev-only" `
        -H "Content-Type: application/json" `
        --data-binary "@$($asset[1])" `
        "$baseURL/$($asset[0])" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed applying $($asset[0])"
    }
    Write-Host "Applied $($asset[0])"
}

Write-Host ""
Write-Host "Local dependencies are ready."
Write-Host "Console: http://127.0.0.1:5173/"
Write-Host "Keycloak admin: http://127.0.0.1:8180/admin/ (admin / admin-local-only)"
Write-Host "Application users: wang.min, analyst.lee, auditor.zhao"
Write-Host "Local password: TubaLocal!123"
