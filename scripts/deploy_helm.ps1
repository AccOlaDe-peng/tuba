param(
    [string]$Release = "tuba",
    [string]$Namespace = "tuba",
    [string]$Values = "deploy\helm\tuba\values-production.yaml",
    [string]$Chart = "deploy\helm\tuba",
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = "Stop"
$helm = (Get-Command helm).Source

& $helm lint $Chart
if ($LASTEXITCODE -ne 0) {
    throw "helm lint failed"
}

& $helm upgrade --install $Release $Chart `
    --namespace $Namespace `
    --create-namespace `
    --values $Values `
    --atomic `
    --wait `
    --timeout "$($TimeoutSeconds)s"
if ($LASTEXITCODE -ne 0) {
    throw "helm upgrade failed"
}

& $helm status $Release --namespace $Namespace
if ($LASTEXITCODE -ne 0) {
    throw "helm status failed"
}
