param(
    [string]$Release = "tuba",
    [string]$Namespace = "tuba",
    [int]$Revision = 0,
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = "Stop"
$helm = (Get-Command helm).Source
$arguments = @("rollback", $Release)
if ($Revision -gt 0) {
    $arguments += $Revision
}
$arguments += @("--namespace", $Namespace, "--wait", "--timeout", "$($TimeoutSeconds)s")

& $helm @arguments
if ($LASTEXITCODE -ne 0) {
    throw "helm rollback failed"
}
