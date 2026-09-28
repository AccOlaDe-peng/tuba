param(
    [Parameter(Mandatory = $true)][string]$StageDirectory,
    [Parameter(Mandatory = $true)][ValidateSet("windows", "linux")][string]$GOOS,
    [Parameter(Mandatory = $true)][string[]]$Commands
)

$ErrorActionPreference = "Stop"
$stage = (Get-Item -LiteralPath $StageDirectory).FullName
$extension = if ($GOOS -eq "windows") { ".exe" } else { "" }
$expected = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($command in $Commands) { [void]$expected.Add("bin/$command$extension") }
[void]$expected.Add("deploy/launcher/tuba-services.$GOOS.example.json")
[void]$expected.Add("deploy/launcher/tuba.env.example")
[void]$expected.Add("deploy/launcher/source-adapter.example.json")
[void]$expected.Add("contracts/events/topics.v1.json")

$webRoot = Join-Path $stage "web"
if (-not (Test-Path -LiteralPath (Join-Path $webRoot "index.html") -PathType Leaf) -or
    -not (Test-Path -LiteralPath (Join-Path $webRoot "config.js") -PathType Leaf)) {
    throw 'Package stage must include web/index.html and web/config.js.'
}
$webFiles = @(Get-ChildItem -LiteralPath $webRoot -File -Recurse -Force)
if ($webFiles.Count -lt 4 -or @($webFiles | Where-Object { $_.Extension -eq '.map' }).Count -gt 0) {
    throw 'Package stage web assets are incomplete or contain source maps.'
}
foreach ($entry in (Get-ChildItem -LiteralPath $webRoot -Recurse -Force)) {
    if (($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'Package stage web assets may not contain reparse points.'
    }
}
$indexContent = Get-Content -LiteralPath (Join-Path $webRoot "index.html") -Raw
if ($indexContent -notmatch '<script\s+src="/config\.js"') {
    throw 'Packaged web index must load its runtime configuration before the application bundle.'
}
foreach ($file in $webFiles) {
    $relative = $file.FullName.Substring($stage.Length).TrimStart([char[]]@([char]0x5C, [char]0x2F)).Replace([char]0x5C, [char]0x2F)
    [void]$expected.Add($relative)
}

$actual = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
Get-ChildItem -LiteralPath $stage -File -Recurse | ForEach-Object {
    $relative = $_.FullName.Substring($stage.Length).TrimStart([char[]]@([char]0x5C, [char]0x2F)).Replace([char]0x5C, [char]0x2F)
    [void]$actual.Add($relative)
}
$unexpected = @($actual | Where-Object { -not $expected.Contains($_) })
$missing = @($expected | Where-Object { -not $actual.Contains($_) })
if ($unexpected.Count -gt 0 -or $missing.Count -gt 0) {
    throw "Package stage file allowlist failed (unexpected=$($unexpected -join ','), missing=$($missing -join ','))."
}

$manifestPath = Join-Path $stage "deploy/launcher/tuba-services.$GOOS.example.json"
$manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
$sensitiveName = '(?i)(PASSWORD|TOKEN|SECRET|API_KEY|PRIVATE_KEY|DATABASE_URL)$'
$envReference = '^\$\{[A-Z_][A-Z0-9_]*\}$'
$manifestReferences = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
$esKeyReferences = [System.Collections.Generic.List[string]]::new()
foreach ($service in $manifest.services) {
    foreach ($property in $service.environment.PSObject.Properties) {
        if ($property.Name -match $sensitiveName -and $property.Value -notmatch $envReference) {
            throw "Sensitive manifest value must be an exact environment reference (service=$($service.name), field=$($property.Name))."
        }
        if ($property.Value -match $envReference) {
            $variable = $Matches[0].Substring(2, $Matches[0].Length - 3)
            [void]$manifestReferences.Add($variable)
            if ($property.Name -eq 'ES_API_KEY') { $esKeyReferences.Add($variable) }
        }
    }
}
$uniqueEsKeyReferences = @($esKeyReferences.ToArray() | Select-Object -Unique)
if ($esKeyReferences.Count -gt 0 -and $esKeyReferences.Count -ne $uniqueEsKeyReferences.Count) {
    throw 'Every Elasticsearch service must use a distinct API key environment variable.'
}

$environmentPath = Join-Path $stage "deploy/launcher/tuba.env.example"
$environmentNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($line in (Get-Content -LiteralPath $environmentPath)) {
    if ($line -match '^\s*#' -or $line -notmatch '^\s*([A-Z_][A-Z0-9_]*)=(.*)$') { continue }
    $name = $Matches[1]
    $value = $Matches[2]
    [void]$environmentNames.Add($name)
    if ($name -notmatch $sensitiveName) { continue }
    $isPlaceholder = if ($name -eq "DATABASE_URL") {
        $value -match '^postgres(?:ql)?://[^:\s]+:replace-me@'
    } else {
        $value -match '^replace-(?:me|with(?:-|$))'
    }
    if (-not $isPlaceholder) { throw "Sensitive example value is not a placeholder (field=$name)." }
}
$missingVariables = @($manifestReferences | Where-Object { -not $environmentNames.Contains($_) })
if ($missingVariables.Count -gt 0) {
    throw "Manifest variables are missing from the example environment file: $($missingVariables -join ',')."
}

Write-Output "Package security audit passed: allowlisted files=$($actual.Count), manifest variables are declared, Elasticsearch keys are distinct, and example secrets are placeholders."
