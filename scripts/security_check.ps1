param([string]$APIBase = "http://127.0.0.1:8788")
$ErrorActionPreference = "Stop"
function Get-LoginSession([string]$Username, [string]$Password) {
    if (-not $Username -or -not $Password) { throw 'Supply TUBA_TEST_ANALYST_USERNAME/PASSWORD and TUBA_TEST_ADMIN_USERNAME/PASSWORD.' }
    $body = @{username = $Username; password = $Password} | ConvertTo-Json -Compress
    Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$APIBase/api/v1/auth/login" -ContentType 'application/json' -Body $body -SessionVariable loginSession | Out-Null
    return $loginSession
}
function Get-Status([string]$URI, $Session) {
    try {
        if ($Session) { Invoke-WebRequest -UseBasicParsing -Uri $URI -WebSession $Session -TimeoutSec 10 | Out-Null }
        else { Invoke-WebRequest -UseBasicParsing -Uri $URI -TimeoutSec 10 | Out-Null }
        return 200
    } catch { return [int]$_.Exception.Response.StatusCode }
}
$analyst = Get-LoginSession $env:TUBA_TEST_ANALYST_USERNAME $env:TUBA_TEST_ANALYST_PASSWORD
$admin = Get-LoginSession $env:TUBA_TEST_ADMIN_USERNAME $env:TUBA_TEST_ADMIN_PASSWORD
$checks = @(
    @{name = 'unauthenticated API'; status = Get-Status "$APIBase/api/v1/me" $null; expected = 401},
    @{name = 'analyst cannot manage members'; status = Get-Status "$APIBase/api/v1/members" $analyst; expected = 403},
    @{name = 'analyst can read anomalies'; status = Get-Status "$APIBase/api/v1/anomalies" $analyst; expected = 200},
    @{name = 'tenant admin can manage members'; status = Get-Status "$APIBase/api/v1/members" $admin; expected = 200}
)
$checks | Format-Table -AutoSize
if (@($checks | Where-Object { $_.status -ne $_.expected }).Count -gt 0) { throw 'security checks failed' }
Write-Host 'security checks passed'
