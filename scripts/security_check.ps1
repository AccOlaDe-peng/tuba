param(
    [string]$APIBase = "http://127.0.0.1:8788",
    [string]$KeycloakBase = "http://127.0.0.1:8180",
    [string]$Realm = "tuba",
    [string]$ClientID = "tuba-web"
)

$ErrorActionPreference = "Stop"

function Get-Token([string]$Username, [string]$Password) {
    $body = @{
        client_id = $ClientID
        grant_type = "password"
        username = $Username
        password = $Password
        scope = "openid profile email"
    }
    return (Invoke-RestMethod -Method Post `
        -Uri "$KeycloakBase/realms/$Realm/protocol/openid-connect/token" `
        -ContentType "application/x-www-form-urlencoded" -Body $body).access_token
}

function Get-Status([string]$URI, [string]$Token) {
    try {
        $headers = @{}
        if ($Token) {
            $headers.Authorization = "Bearer $Token"
        }
        Invoke-WebRequest -UseBasicParsing -Uri $URI -Headers $headers -TimeoutSec 10 | Out-Null
        return 200
    } catch {
        return [int]$_.Exception.Response.StatusCode
    }
}

$analyst = Get-Token "analyst.lee" "TubaLocal!123"
$admin = Get-Token "wang.min" "TubaLocal!123"
$checks = @(
    @{name = "unauthenticated API"; status = Get-Status "$APIBase/api/v1/me" ""; expected = 401},
    @{name = "analyst cannot manage members"; status = Get-Status "$APIBase/api/v1/members" $analyst; expected = 403},
    @{name = "analyst can read anomalies"; status = Get-Status "$APIBase/api/v1/anomalies" $analyst; expected = 200},
    @{name = "tenant admin can manage members"; status = Get-Status "$APIBase/api/v1/members" $admin; expected = 200}
)

$checks | Format-Table -AutoSize
if (@($checks | Where-Object { $_.status -ne $_.expected }).Count -gt 0) {
    throw "security checks failed"
}
Write-Host "security checks passed"
