# Tail the TUBA Winlogbeat instance's own log (not the legacy service's).
param(
    [Parameter(Mandatory = $true)][string]$ComputerName,
    [Parameter(Mandatory = $true)][string]$HostKey,
    [Parameter(Mandatory = $true)][string]$UserEnv,
    [Parameter(Mandatory = $true)][string]$PasswordEnv,
    [int]$Lines = 12
)

$ErrorActionPreference = "Stop"
$user = [Environment]::GetEnvironmentVariable($UserEnv)
$pass = [Environment]::GetEnvironmentVariable($PasswordEnv)
if (-not $user -or -not $pass) { throw "missing $UserEnv / $PasswordEnv in the environment" }

$cred = New-Object System.Management.Automation.PSCredential($user, (ConvertTo-SecureString $pass -AsPlainText -Force))
$session = New-PSSession -ComputerName $ComputerName -Credential $cred -UseSSL -Port 5986 `
    -SessionOption (New-PSSessionOption -SkipCACheck -SkipCNCheck)

try {
    Invoke-Command -Session $session -ScriptBlock {
        param($HostKey, $Lines)
        $dir = "C:\ProgramData\TUBA\winlogbeat-$HostKey\logs"
        $files = Get-ChildItem -Path $dir -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending
        if (-not $files) { "no log files under $dir"; return }
        $newest = $files[0]
        "log file      : " + $newest.Name + "  " + [math]::Round($newest.Length / 1KB, 1) + " KB"
        "--- last $Lines entries ---"
        Get-Content -LiteralPath $newest.FullName -Tail $Lines | ForEach-Object { "  " + $_ }
        "--- error/warn summary across all files ---"
        $all = Get-Content -Path (Join-Path $dir "winlogbeat-*") -ErrorAction SilentlyContinue
        "  lines            : " + $all.Count
        foreach ($needle in @('"log.level":"error"', '"log.level":"warn"', 'authorization', 'SASL', 'kafka')) {
            $count = ($all | Select-String -SimpleMatch $needle).Count
            "  " + $needle.PadRight(24) + " : " + $count
        }
    } -ArgumentList $HostKey, $Lines
}
finally {
    Remove-PSSession $session -ErrorAction SilentlyContinue
}
