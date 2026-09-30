# Start / stop / inspect the TUBA Winlogbeat instance on a Windows source host.
#
# This instance is deliberately separate from any Winlogbeat already installed:
# its own config, its own --path.data and --path.logs, and no Windows Service
# registration. The existing service on 169 feeds the legacy UEBA platform and
# must keep running untouched, so nothing here shares mutable state with it.
#
# The config is copied with Copy-Item -ToSession so its contents (which include
# the source's Kafka SCRAM password) never pass through a command line.
param(
    [Parameter(Mandatory = $true)][ValidateSet("start", "stop", "status")][string]$Action,
    [Parameter(Mandatory = $true)][string]$ComputerName,
    [Parameter(Mandatory = $true)][string]$HostKey,
    [Parameter(Mandatory = $true)][string]$UserEnv,
    [Parameter(Mandatory = $true)][string]$PasswordEnv,
    [Parameter(Mandatory = $true)][string]$ExePath,
    [Parameter(Mandatory = $true)][string]$LocalConfig
)

$ErrorActionPreference = "Stop"
$user = [Environment]::GetEnvironmentVariable($UserEnv)
$pass = [Environment]::GetEnvironmentVariable($PasswordEnv)
if (-not $user -or -not $pass) { throw "missing $UserEnv / $PasswordEnv in the environment" }

$configRemote = "C:\Program Files\TUBA\winlogbeat-$HostKey.yml"
$dataDir = "C:\ProgramData\TUBA\winlogbeat-$HostKey"
$logDir = Join-Path $dataDir "logs"
$stdoutLog = Join-Path $dataDir "stdout.log"

$cred = New-Object System.Management.Automation.PSCredential($user, (ConvertTo-SecureString $pass -AsPlainText -Force))
$session = New-PSSession -ComputerName $ComputerName -Credential $cred -UseSSL -Port 5986 `
    -SessionOption (New-PSSessionOption -SkipCACheck -SkipCNCheck)

try {
    if ($Action -eq "start") {
        Invoke-Command -Session $session -ScriptBlock {
            param($dataDir, $logDir)
            foreach ($d in @("C:\Program Files\TUBA", $dataDir, $logDir)) {
                if (-not (Test-Path -LiteralPath $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
            }
        } -ArgumentList $dataDir, $logDir

        Copy-Item -Path $LocalConfig -Destination $configRemote -ToSession $session
        Write-Output "config copied  : $configRemote"

        $result = Invoke-Command -Session $session -ScriptBlock {
            param($exe, $config, $dataDir, $logDir, $stdoutLog)
            if (-not (Test-Path -LiteralPath $exe)) { return "exe missing: $exe" }
            # Validate before detaching so a bad config fails loudly here.
            $check = & $exe test config -c $config --path.home (Split-Path -Parent $exe) `
                --path.data $dataDir --path.logs $logDir 2>&1
            if ($LASTEXITCODE -ne 0) { return "config test failed ($LASTEXITCODE):`n" + ($check -join "`n") }
            $cmd = 'cmd.exe /c ""' + $exe + '" -c "' + $config + '" --path.home "' + (Split-Path -Parent $exe) +
                   '" --path.data "' + $dataDir + '" --path.logs "' + $logDir + '" > "' + $stdoutLog + '" 2>&1"'
            $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $cmd }
            if ($created.ReturnValue -ne 0) { return "process create failed, Win32 code " + $created.ReturnValue }
            return "started pid=" + $created.ProcessId
        } -ArgumentList $ExePath, $configRemote, $dataDir, $logDir, $stdoutLog
        Write-Output ("launch         : " + $result)
    }

    if ($Action -eq "stop") {
        $stopped = Invoke-Command -Session $session -ScriptBlock {
            param($HostKey)
            $procs = Get-CimInstance Win32_Process -Filter "Name='winlogbeat.exe'" |
                Where-Object { $_.CommandLine -like "*winlogbeat-$HostKey*" }
            $n = 0
            foreach ($p in $procs) { Invoke-CimMethod -InputObject $p -MethodName Terminate | Out-Null; $n++ }
            "terminated $n process(es)"
        } -ArgumentList $HostKey
        Write-Output ("stop           : " + $stopped)
    }

    if ($Action -eq "status") {
        Invoke-Command -Session $session -ScriptBlock {
            param($dataDir, $logDir, $stdoutLog, $configRemote, $HostKey)
            "config present : " + (Test-Path -LiteralPath $configRemote)
            $procs = Get-CimInstance Win32_Process -Filter "Name='winlogbeat.exe'" |
                Where-Object { $_.CommandLine -like "*winlogbeat-$HostKey*" }
            "tuba instance  : " + $(if ($procs) { "running pid=" + ($procs.ProcessId -join ",") } else { "not running" })
            $others = Get-CimInstance Win32_Process -Filter "Name='winlogbeat.exe'" |
                Where-Object { $_.CommandLine -notlike "*winlogbeat-$HostKey*" }
            "other instances: " + $(if ($others) { ($others.ProcessId -join ",") } else { "none" })
            if (Test-Path -LiteralPath $stdoutLog) {
                "stdout.log     : " + (Get-Item $stdoutLog).Length + " B"
                Get-Content -LiteralPath $stdoutLog -Tail 6 | ForEach-Object { "   | " + $_ }
            }
            $own = Join-Path $logDir "winlogbeat"
            Get-ChildItem -Path $logDir -File -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 2 |
                ForEach-Object { "log            : " + $_.Name + "  " + [math]::Round($_.Length / 1KB, 1) + " KB" }
            $registry = Join-Path $dataDir "registry"
            if (Test-Path -LiteralPath $registry) { "registry       : " + (Get-Item $registry).Length + " B" }
        } -ArgumentList $dataDir, $logDir, $stdoutLog, $configRemote, $HostKey
    }
}
finally {
    Remove-PSSession $session -ErrorAction SilentlyContinue
}
