# Read-only reconnaissance of the Winlogbeat install on a Windows source host.
# Credentials come from the environment (TUBA_WIN_<key>_USER / _PASSWORD), which
# the caller loads from the gitignored .env.local so they stay out of transcripts.
# Password values in the remote config are masked before they are printed.
param(
    [Parameter(Mandatory = $true)][string]$ComputerName,
    [Parameter(Mandatory = $true)][string]$UserEnv,
    [Parameter(Mandatory = $true)][string]$PasswordEnv
)

$ErrorActionPreference = "Stop"
$user = [Environment]::GetEnvironmentVariable($UserEnv)
$pass = [Environment]::GetEnvironmentVariable($PasswordEnv)
if (-not $user -or -not $pass) { throw "missing $UserEnv / $PasswordEnv in the environment" }

$cred = New-Object System.Management.Automation.PSCredential($user, (ConvertTo-SecureString $pass -AsPlainText -Force))
$session = @{
    ComputerName  = $ComputerName
    Credential    = $cred
    UseSSL        = $true
    Port          = 5986
    SessionOption = (New-PSSessionOption -SkipCACheck -SkipCNCheck)
}

$remote = {
    $os = Get-CimInstance Win32_OperatingSystem
    "os            : " + $os.Caption + " (build " + $os.BuildNumber + ")"
    "hostname      : " + $env:COMPUTERNAME
    $c = Get-PSDrive C
    "c: free       : " + [math]::Round($c.Free / 1GB, 1) + " GB of " + [math]::Round(($c.Used + $c.Free) / 1GB, 1) + " GB"

    $svc = Get-CimInstance Win32_Service -Filter "Name='winlogbeat'"
    if ($null -eq $svc) { "service       : not installed"; return }
    "service state : " + $svc.State + " (startmode " + $svc.StartMode + ")"

    # PathName carries arguments; the executable is its first quoted token.
    $pathName = $svc.PathName
    if ($pathName -match '^"([^"]+)"') { $exe = $matches[1] } else { $exe = ($pathName -split ' ')[0] }
    $dir = Split-Path -Parent $exe
    "install dir   : " + $dir

    # The service is installed with --path.data / --path.logs overrides.
    $dataDir = if ($pathName -match '--path\.data\s+"([^"]+)"') { $matches[1] } else { Join-Path $dir "data" }
    $logDir = if ($pathName -match '--path\.logs\s+"([^"]+)"') { $matches[1] } else { Join-Path $dir "logs" }
    "data dir      : " + $dataDir
    "logs dir      : " + $logDir

    $config = Join-Path $dir "winlogbeat.yml"
    if (Test-Path -LiteralPath $config) {
        $item = Get-Item -LiteralPath $config
        $digest = (Get-FileHash -LiteralPath $config -Algorithm SHA256).Hash.Substring(0, 16)
        "config        : " + $item.LastWriteTime.ToString("yyyy-MM-dd HH:mm") + "  " + $item.Length + " B  sha256:" + $digest
        "--- config (passwords masked) ---"
        Get-Content -LiteralPath $config | ForEach-Object {
            if ($_ -match '^\s*(password|pass)\s*:') { "  " + ($_ -replace ':.*', ': <masked>') } else { "  " + $_ }
        }
        "--- end config ---"
    }
    else { "config        : absent at " + $config }

    foreach ($sub in @($dataDir, $logDir)) {
        if (Test-Path -LiteralPath $sub) {
            $files = Get-ChildItem -LiteralPath $sub -Recurse -File -ErrorAction SilentlyContinue
            $size = ($files | Measure-Object Length -Sum).Sum
            "  " + $sub.PadRight(28) + " " + $files.Count + " files, " + [math]::Round($size / 1MB, 2) + " MB"
            $files | Sort-Object LastWriteTime -Descending | Select-Object -First 2 |
                ForEach-Object { "      recent: " + $_.Name + "  " + $_.LastWriteTime.ToString("yyyy-MM-dd HH:mm") }
        }
        else { "  " + $sub.PadRight(28) + " absent" }
    }
}

Write-Output ("=== " + $ComputerName + " ===")
Invoke-Command @session -ScriptBlock $remote
