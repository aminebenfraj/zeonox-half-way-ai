$ErrorActionPreference = "Stop"

$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = $MyInvocation.MyCommand.Path
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)

if (-not $isAdmin) {
    Write-Host "Requesting administrator access for Tailscale..."
    $arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
    Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList $arguments
    exit
}

function Get-DotEnvValue {
    param([string]$Name, [string]$Default)

    $envPath = Join-Path $projectDir ".env"
    if (-not (Test-Path -LiteralPath $envPath)) {
        return $Default
    }

    $line = Get-Content -LiteralPath $envPath | Where-Object {
        $_ -match "^\s*$([regex]::Escape($Name))\s*="
    } | Select-Object -Last 1

    if (-not $line) {
        return $Default
    }

    $value = ($line -split "=", 2)[1].Trim().Trim('"').Trim("'")
    if ($value) { return $value }
    return $Default
}

$tailscaleExe = Get-DotEnvValue "TAILSCALE_EXE" "C:\Program Files\Tailscale\tailscale.exe"
$serveTarget = Get-DotEnvValue "TAILSCALE_SERVE_TARGET" "http://127.0.0.1:8799"
$configuredUrl = (Get-DotEnvValue "TAILSCALE_DASHBOARD_URL" "").TrimEnd('/')

if (-not (Test-Path -LiteralPath $tailscaleExe)) {
    throw "Tailscale was not found at '$tailscaleExe'. Install it, or update TAILSCALE_EXE in .env."
}

Write-Host "Checking Tailscale connection..."
$status = & $tailscaleExe status --json | ConvertFrom-Json
if ($status.BackendState -ne "Running") {
    Write-Host "Tailscale is not connected. Starting sign-in/reconnect..."
    & $tailscaleExe up
    if ($LASTEXITCODE -ne 0) {
        throw "Tailscale could not connect. Open the Tailscale tray app, sign in, then run this script again."
    }
    Start-Sleep -Seconds 3
    $status = & $tailscaleExe status --json | ConvertFrom-Json
}

if ($status.BackendState -ne "Running") {
    throw "Tailscale state is '$($status.BackendState)'. Open the Tailscale tray app, sign in, and wait for Connected."
}

Write-Host "Configuring private HTTPS access to $serveTarget ..."
& $tailscaleExe serve --bg $serveTarget
if ($LASTEXITCODE -ne 0) {
    throw "Tailscale Serve setup failed. If an enablement URL appeared above, open it once and run this script again."
}

$status = & $tailscaleExe status --json | ConvertFrom-Json
$dnsName = [string]$status.Self.DNSName
$dashboardUrl = if ($dnsName) {
    "https://$($dnsName.TrimEnd('.'))"
} elseif ($configuredUrl) {
    $configuredUrl
} else {
    "the HTTPS URL shown by 'tailscale serve status'"
}

Write-Host ""
Write-Host "Tailscale remote access is ready." -ForegroundColor Green
Write-Host "Phone URL: $dashboardUrl/" -ForegroundColor Cyan
Write-Host "This URL is private to devices signed into your Tailscale network."
Write-Host ""
& $tailscaleExe serve status

Read-Host "Press Enter to close"
