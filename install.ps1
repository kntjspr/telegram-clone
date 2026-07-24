#Requires -Version 5.1
<#
.SYNOPSIS
    one-click installer for telegram-clone on windows
.DESCRIPTION
    downloads git if missing, clones the repo, walks you through .env setup
    via a tui-ish prompt, installs uv + deps, and fires up main.py
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── helpers ──────────────────────────────────────────────────────────────────

function Write-Header {
    Clear-Host
    $width = [Math]::Min($Host.UI.RawUI.WindowSize.Width, 72)
    $line  = "─" * $width
    Write-Host $line                          -ForegroundColor Cyan
    Write-Host "  telegram-clone  /  windows quickstart" -ForegroundColor White
    Write-Host $line                          -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step([string]$msg) {
    Write-Host "  ▸ $msg" -ForegroundColor Yellow
}

function Write-Ok([string]$msg) {
    Write-Host "  ✔ $msg" -ForegroundColor Green
}

function Write-Err([string]$msg) {
    Write-Host "  ✖ $msg" -ForegroundColor Red
}

function Pause-OnError([string]$msg) {
    Write-Err $msg
    Write-Host ""
    Write-Host "  press any key to exit..." -ForegroundColor DarkGray
    $null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")
    exit 1
}

# refresh PATH in the current session (equivalent of sourcing .bashrc)
function Refresh-Path {
    $machinePath = [System.Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath    = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path    = "$machinePath;$userPath"
}

# ── step 1: git ───────────────────────────────────────────────────────────────

function Ensure-Git {
    Write-Step "checking for git..."

    if (Get-Command git -ErrorAction SilentlyContinue) {
        Write-Ok "git found: $(git --version)"
        return
    }

    Write-Step "git not found — downloading winget installer..."

    # prefer winget if available
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Step "installing git via winget..."
        winget install --id Git.Git -e --source winget --accept-package-agreements --accept-source-agreements
        Refresh-Path
        if (Get-Command git -ErrorAction SilentlyContinue) {
            Write-Ok "git installed"
            return
        }
    }

    # fallback: download the standalone installer
    Write-Step "falling back to direct download..."
    $gitUrl     = "https://github.com/git-for-windows/git/releases/download/v2.47.1.windows.1/Git-2.47.1-64-bit.exe"
    $installer  = "$env:TEMP\git-installer.exe"

    try {
        Invoke-WebRequest -Uri $gitUrl -OutFile $installer -UseBasicParsing
    } catch {
        Pause-OnError "failed to download git installer: $_"
    }

    Write-Step "running git installer (silent)..."
    Start-Process -FilePath $installer -ArgumentList "/VERYSILENT /NORESTART /NOCANCEL /SP-" -Wait
    Remove-Item $installer -Force -ErrorAction SilentlyContinue

    Refresh-Path

    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        Pause-OnError "git still not found after install — restart powershell and try again"
    }

    Write-Ok "git installed"
}

# ── step 2: python / uv ───────────────────────────────────────────────────────

function Ensure-Python {
    Write-Step "checking for python 3.12+..."

    $pyOk = $false
    if (Get-Command python -ErrorAction SilentlyContinue) {
        $ver = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>$null
        if ($ver -match "^3\.(1[2-9]|[2-9]\d)") { $pyOk = $true }
    }

    if ($pyOk) {
        Write-Ok "python $ver found"
    } else {
        Write-Step "python 3.12+ not found — installing via winget..."
        if (Get-Command winget -ErrorAction SilentlyContinue) {
            winget install --id Python.Python.3.12 -e --source winget --accept-package-agreements --accept-source-agreements
            Refresh-Path
        } else {
            Pause-OnError "winget not available and python 3.12+ is missing. install python from https://python.org then re-run this script."
        }
    }
}

function Ensure-Uv {
    Write-Step "checking for uv..."
    if (Get-Command uv -ErrorAction SilentlyContinue) {
        Write-Ok "uv found"
        return
    }

    Write-Step "installing uv..."
    try {
        $uvInstallScript = (Invoke-WebRequest -Uri "https://astral.sh/uv/install.ps1" -UseBasicParsing).Content
        Invoke-Expression $uvInstallScript
    } catch {
        Pause-OnError "failed to install uv: $_"
    }

    Refresh-Path

    # uv lands in ~/.local/bin or ~/.cargo/bin depending on installer version
    $candidates = @(
        "$env:USERPROFILE\.local\bin",
        "$env:USERPROFILE\.cargo\bin",
        "$env:APPDATA\uv\bin"
    )
    foreach ($c in $candidates) {
        if (Test-Path "$c\uv.exe") {
            $env:Path = "$c;$env:Path"
            break
        }
    }

    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        Pause-OnError "uv install completed but binary not found — open a new terminal and run 'uv run src/telegram_clone/main.py' manually from the repo folder"
    }

    Write-Ok "uv installed"
}

# ── step 3: clone ─────────────────────────────────────────────────────────────

function Clone-Repo {
    $repoUrl  = "https://github.com/kntjspr/telegram-clone"
    $repoName = "telegram-clone"
    $target   = Join-Path $env:USERPROFILE $repoName

    Write-Step "cloning $repoUrl..."

    if (Test-Path $target) {
        Write-Ok "repo already exists at $target — pulling latest..."
        Push-Location $target
        git pull --ff-only
        Pop-Location
    } else {
        git clone $repoUrl $target
        Write-Ok "cloned to $target"
    }

    return $target
}

# ── step 4: tui env setup ─────────────────────────────────────────────────────

function Read-Field([string]$label, [string]$hint, [string]$default = "", [bool]$secret = $false) {
    Write-Host ""
    Write-Host "  $label" -ForegroundColor Cyan -NoNewline

    if ($hint)    { Write-Host "  ($hint)" -ForegroundColor DarkGray -NoNewline }
    if ($default) { Write-Host "  [default: $default]" -ForegroundColor DarkGray -NoNewline }

    Write-Host ""
    Write-Host "  > " -ForegroundColor White -NoNewline

    if ($secret) {
        $secStr = Read-Host -AsSecureString
        $val    = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto(
                      [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($secStr))
    } else {
        $val = Read-Host
    }

    if ([string]::IsNullOrWhiteSpace($val) -and $default) { $val = $default }
    return $val
}

function Read-Choice([string]$label, [string[]]$options, [string]$default) {
    Write-Host ""
    Write-Host "  $label" -ForegroundColor Cyan
    for ($i = 0; $i -lt $options.Length; $i++) {
        $marker = if ($options[$i] -eq $default) { "●" } else { "○" }
        Write-Host "    $marker  $($i+1). $($options[$i])" -ForegroundColor White
    }
    Write-Host ""
    Write-Host "  pick [1-$($options.Length)] (enter = $default): " -ForegroundColor White -NoNewline
    $raw = Read-Host
    if ([string]::IsNullOrWhiteSpace($raw)) { return $default }
    $idx = [int]$raw - 1
    if ($idx -ge 0 -and $idx -lt $options.Length) { return $options[$idx] }
    return $default
}

function Setup-Env([string]$repoPath) {
    $envPath = Join-Path $repoPath ".env"

    if (Test-Path $envPath) {
        Write-Host ""
        Write-Host "  .env already exists." -ForegroundColor Yellow
        Write-Host "  overwrite it? [y/N]: " -ForegroundColor White -NoNewline
        $ans = Read-Host
        if ($ans -notmatch "^[Yy]$") {
            Write-Ok "keeping existing .env"
            return
        }
    }

    Write-Host ""
    Write-Host "  ┌────────────────────────────────────────────┐" -ForegroundColor Cyan
    Write-Host "  │   environment setup                        │" -ForegroundColor Cyan
    Write-Host "  │   get API_ID + API_HASH at my.telegram.org │" -ForegroundColor DarkGray
    Write-Host "  └────────────────────────────────────────────┘" -ForegroundColor Cyan

    $apiId      = Read-Field "API_ID"    "from my.telegram.org > API Development Tools"
    $apiHash    = Read-Field "API_HASH"  "from my.telegram.org" "" $true
    $phone      = Read-Field "PHONE"     "with country code, e.g. +1234567890"
    $srcChannel = Read-Field "SOURCE_CHANNEL" "username (no @) or numeric ID"
    $dstChannel = Read-Field "DEST_CHANNEL"   "username (no @) or numeric ID"

    $backend = Read-Choice "TRACKER_BACKEND" @("json", "sqlite", "supabase") "json"

    $sqliteDb    = ""
    $supabaseUrl = ""
    $supabaseKey = ""

    if ($backend -eq "sqlite") {
        $sqliteDb = Read-Field "SQLITE_DB" "path to db file" "clone_tracker.db"
    }

    if ($backend -eq "supabase") {
        $supabaseUrl = Read-Field "SUPABASE_URL" "your project url"
        $supabaseKey = Read-Field "SUPABASE_KEY" "service role key" "" $true
    }

    $notifyErr  = Read-Choice "NOTIFY_ON_ERROR"    @("true", "false") "true"
    $notifyDone = Read-Choice "NOTIFY_ON_COMPLETE" @("true", "false") "true"

    $threads = Read-Field "FAST_TELETHON_THREADS" "leave blank for auto" ""

    $envContent = @"
# generated by install.ps1 — edit as needed

# get these from https://my.telegram.org
API_ID=$apiId
API_HASH=$apiHash

# your phone number with country code (e.g. +1234567890)
PHONE=$phone

# source channel to clone from (username or numeric ID)
SOURCE_CHANNEL=$srcChannel

# destination channel to clone into (username or numeric ID)
DEST_CHANNEL=$dstChannel

# tracker backend: json / sqlite / supabase
TRACKER_BACKEND=$backend

# sqlite settings (only if TRACKER_BACKEND=sqlite)
SQLITE_DB=$sqliteDb

# supabase settings (only if TRACKER_BACKEND=supabase)
SUPABASE_URL=$supabaseUrl
SUPABASE_KEY=$supabaseKey

# telegram notifications on error/completion
NOTIFY_ON_ERROR=$notifyErr
NOTIFY_ON_COMPLETE=$notifyDone

# override FastTelethon parallel senders; leave blank for auto
FAST_TELETHON_THREADS=$threads
"@

    Set-Content -Path $envPath -Value $envContent -Encoding UTF8
    Write-Ok ".env saved"
}

# ── step 5: install deps + run ────────────────────────────────────────────────

function Install-And-Run([string]$repoPath) {
    Push-Location $repoPath

    Write-Step "syncing dependencies (uv sync)..."
    uv sync

    Write-Ok "all dependencies installed"
    Write-Host ""
    Write-Host "  ┌─────────────────────────────────────────────────┐" -ForegroundColor Green
    Write-Host "  │   launching telegram-clone web interface...     │" -ForegroundColor Green
    Write-Host "  │   open http://localhost:5000 in your browser    │" -ForegroundColor DarkGray
    Write-Host "  │   ctrl+c to stop                                │" -ForegroundColor DarkGray
    Write-Host "  └─────────────────────────────────────────────────┘" -ForegroundColor Green
    Write-Host ""

    uv run src/telegram_clone/main.py

    Pop-Location
}

# ── main ──────────────────────────────────────────────────────────────────────

Write-Header
Ensure-Git
Ensure-Python
Ensure-Uv

$repo = Clone-Repo
Setup-Env  $repo
Install-And-Run $repo
