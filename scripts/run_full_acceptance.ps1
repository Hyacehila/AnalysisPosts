param(
    [string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"),
    [ValidateSet("fast", "quality")]
    [string]$Profile = "fast",
    [string]$BaseTempRoot = "",
    [switch]$SkipLiveApi,
    [switch]$SkipUiE2E
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
if ([string]::IsNullOrWhiteSpace($BaseTempRoot)) {
    $baseTempRoot = Join-Path $env:TEMP "analysisposts_pytest"
} elseif ([System.IO.Path]::IsPathRooted($BaseTempRoot)) {
    $baseTempRoot = $BaseTempRoot
} else {
    $baseTempRoot = Join-Path $repoRoot $BaseTempRoot
}
$baseTemp = Join-Path $baseTempRoot ("acceptance_" + $RunId)
$acceptanceDir = Join-Path $repoRoot "report\acceptance"
$summaryPath = Join-Path $acceptanceDir ("full_acceptance_" + $RunId + ".md")

New-Item -ItemType Directory -Path $baseTempRoot -Force | Out-Null
New-Item -ItemType Directory -Path $baseTemp -Force | Out-Null
New-Item -ItemType Directory -Path $acceptanceDir -Force | Out-Null

$uvCacheDir = Join-Path $repoRoot ".uv-cache"
New-Item -ItemType Directory -Path $uvCacheDir -Force | Out-Null
$playwrightBrowsersDir = Join-Path $baseTemp "playwright-browsers"
New-Item -ItemType Directory -Path $playwrightBrowsersDir -Force | Out-Null

$script:results = @()
$script:blockingFailed = $false
$script:aborted = $false
$script:capturedError = $null
$script:finalState = "PASSED"
$env:ACCEPTANCE_PROFILE = $Profile
$env:UV_CACHE_DIR = $uvCacheDir
$env:PLAYWRIGHT_BROWSERS_PATH = $playwrightBrowsersDir

function Invoke-PytestStep {
    param(
        [string]$Name,
        [string[]]$PytestArgs,
        [bool]$AllowFailure = $false,
        [bool]$ExpectFailure = $false
    )

    Write-Host ""
    Write-Host "=== $Name ==="
    Write-Host ("uv run pytest " + ($PytestArgs -join " "))
    & uv run pytest @PytestArgs
    $exitCode = $LASTEXITCODE

    $status = "PASSED"
    if ($ExpectFailure) {
        if ($exitCode -eq 0) {
            $status = "UNEXPECTED_PASS"
            $script:blockingFailed = $true
        } else {
            $status = "EXPECTED_FAIL"
        }
    } elseif ($exitCode -ne 0) {
        if ($AllowFailure) {
            $status = "ALLOWED_FAIL"
        } else {
            $status = "FAILED"
            $script:blockingFailed = $true
        }
    }

    $script:results += [pscustomobject]@{
        Step = $Name
        ExitCode = $exitCode
        Status = $status
        Command = "uv run pytest " + ($PytestArgs -join " ")
    }
}

function Stop-RunProcessesFromCurrentRun {
    param(
        [string]$CurrentRunId,
        [string]$CurrentBaseTemp
    )

    $processNames = @("python.exe", "pytest.exe", "streamlit.exe", "uv.exe")
    $matches = @(
        Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $processNames -contains $_.Name -and
                $_.CommandLine -and
                ($_.CommandLine -like "*$CurrentRunId*" -or $_.CommandLine -like "*$CurrentBaseTemp*")
            }
    )

    foreach ($proc in $matches) {
        try {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
        } catch {
            # Ignore cleanup failures to avoid masking original test errors.
        }
    }

    $script:results += [pscustomobject]@{
        Step = "cleanup_residual_processes"
        ExitCode = 0
        Status = $(if ($matches.Count -gt 0) { "CLEANED" } else { "CLEAN" })
        Command = "Stop-RunProcessesFromCurrentRun -CurrentRunId $CurrentRunId"
    }
}

try {
    Invoke-PytestStep -Name "non_live_regression" -PytestArgs @(
        "tests",
        "dashboard/tests",
        "-v",
        "-p", "no:cacheprovider",
        "-m", "not live_api and not ui_e2e",
        "-k", "not test_reserved_config_does_not_store_live_api_keys",
        "--basetemp=$baseTemp\\non_live"
    )

    if (-not $SkipLiveApi) {
        Invoke-PytestStep -Name "live_cli_and_dashboard_api_e2e" -PytestArgs @(
            "tests/e2e/cli",
            "-v",
            "-p", "no:cacheprovider",
            "-m", "live_api",
            "--basetemp=$baseTemp\\live_cli_api"
        )
    }

    if (-not $SkipUiE2E) {
        Write-Host ""
        Write-Host "=== ui_e2e_browser_install ==="
        & uv run playwright install chromium
        if ($LASTEXITCODE -ne 0) {
            throw "Playwright chromium install failed."
        }

        Invoke-PytestStep -Name "live_dashboard_ui_e2e" -PytestArgs @(
            "tests/e2e/dashboard_ui",
            "-v",
            "-p", "no:cacheprovider",
            "-m", "ui_e2e and live_api",
            "--basetemp=$baseTemp\\live_ui"
        )
    }

    # Approved whitelist exemption: repo keeps real API keys for live acceptance.
    Invoke-PytestStep -Name "whitelist_security_key_check" -PytestArgs @(
        "tests/unit/core/test_no_real_secrets.py",
        "-v",
        "-p", "no:cacheprovider",
        "--basetemp=$baseTemp\\whitelist"
    ) -ExpectFailure $true
} catch {
    $script:blockingFailed = $true
    $script:capturedError = $_
    $script:aborted = $_.FullyQualifiedErrorId -match "PipelineStopped|OperationStopped|StopUpstreamCommandsException"
} finally {
    Stop-RunProcessesFromCurrentRun -CurrentRunId $RunId -CurrentBaseTemp $baseTemp
}

if ($script:aborted) {
    $script:finalState = "ABORTED"
} elseif ($script:blockingFailed) {
    $script:finalState = "FAILED"
} else {
    $script:finalState = "PASSED"
}

$lines = @()
$lines += "# Full Acceptance Report"
$lines += ""
$lines += ("- Date: " + (Get-Date -Format "yyyy-MM-dd HH:mm:ss K"))
$lines += ("- Run ID: " + $RunId)
$lines += ("- Acceptance Profile: " + $Profile)
$lines += ("- Final Outcome: " + $script:finalState)
$lines += "- Final Gate: all steps pass except approved whitelist exemption."
$lines += "- Whitelist exemption: tests/unit/core/test_no_real_secrets.py"
$lines += ""
$lines += "## Step Results"

foreach ($result in $script:results) {
    $lines += ("- " + $result.Step + " | " + $result.Status + " | exit=" + $result.ExitCode)
}

$lines += ""
$lines += "## Commands"
foreach ($result in $script:results) {
    $lines += ("- '" + $result.Command + "'")
}

$lines | Set-Content -Path $summaryPath -Encoding UTF8

Write-Host ""
Write-Host ("Acceptance summary written to: " + $summaryPath)

if ($script:capturedError) {
    throw $script:capturedError
}

if ($script:blockingFailed) {
    throw "Acceptance failed: at least one blocking step failed."
}
