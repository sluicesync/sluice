# PowerShell mirror of the .githooks/pre-commit shell script.
# Runs the same gofumpt + go vet + go test gate from a PowerShell
# session, so Windows users without `make` (or who prefer not to use
# Git Bash) have a one-command path to "would CI accept this?".
#
# Usage:
#   .\scripts\pre-commit.ps1
#
# Exits non-zero if any check fails. The git pre-commit hook itself is
# the bash version (.githooks/pre-commit) and runs through Git for
# Windows' bundled bash; this script is for manual invocation only.

$ErrorActionPreference = 'Stop'

function Red($msg)   { Write-Host $msg -ForegroundColor Red }
function Green($msg) { Write-Host $msg -ForegroundColor Green }

# ---- gofumpt ----
$gofumpt = Get-Command gofumpt -ErrorAction SilentlyContinue
if (-not $gofumpt) {
    Red "gofumpt not installed."
    Write-Host "Install: go install mvdan.cc/gofumpt@latest"
    exit 1
}

$unformatted = & gofumpt -l .
if ($unformatted) {
    Red "gofumpt would reformat the following files:"
    $unformatted | ForEach-Object { Write-Host $_ }
    Write-Host ""
    Write-Host "Run: gofumpt -w ."
    exit 1
}

# ---- go vet ----
& go vet ./...
if ($LASTEXITCODE -ne 0) {
    Red "go vet failed."
    exit 1
}

# ---- go test (fast, no DB) ----
# -race is preferred but requires cgo. On Windows without a C compiler
# (the default for most Go installs) CGO_ENABLED=0, so -race won't
# work; skip it locally and rely on CI to catch races.
$testArgs = @('-count=1')
$cgoEnabled = (& go env CGO_ENABLED).Trim()
if ($cgoEnabled -eq '1') {
    $testArgs += '-race'
}
$testArgs += './...'
& go test @testArgs | Out-Null
if ($LASTEXITCODE -ne 0) {
    Red "tests failed."
    Write-Host "Run: go test ./...  (to see the failure)"
    exit 1
}

Green "pre-commit: OK"
