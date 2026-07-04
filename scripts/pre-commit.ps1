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

# ---- Conflict-marker check (applies to ALL files, not just Go) ----
# Mirrors the .githooks/pre-commit Bash check. Catches the case where a
# cherry-pick / merge / rebase left unresolved markers in a file the
# Go-only gates below would skip (the v0.74.0 cycle's F5 cherry-pick
# left CHANGELOG.md markers on main).
$stagedFiles = (& git diff --cached --name-only --diff-filter=ACM) -split "`n" |
    Where-Object { $_ -ne '' -and (Test-Path $_) }
$markerFiles = @()
foreach ($f in $stagedFiles) {
    $hits = Select-String -Path $f -Pattern '^(<<<<<<<|>>>>>>>)' -ErrorAction SilentlyContinue
    if ($hits) { $markerFiles += $f }
}
if ($markerFiles.Count -gt 0) {
    Red "pre-commit: unresolved merge-conflict markers in staged files:"
    $markerFiles | ForEach-Object { Write-Host $_ }
    Write-Host ""
    Write-Host "Resolve the conflicts and re-stage before committing."
    exit 1
}

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

# ---- go vet, per build-tag combination ----
# Bare `go vet ./...` skips every build-tagged file, and `go build
# -tags=...` skips _test.go files -- so a symbol rename that a tagged
# test still references passes both and only fails when that suite
# finally runs (the v0.58.1 retag class). vet-tags.ps1 discovers every
# tag combo in the tree and type-checks each one; results are cached
# by the Go build cache, so repeat runs cost seconds.
& (Join-Path $PSScriptRoot 'vet-tags.ps1')
if ($LASTEXITCODE -ne 0) {
    Red "vet-tags failed (a build-tagged file no longer type-checks)."
    exit 1
}

# ---- CI coverage guards ----
# These two run in CI's Lint job; without them here, adding
# integration-tagged tests in an uncovered package (or a postgis test
# whose name escapes the job's -run filter) passes the full local gate
# and only fails in CI. They are POSIX-sh scripts; Git for Windows
# ships sh.exe, so soft-skip only if sh is genuinely absent (CI remains
# the source of truth, same policy as the golangci-lint soft-skip).
$sh = Get-Command sh -ErrorAction SilentlyContinue
if ($sh) {
    & sh scripts/check-shard-coverage.sh
    if ($LASTEXITCODE -ne 0) {
        Red "check-shard-coverage failed (integration tests outside every CI shard)."
        exit 1
    }
    & sh scripts/check-postgis-coverage.sh
    if ($LASTEXITCODE -ne 0) {
        Red "check-postgis-coverage failed (postgis test would never run in CI)."
        exit 1
    }
} else {
    Write-Host "sh not found; skipping coverage guards (they still run in CI's Lint job)" -ForegroundColor Yellow
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

# ---- golangci-lint (mirrors CI's Lint job) ----
# Pre-v0.39.1 this gate was missing — gofumpt + go vet caught
# formatting + obvious-bug issues but unused-symbol / revive / etc.
# only ran in CI, which meant lint-only failures slipped through the
# local pre-commit gate for several releases (v0.34.0 → v0.39.0
# inclusive). Adding it here matches the CI job exactly.
#
# Soft-skip when golangci-lint isn't installed (developer convenience)
# rather than hard-block — the CI job is still the source of truth.
$lint = Get-Command golangci-lint -ErrorAction SilentlyContinue
if ($lint) {
    & golangci-lint run
    if ($LASTEXITCODE -ne 0) {
        Red "golangci-lint failed."
        Write-Host "Run: golangci-lint run  (to see the failures inline)"
        exit 1
    }
} else {
    Write-Host "golangci-lint not installed; skipping (install: https://golangci-lint.run/welcome/install/)" -ForegroundColor Yellow
}

Green "pre-commit: OK"
