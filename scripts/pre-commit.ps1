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

# ---- NUL-byte guard on staged text files ----
# AI-tooling parameter layers decode a literal backslash-u-0000 sequence
# into a real NUL byte; this has reached committed docs 4x (2026-07-16).
# A NUL in a text file flips grep/ripgrep into binary mode repo-wide.
$nulFiles = git diff --cached --name-only --diff-filter=ACM |
    Where-Object { $_ -match '\.(md|go|yml|yaml|sh|ps1|txt)$' } |
    Where-Object { (Test-Path $_) -and ([System.IO.File]::ReadAllBytes($_) -contains 0) }
if ($nulFiles.Count -gt 0) {
    Red "pre-commit: staged text file(s) contain a literal NUL byte:"
    $nulFiles | ForEach-Object { Write-Host $_ }
    Write-Host "Spell the escape in prose (backslash-u-0000) instead."
    exit 1
}

# ---- merge-conflict-marker guard on staged text files ----
# A stray conflict marker survived a manual resolution into a committed
# doc once (2026-07-17): the error-code doc-sync test parses table rows
# and skips other lines, so a leftover "<<<<<<< HEAD" rode a green gate.
$conflictFiles = git diff --cached --name-only --diff-filter=ACM |
    Where-Object { $_ -match '\.(md|go|yml|yaml|sh|ps1|txt)$' } |
    Where-Object { (Test-Path $_) -and ((Get-Content -LiteralPath $_) -match '^(<{7}|>{7})( |$)') }
if ($conflictFiles.Count -gt 0) {
    Red "pre-commit: staged text file(s) contain a merge-conflict marker:"
    $conflictFiles | ForEach-Object { Write-Host $_ }
    Write-Host "Finish resolving the conflict before committing."
    exit 1
}

# ---- gofumpt ----
$gofumpt = Get-Command gofumpt -ErrorAction SilentlyContinue
if (-not $gofumpt) {
    Red "gofumpt not installed."
    Write-Host "Install: go install mvdan.cc/gofumpt@latest"
    exit 1
}

# gofumpt walks the FILESYSTEM (unlike go vet's module-scoped ./...),
# so exclude the gitignored .claude/ agent worktrees — live worktree
# agents keep mid-edit files there that are not this tree's problem
# (same reasoning as golangci-lint's .claude/ exclusion, v0.99.236).
$unformatted = & gofumpt -l . | Where-Object { $_ -notmatch '^\.claude[\\/]' -and $_ -notmatch '[\\/]\.claude[\\/]' }
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
# integration-tagged tests in an uncovered package (or a tagged test
# whose name escapes its job's -run filter) passes the full local gate
# and only fails in CI — exactly how the flatfile COVERED_PACKAGES miss
# shipped to a red main (6080ee74 -> ee6468ed) while this block still
# soft-skipped on a missing `sh`. They are POSIX-sh scripts; `sh` is
# rarely on PATH on Windows, but Git for Windows (already a hard
# prerequisite of a git hook) bundles it — so resolve <git-root>\bin\
# sh.exe from git.exe's location before giving up. That wrapper is the
# one that sets up the MSYS PATH; usr\bin\sh.exe is NOT equivalent (it
# can't find dirname/awk/xargs and the guards fail spuriously). If sh
# STILL cannot be found, FAIL with instructions — a silently skipped
# CI-required guard is how the miss above reached main.
$sh = Get-Command sh -ErrorAction SilentlyContinue
if ($sh) {
    $shExe = $sh.Source
} else {
    $shExe = $null
    $git = Get-Command git -ErrorAction SilentlyContinue
    if ($git) {
        # git.exe lives at <root>\cmd\git.exe, <root>\bin\git.exe, or
        # <root>\mingw64\bin\git.exe — walk up to two levels looking
        # for the bin\sh.exe wrapper beside/above it.
        $dir = Split-Path $git.Source -Parent
        foreach ($root in @((Split-Path $dir -Parent), (Split-Path (Split-Path $dir -Parent) -Parent))) {
            if (-not $root) { continue }
            $candidate = Join-Path $root 'bin\sh.exe'
            if (Test-Path $candidate) { $shExe = $candidate; break }
        }
    }
}
if (-not $shExe) {
    Red "sh not found (neither on PATH nor under Git for Windows' bin\) - cannot run the CI coverage guards."
    Write-Host "check-shard-coverage.sh and check-run-filter-coverage.sh are required CI Lint gates;"
    Write-Host "skipping them locally is how a shard-coverage miss ships to a red main."
    Write-Host "Install Git for Windows (bundles sh.exe) or put sh on PATH, then re-run."
    exit 1
}
& $shExe scripts/check-shard-coverage.sh
if ($LASTEXITCODE -ne 0) {
    Red "check-shard-coverage failed (integration tests outside every CI shard)."
    exit 1
}
& $shExe scripts/check-run-filter-coverage.sh
if ($LASTEXITCODE -ne 0) {
    Red "check-run-filter-coverage failed (a tagged test would never run in any workflow)."
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
        # A stale analysis cache reports PHANTOM issues after a sibling
        # git worktree is removed (typecheck/unused findings pointing at
        # files that no longer exist — hit 4× with the worktree-agent
        # flow). Self-heal: clean the cache and retry ONCE. A cold-cache
        # run costs minutes, so this only happens on failure — real
        # findings reproduce identically on the retry.
        Write-Host "golangci-lint failed; retrying once with a clean cache (stale-worktree phantom check)..." -ForegroundColor Yellow
        & golangci-lint cache clean
        & golangci-lint run
        if ($LASTEXITCODE -ne 0) {
            Red "golangci-lint failed."
            Write-Host "Run: golangci-lint run  (to see the failures inline)"
            exit 1
        }
    }
} else {
    Write-Host "golangci-lint not installed; skipping (install: https://golangci-lint.run/welcome/install/)" -ForegroundColor Yellow
}

Green "pre-commit: OK"
