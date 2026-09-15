# vet-tags.ps1 -- PowerShell mirror of scripts/vet-tags.sh.
#
# Type-checks every build-tag combination in use, including tagged
# _test.go files (which `go build -tags=...` skips -- the v0.58.1 retag
# class). Tag combinations are DISCOVERED from the tree via git grep,
# not hand-maintained, so a new tagged suite is gated automatically.
# See vet-tags.sh for the full rationale (incl. why per-combo passes
# rather than one all-tags superset).
#
# Pure ASCII; compatible with Windows PowerShell 5.1 and pwsh 7+.

$ErrorActionPreference = 'Stop'
# Push/Pop rather than Set-Location: pre-commit.ps1 dot-invokes this
# script in-process, and Set-Location would leak into the caller.
Push-Location (Join-Path $PSScriptRoot '..')
trap { Pop-Location; break }

$lines = & git grep -h '^//go:build ' -- '*.go'
if ($LASTEXITCODE -ne 0 -or -not $lines) {
    # Guard against vacuous success: this repo always has tagged files,
    # so empty discovery means discovery itself broke.
    Write-Host 'vet-tags: discovery returned no //go:build expressions -- refusing to pass vacuously.' -ForegroundColor Red
    Pop-Location
    exit 1
}

# GOOS/GOARCH constraints (e.g. `windows`, `!windows`, `linux`, `amd64`) are
# selected by the toolchain's GOOS/GOARCH, NOT passed via -tags, and the
# default `go vet ./...` already type-checks them for the runner's platform.
# Strip them -- and their negations -- before the conjunction parser below so a
# platform-gated file like `//go:build !windows` doesn't trip the guard or get
# mis-treated as a -tags value. A pure-GOOS expression collapses to empty and
# is dropped; a mixed one reduces to its real tag set. Mirror of vet-tags.sh.
$goos = @{}
foreach ($t in @(
    'aix','android','darwin','dragonfly','freebsd','hurd','illumos','ios','js',
    'linux','nacl','netbsd','openbsd','plan9','solaris','wasip1','wasm','windows',
    'zos','386','amd64','arm','arm64','loong64','mips','mips64','mips64le','mipsle',
    'ppc64','ppc64le','riscv64','s390x','cgo','gc','gccgo','unix','boringcrypto')) {
    $goos[$t] = $true
}
$lines = $lines | ForEach-Object {
    $terms = ($_ -replace '^//go:build ', '') -split '\s*&&\s*'
    $kept = $terms | Where-Object { -not $goos[($_ -replace '^!', '')] }
    if ($kept) { "//go:build " + ($kept -join ' && ') }
} | Sort-Object -Unique
if (-not $lines) {
    Write-Host 'vet-tags: no -tags combinations after GOOS strip -- refusing to pass vacuously.' -ForegroundColor Red
    Pop-Location
    exit 1
}

# Expand top-level disjunction: `a || b` is satisfied by the tag set {a} OR
# by {b}, so each disjunct is vetted as its own combination. Mirror of
# vet-tags.sh; see that file for why (the shared Neki backup/restore core is
# `integration || nekiverify`). Only TOP-LEVEL disjunction is handled, which
# is why the grouping guard below stays.
$lines = $lines | ForEach-Object {
    ($_ -replace '^//go:build ', '') -split '\s*\|\|\s*' |
        Where-Object { $_ } |
        ForEach-Object { "//go:build $_" }
} | Sort-Object -Unique

# What remains must be simple conjunctions (`a && b`) -- refuse loudly on
# negation/grouping, or on a stray `|` the expansion above could not have
# produced, rather than silently skipping.
$bad = $lines | Where-Object { $_ -match '[!|()]' }
if ($bad) {
    Write-Host 'vet-tags: unsupported //go:build expression (negation/grouping):' -ForegroundColor Red
    $bad | ForEach-Object { Write-Host "  $_" }
    Write-Host 'vet-tags: extend scripts/vet-tags.ps1 (and vet-tags.sh) to cover it.'
    Pop-Location
    exit 1
}

$combos = $lines |
    ForEach-Object { ($_ -replace '^//go:build ', '') -replace ' *&& *', ',' } |
    Sort-Object -Unique

# `./...` is MODULE-scoped and reaches the gitignored scratch packages under
# workspace/. CI runs the .sh on a fresh checkout with no workspace/, but this
# script is what scripts/pre-commit.ps1 runs on the primary development
# machine, where a throwaway Go main that stopped compiling would fail the
# gate and nothing else — a local-only block whose only escape is --no-verify
# (audit DDD-8; the .sh got this exclusion and this mirror did not, audit
# 2026-09-15 X1). golangci-lint excludes workspace/ for the same reason.
#
# The package list is computed PER COMBO, under that combo's tags, for the
# reason vet-tags.sh records (A0909-TCI-M-1): a package that exists only
# under a tag is absent from an untagged `go list`. And it carries the same
# anti-vacuity floor — a `go list` that silently matched almost nothing would
# otherwise leave `go vet` a truncated universe and print a green line.
# scripts/check-local-gate-parity.sh holds this file to both properties.
$failed = $false
foreach ($tags in $combos) {
    $pkgs = @(& go list "-tags=$tags" ./... | Where-Object { $_ -notmatch '/workspace/' })
    if ($pkgs.Count -lt 40) {
        Write-Host "vet-tags: package list for -tags=$tags came back with only $($pkgs.Count) entries; expected the whole module (~60)." -ForegroundColor Red
        Write-Host "  'go list -tags=$tags ./...' likely failed -- fix that rather than vetting a truncated universe."
        Pop-Location
        exit 1
    }
    Write-Host "vet-tags: go vet -tags=$tags <module, minus workspace/, listed under those tags: $($pkgs.Count) pkgs>"
    & go vet "-tags=$tags" @pkgs
    if ($LASTEXITCODE -ne 0) { $failed = $true }
}

Pop-Location
if ($failed) {
    Write-Host 'vet-tags: FAILED -- one or more tag combinations do not type-check.' -ForegroundColor Red
    exit 1
}
