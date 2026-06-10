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

# Only simple conjunctions (`a && b`) are supported -- refuse loudly on
# negation/disjunction/grouping rather than silently skipping.
$bad = $lines | Where-Object { $_ -match '[!|()]' }
if ($bad) {
    Write-Host 'vet-tags: unsupported //go:build expression (negation/disjunction/grouping):' -ForegroundColor Red
    $bad | ForEach-Object { Write-Host "  $_" }
    Write-Host 'vet-tags: extend scripts/vet-tags.ps1 (and vet-tags.sh) to cover it.'
    Pop-Location
    exit 1
}

$combos = $lines |
    ForEach-Object { ($_ -replace '^//go:build ', '') -replace ' *&& *', ',' } |
    Sort-Object -Unique

$failed = $false
foreach ($tags in $combos) {
    Write-Host "vet-tags: go vet -tags=$tags ./..."
    & go vet "-tags=$tags" ./...
    if ($LASTEXITCODE -ne 0) { $failed = $true }
}

Pop-Location
if ($failed) {
    Write-Host 'vet-tags: FAILED -- one or more tag combinations do not type-check.' -ForegroundColor Red
    exit 1
}
