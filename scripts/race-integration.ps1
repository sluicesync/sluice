# Runs the integration test suite WITH the race detector inside a Linux
# container — the pre-tag gate for concurrency-touching chunks.
#
# Why this exists: the dev box is Windows + CGO_ENABLED=0 and CANNOT run
# `-race` (the detector is a CGO/TSan runtime); integration tests also
# need Docker. So "integration + -race" otherwise exists only on CI's
# Linux runner. For chunks touching concurrency (goroutines, channels,
# shared state, rotation/FSM, crash-recovery, failpoints) this gate must
# pass BEFORE a tag is cut — see CLAUDE.md "Concurrency chunks: the
# -race integration gate runs BEFORE the tag".
#
# Mechanism: a golang Linux container with gcc + CGO_ENABLED=1, the host
# Docker socket bind-mounted so testcontainers-go spawns sibling DB
# containers (Docker-out-of-Docker). Mirrors CI's
#   go test -tags=integration -race -count=1 -timeout=25m ./internal/...
#
# Usage:
#   .\scripts\race-integration.ps1                 # full ./internal/...
#   .\scripts\race-integration.ps1 -Run TestADR0046 -Timeout 40m
#   .\scripts\race-integration.ps1 -Count 5        # beat nondeterminism
#
# Exits non-zero if the suite fails. This is a manual pre-tag gate, not
# a git hook.
#
# Rancher Desktop caveats (this repo's dev environment):
#   - Requires the dockerd (moby) backend, NOT containerd — DooD needs a
#     Docker socket at /var/run/docker.sock. Switch in Rancher Desktop:
#     Preferences > Container Engine > dockerd(moby).
#   - testcontainers-go inside the container reaches sibling containers
#     via host.docker.internal (set as TESTCONTAINERS_HOST_OVERRIDE
#     below); RYUK is disabled (it vanishes under Rancher's daemon — the
#     same reason the bare-metal integration runs need it disabled).
#   - If DooD proves flaky on your local Rancher setup, DO NOT fight it:
#     fall through to the zero-infra alternative (CLAUDE.md option 2) —
#     push the work to `main` and wait for the CI Integration job green
#     BEFORE cutting the tag. One CI cycle either way; no retag churn.

[CmdletBinding()]
param(
    [string]$Run = '',
    [string]$Timeout = '30m',
    [int]$Count = 1,
    [string]$Image = 'golang:1.26'
)

$ErrorActionPreference = 'Stop'

function Red($msg)   { Write-Host $msg -ForegroundColor Red }
function Green($msg) { Write-Host $msg -ForegroundColor Green }
function Info($msg)  { Write-Host $msg -ForegroundColor Cyan }

# Locate docker.exe — on Rancher Desktop it is often absent from PATH.
$docker = 'docker'
if (-not (Get-Command $docker -ErrorAction SilentlyContinue)) {
    $rancher = 'C:\Program Files\Rancher Desktop\resources\resources\win32\bin\docker.exe'
    if (Test-Path $rancher) { $docker = $rancher }
    else { Red 'docker not found (PATH or Rancher Desktop). Is Rancher Desktop running with the dockerd(moby) backend?'; exit 1 }
}

# Confirm a Docker socket exists (DooD needs the moby backend, not containerd).
& $docker info *> $null
if ($LASTEXITCODE -ne 0) { Red 'docker info failed — start Rancher Desktop (dockerd/moby backend).'; exit 1 }

$repo = (Resolve-Path "$PSScriptRoot\..").Path
Info "repo:    $repo"
Info "image:   $Image"
Info "test:    go test -tags=integration -race -count=$Count -timeout=$Timeout $(if ($Run) {"-run '$Run' "})./internal/..."
Info 'Pulling toolchain image + installing gcc in-container (first run is slower)...'

# The in-container script: gcc for CGO/-race, the testcontainers env for
# DooD under Rancher, then the same invocation CI uses.
$runExpr = if ($Run) { "-run '$Run'" } else { '' }
$inner = @"
set -euo pipefail
apt-get update -qq && apt-get install -y -qq gcc >/dev/null
export CGO_ENABLED=1
export TESTCONTAINERS_RYUK_DISABLED=true
export TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal
cd /src
go test -tags=integration -race -count=$Count -timeout=$Timeout $runExpr ./internal/...
"@

# DooD: mount the host Docker socket + the repo; host.docker.internal so
# the in-container test process can reach testcontainers-spawned DBs.
# A named module-cache volume keeps re-runs fast.
& $docker run --rm `
    -v /var/run/docker.sock:/var/run/docker.sock `
    -v "${repo}:/src" `
    -v sluice-race-gomod:/go/pkg/mod `
    --add-host host.docker.internal:host-gateway `
    -w /src `
    $Image bash -c $inner

$code = $LASTEXITCODE
Write-Host ''
if ($code -eq 0) {
    Green 'race-integration: OK (concurrency -race gate passed — safe to tag)'
} else {
    Red "race-integration: FAILED (exit $code) — do NOT cut/force-move the tag."
    Red 'Read the failure as CI ground truth (three-phase Phase A); if DooD itself is flaky, use CLAUDE.md option 2 (push-first, wait for CI Integration green, then tag).'
}
exit $code
