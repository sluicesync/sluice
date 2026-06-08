#Requires -Version 5.1
<#
.SYNOPSIS
    Run the gated Vitess-cluster integration suite across several Vitess
    server versions (roadmap item 1(e): the multi-version matrix).

.DESCRIPTION
    The vendored client stays vitess.io/vitess v0.24.1. Older servers are
    exercised via newer-client -> older-server skew, the direction Vitess
    supports for rolling upgrades (and the one PlanetScale itself runs).

    For each image the script:
      1. docker-pulls it (a non-existent tag -> SKIP, not FAIL);
      2. sets VITESS_LITE_IMAGE so the compose file + harness boot on it;
      3. runs the 'integration vitesscluster' suite;
      4. records PASS / FAIL / SKIP.

    The 'latest' canary is allowed to fail (it tracks an unreleased-relative
    -to-the-client server); a FAIL there does not fail the whole run. A FAIL
    on any pinned version does.

    This is a MANUAL / rig-driven sweep (heavy: each version boots a real
    5-container cluster). It is intentionally not wired into per-PR CI.

.PARAMETER Versions
    Vitess/lite image tags to sweep. KEEP THE PINNED MAJORS IN [v21..v24]
    and BUMP THE MINORS as upstream releases (verify tags exist at
    https://hub.docker.com/r/vitess/lite/tags). v24 must stay in lockstep
    with the vendored client major.

.PARAMETER Run
    -run filter passed to go test. Default exercises the whole cluster suite;
    narrow to e.g. 'TestVitessCluster_Bug27' for a quick geometry-only sweep.

.PARAMETER Timeout
    Per-version go test -timeout. Each version boots + tears down a cluster.

.EXAMPLE
    pwsh scripts/vitess-version-matrix.ps1
.EXAMPLE
    pwsh scripts/vitess-version-matrix.ps1 -Run TestVitessCluster_Bug27 -Versions vitess/lite:v23.0.3,vitess/lite:v24.0.1
#>
[CmdletBinding()]
param(
    [string[]]$Versions = @(
        'vitess/lite:v21.0.6',
        'vitess/lite:v22.0.4',
        'vitess/lite:v23.0.4',
        'vitess/lite:v24.0.1',
        'vitess/lite:latest'
    ),
    [string]$Run = 'TestVitessCluster',
    [string]$Timeout = '25m'
)

$ErrorActionPreference = 'Stop'

function Resolve-DockerBin {
    $cmd = Get-Command docker -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $rancher = 'C:\Program Files\Rancher Desktop\resources\resources\win32\bin\docker.exe'
    if (Test-Path $rancher) { return $rancher }
    throw 'docker not found on PATH (and no Rancher Desktop docker.exe). Cannot run the matrix.'
}

$dockerBin = Resolve-DockerBin
$repoRoot = Split-Path $PSScriptRoot -Parent
Write-Host "Vitess version matrix - client = vitess.io/vitess v0.24.1, run filter = '$Run'" -ForegroundColor Cyan
Write-Host "docker: $dockerBin" -ForegroundColor DarkGray
Write-Host ""

$results = @()
Push-Location $repoRoot
try {
    foreach ($image in $Versions) {
        $isCanary = $image -like '*:latest'
        Write-Host "=== $image ===" -ForegroundColor Yellow

        # Pre-pull so a non-existent tag is a clean SKIP, not a test FAIL.
        & $dockerBin pull $image 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) {
            Write-Host "  image not pullable -> SKIP" -ForegroundColor DarkYellow
            $results += [pscustomobject]@{ Image = $image; Result = 'SKIP'; Canary = $isCanary; Note = 'pull failed (tag missing?)' }
            continue
        }

        $env:VITESS_LITE_IMAGE = $image
        $start = Get-Date
        & go test -tags 'integration vitesscluster' -count=1 -timeout $Timeout -run $Run ./internal/engines/mysql/... 2>&1 | Out-Host
        $code = $LASTEXITCODE
        $elapsed = [int]((Get-Date) - $start).TotalSeconds

        if ($code -eq 0) {
            Write-Host "  PASS (${elapsed}s)" -ForegroundColor Green
            $results += [pscustomobject]@{ Image = $image; Result = 'PASS'; Canary = $isCanary; Note = "${elapsed}s" }
        }
        else {
            $tag = if ($isCanary) { 'FAIL (canary - non-fatal)' } else { 'FAIL' }
            Write-Host "  $tag (${elapsed}s, exit $code)" -ForegroundColor Red
            $results += [pscustomobject]@{ Image = $image; Result = 'FAIL'; Canary = $isCanary; Note = "exit $code, ${elapsed}s" }
        }
    }
}
finally {
    Remove-Item Env:\VITESS_LITE_IMAGE -ErrorAction SilentlyContinue
    Pop-Location
}

Write-Host ""
Write-Host "===== Vitess version matrix summary =====" -ForegroundColor Cyan
$results | Format-Table -AutoSize Image, Result, Canary, Note | Out-Host

# Fail the run only if a PINNED (non-canary) version FAILED. SKIPs and a
# canary FAIL are tolerated.
$hardFailures = @($results | Where-Object { $_.Result -eq 'FAIL' -and -not $_.Canary })
if ($hardFailures.Count -gt 0) {
    Write-Host "$($hardFailures.Count) pinned version(s) FAILED." -ForegroundColor Red
    exit 1
}
Write-Host "All pinned versions passed (skips/canary tolerated)." -ForegroundColor Green
exit 0
