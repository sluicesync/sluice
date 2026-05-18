#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Spin one runner from a sealed golden VHDX (workflow B, step 2 — the
  fast path: ~1-2 min to a live runner, repeat per runner).

.DESCRIPTION
  Copies the golden VHDX to a per-VM disk, optionally grows it, builds
  a MINIMAL per-instance seed (new instance-id -> cloud-init re-runs
  per-instance modules: hostname + fresh runner-token registration
  only; docker/packages/prune timers are already baked in), creates a
  Gen2 VM, boots. Destructive per VM name; supports -WhatIf.

.EXAMPLE
  1..4 | % { .\New-RunnerFromTemplate.ps1 -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
              -Name ("runner-{0:00}" -f $_) -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub) -WhatIf }

.EXAMPLE
  # Org-scoped fleet — one golden, runners shared across all org repos
  # (needs gh token admin:org: gh auth refresh -h github.com -s admin:org).
  1..3 | % { .\New-RunnerFromTemplate.ps1 -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
              -Name ("runner-{0:00}" -f $_) -Org orware-code -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub) }
#>
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'Repo')]
param(
    [Parameter(Mandatory)] [string] $GoldenVhdx,
    [Parameter(Mandatory)] [string] $Name,
    [string] $AdminSshPublicKey,
    [string] $AdminUser    = 'runner',
    [Parameter(ParameterSetName = 'Repo')] [string] $Repo = 'orware/sluice',
    [Parameter(ParameterSetName = 'Org', Mandatory)] [string] $Org,
    [string] $RunnerLabels = 'sluice-linux',
    [int]    $DiskGB       = 0,            # 0 = keep golden size
    [int]    $CpuCount     = 4,
    [int64]  $MemoryBytes  = 12GB,
    [string] $SwitchName   = 'Default Switch',
    [string] $VmDiskDir    = 'C:\HyperV\runners'
)
. "$PSScriptRoot\lib\Common.ps1"
Assert-Prereqs
if (-not (Test-Path $GoldenVhdx)) { throw "Golden VHDX not found: $GoldenVhdx (run Build-GoldenTemplate.ps1 first)." }

New-Item -ItemType Directory -Force -Path $VmDiskDir | Out-Null
$osVhdx = Join-Path $VmDiskDir "$Name-os.vhdx"
if ($PSCmdlet.ShouldProcess($osVhdx, "Copy golden VHDX -> per-VM disk")) {
    Copy-Item $GoldenVhdx $osVhdx -Force
    if ($DiskGB -gt 0) { Resize-VHD -Path $osVhdx -SizeBytes ($DiskGB * 1GB) }  # guest auto-grows root
}

if ($PSCmdlet.ParameterSetName -eq 'Org') {
    $registerUrl = "https://github.com/$Org"
    $token       = New-RunnerToken -Org $Org
    Write-Host "Registration scope: org '$Org' (runner usable across the org's repos)"
} else {
    $registerUrl = "https://github.com/$Repo"
    $token       = New-RunnerToken -Repo $Repo
    Write-Host "Registration scope: repo '$Repo'"
}
$userData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\user-data-clone.template" -Values @{
    HOSTNAME      = $Name
    ADMIN_USER    = $AdminUser
    REPO_URL      = $registerUrl
    RUNNER_LABELS = $RunnerLabels
    RUNNER_TOKEN  = $token
}
$metaData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\meta-data.template" -Values @{
    INSTANCE_ID = "$Name-$(Get-Date -Format yyyyMMddHHmmss)"   # new id -> per-instance modules re-run
    HOSTNAME    = $Name
}
$seedVhdx = Join-Path (Get-SeedDir) "$Name-seed.vhdx"
if ($PSCmdlet.ShouldProcess($seedVhdx, "Build minimal per-instance seed")) {
    New-SeedVhdx -UserData $userData -MetaData $metaData -OutVhdx $seedVhdx | Out-Null
}

$vm = New-RunnerVMObject -Name $Name -OsVhdx $osVhdx -SeedVhdx $seedVhdx `
        -CpuCount $CpuCount -MemoryBytes $MemoryBytes -SwitchName $SwitchName
if ($vm -and $PSCmdlet.ShouldProcess($Name, "Start VM")) {
    Start-VM -Name $Name
    Write-Host "Runner '$Name' started from golden template — live in ~1-2 min. Confirm Idle in repo Settings -> Actions -> Runners."
}
