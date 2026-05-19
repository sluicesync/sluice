#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Spin a runner-less VALIDATION VM from the sealed golden VHDX - the
  local-Hyper-V replacement for the paid Vultr pre-release box. This
  VM runs the release-validation runbook over SSH (incl. the
  `integration vstream` coverage CI does not gate); it registers NO
  GitHub Actions runner.

.DESCRIPTION
  Copies the golden VHDX to a per-VM disk, optionally grows it, builds
  a MINIMAL per-instance seed (hostname only - NO runner token, NO
  config.sh), creates a Gen2 VM, boots. The golden already bakes
  docker + the admin user/SSH key; system Go is NOT baked (runners get
  it via actions/setup-go) - install it once per validation VM as the
  runbook's step 0 (see docs/dev/notes/release-validation-on-vultr.md).
  Destructive per VM name; supports -WhatIf. Needs neither qemu-img
  nor gh (pure golden copy, no token mint).

.EXAMPLE
  .\New-ValidationVM.ps1 -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
      -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub) -WhatIf

.EXAMPLE
  .\New-ValidationVM.ps1 -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
      -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub) -DiskGB 120 -CpuCount 6 -MemoryBytes 16GB
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)] [string] $GoldenVhdx,
    [string] $Name         = 'sluice-validation',
    [string] $AdminSshPublicKey,
    [int]    $DiskGB       = 120,           # the runbook + vttestserver image (2 GB) want headroom
    [int]    $CpuCount     = 4,             # runbook is ~30 min; more cores shorten it
    [int64]  $MemoryBytes  = 12GB,
    [string] $SwitchName   = 'Default Switch',
    [string] $VmDiskDir    = 'C:\HyperV\validation'
)
. "$PSScriptRoot\lib\Common.ps1"
# Validation VM = pure golden copy, no token mint, no image convert.
Assert-Prereqs
if (-not (Test-Path $GoldenVhdx)) { throw "Golden VHDX not found: $GoldenVhdx (run Build-GoldenTemplate.ps1 first)." }

New-Item -ItemType Directory -Force -Path $VmDiskDir | Out-Null
$osVhdx = Join-Path $VmDiskDir "$Name-os.vhdx"
if ($PSCmdlet.ShouldProcess($osVhdx, "Copy golden VHDX -> per-VM disk")) {
    Copy-Item $GoldenVhdx $osVhdx -Force
    if ($DiskGB -gt 0) { Resize-VHD -Path $osVhdx -SizeBytes ($DiskGB * 1GB) }  # guest auto-grows root
}

$userData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\user-data-validation.template" -Values @{
    HOSTNAME = $Name
}
$metaData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\meta-data.template" -Values @{
    INSTANCE_ID = "$Name-$(Get-Date -Format yyyyMMddHHmmss)"   # new id -> per-instance modules re-run
    HOSTNAME    = $Name
}
$seedVhdx = Join-Path (Get-SeedDir) "$Name-seed.vhdx"
if ($PSCmdlet.ShouldProcess($seedVhdx, "Build minimal per-instance seed (no runner)")) {
    New-SeedVhdx -UserData $userData -MetaData $metaData -OutVhdx $seedVhdx | Out-Null
}

$vm = New-RunnerVMObject -Name $Name -OsVhdx $osVhdx -SeedVhdx $seedVhdx `
        -CpuCount $CpuCount -MemoryBytes $MemoryBytes -SwitchName $SwitchName
if ($vm -and $PSCmdlet.ShouldProcess($Name, "Start VM")) {
    Start-VM -Name $Name
    Write-Host "Validation VM '$Name' started from golden - live in ~1-2 min. NO runner registered."
    Write-Host "Find its IP via the MAC->neighbor method (stock Ubuntu has no hv_kvp_daemon):"
    Write-Host "  `$mac=(Get-VMNetworkAdapter -VMName $Name).MacAddress;" `
        "Get-NetNeighbor | ? { (`$_.LinkLayerAddress -replace '[:-]','') -eq `$mac } | Select IPAddress,State"
    Write-Host "Then follow docs/dev/notes/release-validation-on-vultr.md (Local Hyper-V section)."
}
