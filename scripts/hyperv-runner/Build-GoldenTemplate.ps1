#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Build + seal a golden runner VHDX (workflow B, step 1 — README.md).

.DESCRIPTION
  Builds one cloud-image VM that fully provisions (docker, packages,
  daily cron, hourly disk-pressure guard, root grown) but registers NO
  runner. Waits for provisioning to finish, runs `cloud-init clean` so
  the next boot is a fresh instance, shuts down, compacts, and copies
  the disk out as a sealed golden template. Clones made from it
  (New-RunnerFromTemplate.ps1) come up in ~1-2 min.

  The golden image deliberately has no runner registration — clones
  register with their own fresh token, so there is no ghost runner and
  no token baked into the template.

  Requires SSH reachability to the transient build VM to run the seal
  steps (pass -BuildVmIp once it boots, or let it prompt).

.EXAMPLE
  .\Build-GoldenTemplate.ps1 -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub) `
     -AdminSshKeyPath ~/.ssh/id_ed25519 -OutGoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)] [string] $AdminSshPublicKey,
    [Parameter(Mandatory)] [string] $AdminSshKeyPath,
    [Parameter(Mandatory)] [string] $OutGoldenVhdx,
    [string] $AdminUser    = 'runner',
    [int]    $DiskGB       = 120,
    [int]    $DiskPrunePct = 70,
    [string] $SwitchName   = 'Default Switch',
    [string] $BuildVmName  = 'sluice-golden-build'
)
. "$PSScriptRoot\lib\Common.ps1"
Assert-Prereqs

# 1. Build a provision-only VM (user-data with the runner runcmd
#    stripped: we render the full template but pass an empty token and
#    a NO-OP repo so config.sh is skipped — simplest is a dedicated
#    provision-only template). Reuse the full template but disable the
#    runner block via a sentinel the template honours when token empty.
$runnerVer = Resolve-RunnerVersion
$qcow   = Get-UbuntuCloudImage
$osVhdx = Join-Path (Get-WorkRoot) "$BuildVmName-os.vhdx"
if ($PSCmdlet.ShouldProcess($osVhdx, "Convert+resize cloud image")) {
    Convert-CloudImageToVhdx -Qcow2Path $qcow -OutVhdx $osVhdx -SizeGB $DiskGB | Out-Null
}

# Provision-only seed: same provisioning, NO runner registration.
# (cloud-init/user-data-golden.template = full template minus the 3
# runner runcmd lines; kept as a separate file for auditability.)
$userData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\user-data-golden.template" -Values @{
    HOSTNAME         = $BuildVmName
    ADMIN_USER       = $AdminUser
    ADMIN_SSH_PUBKEY = $AdminSshPublicKey.Trim()
    RUNNER_VERSION   = $runnerVer
    DISK_PRUNE_PCT   = $DiskPrunePct
}
$metaData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\meta-data.template" -Values @{
    INSTANCE_ID = "$BuildVmName-$(Get-Date -Format yyyyMMddHHmmss)"
    HOSTNAME    = $BuildVmName
}
$seedVhdx = Join-Path (Get-SeedDir) "$BuildVmName-seed.vhdx"
if ($PSCmdlet.ShouldProcess($seedVhdx, "Build provision-only seed")) {
    New-SeedVhdx -UserData $userData -MetaData $metaData -OutVhdx $seedVhdx | Out-Null
}
New-RunnerVMObject -Name $BuildVmName -OsVhdx $osVhdx -SeedVhdx $seedVhdx -SwitchName $SwitchName | Out-Null
if (-not $PSCmdlet.ShouldProcess($BuildVmName, "Start build VM + wait for provisioning")) { return }
Start-VM -Name $BuildVmName

Write-Host ""
Write-Host "Build VM '$BuildVmName' starting. Find its IP (Get-VMNetworkAdapter -VMName $BuildVmName).IPAddresses,"
Write-Host "then SEAL it (run on the build VM over SSH as $AdminUser):"
Write-Host @"
  sudo cloud-init status --wait
  sudo docker system prune -af --volumes
  sudo cloud-init clean --logs --seed
  sudo rm -f /var/log/runner-provision-complete
  sudo shutdown -h now
"@
Write-Host "After it powers off, finalize the golden VHDX:"
Write-Host @"
  Stop-VM $BuildVmName -TurnOff -ErrorAction SilentlyContinue
  Optimize-VHD -Path '$osVhdx' -Mode Full          # compact
  New-Item -ItemType Directory -Force -Path (Split-Path '$OutGoldenVhdx') | Out-Null
  Copy-Item '$osVhdx' '$OutGoldenVhdx' -Force
  Remove-VM $BuildVmName -Force                     # transient build VM no longer needed
"@
Write-Host "Golden artifact -> $OutGoldenVhdx . Spin the fleet with New-RunnerFromTemplate.ps1."
Write-Warning "The seal/copy steps are intentionally explicit (single-shot, hard to auto-verify headless). Run them by hand once, then the fleet is one command each."
