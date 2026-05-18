#Requires -RunAsAdministrator
<#
.SYNOPSIS
  Build one sluice self-hosted Linux runner VM from the Ubuntu cloud
  image, fully unattended (workflow A in README.md).

.DESCRIPTION
  Downloads/caches the Ubuntu 24.04 cloud image, converts to a dynamic
  VHDX, resizes it (guest root auto-grows to fill on first boot),
  builds a NoCloud seed (provisioning + freshly-minted runner token),
  creates a Gen2 VM (Secure Boot ON via the 3rd-party UEFI CA), boots.

  Destructive: replaces any existing VM of the same name. Supports
  -WhatIf - run that first.

.EXAMPLE
  .\New-RunnerVM.ps1 -Name runner-01 -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub) -WhatIf
.EXAMPLE
  .\New-RunnerVM.ps1 -Name runner-01 -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub)

.EXAMPLE
  # Org-scoped: runner usable across every repo in the org (needs a gh
  # token with admin:org - gh auth refresh -h github.com -s admin:org).
  .\New-RunnerVM.ps1 -Name runner-01 -Org orware-code -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub)
#>
[CmdletBinding(SupportsShouldProcess, DefaultParameterSetName = 'Repo')]
param(
    [Parameter(Mandatory)] [string] $Name,
    [Parameter(Mandatory)] [string] $AdminSshPublicKey,
    [string]  $AdminUser   = 'runner',
    [Parameter(ParameterSetName = 'Repo')] [string] $Repo = 'orware/sluice',
    [Parameter(ParameterSetName = 'Org', Mandatory)] [string] $Org,
    [string]  $RunnerLabels= 'sluice-linux',
    [int]     $DiskGB      = 120,
    [int]     $CpuCount    = 4,
    [int64]   $MemoryBytes = 12GB,
    [int]     $DiskPrunePct= 70,
    [string]  $SwitchName  = 'Default Switch'
)
. "$PSScriptRoot\lib\Common.ps1"
Assert-Prereqs -RequireQemu -RequireGh   # full build: converts cloud image + mints token

$hostName  = $Name
$runnerVer = Resolve-RunnerVersion
Write-Host "Runner version: $runnerVer ; host: $hostName ; disk: ${DiskGB}GB"

# OS disk
$qcow = Get-UbuntuCloudImage
$osVhdx = Join-Path (Get-WorkRoot) "$Name-os.vhdx"
if ($PSCmdlet.ShouldProcess($osVhdx, "Convert+resize cloud image -> ${DiskGB}GB VHDX")) {
    Convert-CloudImageToVhdx -Qcow2Path $qcow -OutVhdx $osVhdx -SizeGB $DiskGB | Out-Null
}

# Registration scope: org (shared across the org's repos) or repo.
if ($PSCmdlet.ParameterSetName -eq 'Org') {
    $registerUrl = "https://github.com/$Org"
    $token       = New-RunnerToken -Org $Org
    Write-Host "Registration scope: org '$Org' (runner usable across the org's repos)"
} else {
    $registerUrl = "https://github.com/$Repo"
    $token       = New-RunnerToken -Repo $Repo
    Write-Host "Registration scope: repo '$Repo'"
}

# Seed (token minted just-in-time; ~1h TTL)
$userData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\user-data.template" -Values @{
    HOSTNAME        = $hostName
    ADMIN_USER      = $AdminUser
    ADMIN_SSH_PUBKEY= $AdminSshPublicKey.Trim()
    REPO_URL        = $registerUrl
    RUNNER_LABELS   = $RunnerLabels
    RUNNER_VERSION  = $runnerVer
    RUNNER_TOKEN    = $token
    DISK_PRUNE_PCT  = $DiskPrunePct
}
$metaData = Expand-Template -TemplatePath "$PSScriptRoot\cloud-init\meta-data.template" -Values @{
    INSTANCE_ID = "$Name-$(Get-Date -Format yyyyMMddHHmmss)"
    HOSTNAME    = $hostName
}
$seedVhdx = Join-Path (Get-SeedDir) "$Name-seed.vhdx"
if ($PSCmdlet.ShouldProcess($seedVhdx, "Build NoCloud CIDATA seed")) {
    New-SeedVhdx -UserData $userData -MetaData $metaData -OutVhdx $seedVhdx | Out-Null
}

$vm = New-RunnerVMObject -Name $Name -OsVhdx $osVhdx -SeedVhdx $seedVhdx `
        -CpuCount $CpuCount -MemoryBytes $MemoryBytes -SwitchName $SwitchName
if ($vm -and $PSCmdlet.ShouldProcess($Name, "Start VM")) {
    Start-VM -Name $Name
    Write-Host ""
    Write-Host "VM '$Name' started. Provisioning is unattended (~3-6 min). Verify:"
    Write-Host "  Get-VM $Name | ft Name,State"
    Write-Host "  # in-guest (ssh $AdminUser@<ip>):"
    Write-Host "  cloud-init status --wait ; df -h / ; systemctl status 'actions.runner.*'"
    Write-Host "  # then confirm Idle in repo Settings -> Actions -> Runners"
}
