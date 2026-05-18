<#
  Shared helpers for the Hyper-V runner build scripts.
  Dot-source:  . "$PSScriptRoot\lib\Common.ps1"

  Targets Windows PowerShell 5.1+ / PowerShell 7+. Hyper-V cmdlets
  require an elevated session with the Hyper-V module installed.
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# .work/ holds the image cache + generated per-VM seed VHDXs (tokens
# inside). Gitignored. One place so cleanup is trivial.
$script:WorkRoot = Join-Path $PSScriptRoot '..\.work' | Resolve-Path -ErrorAction SilentlyContinue
if (-not $script:WorkRoot) {
    $script:WorkRoot = (New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot '..\.work')).FullName
}
$script:CacheDir = New-Item -ItemType Directory -Force -Path (Join-Path $script:WorkRoot 'cache') | Select-Object -ExpandProperty FullName
$script:SeedDir  = New-Item -ItemType Directory -Force -Path (Join-Path $script:WorkRoot 'seeds') | Select-Object -ExpandProperty FullName

# Ubuntu 24.04 LTS generic cloud image (qcow2). Single growable ext4
# root - no LVM, so cloud-init grows it to fill the VHDX on first boot.
$script:CloudImgUrl = 'https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-amd64.img'

function Assert-Prereqs {
    # Admin + Hyper-V are always required. qemu-img is needed ONLY by
    # the cloud-image build path (Convert-CloudImageToVhdx); the
    # from-template path just Copy-Item's the golden and must not
    # demand it. gh is needed ONLY when a token is minted here; a
    # caller passing a pre-minted -RegistrationToken needs no gh.
    # Each entry script opts in to exactly what it uses.
    [CmdletBinding()] param(
        [switch] $RequireQemu,
        [switch] $RequireGh
    )
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    if (-not (New-Object Security.Principal.WindowsPrincipal($id)).IsInRole(
            [Security.Principal.WindowsBuiltinRole]::Administrator)) {
        throw "Must run elevated (Hyper-V cmdlets require Administrator)."
    }
    if (-not (Get-Command Get-VM -ErrorAction SilentlyContinue)) {
        throw "Hyper-V PowerShell module not available. Enable the Hyper-V feature."
    }
    if ($RequireQemu -and -not (Get-Command qemu-img -ErrorAction SilentlyContinue)) {
        throw "qemu-img not on PATH. Install: winget install --id cloudbase.qemu-img (or SoftwareFreedomConservancy.QEMU), then ensure its dir is on PATH in a new shell. Convert-VHD cannot read qcow2."
    }
    if ($RequireGh -and -not (Get-Command gh -ErrorAction SilentlyContinue)) {
        throw "gh CLI not on PATH (needed to mint a runner registration token; pass -RegistrationToken to skip gh)."
    }
}

function Get-UbuntuCloudImage {
    # Download once, cache. Returns the local qcow2 path.
    [CmdletBinding()] param()
    $dst = Join-Path $script:CacheDir 'ubuntu-24.04-cloudimg-amd64.img'
    if (Test-Path $dst) {
        Write-Verbose "cloud image cached: $dst"
        return $dst
    }
    Write-Host "Downloading Ubuntu 24.04 cloud image (~600 MB, one-time)..."
    Invoke-WebRequest -Uri $script:CloudImgUrl -OutFile $dst -UseBasicParsing
    return $dst
}

function Convert-CloudImageToVhdx {
    # qcow2 -> dynamic VHDX, then resize to $SizeGB. Returns VHDX path.
    [CmdletBinding()] param(
        [Parameter(Mandatory)] [string] $Qcow2Path,
        [Parameter(Mandatory)] [string] $OutVhdx,
        [int] $SizeGB = 120
    )
    if (Test-Path $OutVhdx) { Remove-Item $OutVhdx -Force }
    & qemu-img convert -p -O vhdx -o subformat=dynamic $Qcow2Path $OutVhdx
    if ($LASTEXITCODE -ne 0) { throw "qemu-img convert failed ($LASTEXITCODE)" }
    # Grow the virtual disk; the guest's ext4 root auto-expands on boot.
    Resize-VHD -Path $OutVhdx -SizeBytes ($SizeGB * 1GB)
    return $OutVhdx
}

function New-RunnerToken {
    # Short-lived (~1h) GitHub Actions registration token. Minted
    # just-in-time; never persisted to the repo. Repo OR org scope:
    # an org token registers a runner usable across that org's repos
    # (one pool, many projects). NOTE: the org endpoint requires the
    # gh token to carry `admin:org` - `repo` + `read:org` alone returns
    # 403. Grant it once: gh auth refresh -h github.com -s admin:org
    [CmdletBinding(DefaultParameterSetName = 'Repo')]
    param(
        [Parameter(ParameterSetName = 'Repo')] [string] $Repo = 'orware/sluice',
        [Parameter(ParameterSetName = 'Org', Mandatory)] [string] $Org
    )
    if ($PSCmdlet.ParameterSetName -eq 'Org') {
        $api = "orgs/$Org/actions/runners/registration-token"
        $scopeDesc = "org '$Org' (needs gh token scope admin:org)"
    } else {
        $api = "repos/$Repo/actions/runners/registration-token"
        $scopeDesc = "repo '$Repo'"
    }
    $t = & gh api -X POST $api --jq .token 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($t)) {
        throw "Failed to mint runner registration token for $scopeDesc (is gh authed with sufficient scope?)."
    }
    return $t.Trim()
}

function Resolve-RunnerVersion {
    # Latest actions/runner version (no leading 'v').
    [CmdletBinding()] param()
    $tag = & gh api repos/actions/runner/releases/latest --jq .tag_name 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($tag)) {
        throw "Failed to resolve latest actions/runner version via gh."
    }
    return $tag.Trim().TrimStart('v')
}

function Expand-Template {
    # Replace {{KEY}} placeholders from a hashtable. Throws if any
    # {{...}} remains unrendered (catches typos before they hit a VM).
    [CmdletBinding()] param(
        [Parameter(Mandatory)] [string] $TemplatePath,
        [Parameter(Mandatory)] [hashtable] $Values
    )
    $text = Get-Content -Raw -Path $TemplatePath
    foreach ($k in $Values.Keys) {
        $text = $text.Replace('{{' + $k + '}}', [string]$Values[$k])
    }
    $m = [regex]::Match($text, '\{\{[A-Z_]+\}\}')
    if ($m.Success) { throw "Unrendered placeholder $($m.Value) in $TemplatePath" }
    return $text
}

function New-SeedVhdx {
    # Build a tiny FAT32 VHDX labeled CIDATA holding user-data +
    # meta-data - cloud-init's NoCloud datasource auto-detects it.
    # Pure PowerShell, no oscdimg/ADK dependency.
    [CmdletBinding()] param(
        [Parameter(Mandatory)] [string] $UserData,
        [Parameter(Mandatory)] [string] $MetaData,
        [Parameter(Mandatory)] [string] $OutVhdx
    )
    if (Test-Path $OutVhdx) { Remove-Item $OutVhdx -Force }
    New-VHD -Path $OutVhdx -SizeBytes 64MB -Dynamic | Out-Null
    $disk = Mount-VHD -Path $OutVhdx -Passthru | Get-Disk
    try {
        Initialize-Disk -Number $disk.Number -PartitionStyle MBR
        $part = New-Partition -DiskNumber $disk.Number -UseMaximumSize -AssignDriveLetter
        Format-Volume -DriveLetter $part.DriveLetter -FileSystem FAT32 `
            -NewFileSystemLabel 'CIDATA' -Confirm:$false | Out-Null
        $root = "$($part.DriveLetter):\"
        # LF line endings - cloud-init is strict about CRLF in user-data.
        [IO.File]::WriteAllText((Join-Path $root 'user-data'), ($UserData -replace "`r`n", "`n"))
        [IO.File]::WriteAllText((Join-Path $root 'meta-data'), ($MetaData -replace "`r`n", "`n"))
    }
    finally {
        Dismount-VHD -Path $OutVhdx
    }
    return $OutVhdx
}

function New-RunnerVMObject {
    # Create the Gen2 VM: Ubuntu needs the 3rd-party UEFI CA for Secure
    # Boot (kept ON, not disabled). OS disk + CIDATA seed attached.
    [CmdletBinding(SupportsShouldProcess)] param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string] $OsVhdx,
        [Parameter(Mandatory)] [string] $SeedVhdx,
        [int] $CpuCount = 4,
        [int64] $MemoryBytes = 12GB,
        [string] $SwitchName = 'Default Switch'
    )
    if (Get-VM -Name $Name -ErrorAction SilentlyContinue) {
        if ($PSCmdlet.ShouldProcess($Name, "Remove existing VM (replace)")) {
            Stop-VM -Name $Name -TurnOff -Force -ErrorAction SilentlyContinue
            Remove-VM -Name $Name -Force
        }
    }
    if (-not $PSCmdlet.ShouldProcess($Name, "Create Gen2 VM ($CpuCount vCPU, $([int]($MemoryBytes/1GB)) GB)")) {
        return
    }
    $vm = New-VM -Name $Name -Generation 2 -MemoryStartupBytes $MemoryBytes `
        -VHDPath $OsVhdx -SwitchName $SwitchName
    Set-VM -Name $Name -ProcessorCount $CpuCount -AutomaticStartAction Start `
        -AutomaticStopAction ShutDown -CheckpointType Disabled
    Set-VMFirmware -VMName $Name -SecureBootTemplate 'MicrosoftUEFICertificateAuthority'
    Add-VMHardDiskDrive -VMName $Name -Path $SeedVhdx
    Set-VMFirmware -VMName $Name -FirstBootDevice (Get-VMHardDiskDrive -VMName $Name | Where-Object Path -eq $OsVhdx)
    return $vm
}

# Re-export the well-known dirs for callers.
function Get-SeedDir  { $script:SeedDir }
function Get-WorkRoot { $script:WorkRoot }
