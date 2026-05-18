# Hyper-V Ubuntu GitHub Actions runner — automated build

Reproducible, hands-off creation of the `sluice-linux` self-hosted CI
runner VMs. Replaces the hands-on Phase 2 of
[`docs/dev/self-hosted-runner.md`](../../docs/dev/self-hosted-runner.md).

> **⚠️ Review before running.** These scripts perform destructive
> Hyper-V operations (create/replace VMs, write VHDX files). They are
> inert until you execute them in an **elevated** PowerShell on the
> runner host. Every script supports `-WhatIf`. Run that first.

## The root cause this fixes

The recurring runner outage was: a Hyper-V `.vhdx` was extended, but
the **guest's partition / filesystem was never grown** (extending the
virtual disk does not touch the guest's partition table, LVM, or
ext4). The runner kept operating at the old filesystem size on a
bigger-but-unused disk and re-wedged on the same timeline once the CI
Integration job's pulled images (`mysql:8.0`, `postgres:16`,
`pgvector`, `postgis`, `vttestserver`, `vitess/lite`) refilled it.
Recreating manually from Ubuntu Server's *guided installer* would not
fix it — its default LVM layout leaves most of the volume group
**unallocated**, so even a fresh 120 GB VM comes up with a small root.

**This build avoids the trap structurally:**

1. It uses the **Ubuntu cloud image** (single growable ext4 root, no
   LVM), and cloud-init's `growpart` + `resizefs` modules grow root to
   fill the whole VHDX on first boot. Resize the VHDX → the guest
   follows automatically, forever.
2. Disk hygiene is **baked into the image**: the runbook's daily
   `docker system prune` cron *plus* an hourly disk-pressure trigger
   (prune when root > 70 %), because the daily-only cron is what
   failed before.

## Two workflows

### A. From the cloud image (first build / one-off)

`New-RunnerVM.ps1` — downloads + caches the Ubuntu 24.04 cloud image,
converts it to a dynamic VHDX, resizes it, builds a NoCloud seed disk
(hostname + provisioning + a freshly-minted runner token), creates a
Gen2 VM, and boots it. ~5–10 min the first time (image download
dominates), unattended.

### B. From a golden template (spinning the fleet — the fast path)

1. `Build-GoldenTemplate.ps1` — does one cloud-image build **without**
   registering a runner, waits for provisioning to finish (docker,
   prune timers, packages, root grown), runs `cloud-init clean`,
   shuts down, compacts the VHDX. Output: one sealed golden VHDX.
2. `New-RunnerFromTemplate.ps1 -GoldenVhdx ... -Name runner-0N` —
   copies the golden VHDX, attaches a *minimal* per-instance seed
   (new instance-id → cloud-init re-runs per-instance modules:
   hostname + fresh runner-token registration only), boots. ~1–2 min
   to a live runner, repeatable per runner (`-01` … `-04`).

The golden VHDX is the durable artifact: rebuild the whole fleet from
it in minutes after any host event, no re-download, no re-provision.

## Golden seal — the manual one-shot (step 1 detail)

`Build-GoldenTemplate.ps1` boots a transient build VM
(`sluice-golden-build`) and then **stops, printing these steps**: the
seal is deliberately by-hand (single-shot, hard to verify headless).
Do it once; thereafter the fleet is one command each.

1. **Wait for provisioning (~5–10 min: apt + docker + runner tarball),
   then find the VM's IP.** ⚠️ `(Get-VMNetworkAdapter
   ...).IPAddresses` **does not work** here — the stock Ubuntu cloud
   image runs no `hv_kvp_daemon`, so Hyper-V never learns the guest
   IP (it stays `[]` indefinitely even though DHCP succeeded). Resolve
   it from the VM's MAC via the host neighbor table instead:

   ```powershell
   $mac = (Get-VMNetworkAdapter -VMName sluice-golden-build).MacAddress
   Get-NetNeighbor -ErrorAction SilentlyContinue |
     Where-Object { ($_.LinkLayerAddress -replace '[:-]','') -eq $mac } |
     Select-Object IPAddress, State, InterfaceAlias
   ```

   That returns the `172.x` Default-Switch address. Sanity-check it's
   up and provisioning finished:

   ```powershell
   ssh -i $HOME\.ssh\id_ed25519 runner@<IP> 'cloud-init status'   # want: status: done
   ```

2. **SSH in as the admin user** (the key you passed
   `-AdminSshKeyPath`; default user `runner`) and seal:

   ```bash
   ssh -i ~/.ssh/id_ed25519 runner@<VM-IP>
   sudo cloud-init status --wait        # MUST report: status: done
   sudo docker system prune -af --volumes
   sudo cloud-init clean --logs --seed  # next boot re-runs per-instance
   sudo rm -f /var/log/golden-provision-complete /var/log/runner-provision-complete
   sudo shutdown -h now
   ```

   The golden writes `/var/log/golden-provision-complete` (not
   `runner-`); removing both covers the script's printed line and the
   real marker. `rm -f` on a missing path is a harmless no-op.

3. **Finalize the VHDX on the host** (after it powers off):

   ```powershell
   Stop-VM sluice-golden-build -TurnOff -ErrorAction SilentlyContinue
   Optimize-VHD -Path 'C:\code\sluice\scripts\hyperv-runner\.work\sluice-golden-build-os.vhdx' -Mode Full
   New-Item -ItemType Directory -Force -Path (Split-Path 'C:\HyperV\golden\sluice-runner-golden.vhdx') | Out-Null
   Copy-Item 'C:\code\sluice\scripts\hyperv-runner\.work\sluice-golden-build-os.vhdx' 'C:\HyperV\golden\sluice-runner-golden.vhdx' -Force
   Remove-VM sluice-golden-build -Force
   ```

   `Optimize-VHD` needs the VHDX detached (the `Remove-VM` is last on
   purpose; `Stop-VM -TurnOff` first guarantees it's not running).
   Result: `C:\HyperV\golden\sluice-runner-golden.vhdx`.

## Using the golden on another host

The golden VHDX is fully portable; `New-RunnerFromTemplate.ps1` is the
lean path (pure `Copy-Item` + `Resize-VHD` — **no `qemu-img`, no image
download**). The scripts are **local-only** (no `-ComputerName`/CIM
plumbing), so run them *on* each Hyper-V host. If the secondary host
has WinRM you can drive that over PSRemoting; if it only has RDP open
(common — WinRM/SMB off by default), use the **RDP + pre-minted
token** flow below, which needs **no `gh` on the secondary host**.

### RDP flow (secondary host has no WinRM/`gh`) — tested for AURORA-R11

On the **primary** machine (this one — `gh` is authed here):

1. Confirm the sealed golden exists:
   `Test-Path C:\HyperV\golden\sluice-runner-golden.vhdx` (≈5–6 GB).
2. Mint a registration token (≈1 h TTL — do this right before the
   batch). Repo scope:

   ```powershell
   $tok = gh api -X POST repos/orware/sluice/actions/runners/registration-token --jq .token
   ```
   Org scope instead (after `gh auth refresh -h github.com -s admin:org`):
   `$tok = gh api -X POST orgs/orware-code/actions/runners/registration-token --jq .token`
3. RDP to the secondary host **with local drive redirection on** so it
   can read this machine's disks as `\\tsclient\C`:

   ```powershell
   mstsc /v:192.168.4.91 /drive:C       # or enable Local Resources ▸ Drives in the RDP UI
   ```

On the **secondary** host (inside the RDP session, elevated PowerShell):

4. Prereqs there: **Hyper-V** feature + an external/`Default Switch`
   vSwitch. `gh`/`qemu-img` **not** needed.
5. Pull the golden + the `scripts/hyperv-runner` folder across the RDP
   channel (5–6 GB over `\\tsclient` is slow but unattended-ok; a LAN
   SMB share or USB is faster if available):

   ```powershell
   New-Item -ItemType Directory -Force C:\HyperV\golden, C:\sluice-hv | Out-Null
   Copy-Item \\tsclient\C\HyperV\golden\sluice-runner-golden.vhdx C:\HyperV\golden\ -Force
   Copy-Item \\tsclient\C\code\sluice\scripts\hyperv-runner\* C:\sluice-hv\ -Recurse -Force
   ```
6. Provision, passing the token from step 2 (paste its value — it is
   NOT redirected automatically). Names must be **globally unique
   across hosts** (this host uses the `11x` block):

   ```powershell
   cd C:\sluice-hv
   1..3 | % { .\New-RunnerFromTemplate.ps1 `
       -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
       -Name ("runner-1{0}" -f $_) `
       -RegistrationToken '<paste $tok value>' `
       -AdminSshPublicKey (gc \\tsclient\C\Users\orwar\.ssh\id_ed25519.pub) `
       -DiskGB 200 }
   ```
   Add `-Org orware-code` if you minted an org token (token scope and
   `-Repo`/`-Org` must match). `-RegistrationToken` makes the script
   skip `gh` entirely.
7. Confirm each shows **Idle** in repo (or org) Settings → Actions →
   Runners.

Truly remote orchestration (one command from the primary that
provisions onto the secondary's Hyper-V) would still want
`-ComputerName`/CIM plumbing over WinRM — not built; the RDP flow is
the zero-WinRM substitute.

## Org-scoped runners (share across all org repos)

By default runners register at **repo** scope (`-Repo owner/repo`,
default `orware/sluice`) — one runner serves one repo. Pass **`-Org
<name>`** instead to register at **organization** scope: that runner
is usable by *every* repo in the org (one pool, many projects). Works
on both entry scripts:

```powershell
.\New-RunnerVM.ps1          -Name runner-01 -Org orware-code -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub)
.\New-RunnerFromTemplate.ps1 -GoldenVhdx ... -Name runner-01 -Org orware-code -AdminSshPublicKey (gc ~/.ssh/id_ed25519.pub)
```

`-Repo` and `-Org` are mutually exclusive (PowerShell parameter sets;
omitting both = the `-Repo` default). **The golden VHDX is
scope-agnostic** — it registers no runner, so one golden serves repo
*and* org fleets; scope is chosen per clone.

Prerequisite for `-Org`: the `gh` token must carry **`admin:org`**.
`repo` + `read:org` alone returns 403 on the org token endpoint.
Grant once:

```powershell
gh auth refresh -h github.com -s admin:org
```

The org must also allow self-hosted runners and the repos must live
in that org. (To move repos into `orware-code`: GitHub → repo →
Settings → Transfer ownership.) Org runners land in the org's
**Default** runner group unless GitHub-side group/visibility rules
say otherwise — manage that in Org → Settings → Actions → Runner
groups, not in these scripts.

## Prerequisites (on the runner host, once)

- Windows Pro/Enterprise with the **Hyper-V** feature + module, run
  **elevated**. (`vmms` service running — already true on this box.)
- **`qemu-img`** on PATH — the only external tool. Used to convert the
  cloud image (qcow2) to VHDX; `Convert-VHD` cannot read qcow2.
  `winget install --id cloudbase.qemu-img` (lightweight, qemu-img
  only) or `--id SoftwareFreedomConservancy.QEMU` (full, current).
  **These packages often do not add themselves to PATH** — after
  install, verify `qemu-img --version` in a new shell; if missing,
  add its install dir to PATH (or drop `qemu-img.exe` on PATH).
- **`gh`** authed to `orware/sluice` (already true on this box) — used
  to mint runner registration tokens (`repos/.../registration-token`,
  ~1 h TTL, minted just-in-time per VM).
- An external vSwitch (default `Default Switch` works; pass `-SwitchName`).

## Security notes

- Registration tokens are short-lived and injected **only** into the
  per-VM seed VHDX on the host. They are never written to the repo;
  `scripts/hyperv-runner/.work/` (image cache + generated seeds) is
  gitignored.
- The provisioning user gets an SSH public key you supply
  (`-AdminSshPublicKey`) so the host can manage the guest; no password
  auth, no inbound exposure (runners are outbound-only).
- Runners are registered with `--ephemeral` is **not** used here
  (these are long-lived); pair with the repo-variable fail-back
  (`CI_LINUX_RUNNER`) for planned downtime per the runbook.

## Validate tomorrow (suggested order)

1. `./New-RunnerVM.ps1 -Name probe -WhatIf` — read the plan.
2. Real run for one VM; watch `Get-VM probe`, then in-guest
   `cloud-init status --wait`, `df -h /` (expect full disk),
   `systemctl status actions.runner.*`, `crontab -l`, the
   disk-pressure timer. Confirm it appears Idle in repo Settings →
   Actions → Runners.
3. `Build-GoldenTemplate.ps1`, then `New-RunnerFromTemplate.ps1` ×4.
4. Re-activate self-hosted routing: `gh variable set CI_LINUX_RUNNER
   --repo orware/sluice --body sluice-linux` (the fail-back is
   currently *deleted* → CI is on GitHub-hosted until you do this).

See each script's comment-based help (`Get-Help .\New-RunnerVM.ps1
-Full`) for parameters.
