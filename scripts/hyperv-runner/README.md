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

## Prerequisites (on the runner host, once)

- Windows Pro/Enterprise with the **Hyper-V** feature + module, run
  **elevated**. (`vmms` service running — already true on this box.)
- **`qemu-img`** on PATH — the only external tool. Used to convert the
  cloud image (qcow2) to VHDX; `Convert-VHD` cannot read qcow2.
  `winget install --id qemu.qemu` or drop `qemu-img.exe` on PATH.
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
