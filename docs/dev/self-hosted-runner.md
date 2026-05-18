# Self-hosted CI runners + second-workstation setup

Runbook for standing up a **second Windows box** as (a) a full-parity
sluice working machine and (b) a self-hosted GitHub Actions runner pair
for `orware/sluice`. Pair with the `runs-on` selector in
`.github/workflows/ci.yml` (repo variables `CI_LINUX_RUNNER` /
`CI_WINDOWS_RUNNER`).

## Why this topology (ground-truthed, not theorized)

May usage on `orware/sluice`: **492 `ci.yml` + 166 `release.yml` runs
in 17 days** (battle-test campaign — volume is largely transient).
Per `ci.yml` run ≈ 96 billed-minutes. Per-OS reality, confirmed from
run-job timing:

- **macOS: none.** Dropped in v0.20.1 — no 10× legs exist.
- **Windows: tag/dispatch only.** `ci.yml` lines ~107/~316 already
  restrict Test/Build to `["ubuntu-latest","windows-latest"]` on
  `refs/tags/v*` or `workflow_dispatch`, `["ubuntu-latest"]`
  otherwise. Both "Lever-0" matrix trims are **already in-tree**.
- **The recurring driver is the ~34-min Linux `Integration` job
  (`-race` + testcontainers) × ~492 runs.** Everything else is small.

So: a **self-hosted Ubuntu runner** removes the actual cost driver; a
**self-hosted Windows runner** removes the tag-only Windows legs. With
no macOS, **Both ⇒ ~100 % of the billed matrix moves off
GitHub-hosted**; hosted spend → ~$0 except when a runner is down and
you fail back. `release.yml` is **deliberately left on ephemeral
GitHub-hosted** — published-artifact supply-chain cleanliness outweighs
its minor tag-only cost, and it is not the driver.

Networking: GitHub Actions self-hosted runners are **outbound-only**
(the agent long-polls GitHub over HTTPS/443; GitHub never dials in).
**No public IP, no inbound port-forward, no DDNS.** NAT/home-LAN is the
supported topology. `orware/sluice` being **private** neutralizes the
major self-hosted risk (untrusted-PR code execution) — acceptable.

## The remote-automation reality (read before starting)

You cannot remotely enable remote management remotely — there is a
one-time **hands-on bootstrap on the target box**. After that, ~80 % is
scriptable (from the primary box via SSH/`Invoke-Command`, or a single
local script). Genuinely interactive regardless: **Claude Code login**
(OAuth), **Rancher Desktop first-run** (WSL2 enable + reboot), Hyper-V
feature enable (+ reboot), and any **runner registration token** (mint
from the primary box — short-lived).

| Step | Where it must run |
|---|---|
| Enable OpenSSH Server / PSRemoting; confirm `winget` | Hands-on on target (bootstrap) |
| `winget install` Git/Go/Node/pscale/vultr-cli; runner download+config | Scriptable (remote or local) |
| Claude Code `npm i -g` + **login** | Install scriptable; **login interactive** |
| Rancher Desktop install + **first-run/WSL2/reboot** | Install scriptable; **first-run hands-on** |
| Hyper-V enable + Ubuntu-guest create | Hands-on (elevation + reboot) |
| Repo-variable flip to activate runners | Primary box (`gh`) |

## Phase 0 — target-box bootstrap (hands-on, ~15 min)

On the second Windows box (Win 11), elevated PowerShell:

```powershell
# 1. OpenSSH Server (lets the primary box drive the rest over SSH)
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
Start-Service sshd; Set-Service -Name sshd -StartupType Automatic
New-NetFirewallRule -Name sshd -DisplayName 'OpenSSH Server' `
  -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22
# 2. winget present? (ships with Win11; else install App Installer from Store)
winget --version
# 3. Hyper-V (for the Ubuntu guest) — reboots
Enable-WindowsOptionalFeature -Online -FeatureName Microsoft-Hyper-V -All
```

From the **primary box**, confirm reach: `ssh user@<target-lan-ip>
"winget --version"`. Everything below can then be run over that SSH
session or pasted locally.

## Phase 1 — full-parity workstation (scriptable + 2 interactive)

```powershell
winget install --silent --accept-package-agreements --accept-source-agreements `
  Git.Git GoLang.Go OpenJS.NodeJS PlanetScale.CLI Vultr.vultr-cli `
  suse.RancherDesktop Microsoft.PowerShell
npm install -g @anthropic-ai/claude-code      # Claude Code
```

Then, hands-on on the target:

1. **Claude Code login**: run `claude` once → complete the OAuth/login
   prompt (interactive; cannot be scripted).
2. **Rancher Desktop first-run**: launch it once → accept WSL2 install,
   pick the **dockerd (moby)** runtime, let it reboot/initialize.
   testcontainers needs a working Docker daemon — verify `docker run
   --rm hello-world`.
3. **Repos + secrets** (mirror the primary layout under `C:\Code\` and
   `C:\code\`):
   ```powershell
   git clone https://github.com/orware/sluice C:\Code\sluice
   git clone https://github.com/orware/sluice-testing C:\code\sluice-testing
   git clone https://github.com/orware/sluice-validation C:\code\sluice-validation
   cd C:\Code\sluice; git config core.hooksPath .githooks
   ```
   Copy `PLANETSCALE_CREDENTIALS.env` (+ `PLANETSCALE_SERVICE_TOKEN.env`)
   to each repo root **out-of-band** (gitignored secrets — never via
   git; SCP from the primary box). Set the Rancher Desktop quirks per
   `docs/dev/development.md` (`docker.exe` PATH;
   `TESTCONTAINERS_RYUK_DISABLED=true`).

## Phase 2 — Ubuntu guest under Hyper-V (the cost-driver runner)

**Automated.** Hand-building this guest is what caused the recurring
outage: a `.vhdx` was extended but the guest filesystem never grew (and
Ubuntu Server's guided-install LVM leaves most of the VG unallocated),
so the disk re-wedged on the same timeline once the Integration job's
pulled images refilled it. The build is now scripted and structurally
immune to that — see [`scripts/hyperv-runner/README.md`](../../scripts/hyperv-runner/README.md).

Spec unchanged: Gen-2, **≥4 vCPU, ≥12 GB RAM, ≥100 GB disk** (images:
`mysql:8.0`, `postgres:16`, vttestserver ~700 MB, `vitess/lite` ~2 GB).
Docker-in-a-Linux-guest needs no nested virtualization. The scripts use
the **Ubuntu cloud image** (single growable ext4 root — cloud-init
grows it to fill the VHDX on first boot, so resizing the disk is the
only knob) and bake in disk hygiene (the runbook's daily
`docker system prune` cron **plus** an hourly disk-pressure guard that
prunes when root > 70 %, because the daily-only cron is what failed).

Note: a system Go is **not** installed — CI uses `actions/setup-go`,
which provisions Go per-job (the guest *can* run `-race`/CGO/TSan, but
the toolchain comes from the workflow, keeping the image lean).

```powershell
# elevated, on the runner host; qemu-img + gh required (see README)
cd C:\Code\sluice\scripts\hyperv-runner
.\New-RunnerVM.ps1 -Name runner-01 `
  -AdminSshPublicKey (Get-Content ~\.ssh\id_ed25519.pub) -WhatIf   # then drop -WhatIf
```

### Fleet builds — the golden-template path

For more than one runner, build a sealed image once and clone it
(~1–2 min per runner vs a full image build, no re-download):

```powershell
.\Build-GoldenTemplate.ps1 -AdminSshPublicKey (gc ~\.ssh\id_ed25519.pub) `
  -AdminSshKeyPath ~\.ssh\id_ed25519 -OutGoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx
1..4 | ForEach-Object {
  .\New-RunnerFromTemplate.ps1 -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
    -Name ("runner-{0:00}" -f $_) -AdminSshPublicKey (gc ~\.ssh\id_ed25519.pub)
}
```

The golden VHDX is the durable artifact: rebuild the whole fleet from
it in minutes after any host event. Tokens are minted just-in-time per
VM (never baked into the image or committed). The legacy hands-on
sequence is preserved in `README.md` for reference / disaster fallback.

## Phase 3 — native Windows runner on the host (tag-only legs)

Elevated PowerShell on the target host (covers `Test`/`Build` on
`windows-latest`, which fire only on tags/dispatch — no Docker needed
for those jobs):

```powershell
mkdir C:\actions-runner; cd C:\actions-runner
Invoke-WebRequest -Uri https://github.com/actions/runner/releases/latest/download/actions-runner-win-x64.zip -OutFile r.zip
Expand-Archive r.zip -DestinationPath . ; del r.zip
.\config.cmd --url https://github.com/orware/sluice --token <TOKEN> `
  --name sluice-win-host --labels sluice-win --unattended --replace --runasservice
```

## Phase 4 — mint tokens + activate

From the **primary box** (already `gh`-authed). Tokens expire in ~1 h —
mint immediately before each `config` run:

```bash
gh api -X POST repos/orware/sluice/actions/runners/registration-token --jq .token
```

Activate the selector (only after both runners show **Idle** in repo
Settings → Actions → Runners):

```bash
gh variable set CI_LINUX_RUNNER   --repo orware/sluice --body sluice-linux
gh variable set CI_WINDOWS_RUNNER --repo orware/sluice --body sluice-win
# Integration is slower on the self-hosted box: the cross-engine +
# fuzz + CDC -race suite needs ~39m there vs <35m on GitHub-hosted,
# so the default 35m inner / 45m outer budget times out. Give it a
# larger envelope (defaults stay 35m/45m when these are unset, so the
# hosted fail-back keeps fast feedback). INVARIANT: inner < outer.
gh variable set CI_INTEGRATION_TIMEOUT     --repo orware/sluice --body 75m   # inner (go test -timeout)
gh variable set CI_INTEGRATION_JOB_TIMEOUT --repo orware/sluice --body 90    # outer (job timeout-minutes)
```

Set all four together when activating the self-hosted fleet. The
two integration-timeout vars are read by `ci.yml`; raising the
envelope later (suite grows) is a `gh variable set`, no code change.
Observed: 75m budget, ~39m actual — comfortable ~2x headroom.

**Fail-back (a runner is down / box rebooting / over-quota):**

```bash
gh variable delete CI_LINUX_RUNNER   --repo orware/sluice   # → ubuntu-latest
gh variable delete CI_WINDOWS_RUNNER --repo orware/sluice   # → windows-latest
# Also clear the integration-timeout vars: they are read regardless of
# runner, so leaving them at 75m/90m would saddle the hosted fail-back
# with the slow budget (loses fast feedback / regression sensitivity).
gh variable delete CI_INTEGRATION_TIMEOUT     --repo orware/sluice  # → 35m default
gh variable delete CI_INTEGRATION_JOB_TIMEOUT --repo orware/sluice  # → 45 default
```

This is the manual fail-over lever — GitHub does **not** auto-fail-over
an offline self-hosted runner (jobs queue up to 24 h). Because the
autonomous release flow is CI-gated, **a dead Linux runner blocks
releases**: if the box will be down, delete `CI_LINUX_RUNNER` first.

## Security / ops

- **Outbound-only**; no inbound exposure. Keep the box patched; the
  runner auto-updates itself but the OS does not.
- **Private repo** ⇒ untrusted-PR execution risk is minimal; do **not**
  point these runners at a public fork without `required-approval` for
  outside contributors.
- `release.yml` stays GitHub-hosted (ephemeral, auditable published
  artifacts) — do not move it self-hosted for a marginal cost win.
- Single physical box = SPOF for the CI gate. The repo-variable
  fail-back is the mitigation; keep it muscle-memory.
- Org move (if you later transfer for Blacksmith): runners can be
  re-registered org-level; only the `--url` and `--repo`/`gh variable`
  scope change — the selector expression is unaffected.

## Maintenance checklist

- [ ] `docker system prune` cron alive on the Ubuntu guest; free disk
      > 30 GB.
- [ ] Both runners **Idle** in Settings → Actions → Runners.
- [ ] Runner agent version current (auto-updates; verify after long
      downtime).
- [ ] On planned downtime: delete the relevant `CI_*_RUNNER` variable
      **before** taking the box down, restore after.
- [ ] Quarterly: a deliberate fail-back drill (delete vars, confirm a
      PR runs green on GitHub-hosted, restore).
