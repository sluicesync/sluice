# Prep: continuous-validation on Vultr (migration from operator's workstation)

> **SUPERSEDED (2026-05-19): the Vultr box is being retired** (paid +
> idle + stale; see `release-validation-on-vultr.md` host-migration
> banner). The "migrate continuous validation *to Vultr*" premise no
> longer applies. If continuous validation is ever moved off the
> operator's workstation, the target is now a **local Hyper-V VM**
> (provision via `New-ValidationVM.ps1`), not Vultr. This doc is kept
> for the discovery/rationale it captured; the host is the only part
> that changed.

When to pick this up: when the operator decides the cycle-time / GitHub-issue-round-trip cost of running continuous validation on a separate workstation outweighs the per-day Vultr cost. The decision is deferred as of writing; this doc captures the plan so a future session can act on it without re-doing the discovery.

## Current state (verified 2026-05-13)

- **Vultr box**: `sluice-test-lax-1` (ID `<previous-vultr-instance-ID>`), `<previous-vultr-IP>`, `vhf-3c-8gb`, Ubuntu 24.04, active, 3-day uptime, idle.
- **SSH access from this session**: works via `ssh root@<previous-vultr-IP>` (the local `~/.ssh/id_ed25519` key was registered as `<previous-vultr-SSH-key-ID>` during provisioning — see [`release-validation-on-vultr.md`](release-validation-on-vultr.md) for the full provisioning runbook).
- **Tooling installed**: Go 1.26.2 (matches `go.mod`), Docker 29.1.3 (daemon running, no containers). Matches the pre-release validation runbook's needs.
- **`/root/code/sluice` clone**: **STALE** at commit `1a978293` (v0.32.2-era, ~Nov 2025). Current main is far ahead — needs `git pull` (or a fresh `git bundle` push from Windows) before any continuous-validation work.
- **PlanetScale credentials**: **NOT present** on the box. Operator's local rig has `PLANETSCALE_CREDENTIALS.env` and `PLANETSCALE_SERVICE_TOKEN.env` at the `sluice-testing` repo root.
- **Continuous-validation scaffolding**: **NOT present**. No `RUNBOOK.md`, no supervisor scripts, no cycle-report dir, no `BUG-CATALOG.md` on the box. All currently lives at `C:\code\sluice-testing\` on operator's local rig.

## Why migrate (operator's framing)

Current setup: continuous validation runs on operator's separate workstation. Each finding flows back to this development session via the operator manually filing a GitHub issue with the bug detail and log excerpts. That works but adds round-trip latency between "agent on testing rig observes a stall" and "agent in dev session can act on it."

Moving validation to the Vultr box would:

- **Eliminate the GitHub-issue intermediation** — agent in dev session reads logs / cycle reports directly from the box via SSH, files internal observations as `BUG-CATALOG` entries that the dev session reads.
- **Centralise observation surface** — one box hosting both the steady-state validation streams AND the pre-release validation runbook, so the dev session has one place to look.
- **Free up the operator's workstation** for other work (and let it sleep at night instead of holding stream processes).

Cost: the Vultr instance is already running ($48/mo). Migrating doesn't change billing.

## Concrete migration plan (5 steps, low-touch, reversible)

### Step 1 — Update source

```bash
ssh root@<previous-vultr-IP> 'cd /root/code/sluice && git pull'
```

If deploy-key wasn't set up (current state unclear), fall back to the `git bundle` push from Windows per the provisioning runbook:

```powershell
cd C:\code\sluice
git bundle create C:\code\sluice.bundle --all
scp C:\code\sluice.bundle root@<previous-vultr-IP>:/root/sluice.bundle
ssh root@<previous-vultr-IP> 'cd /root/code && rm -rf sluice && git clone /root/sluice.bundle sluice'
```

Verify with `ssh root@<previous-vultr-IP> 'cd /root/code/sluice && git rev-parse --short HEAD'` — should match `main`'s tip.

### Step 2 — Pre-stage current release binary

Saves a `go build` per cycle. Build on Vultr (matches the production binary the operator has been running):

```bash
ssh root@<previous-vultr-IP> 'export PATH=$PATH:/usr/local/go/bin && cd /root/code/sluice && \
    git checkout v0.48.0 && \
    go build -ldflags "-X main.version=0.48.0 -X main.commit=$(git rev-parse --short HEAD)" \
        -o /root/bin/sluice-0.48.0 ./cmd/sluice && \
    /root/bin/sluice-0.48.0 --version'
```

(The release-binary download via `gh release download` is also viable — `gh` may need installing on the box; the build-from-source path is dependency-free.)

### Step 3 — Copy PlanetScale credentials (one-time)

From operator's workstation:

```powershell
scp C:\code\sluice-testing\PLANETSCALE_CREDENTIALS.env root@<previous-vultr-IP>:/root/code/sluice/
scp C:\code\sluice-testing\PLANETSCALE_SERVICE_TOKEN.env root@<previous-vultr-IP>:/root/code/sluice/
ssh root@<previous-vultr-IP> 'chmod 600 /root/code/sluice/PLANETSCALE_*.env'
```

These are gitignored upstream; the `chmod 600` is defence-in-depth on the single-tenant box.

### Step 4 — Replicate the testing-repo skeleton

The `sluice-testing` git repo can stay as the artifact-of-record on operator's workstation. On the Vultr box, set up a parallel structure that writes the same files (cycle reports, BUG-CATALOG entries, supervisor logs) but commits/pushes back to the same `orware/sluice-testing` repo from the box.

```bash
ssh root@<previous-vultr-IP> 'mkdir -p /root/sluice-validation && cd /root/sluice-validation'
# Clone the artifact-of-record repo (needs the operator's GitHub auth or a deploy key)
ssh root@<previous-vultr-IP> 'cd /root/sluice-validation && git clone git@github.com:orware/sluice-testing.git'
```

Plus operator-side `scp` of any custom supervisor scripts:

```powershell
scp C:\code\sluice-testing\work\supervisor_*.sh root@<previous-vultr-IP>:/root/sluice-validation/sluice-testing/work/
```

### Step 5 — Cycle-subagent invocation pattern (defer until 1-4 verified)

Once the box is set up, the cycle subagent runs **on the Vultr box via SSH-from-this-session**. Pattern:

- Agent in dev session spawns a `general-purpose` subagent with prompt: "SSH to root@<previous-vultr-IP>; run the v0.48.0 cycle per `/root/sluice-validation/sluice-testing/NEXT-CYCLE.md`; write `session-reports/v0.48.0.md` on the box; `git push` to `orware/sluice-testing`; report verdict (CLEAN / BUG_FOUND) back."
- Subagent operates entirely over SSH; no local tooling needed in the dev-session workspace.
- Cycle artifacts (binary dir, session report, BUG-CATALOG additions, supervisor logs) live on the box AND get committed to `sluice-testing` repo → operator can review on workstation either way.

Hold this step until steps 1-4 are verified — the SSH-from-subagent pattern is new (different from the prior cycle pattern where the agent ran locally on operator's workstation). Worth a small first-cycle run to confirm SSH idle behaviour, log-tailing pattern, and recovery if SSH drops mid-cycle.

## The one concern worth re-evaluating after migration

Vultr LAX → PlanetScale us-east cross-region latency is different from operator's workstation. Some findings depend on the latency profile to fire:

- **#14** (VStream COPY-dedup) — surfaced under sustained write rate during cold-start; latency affects retry rate
- **#18** (cross-region batch sizing > 50) — directly latency-driven; the Vitess 20s tx-killer fires only when the batch's commit window crosses that threshold
- **#21** (PS-MySQL TCP-reset cascades) — operator observed these on workstation; whether they manifest at the same rate from Vultr LAX is unknown

**Recommendation: dual-run for one week.** Keep operator's workstation rig going, add the Vultr rig. Compare finding rates. If Vultr surfaces the same shapes at similar cadence, switch over fully and decommission the workstation rig. If Vultr is too quiet, we'd want a workstation closer to PlanetScale's region — that's a separate decision (different Vultr region? Operator's local rig stays? Move to a different cloud?).

## Pointers for future sessions

- Provisioning + standing config: [`release-validation-on-vultr.md`](release-validation-on-vultr.md)
- Vultr CLI: `C:\vultr-cli\vultr-cli.exe`, config `C:\vultr-cli\vultr-cli.yaml`
- Testing-repo conventions (NEXT-CYCLE.md, RUNBOOK.md, BUG-CATALOG.md, session-reports/): operator's workstation at `C:\code\sluice-testing\`, plus `orware/sluice-testing` repo on GitHub
- Auto-memory pointer: `reference_vultr_continuous_validation.md`
