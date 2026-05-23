# Release validation (local Hyper-V validation VM; Vultr retired)

> **HOST MIGRATION COMPLETE (2026-05-19): the paid always-on Vultr
> box (`sluice-test-lax-1`, ID `<previous-vultr-instance-ID>`, `<previous-vultr-IP>`) is
> DECOMMISSIONED — instance deleted, verified 0 instances on the
> account.** It was idle + its clone stale (v0.32.2-era); with local
> Hyper-V Ubuntu provisioning + a runner fleet in place, the same
> activity now runs on a **local runner-less validation VM** at zero
> recurring cost. Parity was proven before decommission (the
> validate-before-decommission tenet): all three suites green on the
> local VM — SUITE1 integration `-race` @ `-timeout=75m` (RC=0,
> 39:30; the `-timeout=30m` here was a stale under-budget — see the
> SUITE1 comment below), SUITE2 postgis, and **SUITE3 `integration
> vstream`** (the CI-skipped Vitess coverage that is this box's whole
> reason to exist). The runbook commands (the `go test` block) are
> host-agnostic and unchanged; only provisioning + source-sync + IP
> discovery differ. The Vultr-specific provisioning text below is now
> purely historical (the box no longer exists). Filename kept
> (`release-validation-on-vultr.md`) to avoid breaking roadmap §10 /
> `prep-continuous-validation-on-vultr.md` / memory references; "the
> validation box" means the local VM.

## Local Hyper-V validation VM (canonical host)

A **runner-less** clone of the sealed golden VHDX — explicitly NOT a
GitHub Actions runner (it runs this runbook over SSH, registers no
runner).

1. **Provision (elevated PowerShell on the Hyper-V host):**
   ```powershell
   cd C:\code\sluice\scripts\hyperv-runner
   .\New-ValidationVM.ps1 -GoldenVhdx C:\HyperV\golden\sluice-runner-golden.vhdx `
       -AdminSshPublicKey (gc $HOME\.ssh\id_ed25519.pub) -DiskGB 120 -CpuCount 6 -MemoryBytes 16GB
   ```
2. **Find its IP** (stock Ubuntu cloud image has no `hv_kvp_daemon`,
   so `Get-VMNetworkAdapter ... .IPAddresses` is empty — use the
   MAC→host-neighbor method, same as the golden seal):
   ```powershell
   $mac=(Get-VMNetworkAdapter -VMName sluice-validation).MacAddress
   Get-NetNeighbor | ? { ($_.LinkLayerAddress -replace '[:-]','') -eq $mac } | Select IPAddress,State
   ```
3. **One-time Go bootstrap.** The golden bakes Docker + the admin
   user/SSH key but **NOT system Go** (CI runners get Go via
   `actions/setup-go`; this VM needs it installed once, like the old
   Vultr box did). SSH in (`ssh -i ~/.ssh/id_ed25519 runner@<IP>`):
   ```bash
   V=$(curl -fsSL https://go.dev/VERSION?m=text | head -1)   # e.g. go1.26.3
   curl -fsSL "https://go.dev/dl/${V}.linux-amd64.tar.gz" | sudo tar -C /usr/local -xz
   echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
   docker pull vitess/vttestserver:mysql80   # 2 GB; the vstream-suite image, pre-pull
   ```
   (go.mod's `toolchain` directive auto-fetches the exact pinned
   version at test time, so installing current stable is sufficient.)
4. **Sync source + run the runbook below** (the `go test` block is
   identical to the historical Vultr steps — host-agnostic).

The VM is on-demand: spin it for a release pass, leave it stopped or
`Remove-VM` it between releases (re-provision in ~1-2 min from the
golden). No always-on cost.

---

## (Historical) Vultr test box

Pre-release smoke-and-coverage runbook executed on the always-on Vultr instance (`sluice-test-lax-1`, see `C:\vultr-cli\sluice-test-spin-up.md` for provisioning). The intent: before tagging a release, exercise the build-tag combinations that CI doesn't gate on so the operator gets the "would this catch a regression that CI wouldn't?" signal locally.

## What CI gates today

| Job | Build tag | When it runs |
|---|---|---|
| `Test (ubuntu-latest)` | (none) | Every push/PR |
| `Integration` | `integration` | Every push/PR (Ubuntu only) |
| `Integration (PostGIS)` | `integration postgis` | Every push/PR (Ubuntu only) |
| `Lint` | (none) | Every push/PR |
| `Build (ubuntu-latest)` | (none) | Every push/PR |
| `Test (windows-latest)` | (none) | Tag pushes / `workflow_dispatch` only |
| `Build (windows-latest)` | (none) | Tag pushes / `workflow_dispatch` only |
| `PlanetScale Verify` | `psverify` | `workflow_dispatch` only |

Notably **not** gated by CI: `integration vstream` (vttestserver-based Vitess coverage). Image is 2.04 GB and adds ~10 min to a CI run; cost-prohibitive to add to every PR.

## What the Vultr box runs (release-validation runbook)

Sync source first (one of):
- Bundle path: `git bundle create C:\code\sluice.bundle --all` from Windows, `scp` to `root@<vultr-ip>:/root/`, then on Vultr `cd /root/code && rm -rf sluice && git clone /root/sluice.bundle sluice`.
- Deploy-key path: `git -C /root/code/sluice fetch && git checkout <tag-or-sha>`.

Then run on the Vultr box (single SSH session, ~30 min total):

```bash
cd /root/code/sluice
export PATH=$PATH:/usr/local/go/bin

# 1. Full integration suite (the default CI gate, replicated).
#    -timeout MUST mirror CI's CI_INTEGRATION_TIMEOUT (75m). The old
#    30m here was a stale under-budget: CI's integration job already
#    runs ~39m and was bumped 35m->75m ("stale budget, grown suite",
#    commit c8f1ca5); this standalone runbook copy never got the
#    bump, so SUITE1 timed out at exactly 30m on the validation VM
#    (2026-05-19 ground truth: panic "test timed out after 30m0s",
#    zero real failures, maxRSS ~970MB, no OOM/swap — purely the
#    budget, made worse by the VM's slower-than-CI virtual-disk I/O,
#    not a code or memory defect). engines/mysql alone is ~14.5m.
time go test -tags=integration -race -count=1 -timeout=75m ./internal/...

# 2. PostGIS suite (parallel job in CI; here run sequentially after #1)
time go test -tags="integration postgis" -race -count=1 -timeout=15m \
    -run TestMigrate_PostGIS_ ./internal/pipeline/...

# 3. VStream suite (NOT in CI; this is the Vultr-only coverage)
time go test -tags="integration vstream" -race -count=1 -timeout=20m \
    -run TestVStream_VTTestServer ./internal/engines/mysql/...

# 4. PlanetScale Verify (only when credentials are present on the box)
if [[ -f /root/code/sluice/PLANETSCALE_CREDENTIALS.env ]]; then
    time go test -tags=psverify -race -count=1 -timeout=15m -v \
        -run "TestPS" ./internal/...
fi
```

Reference timings (vhf-3c-8gb, warm Docker image cache, sequential):
- Step 1: ~23 min
- Step 2: ~4 min
- Step 3: ~4 min
- Step 4: depends on PlanetScale account latency, typically <5 min when creds are loaded
- Total: ~30–35 min

## Why this lives on Vultr instead of in CI

1. **Cost.** Adding `integration vstream` to every PR roughly doubles the CI minutes spent on integration jobs. The image alone is 2.04 GB to pull. For a private repo, that adds up fast.
2. **Frequency.** Pre-release validation is per-tag (~weekly cadence in active development), not per-PR. The Vultr box absorbs the per-tag cost without the per-PR multiplier.
3. **Signal already strong on PRs.** The `integration` + `integration postgis` CI jobs catch the vast majority of code regressions. `vstream` mainly catches Vitess-specific issues that don't typically come up while editing non-VStream code paths.

## When `vstream` SHOULD move into CI

Trigger conditions:
- A code change touches `internal/engines/mysql/cdc_vstream*.go` AND there's no Vultr run in the change's review loop. Adding a tag-push-gated `Integration (VStream)` CI job (mirroring the postgis split) is the answer if this becomes frequent.
- A bug surfaces in production that vstream-tag tests would have caught. The historical pattern: Bug 27 (POINT bytes mis-parsed) was a vstream-only quirk that wasn't gated by CI until the postgis chunk; a similar pattern would justify graduating vstream to a CI gate too.

## What's NOT covered yet (gaps)

- **Mid-stream live add-table for VStream (Phase 2.5).** Roadmap item 10 — VStream's table-scope semantics differ enough from binlog that the v0.27.0 ADR-0034 filter-flip mechanism doesn't transfer 1:1. Demand-driven; track when an operator surfaces a real PS workload that needs `--no-drain`.
- **VStream RESHARD events.** The reader has guards (`TestVStreamReader_ApplyReshardState` unit-level), but no integration test boots vttestserver, triggers a reshard mid-stream, and asserts sluice's behavior. Vitess's reshard tooling isn't trivial to script in a test container, so this is a deliberate gap.
- **VStream POINT/POLYGON live decode.** Bug 27's fix is unit-tested via the `decodeVStreamCellGeometry` path; an end-to-end vttestserver-with-spatial-data test would close the loop. ~1 day of test work, deferred until an operator hits the gap.
- **VStream over TLS.** vttestserver runs without TLS by default; PlanetScale terminates TLS at vtgate. The TLS path is exercised only by `psverify` against real PlanetScale, not by the Vultr-box runs.

## See also

- `internal/engines/mysql/cdc_vstream_integration_test.go` — the existing vttestserver-based suite this runbook exercises
- `docs/dev/notes/psverify-status.md` — PS-PG verification setup (separate from VStream coverage)
- `docs/dev/branch-protection.md` — current required CI checks
- `C:\vultr-cli\sluice-test-spin-up.md` — Vultr instance provisioning runbook
