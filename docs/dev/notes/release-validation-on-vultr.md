# Release validation on the Vultr test box

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

# 1. Full integration suite (the default CI gate, replicated)
time go test -tags=integration -race -count=1 -timeout=30m ./internal/...

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
