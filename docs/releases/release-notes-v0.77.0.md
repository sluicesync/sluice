# sluice v0.77.0 — `sluice backup compact` + Bug 85 lease-GC wire-up fix

**Headline:** Minor bump driven by **`sluice backup compact`** (ADR-0046 §14d, Task #15) — the next backup-chain piece after rotation (#14, v0.67.0) and prune (§14c). Bundles in the **Bug 85 fix** for v0.76.0's lease-GC dead-code regression that the v0.76.0 post-release cycle caught. Drop-in upgrade.

## Added

- **`sluice backup compact --merge-window DUR`** *(Task #15, ADR-0046 §14d)*. Concatenates consecutive lineage segments whose `CreatedAt` gaps fall within an operator-supplied window into one merged segment. "Naive" = **byte-level chunk concat** — chunk files are moved verbatim; bytes are NEVER decompressed, recompressed, or re-encrypted. Event-level dedup deferred to #16.

  **Three loud-failure pre-flights** run before any mutation:

  1. **Mixed codecs within a merge group** → refuse loudly. Decompress + recompress is #16's smart-compact territory.
  2. **Divergent encryption keysets within a group** → refuse loudly with keyset fingerprints named. Re-keying across segments is a silent data-handling change; the operator gets a refusal pointing at the recovery options.
  3. **Position gaps between consecutive sources** → refuse loudly. Discovered during integration testing: PG's rotation FSM emits `seg[i].EndPosition <= seg[i+1].StartPosition` handoffs, with equality the common case. A strict-less gap means events live ONLY in the later segment's full snapshot — which naive compact is about to drop. The trade-off: compact is a no-op on continuously-written PG chains under steady CDC load (those operators get the event-level compactor in #16). Aligned-position chains compact happily.

  **Atomic safety**: staging-dir → final-dir → atomic `lineage.json` Put (the commit boundary) → orphan source sweep. Mid-compact crash leaves `.compact-staging-*` dirs that the next run sweeps. `--dry-run` mirrors prune (per-group plan, same pre-flights, no storage touch). Never auto-runs.

## Fixed

- **`fix(pipeline): Bug 85 — wire LeaseManager.WithGC at engagement time (#37)`**. v0.76.0 shipped Task #21's lease GC sweep but missed the streamer-side wire-up: `engageShardCoordination` constructed the `LeaseManager` but never called `mgr.WithGC(...)`. The `m.gcDeps` field stayed nil; the heartbeat-loop's GC-trigger guard always evaluated false; the sweep was dead code in production despite v0.76.0's release notes claiming otherwise. **Caught by the v0.76.0 post-release cycle within hours**, validating the autonomous-cycle pattern.

  **Why the v0.76.0 tests missed it**: unit + integration tests exercised `SweepConsolidationLeases` + `LeaseManager.WithGC` directly. The production glue (the type-assertion chain inside `engageShardCoordination`) wasn't pinned. Same "validate end-to-end before building more" tenet the v0.73.0 Bug 83 chain hammered home, applied here at the test layer.

  v0.77.0 adds the wire-up + two pin tests (`TestEngage_WiresGCDepsWhenAllSurfacesPresent` and `TestEngage_InheritsNoGCDefaultWhenSurfacesMissing`).

  Severity b/c — operational regression (lease table grew unboundedly on long-lived Shape A deployments) but NOT silent-loss-class; recoverable via manual DELETE.

## Docs

- **`docs(adr-0046): §14d compact amendment`**. Documents the locked design including the three loud-failure pre-flights, atomic-safety commit boundary, and the explicit deferral of event-level dedup to §14e (#16).

## Tests

- **`test(pipeline): chain_compact_test.go`** — unit pin matrix on `CompactChain` (in-window groups merged, out-of-window left alone, mixed-codec / divergent-keyset / position-gap refusals, dry-run, empty-chain, single-segment-group skip, stale staging cleanup).
- **`test(pipeline): backup_compact_integration_test.go`** — PG container drives a real backup chain through rotation + compact; exercises the loud-failure position-gap refusal as the load-bearing safety pin.
- **`test(cmd/sluice): backup_test.go`** — kong-parse pins for the compact flag surface.
- **`test(pipeline): shard_consolidation_engage_test.go`** — Bug 85 pins.

## Compatibility

- **Drop-in upgrade from v0.76.0.** The new `sluice backup compact` subcommand is opt-in (never auto-runs).
- **Lineage catalog forward-compatible**: compact appends `seg-merged-<groupID>/` + `cap_reason: "compacted"`, both additive. Older sluice reading a compacted lineage silently ignores the unknown cap_reason.
- **Bug 85 upgrade note**: operators running long-lived Shape A deployments on v0.76.0 should upgrade for lease-table GC to actually function. Accumulated lease rows become GC-eligible immediately on the next heartbeat sweep after upgrade.
- **MySQL paths** unchanged.

## Public-release readiness

The v0.73 → v0.77 chain has now closed (across 10 releases):
- 2026-05-22 PG-internals research backlog (F1–F9)
- Pre-public-release prep cluster (#17 / #18 / #19)
- ADR-0054 Shape A Phase 2 v1 surface end-to-end (#20 / #21 / #37 Bug 85)
- v0.73.0 → v0.73.2 hotfix chain (Bug 83, Bug 84)
- Backup chain GH #20 §14a / §14b / §14c / §14d (only smart-compact §14e #16 remains)

Repo is structurally ready for the private→public flip.

## Cross-references

- [ADR-0046 — Native bounded-segment lineage model + inline rotation](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0046-inline-backup-chain-rotation.md) *(§14d amendment added)*
- [ADR-0054 — Shape A Phase 2](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) — Bug 85 closes its v1 GC follow-up properly
- [v0.76.0 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.76.0)
