# sluice v0.77.1 — Bug 85.b hotfix (lease-GC actually fires now)

**Headline:** Patch release fixing the v0.77.0 fix-attempt for Bug 85. v0.77.0's wire-up used the wrong type-assertion surface for the position orderer (`applier.(ir.PositionOrderer)`); the assertion silently failed on every real engine, leaving the lease GC sweep dead code for a second consecutive release. v0.77.1 asserts on `s.Source.(ir.PositionOrderer)` where the orderer actually lives. Caught by the v0.77.0 post-release cycle.

## Why this is a patch, not a feature

This release contains no operator-visible new functionality. It fixes a regression on a previously-claimed feature (the v0.76.0 lease GC sweep). Operators running long-lived Shape A deployments on **v0.76.0 OR v0.77.0** should upgrade — neither prior release's GC sweep actually fired.

## Fixed

- **`fix(pipeline): Bug 85.b — assert PositionOrderer on s.Source not applier (#38)`**

  v0.77.0's Bug 85 fix attempt added `mgr.WithGC(...)` wiring to `engageShardCoordination` but used the wrong type-assertion surface:

  ```go
  // v0.77.0 (WRONG):
  orderer, ok := applier.(ir.PositionOrderer)

  // v0.77.1 (RIGHT):
  orderer, ok := s.Source.(ir.PositionOrderer)
  ```

  `PositionAtOrAfter` is defined on the `Engine` factory type (e.g. `internal/engines/postgres/position_orderer.go:33` carries `func (Engine) PositionAtOrAfter(...)`), NOT on `*ChangeApplier`. The v0.77.0 type-assertion silently failed on every real engine → `m.gcDeps` stayed nil → the heartbeat-loop's GC-trigger guard always evaluated false → the sweep STILL was dead code in production.

  **Same Bug 85 failure mode, second cycle.** The v0.77.0 unit pin test passed because `supportingApplier` stubbed `PositionAtOrAfter` on itself — test/production surface mismatch hiding the real gap. The v0.77.0 cycle caught it via end-to-end log observation (35 heartbeat ticks under continuous load, zero GC log lines).

  v0.77.1 fixes the assertion surface + updates the pin tests to use the right surfaces (orderer on engine, lister+deleter on applier). The previously-misleading `supportingApplier.PositionAtOrAfter` stub is **removed** — it was actively masking the production gap. `stubNamedEngine` now implements `PositionAtOrAfter` so the engagement test's assertion succeeds at the right surface. New `TestEngage_NoGCWhenSourceLacksOrderer` pin guards the no-GC default when a future engine doesn't ship a PositionOrderer.

## Tests

- **`test(pipeline): shard_consolidation_engage_test.go`** — updated Bug 85 pins to assert on the right surfaces (orderer on engine, lister+deleter on applier). New `TestEngage_NoGCWhenSourceLacksOrderer` regression guard for the no-GC default.

## Known gap (tracked as follow-up)

A **real-engine ChangeApplier integration test** that exercises the heartbeat-fires-sweep path end-to-end is still missing. Without it, future regressions of this test/production surface-mismatch class can hide for another release cycle. The v0.76.0 / v0.77.0 misses both share the same root cause: production wire-up not pinned end-to-end. Tracked as a follow-up; expected in a future minor release.

## Compatibility

- **Drop-in upgrade from v0.77.0** (or earlier v0.76.x/v0.77.0).
- **Behaviour change**: lease GC sweep now actually fires. Operators with accumulated lease rows from v0.76.0+ deployments see them GC'd on the next heartbeat sweep (every ~5 min) after upgrading — assuming all streams have advanced past their anchors.
- **MySQL paths** unchanged.

## Cross-references

- [v0.77.0 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.77.0) — prior release; Bug 85.b is the wire-up fix that v0.77.0's Bug 85 fix attempt missed
- [ADR-0054 — Shape A Phase 2](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md)
- The post-release regression cycle that caught this: the v0.77.0 cycle
