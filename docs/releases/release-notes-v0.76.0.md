# sluice v0.76.0 — ADR-0054 Phase 2 v1 closure

**Headline:** Closes the two v1-ship-deferred items called out in ADR-0054's "known follow-ups" — automatic lease GC for `sluice_shard_consolidation_lease` (#21) and `ProbeAlterColumnType v2` IR-type matching (#20). With this release the Shape A Phase 2 v1 surface is **fully closed**: lease lifecycle from acquire → apply → record → garbage-collect, and crash-recovery probing that distinguishes "column exists" from "column exists with the right type." Drop-in upgrade from v0.75.0; behavior-additive (the new GC is invisible to operators; the stricter probe catches a previously-silent silent-divergence shape).

## What this closes

ADR-0054 Phase 2 v1 (shipped across v0.73.0 → v0.74.x) deliberately deferred two follow-ups to a later release so the load-bearing lease + classifier + applier surface could land first:

- **#21 Lease GC sweep.** Without GC, `sluice_shard_consolidation_lease` accumulates one row per consolidated target table per DDL boundary indefinitely. Long-lived deployments would see the table grow unbounded.
- **#20 ProbeAlterColumnType v2.** v1 was existence-only: the takeover-recovery probe returned `Applied` when the column was present, regardless of whether the column's catalog-reported type matched the IR's expected type. A column that was dropped + re-added with the wrong type would silently pass — silent-divergence-class.

v0.76.0 closes both. The Phase 2 v1 surface is now fully complete.

## Fixed

- **`fix(pipeline, engines/{postgres,mysql}): #21 — automatic lease GC sweep`**. New `pipeline.SweepConsolidationLeases` enumerates every `APPLIED` row in `sluice_shard_consolidation_lease`, computes the per-engine min-stream LSN from `sluice_cdc_state` via `ir.PositionOrderer`, and DELETEs rows every live stream has advanced past the anchor of. Wired into the existing `LeaseManager` heartbeat goroutine (every 30 ticks ≈ 5 min by default) — no new CLI surface; the sweep runs invisibly during active coordination and pauses naturally when no lease is held. Additive `anchor_position TEXT NULL` + `source_engine TEXT NULL` columns on the lease table (`ADD COLUMN IF NOT EXISTS` on PG, detect-then-ALTER on MySQL 8.0.x) carry the source-side CDC position the boundary was observed at; `FinalizeLeaseApply` persists it. **Safety contract**: HELD / EXPIRED rows are never deleted regardless of position; legacy v0.75.0 rows (NULL anchor) are defensively retained until a new boundary on the same target rewrites them; stopped streams (with `stop_requested_at` set) still count as authoritative for their persisted position. **Loud-failure semantics**: GC errors are LOGGED at WARN but never propagated up — retention is a maintenance op, not a correctness one; a failing sweep cannot crash an otherwise-healthy stream.

- **`fix(engines/{postgres,mysql}): #20 — ProbeAlterColumnType v2 (IR-type matching)`**. The probe is no longer existence-only. v2 introspects the column's catalog-reported IR type via the same per-engine `translateType` helper the schema reader uses, then compares against the IR-derived expected type from the lease row's recorded shape. Match → `Applied`. Mismatch → `Inconsistent` with an error naming expected vs observed type and pointing at the operator-actionable recovery path (drained model). Catches the previously-silent silent-divergence shape where a column drops + re-adds with the wrong type. PG's `numeric` unconstrained vs constrained distinction (`Decimal.Unconstrained=true` for bare `NUMERIC`) is preserved; MySQL `VARCHAR` length is compared (charset deliberately excluded per the ADR-0054 v0.73.2 normalizer amendment — see ADR-0054 §"v0.76.0 closure update").

## Docs

- **`docs(adr-0054): v0.76.0 closure update`**. New section under ADR-0054's Decision Points documenting both v1 follow-ups landing: GC sweep cadence + two-condition safety contract; probe v2 outcomes + type-comparison logic. ADR-0054's "known follow-ups" list is now closed except for the explicit Phase 3+ items (operator-issued DDL, cross-region lease semantics) which remain on the roadmap.

## Tests

- **Unit pins (`internal/pipeline/shard_consolidation_lease_gc_test.go`)** — full GC two-condition safety matrix: all streams past anchor → deleted; one stream behind → retained; HELD row → never deleted regardless of position; EXPIRED row → never deleted; legacy NULL-anchor row → retained defensively; empty table → no-op; no streams → conservatively skipped; mixed fleet → exactly the eligible row deleted; per-row delete failure accumulates the error and continues; engine-without-deleter is a no-op; orderer error retains the row defensively.

- **Integration pin (`internal/engines/postgres/shard_consolidation_lease_gc_integration_test.go`)** — drives a real Postgres container through `EnsureControlTable` (verifies the additive migration lands), `FinalizeLeaseApply` with a populated anchor, `ListLeases` round-trip, and `pipeline.SweepConsolidationLeases` against the engine's own `ListStreams` + `PositionOrderer`. Two flows: stream-past-anchor → deletes; stream-behind-anchor → retains.

- **Probe v2 pins** on both engines (`shard_consolidation_probe_integration_test.go`) — `TestShardConsolidationProber_AlterColumnType_V2` lands a real `ALTER COLUMN INT → BIGINT`, asserts `Applied`, then drops + re-adds the column with the WRONG type (TEXT) and asserts `Inconsistent` + an error message naming expected and observed types. PG-specific `TestShardConsolidationProber_AlterColumnType_V2_NumericUnconstrained` pins the bare `NUMERIC` vs `NUMERIC(p,s)` distinction the v2 probe must surface.

## Compatibility

- **Drop-in upgrade from v0.75.0.** No CLI surface change. The new behaviour is automatic GC (operationally invisible — no new flags) and a stricter `ProbeAlterColumnType` that catches a previously-silent silent-divergence shape (operators relying on a wrong-type drop+re-add to silently pass would now see a loud refusal with a recovery hint, which is the intended behaviour per the loud-failure tenet).
- **Additive migration** on `sluice_shard_consolidation_lease`: `anchor_position TEXT NULL` + `source_engine TEXT NULL`. Written by all v0.76.0+ boundaries; legacy v0.75.0 rows with NULL anchors are defensively retained by the GC sweep and harmlessly accumulate until a new boundary on the same target rewrites them.
- **MySQL paths** unchanged outside Shape A coordination.

## Who needs this

- **Operators running long-lived sluice deployments with Shape A.** The lease table no longer grows unboundedly; cleanup happens automatically during active coordination.
- **Operators concerned about silent divergence on takeover recovery.** The probe v2 catches the wrong-type drop+re-add shape that v1 silently accepted; a column-type mismatch now refuses loudly with an actionable recovery path.
- **Anyone NOT running Shape A** sees no observable change.

## Public-release readiness

The v0.73 → v0.76 chain has now closed:
- The 2026-05-22 PG-internals research backlog (F1–F9)
- The pre-public-release prep cluster (#17 / #18 / #19)
- The ADR-0054 Shape A Phase 2 v1 surface end-to-end (#20 / #21)
- The v0.73.0 → v0.73.2 hotfix chain (Bug 83, Bug 84)

The repo is structurally ready for the private→public flip.

## Cross-references

- [ADR-0054 — Shape A Phase 2 live cross-shard DDL coordination](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md) (v0.76.0 closure update appended)
- [ADR-0048 — Multi-source aggregation Shape A](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0048-multi-source-aggregation-shape-a.md) §4 — the original v1 follow-ups list this closes
- [v0.75.0 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.75.0) — the prior release
