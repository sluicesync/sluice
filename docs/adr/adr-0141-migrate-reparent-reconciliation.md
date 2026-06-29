# ADR-0141: Reparent-reconciliation for `migrate`

- Status: Accepted
- Date: 2026-06-29
- Deciders: sluice maintainers
- Relates: ADR-0113 (restore reparent-reconciliation), ADR-0110 (coordinated grow-gate), ADR-0108 (cold-copy reparent-retry)

## Context

ADR-0113 fixed a CRITICAL silent-loss class on `restore`: a parallel bulk-copy into a fresh non-Metal PlanetScale MySQL target silently under-copies rows during the target's first storage-grow **reparent** — `rc=0`, every table logged complete, but tables short vs the manifest. The grow-gate (ADR-0110) reduces but cannot eliminate the loss: it trips on the *first observed transient*, but the lost rows were committed-and-acked in the window **before** any worker saw a transient, and the grow promotes a new primary from a replica behind the async-acked window, dropping those rows.

ADR-0113 wired reparent-reconciliation into the **restore** path only. Bug 175 (testing-rig, 2026-06-29) confirmed the **same** silent-loss on the `migrate` path: a live `sluice migrate` into a non-Metal PlanetScale MySQL target crossing a storage-grow reparent under-copied 5,500,003 → 5,496,003 rows (4,000 lost in whole ~500-row batches) and still exited `rc=0` "migration complete". On `migrate` the reparent observer was never set and there was no reconciliation phase, so dropped rows stayed lost.

The engine side already carried everything needed: the MySQL `RowWriter` implements `ir.ReparentObserverSetter` and calls `notifyReparent(table)` at the same `flushWithReparentRetry` grow-gate trip point (ADR-0113). Only the migrate **orchestrator** was missing the wiring + the reconciliation phase.

## Key insight

A CDC stream cannot recover a dropped acked change — the source has moved on. **`migrate` can** — for the same reason `restore` can. Restore's source is its immutable, replayable chunk set; **migrate's source is the live source database, which is also replayable.** A plain `migrate` already runs under a **static-source precondition** (it captures no cross-table snapshot consistency — each parallel reader observes its own per-connection snapshot, the documented ADR-0019 v1 window). So re-reading a single table from the source during reconciliation is sound: it makes the target match the *current* source for that table, regardless of what the reparent dropped. A reparent drop just means "some applied rows were lost — re-read them from the source."

## Decision

**Reparent-reconciliation for `migrate`**, mirroring ADR-0113. Keep the fast parallel bulk-copy; add a reconciliation phase that re-derives any reparent-touched table from the source.

1. **Track touched tables.** A run-shared `reparentTracker` (the exact type ADR-0113 introduced — reused, not duplicated) is constructed once per migrate run in `phaseBuildCopyDeps` and carried on `parallelBulkCopyDeps`. Its `.mark` is wired engine-neutrally via the existing `applyReparentObserver` onto **every** cold-copy writer at exactly the points the grow-gate is wired:
   - the top-level writer, centrally in `runBulkCopyPhases` (next to `applyGrowGate(rw, …)`);
   - every per-chunk / per-table writer, in `openOneChunkConn` (next to `applyGrowGate(wr, …)`).
   A nil tracker (every non-migrate construction, e.g. the sync cold-start deps) makes `applyReparentObserver` a no-op — pre-fix behaviour, byte-for-byte.

2. **Reconcile after the bulk-copy.** In the MySQL fallback branch of `runBulkCopyPhases` (the non-overlapped `runBulkCopyTablePool` path), after the copy completes and **before** identity-sync / indexes, drain the tracker. For each touched table: `TRUNCATE` the target table (via `ir.TableTruncator`, which the MySQL `RowWriter` implements) then re-copy that **one** table **serially** from the source via `copyTable` (within-table parallelism = 1 — the single-stream pace that never outruns replication) into the now-empty table. No primary-key / UPSERT needed — the truncate leaves a clean target and indexes/constraints are later phases — exactly the cold-restore TRUNCATE+redo shape. The serial redo reuses the top-level writer, which carries the observer.

3. **Loop until clean.** A redo runs through the same `flushWithReparentRetry` + observer, so a redo that *itself* reparents re-marks its table; repeat until a full pass drains empty. Bounded by the shared `reconcileMaxRounds` cap (10): a target reparenting on every serial redo surfaces a LOUD non-convergence error naming the still-touched tables and the `--bulk-parallelism 1` / pre-sized-target remedy, rather than looping forever.

## Correctness

- Re-reading a table from the (static-precondition) source into a truncated target makes it **exactly match the current source**, regardless of what the reparent dropped — no per-table `COUNT(*)` gate needed (impractical at billion-row scale). **The reconciliation IS the guarantee**, exactly as in ADR-0113.
- Convergence proxy: an empty touched-set ⇒ no reparent occurred during the pass ⇒ no loss, since a reparent is the only loss vector this addresses. Conservative touch-tracking (marking on *any* classified transient, not only confirmed loss) over-reconciles slightly rather than ever missing a lossy table — safe by construction.
- **Static-source assumption (explicit).** The redo re-reads the *live* source table, so a row inserted/updated/deleted on the source between the initial copy and the redo is reflected in the redo. This is sound under migrate's existing precondition (a static source; plain migrate already has no cross-table snapshot consistency — ADR-0019) and is the same trade-off the rest of the migrate cold-copy already makes. The reconciliation makes the touched table match the source *as of the redo*. This is called out in the code comments on `reconcileMigrateReparentTouched` / `reapplyMigrateTableForReconcile`.

## Consequences

- `migrate` becomes **correct by construction** on a reparenting non-Metal PlanetScale MySQL target — the Bug 175 silent under-copy is closed.
- Cost: reparent-touched tables are copied twice (once in the fast parallel/serial pass, once in the serial redo). Bounded to the touched subset, only when a reparent actually occurred; no-op (zero cost) on every clean run.
- Engine scope: **MySQL first.** The touch-observer is wired engine-neutrally, but only the MySQL `RowWriter` implements `ir.ReparentObserverSetter` today (the confirmed-affected PlanetScale-MySQL path); on PG it is a no-op, and the reconciliation slots only into the MySQL fallback branch — the PG overlapped copy+index path is untouched. The machinery is engine-neutral (all in the pipeline package via the optional `ir` interfaces) so PG can adopt the observer if a PG reparent-loss is ever demonstrated.

## Alternatives considered

- **Grow-gate alone (already wired into migrate, ADR-0110):** necessary but insufficient — reactive, cannot recover the pre-transient window. Kept (it calms the target + reduces loss); reconciliation is layered on top, identically to ADR-0113.
- **Mandatory post-migrate `COUNT(*)` verify:** impractical on very large tables (statement-timeout); reconciliation gives the same guarantee without a full scan.
- **Snapshot-pinned redo (read the table at a pinned consistent point):** migrate has no pinned snapshot (ADR-0019), and adding one is a separate, larger chunk; re-reading the live source is sound under the existing static-source precondition, so it is the right scope here.
- **Revert migrate parallel copy to serial:** the safe-but-slow floor; reconciliation keeps the parallel speed AND correctness instead.
