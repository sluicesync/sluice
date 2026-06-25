# ADR-0113: Reparent-reconciliation for concurrent restore

- Status: Accepted
- Date: 2026-06-23
- Deciders: sluice maintainers
- Relates: ADR-0112 (within-table chunk parallelism), ADR-0110 (coordinated grow-gate), ADR-0108 (cold-copy reparent-retry)

## Context

ADR-0112's concurrent restore was found (Track C live A/B, 2026-06-23) to **silently under-copy rows** into a fresh non-Metal PlanetScale MySQL target during its first storage-grow reparent: `rc=0`, every table logged complete, but six tables short by ~17 M rows total vs the manifest; the serial restore matched the manifest exactly.

Root cause (Phase-A log forensics + a second live run): during the grow the volume fills (`Error 1114 table is full`), replication stalls, MySQL semi-sync times out and **falls back to async**, and the grow **reparents** (`Error 1105 TerminateAll`) — the new primary is promoted from a replica behind the async-acked window, so committed-and-acked rows are dropped. Parallel writers (8-wide) build a large async-acked window → large loss; the single RTT-bound serial writer never outruns semi-sync, so it loses nothing.

Wiring the ADR-0110 grow-gate into restore (so workers quiesce together on the first transient) **reduced** the loss (~17 M → ~5.4 M, 6 → 4 tables) but did **not** eliminate it: the gate is reactive — it trips on the *first observed transient*, but the lost rows were committed-and-acked in the window **before** any worker saw a transient. No pause, however well-timed reactively, can recover the pre-transient window.

## Key insight

A CDC stream cannot recover a dropped acked change — the source has moved on. **Restore can.** Its source is the **immutable, infinitely replayable chunk set**, and the target is a known cold target. So restore can always reach exactly-once by **re-applying from the chunks until the target matches the (replayable) source.** A reparent drop just means "some applied rows were lost — re-apply them." This is the restore analog of CDC's "idempotent apply + replay from a durable position", and restore is in a *better* position because the chunks never expire.

## Decision

**Reparent-reconciliation.** Keep the fast parallel bulk-copy; add a reconciliation phase that re-derives any reparent-touched table from its durable chunks.

1. **Track touched tables.** When a writer hits a classified grow/reparent transient (the same point that trips the grow-gate, `flushWithReparentRetry`), it marks that table on a run-shared reparent tracker. Wired engine-neutrally via an optional `ir.ReparentObserverSetter` the writer implements; the restore sets the observer onto every writer through the single `openTargetRowWriter` construction path, alongside the grow-gate.

2. **Reconcile after the bulk-copy.** If the tracker is non-empty after `runRestoreTablePool`, for each touched table re-derive it from its chunks:
   - **Non-DataOnly (normal cold restore):** `TRUNCATE` the table (via `ir.TableTruncator`, implemented by both engines) then re-apply its chunks **serially** (within-table parallelism = 1 — the proven-safe pace). No primary-key requirement; the cold target has no indexes/constraints yet (those are later phases), so TRUNCATE is clean and cheap.
   - **DataOnly (chain rotation segment):** re-apply the chunks **idempotently** (the segment's `WriteRowsIdempotent` UPSERT) without truncating — truncation would wipe a prior segment's rows; idempotent re-apply converges.

3. **Loop until clean.** A reconciliation redo runs through the same `flushWithReparentRetry` + grow-gate, so a redo that *itself* hits a reparent re-marks its table; repeat until a full pass observes no new touches. Bounded (a small max-round cap) so a target reparenting on every redo surfaces loudly rather than looping forever — the redo's own ~30-min per-batch wall-clock budget is the inner floor.

**Why serial redo is safe + fast enough:** by reconciliation time the volume has already grown to its final size (the grows happened pulling in the full dataset), so a redo writes into an already-grown volume and does not trigger a fresh reparent. The redo is serial (the pace that never loses) and runs only over the *touched* subset (typically a handful of tables), so the bulk restore stays fast (parallel) and only the few reparent-exposed tables pay the serial-redo cost. Best of both: fast bulk + correct.

## Correctness

- Re-deriving a table from its immutable chunks into a truncated (or idempotently-converged) target makes it **exactly match the manifest**, regardless of what the reparent dropped — no per-table `COUNT(*)` gate needed (that is impractical at billion-row scale). The reconciliation *is* the guarantee.
- Per-chunk SHA-256 + the layer-2 row-count sum still run on every (re-)apply, so the integrity floor is unchanged.
- Conservative touch-tracking: marking on *any* classified transient (not only confirmed loss) over-reconciles slightly rather than ever missing a lossy table — safe by construction.

## Consequences

- Parallel restore becomes **correct by construction** on a reparenting target — the default can stay auto-on (ADR-0112) and be safe.
- Cost: reparent-touched tables are applied twice (once in the fast parallel pass, once in the serial redo). Bounded to the touched subset, only when a reparent actually occurred.
- Engine scope: the touch-observer is wired on the MySQL writer (the confirmed-affected PlanetScale-MySQL path) first; the reconciliation machinery is engine-neutral and the PG writer can adopt the observer when a PG reparent-loss is demonstrated.

## Alternatives considered

- **Grow-gate alone (ADR-0110 wired into restore):** necessary but insufficient — reactive, can't recover the pre-transient window. Kept (it reduces loss + calms the target); reconciliation is layered on top.
- **Proactive telemetry-driven gate:** trips before the fill, but ADR-0110 documents the volume metrics go unstable across a reparent, so the timing is unreliable; a partial prevention, not a guarantee.
- **Mandatory post-restore `COUNT(*)` verify:** impractical on very large tables (statement-timeout); reconciliation gives the same guarantee without a full scan. A scale-aware per-chunk indexed-range verify (to re-apply only short chunks) is a future optimization on top of this.
- **Revert parallel default to serial:** the safe-but-slow floor; reconciliation lets us keep parallel's speed AND correctness instead.
