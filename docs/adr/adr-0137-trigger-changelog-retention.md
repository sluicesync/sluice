# ADR-0137: Trigger-CDC change-log retention / pruning (`sluice trigger prune`)

## Status

**Accepted — three phases shipped: Phase A v0.99.151, Phase B v0.99.174, Phase C
(the source-side consumer registry, roadmap item 115) unreleased on `main`.**
Proposed 2026-06-28. Phase A is `sluice trigger prune`; Phase B is
`--auto-prune-change-log`. Roadmap item 49 follow-up —
addresses Bug 165 (and the shared growth vector behind pgtrigger Bug 159). Phase A: an
operator-run `sluice trigger prune` command that safely reaps consumed change-log rows.
Phase B: the opt-in in-stream sidecar (§Decision 2, which had already been rewritten to
"IMPLEMENTED" while this header still called it deferred — a self-contradiction the G-17
gate could not see, because it only compares the header against the index row and the
index row repeated the same stale "Phase B (deferred)").

## Context

The trigger-CDC engines — `sqlite-trigger` (ADR-0135), `d1-trigger` (ADR-0136), and
`pgtrigger` (ADR-0066) — capture every source change as a row in `sluice_change_log`
(with before/after images) and **never reap consumed rows**. The CDC reader advances a
watermark (`{"last_id":N}`) but issues no `DELETE` against the change-log, so it grows
unbounded for the life of the sync. Bug 165 measured a 476 MB source `.db` bloat to
**1.06 GB / 732,842 change rows** in a single 3-minute / 343k-op run; on a long-running
write-heavy continuous sync the change-log dwarfs the base tables and eventually fills
disk (on D1 it is also billable rows-written/storage). Exactly-once is unaffected — this
is pure storage growth — but it is a real operational problem.

### The correctness crux (silent-loss avoidance — load-bearing)

A change-log row may be pruned **only if it is DURABLY APPLIED on the target** — i.e. its
`id` is `<=` the watermark the applier has **persisted to the target's cdc-state**.
The exactly-once contract advances that persisted watermark only on durable apply, so the
persisted `last_id` *is* the durably-applied frontier. The CDC reader's own read-position
runs AHEAD of it (it reads + emits faster than the applier durably commits). **Pruning
based on the reader's read-position would delete rows that are not yet durably applied; a
crash before they apply would then warm-resume to `id > durable_watermark` and find those
rows GONE — silent permanent loss.** So pruning must key off the *target-persisted*
durable watermark, never the source reader's read cursor.

## Decision

1. **Phase A — `sluice trigger prune` (operator-run, cron-able), safe by construction.**
   A command that:
   - connects to the **target** and reads the durably-persisted CDC position for the
     stream (the same cdc-state row the applier writes / `sync status` reads) → extracts
     the applied `last_id`;
   - connects to the **source** and `DELETE`s `sluice_change_log` rows with
     `id <= (applied_last_id - safetyMargin)`, where `safetyMargin` keeps the most-recent
     N rows (default a small N, operator-tunable) as belt-and-suspenders;
   - on SQLite/D1 optionally `VACUUM`s to reclaim file space (PG relies on autovacuum);
   - **refuses loudly** if it cannot read the target's durable position (it must NEVER
     prune blind — no position ⇒ no safe lower bound ⇒ abort).
   - Engine-dispatched and SHARED across `sqlite-trigger` / `d1-trigger` / `pgtrigger`
     (the change-log + cdc-state shapes are common); on D1 the DELETE runs over the
     `/query` HTTP API, on SQLite over the file, on PG over SQL.
   This is unambiguously safe (it reads the authority's durable frontier, prunes strictly
   below it with a margin) and decoupled from the live stream. An operator schedules it
   (cron / sidecar / a `--prune-interval` wrapper) alongside a continuous sync.

2. **Phase B — automatic in-stream pruning (IMPLEMENTED).** The streamer runs an
   opt-in (`--auto-prune-change-log`) failure-isolated sidecar that prunes the source
   change-log on a wall-clock cadence (`--auto-prune-interval`, default 5m; margin
   `--auto-prune-keep`, default 1000) so operators don't schedule a cron. It removes the
   manual-scheduling burden of Phase A while keeping Phase A's exact safety contract.

   **Implementation notes / divergence from the original sketch (flagged per convention):**
   - *Durable-frontier source.* Rather than plumbing the applier's per-checkpoint
     durable-persist signal back to a source-side hook (the original sketch), the sidecar
     reads the target's durably-persisted position on each cadence via the applier's
     `ReadPosition` — the SAME cdc-state row `sluice trigger prune` reads — and hands that
     token to the source. This is simpler, needs no new checkpoint→source plumbing, and is
     just as safe (the persisted position IS the durably-applied frontier). The timer-based
     cadence also naturally satisfies "do not prune on every checkpoint".
   - *Engine-neutral seam.* A new optional IR capability `ir.ChangeLogPruner`
     (`PruneConsumedChangeLog(ctx, durablePositionToken, keep)`) is implemented on the
     trigger engines' CDC-reader types (sqlite-trigger / d1-trigger / pgtrigger). The engine
     decodes the token with its OWN codec (reusing `AppliedLastID`, which refuses a FOREIGN
     token loudly), computes `cut = appliedLastID - keep`, and reaps `id <= cut` — keeping the
     position codec inside the engine so the streamer stays engine-neutral (it never imports a
     trigger package). A non-trigger source doesn't implement the interface ⇒ typed-nil no-op.
   - *Failure isolation (the one deliberate divergence from Phase A).* Unlike the Phase-A
     command, which fails LOUD, a Phase-B prune error is logged at WARN and SWALLOWED — it is
     background housekeeping and must never break or stall the sync, mirroring the ADR-0107
     telemetry sidecars.
   - *Default OFF (zero-value-safe).* `--auto-prune-change-log` defaults false: auto-DELETEing
     source rows is an explicit operator opt-in for the first cut, and the zero value is the
     safe/pre-Phase-B default for every construction (CLI, tests, broker/chain, future callers).
     Default-ON is a possible future once the cadence is field-proven on real continuous syncs.

3. **Phase C — the SOURCE-SIDE CONSUMER REGISTRY (roadmap item 115, IMPLEMENTED).**
   Phases A and B both cut at ONE stream's frontier by change-log `id`. A source change
   log is shared by every sync reading that database, so on the staged/wave shape the
   docs recommend — several syncs off one source, disjoint table sets, necessarily
   different speeds — the faster stream deleted the rows between the two frontiers before
   the slower one read them: silent, permanent, undiscoverable from the slow side (audit
   2026-08-01 S4). v0.110.0 warned about it; this phase fixes it.

   - *The registry.* `sluice trigger setup` installs a third source-side table,
     `sluice_change_log_consumers (consumer_id TEXT PRIMARY KEY, applied_id, updated_at)`,
     and the change-log `schema_version` moves 1 → 2. EVERY trigger-CDC stream publishes
     its durably-applied frontier there on a one-minute cadence — **whether or not it
     opted into auto-prune**, because the stream that loses rows is typically the one
     WITHOUT the flag. The prune then cuts at
     `min(MIN(applied_id) across the registry, this stream's freshly-read frontier) - keep`.
   - *Engine-neutral seam.* A new OPTIONAL COMPANION capability
     `ir.ChangeLogConsumerRegistry` (`RegisterChangeLogConsumer` +
     `PruneConsumedChangeLogToRegisteredMin`) sits beside `ir.ChangeLogPruner`. A source
     that exposes the base pruner WITHOUT the companion is **not pruned at all** — the
     sidecar fails CLOSED and says so at ERROR. A fail-open default would recreate the
     defect for exactly the engine someone forgot to migrate. The cut decision itself
     lives once, in `internal/engines/internal/triggercdc.RegistryCut`, shared by all
     three engines.
   - *The empty-registry hazard (the primary one).* An empty registry is
     indistinguishable from one nobody has written to yet, so reading it as "no consumers
     ⇒ prune everything" would be a worse silent-loss bug than the original. `RegistryCut`
     REFUSES on an empty registry, and refuses again when the calling stream's own
     `consumer_id` is absent — a cut derived from peers alone is not a safe bound for the
     caller.
   - *Cold-copy window.* Registration starts at the cold-start SNAPSHOT OPEN, not at first
     apply: the CDC anchor is already taken, a bulk copy can run for hours, and every
     change captured in that window is one the copying stream must eventually apply. It
     publishes frontier 0 until the apply loop starts, which blocks every peer's prune —
     the safe direction.
   - *Stale consumers are WARNED about, never evicted.* A registration that has gone quiet
     (>30 min) still holds the prune back and is named at WARN. Auto-eviction would delete
     the rows a stream that is down for maintenance has not read — the exact silent loss
     the registry exists to prevent. An operator releases it by deleting its registry row.
   - *Cross-version.* A NEW binary meeting an UN-MIGRATED change log (no registry table,
     or `schema_version` < 2) refuses to auto-prune and names `sluice trigger setup` as
     the migration — which is idempotent, so re-running it IS the migration. The version
     half of that check is what catches an OLD binary sharing the source: re-running ITS
     `trigger setup` leaves the registry table but rewrites `schema_version` back to 1,
     and such a binary streams without ever registering. **UNCLOSED, and stated in the
     flag help + a startup WARN:** a pre-registry peer that merely STREAMS (never runs
     setup) leaves no trace on the source, so it cannot be detected — every sync on a
     shared source must be upgraded before enabling the flag.
   - *Migration is a maintenance-window action.* On PG the migration's own
     `CREATE TABLE sluice_change_log_consumers` is picked up by the engine's DDL event
     trigger as an `op='X'` marker, which a live reader turns into a loud schema-change
     refusal. Re-running `trigger setup` already drops and recreates the capture triggers,
     so it was never safe against a live stream; this does not change that, it just makes
     the reason visible (pinned in `TestItem115_SetupMigratesAV1Install_PG`).
   - *The operator command is CLAMPED, not gated.* `sluice trigger prune` lowers its
     computed cut to the registry MIN and prints that it did. It deliberately does NOT
     fail closed when there is no registry evidence (no table, or no rows): refusing an
     explicit operator action that has been safe for a single stream since Phase A would
     be a regression, where clamping is a strict improvement.

## Consequences

- Operators can bound change-log growth with a scheduled `sluice trigger prune`, safely:
  only durably-applied rows (minus a margin) are removed, so warm-resume — which reads
  `id > durable_watermark` — never needs a pruned row.
- One command across all three trigger engines; the retention story Bug 159 and Bug 165
  both pointed at is now shared.
- Bounding growth on a continuous sync is automatic once `--auto-prune-change-log` is set
  (Phase B); with the flag off — the default — it stays an explicit operator action.
- Several syncs may share one trigger-CDC source with auto-prune enabled (Phase C): the
  prune waits for the slowest registered consumer. The cost is that a stopped-but-still-
  registered sync holds the change log until an operator removes its registry row, and a
  source installed before v2 must be migrated (`sluice trigger setup`) before auto-prune
  will do anything at all.

## Alternatives considered

- **Prune in the CDC reader keyed on its read-position.** REJECTED — the read cursor is
  ahead of the durable frontier; pruning there risks deleting not-yet-applied rows =
  silent loss on resume (the crux above).
- **A TTL / max-rows cap enforced by the capture trigger.** REJECTED — a trigger can't
  know the consumer's durable frontier, so a TTL could delete an un-applied row (silent
  loss) on a slow/stopped consumer.
- **Automatic in-stream prune as the first cut.** Deferred to Phase B — correct but needs
  the durable-checkpoint→source-prune plumbing; the safe operator command ships first.
