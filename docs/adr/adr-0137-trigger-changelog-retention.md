# ADR-0137: Trigger-CDC change-log retention / pruning (`sluice trigger prune`)

## Status

**Proposed (2026-06-28).** Roadmap item 49 follow-up — addresses Bug 165 (and the
shared growth vector behind pgtrigger Bug 159). Phase A: an operator-run `sluice trigger
prune` command that safely reaps consumed change-log rows. Phase B (automatic in-stream
pruning) is deferred.

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

## Consequences

- Operators can bound change-log growth with a scheduled `sluice trigger prune`, safely:
  only durably-applied rows (minus a margin) are removed, so warm-resume — which reads
  `id > durable_watermark` — never needs a pruned row.
- One command across all three trigger engines; the retention story Bug 159 and Bug 165
  both pointed at is now shared.
- Until Phase B lands, bounding growth on a continuous sync is an explicit operator action
  (documented), not automatic.

## Alternatives considered

- **Prune in the CDC reader keyed on its read-position.** REJECTED — the read cursor is
  ahead of the durable frontier; pruning there risks deleting not-yet-applied rows =
  silent loss on resume (the crux above).
- **A TTL / max-rows cap enforced by the capture trigger.** REJECTED — a trigger can't
  know the consumer's durable frontier, so a TTL could delete an un-applied row (silent
  loss) on a slow/stopped consumer.
- **Automatic in-stream prune as the first cut.** Deferred to Phase B — correct but needs
  the durable-checkpoint→source-prune plumbing; the safe operator command ships first.
