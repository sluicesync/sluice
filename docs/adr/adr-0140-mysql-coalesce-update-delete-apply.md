# ADR-0140: Coalesce UPDATE/DELETE in MySQL CDC apply (the Bug 169 U/D tail)

## Status

**Accepted (2026-06-29).** Extends ADR-0139 (multi-row INSERT coalescing) to UPDATEs and DELETEs. Justified by the Tier 1 (local `tc netem`) + Tier 2 (real PlanetScale-MySQL) update/delete-heavy WAN characterization. MySQL target only.

## Context

ADR-0139 made INSERT-heavy MySQL CDC apply round-trip-efficient by coalescing consecutive same-shape inserts into one multi-row `INSERT … ON DUPLICATE KEY UPDATE`. UPDATEs and DELETEs were left on the serial per-change path (one `tx.ExecContext` = one round trip each). The U/D characterization measured the cost:

- **Tier 1 (local, `tc netem`):** pure-U/D apply ≈ `W/(2·RTT)` — 22/s @80 ms, 11/s @160 ms (W=4); a 50/40/10 mix = 28/s @80 ms. Insert-only stayed in the thousands/s. ~100× gap. Exactly-once held throughout.
- **Tier 2 (real PlanetScale-MySQL, ~101 ms):** pure-U/D ≈ 37/s capped; on the **default** config the Vitess 20 s transaction-killer fires (a lane's U/D batch needs ~750 round trips ≫ 20 s), and the AIMD back-off + in-lane split recover with a ~115 s dead-window + sawtooth → net ~13/s (≈3× worse than capped). It **converges** (not a permanent stall) and exactly-once held in all cells.

So any sustained U/D fraction above a few percent cannot keep up with a 1k+/s source over WAN, and on Vitess it additionally pays the recurring tx-killer tax. Lane scaling (`W/(2·RTT)`) can't close a 100× gap. The PG target is unaffected — `dispatchPipelined` already queues U/D onto one `pgx.Batch`/`SendBatch` (ADR-0092), so PG U/D apply is already round-trip-efficient; **this tail is MySQL-specific** (MySQL has no client-side statement-pipelining primitive).

### The current U/D shape (load-bearing for the design)

`buildUpdateSQL` and `buildDeleteSQL` key their `WHERE` on the **full before-image** (`buildWhereClause(before)` — every non-generated column `= ?`, `IS NULL` for nulls), not the primary key. UPDATE is `UPDATE t SET <after> WHERE <all before cols>`; DELETE is `DELETE FROM t WHERE <all before cols>`. This is an exact-prior-row match (and the only option for a keyless table).

## Decision

**Coalesce consecutive, same-table, KEYED, non-PK-changing UPDATEs and DELETEs by primary key**, reusing parameterised builders so value encoding is unchanged. No multi-statement, no client-side interpolation (the value-fidelity hazard is avoided by construction).

1. **UPDATE → upsert.** A keyed, non-PK-changing UPDATE is applied as the **same multi-row `INSERT (after-image) … ON DUPLICATE KEY UPDATE col = new.col` upsert the INSERT path already uses** (ADR-0139's `buildMultiRowInsertSQL`). The row exists (see correctness below), so MySQL takes the `ON DUPLICATE KEY UPDATE` branch and sets the after-image by PK — identical end state to the serial UPDATE. Consecutive inserts *and* update-upserts to the same table+shape coalesce into ONE multi-row statement.
2. **DELETE → keyed `IN`.** Consecutive keyed DELETEs to one table coalesce into one parameterised `DELETE FROM t WHERE (pk1,…) IN ((?,…),(?,…),…)` (single-column PK uses `WHERE pk IN (?,?,…)`). PK values bind through the same `prepareApplierValue` codec.
3. **Exclusions stay serial (full-before path, unchanged):** keyless tables (no PK to key on — also the ADR-0089 at-least-once contract), PK-changing UPDATEs (would orphan the old-PK row — already routed to the laneapply barrier; the single-lane path guards on `before-PK != after-PK`), and any change whose table/shape/kind differs from the pending run (flush-then-proceed, preserving apply order). A DELETE flushes a pending upsert-run and vice-versa (different statement kinds), so order across kinds is preserved.

### Correctness (the load-bearing analysis)

- **The `WHERE` semantics change from full-before-match to PK-based — and that is correct, indeed self-healing, for a keyed CDC stream.** The source's intent is "PK's row becomes <after>" / "PK's row is gone"; keying by PK realises exactly that. The old full-before match only *adds* a guard that silently skips when the target isn't in the exact expected prior state — for CDC that just means a replayed/already-applied change, which the PK form handles idempotently anyway, or a drifted target, which the PK form *corrects* to the source state rather than leaving diverged. sluice never treated a 0-row UPDATE/DELETE as an error, so no behaviour that depended on the skip is lost.
- **upsert-for-UPDATE never resurrects a deleted row in a valid stream.** A CDC UPDATE is only ever emitted for a row that existed at capture (an UPDATE matching 0 source rows fires no trigger / writes no binlog row event), and sluice preserves per-PK order (same PK → same lane → source order), so by the time an UPDATE applies, its INSERT has applied and the row exists → the upsert takes the UPDATE branch. The only way to upsert onto an absent row — a DELETE then UPDATE of the *same* PK with no intervening re-INSERT — is not a sequence a correct source emits (the source UPDATE-after-DELETE is itself a no-op). Pinned by an explicit delete-then-update integration test.
- **PK-changing UPDATE excluded.** Upserting the after-image at the new PK would leave the old-PK row orphaned; these route to the serial/barrier path (the lane router already does via `PKChangedUpdate`; the single-lane coalescer guards explicitly).
- **Apply order preserved.** Only consecutive same-table, same-kind (upsert-run vs delete-run), same-shape, keyed changes coalesce; any boundary flushes the pending run first. Within one multi-row upsert, MySQL resolves same-PK rows left-to-right (last wins) = serial order; within one `DELETE … IN`, set membership is order-independent.
- **Idempotency (ADR-0010) preserved.** Upsert and delete-by-PK are both idempotent — replay of any prefix reproduces the same final state.
- **Value fidelity unchanged.** Every value still binds to a `?` through `prepareApplierValue` (upserts reuse `buildMultiRowInsertSQL` verbatim; `DELETE … IN` binds PK values the same way). No interpolation, no per-type dispatch — the same property that made ADR-0139 safe. The value-fidelity-reviewer must still re-derive the family matrix for the new `DELETE … IN` builder (composite-PK tuple binding) and the update-as-upsert path.

### Scope

MySQL target, keyed tables, the concurrent-lane path and the single-lane batch path (both drive the ADR-0139 `mysqlBatchTx` accumulator — extend it to buffer an upsert-run AND a delete-run). PG is out of scope (already pipelines U/D). The serial fallback (non-pgx / keyless / PK-change / partial unsupported shape) is unchanged.

## Consequences

- U/D-heavy MySQL CDC apply over WAN moves from ~`W/(2·RTT)` toward the batched band (round trips per lane-batch ≈ constant, not per row), and — because batches then commit fast — the Vitess 20 s tx-killer stops firing, so the ADR-0139-era interim "cap AIMD on Vitess" stopgap becomes unnecessary (this fix subsumes it).
- A behaviour change for keyed UPDATE/DELETE matching (full-before → PK). Documented; justified above; gated by the value-fidelity + correctness review and the `-race` integration suite before tag.
- Keyless U/D keep the serial full-before path and its existing throughput/at-least-once profile.

## Validation

Unit: the `DELETE … IN` builder (single + composite PK, arg binding, value families); the coalescer state machine (upsert-run vs delete-run vs serial flush boundaries; keyless / PK-change / shape / table / kind transitions; order). Integration (real MySQL, `//go:build integration`): a mixed insert/update/delete workload over the full value-family matrix applied serially (oracle) vs single-lane-coalescing vs W=4 concurrent must produce byte-identical target state with exactly-once, plus targeted pins for delete-then-update (no resurrection), PK-changing update (serial, no orphan), keyless U/D (serial), and idempotent replay. A re-measure on the latency rig should lift U/D-heavy apply from ~37/s toward the batched band. `-race` integration before tag (concurrency change).
