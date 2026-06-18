# ADR-0102: Native-MySQL per-table plain-INSERT write fan-out (W × D cold-copy)

## Status

Accepted. The documented follow-up to [ADR-0101](adr-0101-native-mysql-concurrent-cold-copy.md): it takes the native-MySQL (binlog-flavor) concurrent cold-copy from **W × 1** (W tables concurrent, ONE plain-INSERT writer each) to **W × D** by composing the [ADR-0097](adr-0097-parallel-writer-fanout-vstream-snapshot-copy.md) per-table write fan-out onto the plain-INSERT path. It **reuses ADR-0097's PK-hash partition verbatim** (`partitionRowsByPK` / `pkWorkerIndex` — the exactly-once routing) and the ADR-0100 W-pipeline consumer verbatim (`runConcurrentTableCopy`); its only new core is a **plain-INSERT** N-worker writer surface (`ir.ParallelCopyWriter.WriteRowsParallel`) — the mirror of ADR-0097's idempotent `WriteRowsIdempotentParallel`, calling the engine's plain batched-INSERT core instead of the upsert core. Does not touch the VStream path, the Postgres cold-copy path, the ADR-0079 fast shareable-snapshot path, or `sluice migrate`.

## Context

### The measured problem (Track D, validated live, v0.99.70)

Track D re-test (`lst-mysql-d`, `copy_table_parallelism=4`) measured **ADR-0101 native concurrent cold-copy at ~10.4 MB/s vs ~0.66 MB/s serial = ~15.7×** — a large win that removed the serial-consumer ceiling. But ADR-0101 is **W × 1**: N tables copied concurrently, each by ONE plain-INSERT writer. Vitess → PS-MySQL on the same path was **~26 MB/s** (W × D, the VStream COPY fanned each table across D writers via ADR-0097/0100). The remaining ~2.6× gap is exactly the per-table fan-out ADR-0101 §24 / §88 / §116 flagged as the documented follow-up.

### Why D fan-out helps on this path specifically

PS-MySQL blocks `LOAD DATA LOCAL INFILE` (vtgate), so the native writer falls back to batched INSERT. A single batched-INSERT connection across a region is **RTT-bound**: each multi-row statement pays the round-trip latency, and one connection can only have one statement in flight. Fanning a table's rows across D connections (each its own batched-INSERT stream, each its own pinned pooled connection) overlaps D round-trips — the same lever ADR-0097 proved for the VStream idempotent path, where 3–4 streams already beat PG's single-stream COPY with near-linear scaling.

### Why ADR-0101 stopped at W × 1

ADR-0101 reused the ADR-0100 consumer, which on the non-idempotent (native) branch calls the plain per-table helper `copyTable` — a SINGLE-writer plain INSERT. The ADR-0097 fan-out (`copyTableColdStartIdempotentMaybeParallel` → `WriteRowsIdempotentParallel`) lived ONLY on the idempotent (upsert) branch. A fresh-target native cold-copy is plain INSERT, not upsert, so it could not route through the existing idempotent fan-out without either (a) duplicating the routing, or (b) generalizing the fan-out to a plain-INSERT mode. This ADR picks (b)-by-reuse: the **routing** (`partitionRowsByPK`) is shared verbatim; only the **writer surface** is duplicated (plain vs upsert), because the engine's plain and upsert batched-INSERT cores are genuinely different SQL.

## Decision

Add a **plain-INSERT N-worker writer capability** `ir.ParallelCopyWriter` (`WriteRowsParallel(ctx, table, workers []<-chan Row) error`) — the plain-INSERT mirror of `ir.ParallelIdempotentCopyWriter.WriteRowsIdempotentParallel`. Add a pipeline helper `copyTablePlainMaybeParallel` — the plain-INSERT mirror of `copyTableColdStartIdempotentMaybeParallel` — that:

- routes through `WriteRowsParallel` when **degree > 1 AND the writer implements `ir.ParallelCopyWriter` AND the table has a usable PRIMARY KEY** (the partition key);
- falls through to the serial single-writer `copyTable` otherwise (no-PK table, non-capable writer, degree 1) — always correct, just no speedup.

The fan-out reuses **`partitionRowsByPK` verbatim** (the same dispatcher ADR-0097 proved: every row to exactly one of D channels, same PK → same worker). The MySQL `WriteRowsParallel` runs one worker per channel, each pinning its own pooled `*sql.Conn` and running the **existing plain `writeBatchedConn` core** (refactored out of `writeBatched` so the serial and fan-out paths share byte-identical INSERT mechanics + Vector-B warning probe). It returns only after every worker has drained and durably committed (the ADR-0007 position-after-all join).

Then the ADR-0100 consumer's native branch (`needsIdempotent == false`) calls `copyTablePlainMaybeParallel` instead of `copyTable`, so each of the W native pipelines fans its active table across D writers → **W × D**.

```
 group 0 (reader conn 0): [a, c] ──ReadRows(a)→ partitionRowsByPK ─┬─ INSERT worker 0 (own conn)
                                                                    ├─ INSERT worker 1 (own conn)
                                                                    └─ … D workers
 group 1 (reader conn 1): [b, d] ──ReadRows(b)→ partitionRowsByPK ─┬─ INSERT worker 0
   …                                                               └─ … D workers
        total target write concurrency = W groups × D workers = W × D
```

The six design questions, resolved:

### 1. Plain-INSERT fan-out vs generalizing the idempotent writer (the chosen-vs-rejected design)

**Chosen: a separate plain `ir.ParallelCopyWriter` surface that reuses ADR-0097's `partitionRowsByPK` routing.** **Rejected: routing native through the existing idempotent `WriteRowsIdempotentParallel` (upsert mode) on a fresh target.**

The rejected option (route native through the upsert path) would technically work on a fresh empty target — there are no existing rows to collide with, so ON DUPLICATE KEY UPDATE never fires — but it is **wrong in three ways**: (1) it would refuse no-PK / keyless tables loudly (`errKeylessIdempotent`), even though a plain-INSERT native cold-copy on a fresh target handles a keyless table fine (it just doesn't fan it out); (2) it pays the upsert SQL cost (the `AS new ON DUPLICATE KEY UPDATE` clause + SET-list) for no benefit on a fresh target; (3) it semantically conflates "the source re-emits rows so we MUST upsert" (Bug 125, VStream) with "the source is gap-free so we plain-INSERT" (native binlog) — the exact distinction `needsIdempotent` carries. Keeping a distinct plain surface preserves that distinction and keeps the keyless-table behaviour correct.

The duplication is minimal and bounded: the **routing** (the load-bearing exactly-once part) is shared verbatim (`partitionRowsByPK`); only the writer's N-worker execution shell is mirrored, and even that delegates to the shared `writeBatchedConn` core. The mirror is ~40 lines of goroutine-lifecycle boilerplate identical in shape to `WriteRowsIdempotentParallel` (deliberately, so they review as a pair).

### 2. Exactly-once across W × D — same PK-hash partition, gap-free fresh-target plain INSERT

Each row is written by **exactly one** of the W × D workers:

- **Across W (between groups):** inherited from ADR-0101/0099 — the table partition is disjoint, so each table is owned by exactly one reader-group pipeline. No table is read or written by two groups.
- **Across D (within a group's active table):** `partitionRowsByPK` hashes every row to exactly one of the D worker channels (no drop, no fan-to-many), and the same PK always hashes to the same worker. This is the ADR-0097 routing, re-pinned for the plain path with the SAME family-coverage tests (single-int PK, composite PK, no-PK→serial fallback).

The **fresh-target precondition** makes plain INSERT safe under fan-out: the native concurrent path is gated to the cold-start-from-scratch path, and #52 (PR #244/#246) makes that path RESET a populated native-MySQL target before re-copy. So every target table is empty when the W × D INSERTs begin, the snapshot reader is gap-free + overlap-free (each row read exactly once from a frozen REPEATABLE-READ view, no re-emission), and the disjoint partition means no two workers ever touch the same row → **no overlap, no duplicate-key collision, no row written twice.** Plain INSERT (not upsert) is therefore correct: there is nothing to absorb.

**No-PK / un-partitionable table:** `copyTablePlainMaybeParallel` requires a usable PK to fan out; a no-PK table routes to the serial single-writer `copyTable` (W × 1 for that table). Never a wrong or partial fan-out — a no-PK table cannot be PK-hash-partitioned, so it is never split. (Unlike the idempotent path, a no-PK plain-INSERT native table is NOT refused — it is fully copied serially; the gap-free snapshot has no re-emission to duplicate.)

### 3. Connection budget — W × D + N readers, bounded, honest WARN (extends ADR-0101 §5)

During a native W × D concurrent copy: the **source** holds N pinned reader connections + the one binlog-dump CDC connection; the **target** now holds up to **W × D** writer connections (W groups × D pooled `*sql.Conn` per active table — each group has at most one table active at a time, so its D workers are the only writers it contributes). D is clamped by `resolveCopyFanoutDegree` to `[1, maxCopyFanoutDegree=64]`; W (= N) is clamped to `[1, min(len(tables), 32)]` by ADR-0101.

Consistent with ADR-0099/0100/0101 §5's **honest-WARN** approach: MySQL has no `ir.TargetConnectionBudgetProber` (no connection-slot model is queried), so neither N nor D is auto-clamped by the pipeline preflight on this path. The ADR-0101 native-open INFO line is **extended to name D and the W × D product** alongside N, restating the operator contract: `W × D ≤ --max-target-connections` AND `N ≤ source max_connections` are the operator's responsibility (no false auto-clamp claimed that isn't implemented). This composes the two existing knobs — `--copy-fanout-degree` (D) and the `copy_table_parallelism` source-DSN param (W = N) — without inventing a third. A future enhancement folds the product into a real prober-backed clamp.

### 4. Zero-value-safe — D = 1 is byte-identical to ADR-0101, W = 1 is serial

Both knobs are zero-value-safe (the v0.99.51 trap), and the composition preserves it:

- **D = 1** (no fan-out): `copyTablePlainMaybeParallel` sees degree 1 and calls the serial `copyTable` — **byte-identical to ADR-0101's W × 1 native path.** The Go zero value of `CopyFanoutDegree` resolves through `resolveCopyFanoutDegree` to the default (4), so an operator who sets `copy_table_parallelism>1` but leaves `--copy-fanout-degree` unset gets W × 4; an operator who explicitly sets `--copy-fanout-degree=1` gets W × 1 (today's ADR-0101 behaviour exactly).
- **W = 1** (no cross-table concurrency): the engine surfaces no concurrent partition, the serial table loop runs, and each table goes through `copyTablePlainMaybeParallel` too — so a SINGLE-table or `copy_table_parallelism=1` native cold-copy STILL gets D-way per-table fan-out (1 × D). This is a strict improvement over ADR-0101 (which had no fan-out at all on the serial loop's plain branch) and is itself zero-value-safe: D = 1 there is byte-identical to the pre-ADR-0102 serial `copyTable`.

There is no (W, D) input that produces "zero workers / copies nothing": both factors floor at 1.

### 5. Mid-COPY durable watermark — stays disabled, same argument as ADR-0097 §3

The native concurrent path already never wires the ADR-0072 mid-COPY durable watermark (ADR-0101 §7 / ADR-0100 §6: under W concurrent consumers the durable flushed-row frontier is not order-equivalent to any single stream's enqueue order). Adding D within each group does not change that — it only makes the non-order-equivalence stronger (per-worker flush order × per-group). `WriteRowsParallel` runs every worker with `reportDurable=false` (the same flag the idempotent fan-out passes), and the native concurrent path sets no `CopyDurableProgressSink` anyway. The SOLE durability guarantee for a fanned-out table is the whole-table join: `WriteRowsParallel` returns only after every worker durably commits, and the single FTWRL-recorded position (ADR-0101 §7) is read only after the W-way errgroup joins. Resume never fans out (ADR-0095 single-stream v1), so no resume path consumes a mid-COPY fan-out cursor.

### 6. Log clarity — name the native concurrency, don't say "serial cold-start" when it isn't

ADR-0079's PG-only fast-path gate (`coldStartFastEligible`) logs `"sync cold-start: source snapshot is not shareable (per-session / single-stream); using serial cold-start"` whenever the source has no exported shareable snapshot — which includes native MySQL. But native MySQL with `copy_table_parallelism>1` then engages ADR-0101/0102 concurrency ONE layer down (inside `runBulkCopyWithOpts` → `runConcurrentTableCopy`), so the message reads as a contradictory "fell back to serial" when it did not — it bit the Track-D measurement interpretation. The fix: the message no longer asserts "serial cold-start" unconditionally; it states the ADR-0079 fast-path was not taken, and a follow-on INFO from the native opener (ADR-0101 §5, now naming N × D) reports the actual concurrency engaged. The operator can tell ADR-0101/0102 native concurrency is active from the engine's `"native concurrent cold-copy: opened consistent multi-table snapshot"` line.

## Alternatives considered

- **Route native plain-INSERT through the idempotent `WriteRowsIdempotentParallel` (upsert) on a fresh target.** Works (no collisions on an empty target) but refuses no-PK tables, pays the upsert SQL cost for nothing, and conflates the gap-free vs re-emitting distinction `needsIdempotent` carries. **Rejected** (§1) for a distinct plain surface that reuses only the routing.
- **A new combined "parallel writer" interface covering both plain and upsert.** Collapsing `WriteRowsParallel` and `WriteRowsIdempotentParallel` into one method with a mode flag couples two genuinely-different SQL cores behind one signature and muddies the `needsIdempotent` dispatch the pipeline already keys on. **Rejected**; two narrow capability interfaces mirror the existing `RowWriter` / `IdempotentRowWriter` split.
- **Read-side PK-range chunking instead of write-side fan-out (the ADR-0019 migrate lever).** The native concurrent reader is N pinned snapshot transactions; range-chunking each table across the SAME pinned connection serialises on that one InnoDB connection (it can run one SELECT at a time — ADR-0101 §6), so read-side chunking would need yet more pinned snapshot connections per table. Write-side fan-out reuses the proven ADR-0097 lever and the existing connection pool. **Rejected for v1**; read-side chunking is the `migrate`-path lever, not the `sync` cold-start one.
- **Leave native at W × 1 and tell operators to use Vitess for parity.** Abandons the self-managed-MySQL → PlanetScale migration path's throughput. **Rejected**; the lever is a small, proven reuse.

## Consequences

- **Native vanilla-MySQL → PS-MySQL cold-copy rises from W × 1 toward W × D**, closing most of the remaining ~2.6× gap to the Vitess W × D ceiling (~26 MB/s) measured on Track D. Bounded by source/target/network capacity and the W × D connection budget, not by sluice's writer.
- **Reuses ADR-0097's PK-hash routing verbatim** (`partitionRowsByPK` / `pkWorkerIndex`) and the ADR-0100 consumer verbatim — the only new core is the plain-INSERT N-worker writer surface (mirror of the idempotent one) + the `writeBatchedConn` extraction.
- **D = 1 is byte-identical to ADR-0101's W × 1 native path**; W = 1 gains 1 × D per-table fan-out (a strict improvement). No (W, D) input copies nothing.
- **No-PK native table routes serial** (full copy, single writer) — never a partial fan-out, and NOT refused (unlike the idempotent keyless case; the gap-free snapshot has no re-emission to duplicate).
- **Connection budget is the operator's responsibility on this path** (no MySQL prober) with an honest WARN naming N × D — no false auto-clamp.
- **Mid-COPY durable watermark stays disabled** on the concurrent path (the ADR-0097 §3 / ADR-0100 §6 argument), so the whole-table join is the durability guarantee.
- **Concurrency chunk → `-race`-before-tag.** This adds D concurrent writer goroutines per active table per group (W × D total), each pinning its own pooled connection, plus the ctx-cancel/cleanup path. Per the project rule, the integration **`-race`** gate MUST pass **before** any tag is cut (push-first, tag-after, or `scripts/race-integration.ps1`). CGO is off on the dev box, so `-race` is CI-only here.

## Testing

- **Plain fan-out dispatch (unit):** `copyTablePlainMaybeParallel` routes to `WriteRowsParallel` when degree > 1 + writer capable + PK present; falls back to serial `copyTable` for a no-PK table, a non-capable writer, or degree 1. Mirrors the ADR-0097 `copyTableColdStartIdempotentMaybeParallel` dispatch pins.
- **Exactly-once routing (unit):** reuses the ADR-0097 `partitionRowsByPK` exactly-once + same-PK→same-worker + composite-PK + cancel-no-leak pins (the routing is shared verbatim, re-asserted on the plain path's helper).
- **Loud abort + Bug-68 reader-stream gate (unit):** a worker error fails the copy loudly; a reader scan error surfaces after the writers return (no silent truncation).
- **W × D end-to-end (integration, `integration` tag, real MySQL with RELOAD):** cold-copy a multi-table DB with `copy_table_parallelism=N>1` AND `--copy-fanout-degree=D>1` through the FULL pipeline → (a) multiple tables receive rows concurrently AND each table's writes are split across D connections (target PROCESSLIST shows > 1 connection inserting into the same table); (b) target `COUNT(*)` + content checksum == source per table (no gap/dup); (c) a no-PK table is fully copied via the single-writer fallback; (d) D = 1 is byte-identical to the ADR-0101 W × 1 result; (e) clean CDC handoff from the ONE recorded position.
- **Connection-budget WARN (integration):** the native open INFO/WARN names N × D and the operator contract.
- **Concurrency `-race` (CI):** cancel mid-copy with W × D workers running → goroutine-count delta == 0, all connections closed, copy reports the cancel (not success).

## Silent-loss surfaces for value-fidelity review

Three invariants, each a silent-loss class if broken, called out explicitly:

1. **Every row written by exactly one of the W × D workers.** Across W: the disjoint table partition (ADR-0101 §2, inherited from ADR-0099, unit-pinned). Across D: `partitionRowsByPK` routes each row to exactly one channel, same PK → same worker (ADR-0097, unit-pinned). A row in zero channels is silently never written; a row in two is double-written. The plain-INSERT fresh-target precondition (#52 reset + gap-free snapshot) means a double-write would surface as a loud duplicate-key error, never a silent overwrite — but the routing's exactly-once is the primary guard.
2. **The fresh-target precondition holds before any plain INSERT.** The native concurrent path is the cold-start-from-scratch path, and #52 RESETs a populated native target before re-copy. If a populated target ever reached this path, a plain INSERT would collide loudly (duplicate key) — never silently corrupt — but the precondition is what makes plain INSERT (not upsert) the right choice. Value-fidelity review should confirm no non-fresh path can reach `WriteRowsParallel`.
3. **No-PK table is never fan-out-partitioned.** A table with no usable PK cannot be PK-hash-routed; `copyTablePlainMaybeParallel` routes it to the single-writer serial `copyTable`. A bug that fanned out a no-PK table would route every row to the same worker (the nil-PK hash) — not a loss, but a silent loss of the fan-out's exactly-once basis if the fallback regressed. The dispatch gate (PK-present check) is unit-pinned.
