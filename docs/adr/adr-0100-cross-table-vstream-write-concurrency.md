# ADR-0100: Cross-table VStream cold-copy WRITE concurrency (K end-to-end read→write pipelines)

## Status

Accepted. The WRITE-side companion to [ADR-0099](adr-0099-cross-table-vstream-copy-concurrency.md) (the K-independent-read-streams lever). Builds on [ADR-0099](adr-0099-cross-table-vstream-copy-concurrency.md) (the disjoint table partition + K concurrent VStream COPY producers + the parallelism-agnostic set-min stitch — all reused verbatim), [ADR-0097](adr-0097-parallel-writer-fanout-vstream-snapshot-copy.md) (the per-table D-way write fan-out — composes multiplicatively: W tables × D writers), [ADR-0098](adr-0098-auto-shard-aware-vstream-resume.md) (the auto-shard-aware resume — composes per pipeline), [ADR-0095](adr-0095-vstream-auto-shard-by-table-copy.md) (the per-table auto-shard COPY + the per-shard set-min stitch), [ADR-0010](adr-0010-idempotent-applier.md) (idempotent apply — absorbs the per-table overlap each stream produces), and [ADR-0007](adr-0007-position-persistence.md) (position-then-data — the global position advances only after every pipeline completes). Does not touch the binlog (vanilla MySQL) or Postgres cold-copy paths, the `sluice migrate` path, or the ADR-0079 fast shareable-snapshot path.

## Context

### The measured problem (validated; not re-litigated here)

[ADR-0099] made the VStream cold-copy READ side concurrent: K independent vtgate VStreams, each over a disjoint table group, all filling one shared `rowBuffer` (one queue per table). Live Track-B measurement nonetheless held at **~1.4×** — the same ceiling [ADR-0097]'s write fan-out alone reached, and unchanged by combining K=4 reads × D=4 writers.

The decisive datum: with K=4 read streams and D=4 write fan-out, the **target PROCESSLIST showed exactly ONE table receiving rows at a time** — 28 of 28 polls over 90 s, never two tables concurrent. So K (reads) and D (per-table writers) are *orthogonal* levers on a *serial-write* workload; their product is bounded by the serial consumer, hence ~1.4× regardless.

### The structural cause: the orchestrator's serial consumer

The VStream cold-copy takes the serial `runBulkCopyWithOpts` path (`internal/pipeline/migrate.go`), **not** the ADR-0079 fast parallel path — the fast path's `chunkReaderFactory` mints *independent* snapshot-pinned readers (`SET TRANSACTION SNAPSHOT`), which only a **shareable** Postgres snapshot supports; VStream's rows come from one buffered `stream.Rows`, so it is gated out (`coldStartFastEligible` → "source snapshot is not shareable"). The serial path's core is:

```go
for _, table := range schema.Tables {
    copyTableColdStartIdempotentMaybeParallel(ctx, rows, rw, table, …) // ReadRows(table) → write
}
```

This loop drains **one table at a time**. `ReadRows(table)` pops `rowBuffer[table]`; the write (with optional D fan-out) runs to completion; only then does the loop advance to the next table. ADR-0099's K producers fill K queues concurrently, but the consumer empties them one at a time → one table is written at any instant. The bottleneck is the **serial one-table-at-a-time write consumer**, exactly as the PROCESSLIST showed.

The fix is to make the WRITE side concurrent across tables.

## Decision

Turn ADR-0099's K read streams into **K end-to-end read→write pipelines**: each of the W = K disjoint table groups is read AND written by its own consumer goroutine, instead of all K producers feeding one shared serial consumer. The engine already partitions the in-scope tables into K disjoint groups ([ADR-0099] `partitionTablesForStreams`); the pipeline learns that exact partition through a new optional capability surface and drives **W concurrent group-consumers**, each looping its group's tables serially through the *existing* `copyTableColdStartIdempotentMaybeParallel` (so the [ADR-0097] D fan-out composes per table).

```
                       W = K group-consumer goroutines
group 0: [a, c] ── ReadRows(a)→write(a) ─ ReadRows(c)→write(c)   ┐
group 1: [b, d] ── ReadRows(b)→write(b) ─ ReadRows(d)→write(d)   ├─ join (errgroup)
   …                              …                              ┘
                                                                  │
                              all W complete → schema phases (indexes, …) → CDC handoff
```

Because the partition is disjoint, **each table is read by exactly one producer stream AND written by exactly one consumer pipeline** — the producer↔consumer coupling stays 1:1 per table (the `rowBuffer[table]` queue has one producer and one consumer, unchanged from ADR-0099). The total write concurrency is `W × D` (W tables concurrent, each through a D-way fan-out), composing multiplicatively as ADR-0099 §"read-side analogue" anticipated.

The user runs one `sluice sync start` command; W is invisible above the snapshot contract. W = K reuses the existing `vstream_copy_table_parallelism` knob — **no new knob** (see §3).

### Where the write concurrency lives — and why the engine surfaces the partition

The serial consumer is in the **pipeline** layer (`runBulkCopyWithOpts`); the partition (`partitionTablesForStreams`) is computed in the **mysql engine** layer at snapshot open. The IR-first tenet forbids the pipeline importing the engine or re-deriving an engine-internal partition. So the engine surfaces its already-computed groups via a new **optional** reader capability `ir.ConcurrentCopyPartitioner`:

```go
type ConcurrentCopyPartitioner interface {
    RowReader
    // ConcurrentCopyGroups returns the disjoint table groups the cold-start
    // bulk copy may write CONCURRENTLY — one consumer pipeline per group.
    // nil / ≤1 group ⇒ no cross-table write concurrency (serial, today's path).
    ConcurrentCopyGroups() [][]string
}
```

`runBulkCopyWithOpts` type-asserts on it. When present **and** it returns ≥2 groups, the serial table loop is replaced by a W-goroutine errgroup, one per group, each calling the unchanged per-table copy helper. Absent (PG, vanilla MySQL, single-stream VStream) or ≤1 group → the existing serial loop runs **byte-identically**. The engine returns exactly the groups it partitioned the producers into (stored on the snapshot stream at open), so producer partition ≡ consumer partition by construction — the disjointness and coverage ADR-0099 already unit-pins is inherited, not re-derived.

The seven design questions, resolved:

### 1. Per-stream pipelines vs a shared write pool — pick the pipelines

**Chosen: K read→write pipelines** (each consumer writes its own group). **Rejected alternative: K producers → shared buffers → a W-worker write pool draining different tables.**

The pipeline shape wins decisively:

- **It reuses the disjoint partition ADR-0099 already computed and unit-pins.** The write pool would need its *own* table→worker assignment and a scheduler to hand idle workers the next unwritten table — new coverage/disjointness invariants to prove, on top of ADR-0099's.
- **It keeps the 1:1 producer↔consumer-per-table coupling intact.** ADR-0099's per-stream byte sub-budget (`perStreamCap = cap/K`) and the `tableStreamIdx` drain-credit logic both assume one consumer per table's queue, draining in step with one producer. A shared pool that lets *any* worker drain *any* table would break the per-stream backpressure accounting (the deadlock fix ADR-0099 §2) — the very wedge ADR-0099 spent a unit pin removing.
- **It needs no new knob and no new scheduler.** W = K; the consumer count equals the producer count, so the per-stream sub-budget is exactly right (one consumer drains the one table its paired producer is filling).
- **It reuses the existing per-table copy helper verbatim.** Each group goroutine calls `copyTableColdStartIdempotentMaybeParallel` — the same function the serial loop calls — so D fan-out, redaction, shard-stamping, the Bug-68 loud gate, and the no-PK idempotent guard all compose for free.

The write pool's only theoretical edge — rebalancing when one group's tables finish early — is marginal because ADR-0099's partition is already size-balanced (greedy LPT when estimates exist) and the dominant cost on a 302 GB source is per-table wire throughput, not scheduling slack. **Rejected** in favor of the simpler, invariant-reusing pipeline shape.

### 2. Composition with D (ADR-0097) — multiplicative, unchanged per-table helper

Total write concurrency = **W tables concurrent × D writers per table**. Each group goroutine calls `copyTableColdStartIdempotentMaybeParallel(…, fanoutDegree)` — the existing entry that routes to the D-way `WriteRowsIdempotentParallel` when eligible (PK present, writer implements `ParallelIdempotentCopyWriter`, D > 1), else the serial idempotent write. Nothing about the fan-out changes: it just runs inside W concurrent callers instead of one. The MySQL `RowWriter` holds a `*sql.DB` **pool**, so W × D concurrent `conn.ExecContext` calls each pin their own pooled connection — concurrent calls on the one shared writer are safe (its only shared mutable is `warnedClamp`, a `sync.Map`, and `copyDurableProgress`, which stays unwired on this path — see §6). One writer, W × D connections.

### 3. Connection budget — W × D bounded, honest WARN (no false clamp)

During the concurrent copy the target connection count is **W × D** (W group goroutines, each driving a D-way fan-out on its active table), plus K source-side gRPC streams (one HTTP/2 conn, K logical streams — ADR-0099 §6), plus the one CDC connection opened later.

Consistent with [ADR-0099] §3's honesty fix, **W is NOT folded into the connection-budget preflight in v1.** That preflight (`coldStartOpenTargetWriters` → `resolveTargetCopyParallelism`) resolves only D (the write fan-out degree); W = K is a *source*-DSN knob (`vstream_copy_table_parallelism`) parsed entirely inside the mysql engine, invisible to the pipeline-layer preflight. ADR-0099 already emits a WARN naming K and stating the `K × D ≤ --max-target-connections` operator contract; that warning now covers the write-concurrency demand too (W = K, so `W × D = K × D` — the same product). **No additional clamp is claimed that isn't implemented** (the loud-failure / honesty tenet). The existing D-axis preflight still fires its loud refusal / `--max-target-connections` cap exactly as before. A future enhancement folds W = K into the preflight for a true product-clamp; v1 documents the manual contract.

### 4. Position / CDC handoff — the correctness crux (no advance until ALL pipelines complete)

This is the silent-loss surface. The global CDC position **must advance only after every table across all W pipelines is fully and durably written** ([ADR-0007]) — a pipeline still writing when the position commits is silent loss.

This is guaranteed by composing two existing barriers, unchanged:

1. **The read/stitch barrier (ADR-0099, engine side):** the engine's `copyPumpAutoShardConcurrent` records the stitched CDC position (the per-shard set-min across the union of all K producers' per-table snapshots) **only after `wg.Wait()` joins all K producer goroutines** — and a producer's goroutine only returns after its *last* table reaches `COPY_COMPLETED`. The `stream.Position` is the zero Position until then.
2. **The write barrier (this ADR, pipeline side):** `runBulkCopyWithOpts` returns only after the W consumer goroutines all join (`errgroup.Wait`). The streamer then reads `stream.Position` and persists the CDC anchor (`coldStartBeginCDC`) **strictly after** `coldStartRunCopy` returns nil.

The happens-before chain proving gaplessness:

- A consumer pipeline finishing table `t` ⟹ it drained `rowBuffer[t]` to empty AND `tableCopyComplete[t]` was set (the `ReadRows` close condition) ⟹ the producer reached `t`'s `COPY_COMPLETED` and recorded `t`'s snapshot `P_t` into the shared `perTableSnapshots`. So **every table's `P_t` is captured before its consumer returns.**
- All W consumers returning ⟹ every table fully written to the target AND every `P_t` captured.
- The errgroup join (write barrier) precedes the streamer's `stream.Position` read; the producer join (read barrier) precedes the stitch that *populates* `stream.Position`. The CDC position read therefore happens-after both joins ⟹ after every table is durably written and every `P_t` is stitched.
- The stitched `P_start = ⋂ over every table P_t ⊆ every P_t`, so CDC replays `(P_start, P_t]` for every table regardless of which pipeline wrote it — no committed change after any table's snapshot is skipped (gapless), and the overlap re-delivery is absorbed idempotently ([ADR-0010], `CopyNeedsIdempotentWriter() == true`). The stitch is ADR-0099's verbatim; this ADR does not touch it.

**No position advance mid-copy:** there is no per-pipeline position write. The position is read once, after all W pipelines and all K producers have joined.

### 5. Exactly-once — each table written by exactly one pipeline

The partition is disjoint ([ADR-0099] §1, unit-pinned coverage + disjointness), so each table is in exactly one group ⟹ written by exactly one consumer pipeline. No table is written by zero pipelines (would be silently un-copied — the worst silent-loss class) or by two (would double-write into one queue's drain — broken by construction since each queue has one consumer). The idempotent upsert ([ADR-0010] / Bug-125) absorbs the per-table snapshot→CDC overlap; snapshot rows within a table have no intra-table write-order requirement (the writer upserts), so concurrent *tables* never interact. The W-goroutine consumer inherits ADR-0099's three guards (coverage, disjointness, stable partition) directly because it consumes the **same** groups the engine surfaces.

### 6. Mid-COPY durable checkpoint — stays DISABLED on the concurrent path (verified in code, not comment)

The ADR-0072 Phase B mid-COPY durable-write checkpoint MUST stay disabled under concurrent-table writes (the [ADR-0097] §3 lesson: concurrent writers' flushed-row frontiers are not order-equivalent to the reader's enqueue order, so a mid-COPY breadcrumb could checkpoint past an unwritten early row → silent-loss-on-resume).

It is disabled by **two independent code facts**, both verifiable:

1. **The engine pump records no mid-COPY breadcrumb on the concurrent path.** ADR-0099's `copyStream.pumpTable` (the concurrent pump) deliberately omits the `maybeCheckpoint` call the sequential `pumpOneTableCopy` runs — so no per-table cursor is ever persisted during a concurrent copy. (Confirmed by code-read: there is no checkpoint call in `cdc_vstream_copy_concurrency_pump.go`.)
2. **The pipeline does not wire the durable-progress reporter when the concurrent consumer is engaged.** `runBulkCopyWithOpts` wires `SetCopyDurableProgress` only on the serial path; the concurrent branch (this ADR) skips the wiring, and even if a writer's reporter were set, the per-table D fan-out passes `reportDurable=false` ([ADR-0097]). So no watermark advances mid-copy.

The sole durability guarantee for the concurrent path is the whole-copy join + the post-join stitched position (§4). On resume, each pipeline re-copies its group from the per-table cursor / from-beginning (idempotent upsert absorbs the overlap — §7), so no resume path consumes a mid-COPY breadcrumb anyway. A unit pin asserts no checkpoint sink is invoked during a concurrent copy.

### 7. Resume — composes with ADR-0098, per pipeline, over the stable partition

On resume the engine re-derives the **same** partition ([ADR-0099] §5: `partitionTablesForStreams` is a deterministic pure function of `(sorted tables, K)`, shuffle-invariant) and seeds the persisted cursor's in-progress table into whichever group contains it ([ADR-0099] `copyPumpAutoShardConcurrent` already places the seed). The pipeline consumer reads the **same** surfaced groups (the engine stores the partition once at open; cold-start and resume produce identical groups), so the consumer side composes automatically: the group whose producer is seeded from the cursor has its tables re-read/re-written (the in-progress table resumes past its PK, the tables before it in that group re-copy idempotently, the tables after it copy fresh — [ADR-0098] per group), the other W-1 groups re-copy their whole groups idempotently. Every table contributes a captured `P_t`, so the stitch's correctness on resume is identical to a fresh cold-start. The resume's K must match the cold-start's K (the operator resumes with the same DSN) — the same operator contract [ADR-0099] §5 already documents; a changed-K resume re-derives a different partition and is "restart the cold-start", not silently honored. The concurrent-write resume is pinned (a resume with W > 1 → exactly-once).

### 8. Zero-value-safe — K = 1 (or no groups surfaced) is byte-identical to today

W = K is tied to the existing `vstream_copy_table_parallelism` knob ([ADR-0099] §3, `resolveCopyTableParallelism`): the Go zero value / absent param / a one-table scope all resolve to K = 1, and at K = 1 the engine surfaces **no concurrent groups** (`ConcurrentCopyGroups()` returns nil — ADR-0099 only populates the groups when K > 1). With nil/≤1 groups the pipeline takes the **serial table loop, byte-identically** — no new goroutine, no errgroup, the exact pre-ADR-0100 path. There is no new knob to mis-default (the v0.99.51 trap is avoided by tying to the existing zero-value-safe one). Confirmed by unit pin: a reader that doesn't implement `ConcurrentCopyPartitioner`, and one that returns nil/1 group, both take the serial loop.

## Alternatives considered

- **K producers → shared buffers → a W-worker write pool.** Needs its own table→worker assignment + scheduler, new coverage/disjointness invariants, and breaks ADR-0099's per-stream byte sub-budget (which assumes one consumer per table's queue — the deadlock fix). **Rejected** in favor of the K-pipeline shape that reuses the disjoint partition verbatim (§1).
- **A new `--copy-table-write-parallelism` (W) knob distinct from K.** Lets W ≠ K. But W < K starves K producers behind fewer consumers (the per-stream sub-budget assumes one consumer per producer — a producer with no consumer wedges on its sub-cap); W > K spins idle consumers (only K queues exist). W = K is the only ratio the per-stream backpressure is correct for. **Rejected** — W = K, no new knob (also avoids a new zero-value-default trap).
- **Lift ADR-0079's shareable-snapshot gate to route VStream through the fast cross-table pool.** The fast pool's parallel readers `SET TRANSACTION SNAPSHOT`, which VStream cannot offer (no shareable snapshot; the rows come from one buffered stream). Routing VStream through it would mint independent readers that re-open the COPY from scratch per table with no shared stitch — losing the gapless set-min handoff. **Rejected**; the gate stays as ADR-0079 wrote it.
- **Open W independent target RowWriters (one per pipeline).** Needs a writer factory threaded into the streamer's cold-start. Unnecessary: the one `*sql.DB`-pooled writer is concurrency-safe for W × D pinned-connection callers, and sharing it keeps the streamer's open/close lifecycle unchanged. **Rejected** in favor of the shared writer.
- **Fold W = K into the connection-budget preflight for a real product-clamp.** A genuine cross-layer improvement, but it requires threading the source-DSN-parsed K up through the orchestrator (the same plumbing ADR-0099 §3 deferred). **Deferred** as a clean follow-on; v1 ships the honest WARN (no false clamp).

## Consequences

- **PS/Vitess cold-copy throughput rises with W = K on the write-bound path**, removing the serial-consumer ceiling that pinned ADR-0099 + ADR-0097 at ~1.4×. The PROCESSLIST should now show up to W tables receiving rows concurrently. Bounded by source/target/network capacity and the `W × D` connection budget, not by sluice's consumer.
- **Composes multiplicatively with the ADR-0097 D fan-out** (W tables × D writers each) and reuses ADR-0099's K read streams (W = K). The three levers (K reads, W = K concurrent table-writes, D per-table writers) now all engage on one cold-copy.
- **The snapshot→CDC seam is at-least-once across W pipelines** (per-table overlaps re-applied idempotently). Identical correctness to ADR-0099's single-consumer seam — the stitch is unchanged, just drained by W consumers instead of one.
- **One new silent-loss-class surface, four guards.** (a) Every table written by exactly one pipeline — inherited from ADR-0099's coverage/disjointness unit pin (the consumer reads the *same* groups). (b) The global position advances only after all W pipelines AND all K producers join — structural (the errgroup join + the producer join both precede the position read). (c) Mid-COPY checkpoint disabled on the concurrent path — two independent code facts (no engine breadcrumb + no pipeline wiring), unit-pinned. (d) The partition is stable cold-start vs resume — inherited from ADR-0099's pure-function determinism. All four are the review-critical invariants.
- **Concurrency chunk → `-race`-before-tag.** This adds W concurrent consumer goroutines draining the shared `rowBuffer`/mutex/cond, plus the W-way errgroup join and cancel, on top of ADR-0099's K producers. Per the project rule, the integration **`-race`** gate MUST pass **before** any tag is cut (push-first, tag-after, or `scripts/race-integration.ps1`). CGO is off on the dev box, so `-race` is CI-only here.
- **K = 1 (no surfaced groups) is byte-identical to today** — the serial table loop, no errgroup. The zero value and the absent DSN param both resolve to K = 1, so every non-opt-in caller is unchanged.

## Testing

- **Concurrent consumer dispatch / zero-value-safe (unit):** a fake idempotent reader that implements `ConcurrentCopyPartitioner` returning ≥2 groups drives the W-goroutine path; one returning nil / 1 group, and one not implementing the surface at all, drive the serial loop (asserted via a dispatch observer, not timing). Pins that the zero value / absent surface is the serial path byte-identically.
- **Multiple tables written concurrently — the missing proof (unit):** a recording idempotent writer timestamps each table's write-window open/close; with W = 2 over 4 tables and a reader that releases rows for two groups simultaneously, assert ≥2 tables' write-windows OVERLAP (the exact thing the PROCESSLIST showed missing — one-at-a-time). This is the load-bearing write-concurrency pin; it FAILS on the serial loop (no overlap) and PASSES on the W-pipeline path.
- **Global position only after ALL pipelines complete (unit):** a fake where one pipeline's table lags; assert the streamer reads `stream.Position` only after the lagging pipeline's last table is written (the write barrier) and the engine stitch only fires after all K producers join (the read barrier) — the position is the zero value until both.
- **Exactly-once partition / coverage (unit):** inherited from ADR-0099's `partitionTablesForStreams` coverage+disjointness pin; an additional pin asserts the consumer writes each table exactly once (a recording writer's per-table call count == 1 across W goroutines, no table missing).
- **Any pipeline error aborts loudly, no position advance (unit):** a forced write error in one of W consumers fails the whole copy (`errgroup.Wait` returns it), cancels the others, and the streamer advances no position.
- **ctx-cancel leaks nothing (unit, `-race`):** cancel mid-copy with W consumers running — goroutine-count delta == 0 after, no leaked connections, copy reports the cancel (not success).
- **Mid-COPY checkpoint disabled (unit):** assert no `CopyDurableProgress` / checkpoint sink is invoked during a concurrent copy (the §6 silent-loss guard).
- **Integration (`vstream` / `vitesscluster` tag):** cold-copy a multi-table keyspace with W = K > 1 through the FULL pipeline (real target) → (a) assert MULTIPLE tables receive rows in an overlapping window (poll the target's per-table row counts and assert >1 table advancing concurrently, the PROCESSLIST analogue) OR wall-clock << sum-of-per-table-serial; (b) target `COUNT(*)` + content checksum == source per table (no gap/dup); (c) clean CDC handoff from the stitched position. Plus an interrupt+resume with W > 1 → still complete + exactly-once (composes with ADR-0098).

## Silent-loss surfaces for value-fidelity review

Four invariants, each a silent-loss class if broken, called out explicitly:

1. **Every in-scope table is written by exactly one pipeline** (coverage + disjointness of the partition — the consumer reads the SAME groups the producers used). A table in zero groups is silently never written; a table in two groups would corrupt one queue's drain. Guard: ADR-0099's `partitionTablesForStreams` coverage/disjointness unit pin + the per-table write-count == 1 pin.
2. **The global CDC position advances only after ALL W pipelines AND all K producers complete.** A position recorded after a subset would let CDC start past an un-written table → silent loss. Guard: the W-way errgroup join (write barrier) + ADR-0099's K-way producer join (read barrier) both structurally precede the streamer's `stream.Position` read.
3. **The mid-COPY durable checkpoint is disabled on the concurrent path.** A breadcrumb checkpointed past an un-written early row under concurrent writers (ADR-0097 §3) resumes past it → silent loss. Guard: two independent code facts (no engine breadcrumb in the concurrent pump; no pipeline durable-progress wiring on the concurrent branch), unit-pinned.
4. **The partition is stable (identical) across cold-start and resume.** A drifting partition could re-assign a table to a different pipeline than the one whose cursor names it → missed or double-written. Guard: ADR-0099's pure-function determinism (sort-before-assign, shuffle-invariant) — the consumer inherits it by reading the engine's surfaced groups.
