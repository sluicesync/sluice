# ADR-0097: Parallel writer fan-out on the VStream/CDC snapshot bulk-copy (cold-start) path

## Status

Accepted. Builds on [ADR-0010](adr-0010-idempotent-applier.md) (idempotent apply — the upsert that makes the per-worker writes order-independent), [ADR-0007](adr-0007-position-persistence.md) (position-then-data durability ordering — the handoff invariant the fan-out must not break), [ADR-0072](adr-0072-resumable-coldstart-copy.md) (resumable COPY cursor + the v0.99.9 durable-write watermark — which this ADR DISABLES on the fan-out path; §3/§6), [ADR-0028](adr-0028-memory-bounded-streaming.md) (the ~1 MB / `--max-buffer-bytes` per-batch sizing each worker mirrors), and [ADR-0095](adr-0095-vstream-auto-shard-by-table-copy.md) (per-table sequential COPY — so this fan-out applies to the *one active table* at a time). It is the WRITE-side analogue of [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md)'s read-side PK-range chunking, for the path where read-side chunking is impossible. It does **not** touch the `sluice migrate` cross-table pool ([ADR-0076](adr-0076-cross-table-copy-worker-pool.md)), the LOAD DATA fast path, the PG raw-COPY path, or the [ADR-0092](adr-0092-pipelined-cdc-apply.md) CDC *tail* apply.

## Context

### The measured problem (validated; not re-litigated here)

A live experiment proved the root cause and the lever. A VStream/PlanetScale-source cold-copy of a single table to a **PlanetScale-MySQL target** runs at ~13 GB/h (~3.4 MB/s). PlanetScale blocks `LOAD DATA LOCAL INFILE`, so the MySQL `RowWriter` falls back to the single-connection multi-row batched-`INSERT` path (`writeBatched` / `writeBatchedIdempotent`), and that one pinned connection is **cross-region-RTT-bound** — exactly the wire-not-the-DB ceiling [ADR-0092] characterized for the CDC tail, here on the snapshot bulk copy.

Running **N independent single-table `sync` processes** concurrently scaled near-linearly: N=3 → ~128 GB/h, N=6 → ~151 GB/h, no per-stream degradation and no target ingest ceiling. 3–4 parallel INSERT streams already beat PG's single-stream COPY (~43 GB/h). The fix is to internalize that parallelism into ONE process: fan the single incoming snapshot row-stream out to N concurrent batched-INSERT writer workers against the target.

### Why this is a different path from `sluice migrate`

`sluice migrate` (`runBulkCopyPhases` / `internal/pipeline/migrate_parallel.go`) already has intra-table PK-range parallel chunking ([ADR-0019]): that path can issue arbitrary PK-range `SELECT`s against the source, so it parallelizes the **read** side.

The **`sluice sync` / VStream cold-start** path is structurally different: it drains a single vtgate VStream snapshot per table (`copyTableColdStartIdempotent` in `migrate_copytable.go`, reached via `runBulkCopyWithOpts` on the serial cold-start branch — `streamer_coldstart.go::coldStartRunCopy`). You **cannot PK-range-chunk the READ side**: vtgate streams the snapshot down one logical channel; there is no arbitrary range-`SELECT` to split. The serial cold-start path is deliberately *not* wired into the [ADR-0076] cross-table pool (see the `migrate.go` comment at the `runBulkCopyWithOpts` dispatch: "Only `sluice migrate` … drives cross-table concurrency"). So the **only** lever on this path is **write-side fan-out**: one reader goroutine pulling the incoming row channel, dispatching rows to N writer workers, each owning its own target connection.

### Why ADR-0092 does not already provide the machinery

[ADR-0092] pipelined the CDC **tail** apply (the `pgx.Batch` flush of a committed change batch), is **Postgres-only**, and operates on the apply hot path — not the snapshot bulk copy, and not the MySQL batched-INSERT writer this gap lives on. There is no reusable writer-pipelining seam to build on; this is a new, engine-neutral pipeline-level fan-out. (Confirmed by reading ADR-0092 and `internal/engines/mysql/row_writer_batch.go`.)

## Decision

Add a **write-side fan-out** to the idempotent cold-start copy path. A single reader goroutine consumes the (already redacted + shard-stamped) snapshot row channel and **PK-hash-partitions** each row to one of N per-worker channels; N writer workers each run the engine's existing idempotent batched-INSERT path against the target, each pinning its **own** connection from the writer's pool with its own ~1 MB batch buffer. The table is marked complete (and its position recorded) only **after every worker has fully drained, flushed, and committed its batches**.

The seven design questions, resolved:

### 1. Where the fan-out lives — engine-neutral pipeline level, gated by an opt-in writer capability

The fan-out lives in the **pipeline** (`internal/pipeline`), not in the MySQL engine, so any connection-bound idempotent `RowWriter` benefits and the orchestrator stays engine-neutral (IR-first tenet). But the *fan-out work itself* — running N concurrent idempotent batched writes, each on its own pinned connection — is engine-specific (the MySQL writer's `writeBatchedIdempotent` pins one `*sql.Conn` from the shared `*sql.DB` pool; running N of them concurrently is the whole point). We reconcile these with a new **optional writer capability**:

```go
// ir
type ParallelIdempotentCopyWriter interface {
	IdempotentRowWriter
	// WriteRowsIdempotentParallel runs the idempotent batched-copy
	// over len(workers) concurrent workers, each consuming one of the
	// supplied per-worker channels and pinning its own target
	// connection. Returns only after EVERY worker has drained and
	// durably committed (or the first worker error, loudly).
	WriteRowsIdempotentParallel(ctx context.Context, table *Table, workers []<-chan Row) error
}
```

- The **MySQL** target implements it: it spawns one goroutine per supplied channel, each calling the existing `writeBatchedIdempotent` core (one pinned `*sql.Conn`, ~1 MB batch buffer, the Vector-B warning probe) on that channel. Every worker runs with the mid-COPY durable-progress reporter **suppressed** (`reportDurable=false`) — the flat per-flush durable count is not order-equivalent to the reader's enqueue-order frontier under fan-out, so advancing the watermark mid-COPY could checkpoint past an un-flushed early row (§3). The whole-table join is the sole durability guarantee for a fanned-out table.
- The **pipeline** owns the reader goroutine + the PK-hash partition + the per-worker channel lifecycle + the exactly-once routing guarantee. It calls `WriteRowsIdempotentParallel` when degree > 1 **and** the writer implements the capability **and** the table has a usable partition key; otherwise it falls through to today's serial `WriteRowsIdempotent` — never silently doing nothing.
- **PG targets are unaffected by default.** The eligible cold-start copy onto PG uses the fast raw-COPY / parallel snapshot path (ADR-0079) when the source exports a shareable snapshot; the VStream-source serial idempotent path that reaches this fan-out is the PS-MySQL gap. PG's `WriteRowsIdempotent` does not implement the parallel capability, so it stays serial — the default differs by writer capability, exactly as the design requires.

**Rejected:** putting the fan-out inside the MySQL engine (a single `WriteRowsIdempotent` that internally fans out). It would bury the exactly-once routing + position-handoff ordering inside one engine, duplicate it for the next engine, and hide the correctness-critical partition from the orchestrator that owns the position write. Keeping the *routing* in the pipeline and only the *N-worker execution* in the engine is the clean split.

### 2. Row→worker routing — PK-hash partition (recommended), not round-robin

Each row is routed to **exactly one** worker by `worker = fnv1a(pk_bytes) % degree`, where `pk_bytes` is the stable encoding of the row's primary-key column values (the same PK columns `tablePKColumns` already extracts for redaction seeding).

Why PK-hash over round-robin:

- **No same-PK row can be in two workers' in-flight batches simultaneously.** During a VStream COPY the same PK can be **re-emitted** (Bug 125 — vtgate re-streams rows already past the scan during binlog catchup; this is *exactly why* the path is the idempotent one). With round-robin, two re-emissions of the same PK could land in two different workers' concurrent batches and race two `ON DUPLICATE KEY UPDATE`s on the same row — a deadlock/lock-contention hazard and an ordering ambiguity. PK-hash pins every emission of a given PK to the **same** worker, so re-emissions serialize naturally within that one worker's batch stream. Robust even if the stream ever carries an update for a PK.
- **Order-independence across workers is already guaranteed** by [ADR-0010]: each write is an upsert (`ON DUPLICATE KEY UPDATE` / `ON CONFLICT DO UPDATE`), so the *interleaving across* workers is irrelevant to the final state. PK-hash adds the *intra-PK* serialization that makes even the re-emission case clean.

**Exactly-once is the load-bearing invariant:** the reader sends each row to exactly one channel (no drop, no dup, no fan-to-many). It is pinned by a unit test that feeds a known multiset of PKs through the partitioner and asserts the union of all worker channels equals the input exactly (count + per-PK membership), with no PK appearing on two channels.

**No-usable-PK tables fall back to serial.** PK-hash needs a partition key. A table with no PRIMARY KEY (the Bug-125 no-PK case, which the MySQL idempotent writer keys on a UNIQUE index) routes through the existing serial `copyTableColdStartIdempotent` path unchanged — the fan-out is a pure additive optimization for PK'd tables and never weakens the no-PK loud-refusal/unique-key contract.

### 3. Snapshot→position handoff — no position advances until every worker is durably committed

This is the silent-loss-on-resume guard. The VStream snapshot Position is finalized asynchronously at `COPY_COMPLETED` and the cold-start anchor is persisted by `coldStartBeginCDC` **after** `coldStartRunCopy` returns. The fan-out must preserve that ordering exactly: `WriteRowsIdempotentParallel` returns success **only after every worker goroutine has**:

1. drained its channel to close,
2. flushed its final batch, and
3. had that batch's `ExecContext` (the implicit-commit single-statement upsert) return success.

The reader goroutine closes all N worker channels only after the source stream is fully drained (or the context is cancelled / an error fires). `WriteRowsIdempotentParallel` joins all N workers and returns the first non-nil worker error (or nil). The orchestrator advances the position (the `coldStartBeginCDC` anchor write) only on a nil return — unchanged from today, because the call sits in the same place `WriteRowsIdempotent` did. A worker still in-flight when the position commits would be a silent-loss-on-resume gap; the join-before-return makes that structurally impossible.

**The mid-COPY durable-write watermark (ADR-0072 Phase B / v0.99.9) is DISABLED on the fan-out path** — and this is load-bearing, not an optimization skipped. The watermark works by the snapshot reader recording enqueue-order breadcrumbs (`rowsCovered = enqueuedRows`) and persisting, mid-COPY, the highest breadcrumb whose `rowsCovered <= durableRows`, where `durableRows` is the FLAT global count of flushed rows summed across writers. Under a single serial FIFO stream that count is order-equivalent to the enqueue frontier: durable-count K ⟹ the first K *enqueued* rows are durable, so the breadcrumb at `rowsCovered=K` is genuinely safe. **Under fan-out this equivalence breaks.** Rows flush in per-worker order with independent batch buffers, so a fast worker can drive `durableRows` to K while a lagging worker still holds an EARLY-enqueued row (routed to it by PK-hash) un-flushed. The breadcrumb at `rowsCovered=K` would then be persisted as durable mid-COPY while that early row is *not yet on the target*; a hard crash after the checkpoint write but before the lagging worker flushes resumes (`OpenSnapshotStreamFromPosition`) from the persisted VGTID — PAST the un-flushed early row — which is never re-copied (silent loss, exit 0). ADR-0095's "resume runs serial" does not save this: the corrupt breadcrumb is written *during* the fan-out copy and is exactly what the serial resume consumes. We therefore make the mid-COPY checkpoint **provably inert on fan-out tables**: every fan-out worker runs the idempotent core with `reportDurable=false`, so `copyDurableProgress` is never called and no mid-COPY breadcrumb is ever persisted for a fanned-out table. The **whole-table join is the SOLE durability guarantee** for such a table — `WriteRowsIdempotentParallel` returns only after every worker has durably committed, and the orchestrator persists the fully-durable COPY_COMPLETED position only after that return. Serial (non-fan-out) tables in the same run keep the ADR-0072 mid-COPY checkpoint unchanged.

### 4. Connection budget + degree — conservative default, bounded by the active table

With [ADR-0095] the VStream snapshot copies **one table at a time**, so the fan-out's N writer connections apply to the single active table — total target connections during cold-start copy = N (+ the source-side stream + the CDC connection opened later). The default degree is **conservative**: the experiment showed 3–4 already wins, so the default is **4**. The connection-budget preflight (`resolveTargetCopyParallelism`, already run in `coldStartOpenTargetWriters`) accounts for the fan-out degree so the loud refusal still fires if the target has no slots and `--max-target-connections` is honored. We do **not** default aggressive; operators raise it explicitly via the flag for a known-large cross-region copy.

### 5. The zero-value-safe default trap (the v0.99.51 lesson)

The degree field must be safe at the Go **zero value** for *every* constructor (CLI, broker/chain paths, all tests, future callers), not just the CLI. The field is named for the *common/safe behavior*, and `0` maps to the safe default — **never** to "0 workers = copies nothing / hang":

- The `Streamer` field is `CopyFanoutDegree int`. The resolution helper `resolveCopyFanoutDegree(n int) int` maps `n <= 0` → the default (4), `n == 1` → serial (the no-fan-out path), `n > 1` → `n` capped at a sane ceiling. So a zero-value-constructed `Streamer` (every test, every non-CLI caller) gets the default degree, and the *path* it takes for a degree of 1 is the existing serial path. There is no value of the field that produces "no workers."
- Pinned by a unit test: a zero-value `Streamer{}` resolves to a usable degree and a degree-1 resolves to the serial path; neither hangs nor copies nothing.

The reason this is named for the safe default rather than `EnableFanout`-defaulting-true: an `Enable…` bool defaulting true *by intent* silently inverts to false for every non-CLI constructor (the exact v0.99.51 `AutoResnapshotOnInvalidPosition` trap). An `int` whose zero value resolves to the chosen default has no such inversion.

### 6. Resume (ADR-0072) — mid-COPY checkpoint DISABLED on fan-out; whole-table join is the durability guarantee

The ADR-0072 mid-COPY durable-write watermark does **not** compose with fan-out — it is **disabled** on the fan-out path (§3): the flat flushed-row count is not order-equivalent to the reader's enqueue-order frontier across N workers, so a mid-COPY breadcrumb could checkpoint past an un-flushed early row (silent-loss-on-resume). On a fanned-out table the only durability guarantee is the whole-table join at COPY_COMPLETED (no position advances until every worker is durably committed). The interrupted-cold-start *process-restart* resume (`OpenSnapshotStreamFromPosition`, a position carrying a `TablePKs` cursor) stays on the single-stream serial resume path per [ADR-0095]'s documented v1 limitation — and because the fan-out copy never persisted a mid-COPY cursor, a resume either restarts the whole table's COPY from the last fully-durable position (the idempotent upsert absorbs the re-copied overlap) or, if the table completed, skips it. This is a consistent, explicit v1 boundary, not a correctness gap.

### 7. Errors + lifecycle — loud abort, deterministic shutdown, no leaks

- **Any worker error aborts the whole table copy LOUDLY.** The first worker to error cancels a shared child context; the reader stops dispatching, the other workers observe the cancel and unwind, and `WriteRowsIdempotentParallel` returns that error wrapped. The orchestrator propagates it as a hard `PhaseBulkCopy` failure (exit non-zero, **no position advance**). No partial silent success.
- **Clean shutdown on ctx-cancel.** The reader and all workers select on `ctx.Done()`; on cancel they drop pending sends/receives and exit. The reader closes the worker channels in a `defer`, so the workers always see a closed channel even on the error/cancel path — no leaked goroutines, no leaked pinned connections (each worker's `*sql.Conn` is `Close()`d in its own `defer`).
- **Reader stream error still surfaces (Bug 68).** After the workers join, the path still calls `readerStreamErr(rr, table)` so a mid-stream scan/decode abort fails loudly rather than reporting a silently-truncated table — identical to the serial path's loud-failure gate.

## Consequences

- **PS-MySQL VStream cold-copy throughput rises ~degree-fold** on the cross-region batched-INSERT path (the experiment's near-linear scaling to 3–4×), with no source-side change and no read-side chunking — closing the gap vs PG's single-stream COPY. Bounded by target ingest / connection budget, not by sluice.
- **A second concurrent path through the cold-start copy hot path** (N workers vs one). Mitigated by reusing the *identical* `writeBatchedIdempotent` core per worker (value fidelity and the Vector-B warning probe unchanged — fan-out changes *how many* connections, never *how a value is encoded*), keeping the serial fall-back, and pinning the routing/lifecycle with `-race`.
- **Durability and the snapshot→CDC seam**: no position advances until every worker is durably committed (§3) — the whole-table join is the SOLE durability guarantee for a fanned-out table. The ADR-0072 mid-COPY watermark is **disabled** on fan-out tables (a flat flushed-row count is not order-equivalent to the enqueue frontier across workers, so a mid-COPY breadcrumb could checkpoint past an un-flushed early row — silent-loss-on-resume; §3). Serial tables keep their mid-COPY checkpoint. The idempotent upsert makes cross-worker interleaving irrelevant to final state ([ADR-0010]).
- **Concurrency chunk → `-race`-before-tag.** This adds goroutines, per-worker channels, shared batch state, and the reader→workers handoff. Per the project rule, the integration **`-race`** gate MUST pass **before** any tag is cut (push-first, tag-after, or `scripts/race-integration.ps1`). CGO is off on the dev box, so `-race` is CI-only here.

## Testing

- **Exactly-once routing (silent-loss class, load-bearing):** a unit test feeds a known multiset of PKs (incl. duplicate/re-emitted PKs, the Bug-125 shape) through the partitioner and asserts the union of all worker channels equals the input exactly — no drop, no dup, every PK on exactly one worker, and every re-emission of a given PK on the *same* worker.
- **Flush-before-position:** a fake parallel writer records that the call returns only after every worker's final flush; the orchestrator asserts no position advance precedes the return.
- **Mid-COPY checkpoint inert under a lagging worker (silent-loss class, load-bearing):** drive the fan-out copy with one worker forced to hold its batch un-flushed (a slow/large batch) while a fast worker flushes later-enqueued rows; trigger the mid-COPY checkpoint path and assert it never persists a breadcrumb past the un-flushed early-enqueued row — concretely, that NO mid-COPY durable-progress is reported on the fan-out path at all (the writer runs every worker with `reportDurable=false`), so the persisted position cannot advance ahead of the slowest worker's frontier. Pinned at the writer level (the `copyDurableProgress` callback is provably never invoked under `WriteRowsIdempotentParallel`).
- **Loud abort:** a forced worker error makes `WriteRowsIdempotentParallel` return non-nil, the table copy fails (`PhaseBulkCopy`), and no position is advanced.
- **ctx-cancel mid-copy:** cancelling mid-stream leaks no goroutines (goroutine-count delta == 0 after) and no pinned connections, and does not report success.
- **Zero-value-safe degree:** a zero-value `Streamer{}` resolves to the default degree and takes the fan-out path; degree 1 takes the serial path; neither hangs nor copies zero rows.
- **Integration (`integration` tag, real MySQL container):** a sizable table cold-copied via the snapshot idempotent path with fan-out degree > 1 → `target COUNT(*)` and a content checksum match the source EXACTLY (no missing/dup rows), and a forced worker error fails the copy loudly. A true Vitess/VStream container is impractical in the default integration shard, so the fan-out is exercised on the equivalent `copyTableColdStartIdempotent` snapshot-drain code path with a MySQL source that declares `IdempotentCopyReader` — the same machinery the VStream reader drives. The coverage boundary (real vtgate re-emission timing) is noted and is covered by the `vstream`/`vitesscluster` suites at the engine level.

## Alternatives considered

- **Round-robin row→worker routing.** Simpler, but lets two re-emissions of the same PK (Bug 125) land in two concurrent workers' batches → same-row concurrent upsert / deadlock hazard and ordering ambiguity. PK-hash pins each PK to one worker and eliminates the class. **Rejected.**
- **Fan-out inside the MySQL engine's `WriteRowsIdempotent`.** Buries the exactly-once routing and the position-handoff ordering inside one engine, hides the correctness-critical partition from the orchestrator that owns the position write, and must be re-done per engine. **Rejected** in favour of pipeline-owned routing + an engine-implemented N-worker capability.
- **Parallelize the READ side (PK-range chunk the VStream snapshot).** Impossible: vtgate streams the snapshot; there is no arbitrary range-`SELECT` to split (this is the very reason the path differs from `sluice migrate`). **Rejected** (not available).
- **N independent `sync` processes (the experiment's manual workaround).** Works, but pushes per-table sharding, N control-table rows, and N snapshot→CDC seams onto the operator. Internalizing the parallelism into one process is the whole point of the chunk. **Rejected** as the supported path.
- **An `EnableCopyFanout bool` defaulting true.** The v0.99.51 zero-value trap: false for every non-CLI constructor. An `int` whose zero value resolves to the default has no inversion. **Rejected** in favour of the degree int.
- **Aggressive default degree (e.g. 8–16).** The experiment shows 3–4 already beats PG single-stream COPY; an aggressive default risks blowing PS connection caps. **Rejected** in favour of a conservative default of 4, operator-raisable.
