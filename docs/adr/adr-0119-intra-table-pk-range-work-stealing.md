# ADR-0119: Intra-table PK-range work-stealing for the native-MySQL concurrent cold-copy

## Status

**Proposed (2026-06-25).** Roadmap item 21, **tier (b)**. Builds on tier (a) —
table-level work-stealing, [ADR-0111]-era `runWorkStealingTableCopy` /
`ir.WorkStealingCopyReader`, shipped v0.99.74 — by letting an idle copy pipeline
steal a **PK-range chunk** of an in-progress LARGE table, so the copy stays
N-wide down to a *chunk*, not down to the last whole table. Scoped to the
**native-MySQL** consistent-N-snapshot path (the only reader that implements
`WorkStealingCopyReader`). VStream and PG cold-start are out of scope (see
Context).

**Throughput optimization, not a correctness gate** — the static partition + the
tier-(a) table queue are already correct and fast for the bulk of the copy; this
reclaims the tail and makes the width robust to byte/row-width skew (the static
balance is by row-count estimate, but copy time is bytes-driven). It is, however,
**exactly-once-CRITICAL in implementation**: a mis-tiled chunk range is a
silent-loss / silent-dup class defect, so the coverage invariants below are
mandatory and pinned across the full PK-family matrix.

`-race`-before-tag concurrency chunk (goroutines + shared claim cursor + the
per-work-item cursor map): the `-race` integration gate MUST pass before any tag.

## Context

The native-MySQL concurrent cold-copy ([ADR-0101]/[ADR-0102]) partitions the
in-scope tables into N disjoint groups once, opens N pinned CONSISTENT-SNAPSHOT
readers under one FTWRL (one binlog anchor P), and — since tier (a) — flattens
the partition into ONE work list that N pipelines drain by **atomically claiming
whole tables** off a shared cursor (`runWorkStealingTableCopy`,
`copy_concurrent_tables.go`). That closed most of the static-partition tail
taper, but the residual tail is still **one whole large table**: once fewer than
N tables remain, the pipelines that finished idle while one grinds the last big
(often blob/wide-row) table. On a skewed corpus (one huge table + many small),
the tail is bounded by that single table's copy time, not by total/N.

The `migrate` path already splits a large table into PK-range chunks (keyset
chunking, [ADR-0096], `chunk.go` `computeChunkBoundaries` /
`computeKeysetChunkBoundaries`) and copies them concurrently, reading each chunk
with the collation-correct upper-bound clip pushed into SQL
(`ReadRowsBatchBounded`, the Bug-74 contract). Tier (b) brings that splitting to
the work-stealing queue: the unit of stealable work becomes a **(table, chunk)**,
not a whole table.

**Why native-MySQL only.** The VStream COPY path scopes each of its K streams to
its group's tables at the *source* (one logical channel per table) — a stealing
consumer would have no rows for an out-of-group table, and a single channel is
not arbitrarily range-splittable. The native path is different: all N readers
share one FTWRL cut, so **any** connection can read **any** `(table, PK-range)`
of the snapshot — exactly what intra-table stealing needs. PG cold-start uses a
different orchestrator entirely and already streams with tight backpressure.

## Decision

### 1. The stealable unit becomes a work item, not a table name

`runWorkStealingTableCopy` builds a flat `[]copyWorkItem` instead of
`[]tableName`. A work item is `{table *ir.Table, chunkIndex int, lowerPK,
upperPK []any}`. For each table in the (flattened, deterministic) partition:

- **Chunk-eligible AND large** → emit M chunk items (chunkIndex 0..M-1) with the
  half-open `(lowerPK, upperPK]` bounds from the existing `chunk.go` boundary
  functions (nil end-caps: chunk 0 `lowerPK==nil`, last chunk `upperPK==nil`).
- **Otherwise** → emit one whole-table item (`chunkIndex == -1`, both bounds
  nil), byte-identical to today's behavior.

Eligibility reuses `canParallelChunkTable` (orderable single/composite PK; a
no-PK / non-orderable / sluice-injected-PK table is never chunked — so the
**keyless at-least-once contract is unchanged**, keyless tables stay whole). The
"large" gate reuses the within-table threshold `resolveBulkParallelMinRows`
against the reader's `CountRows` estimate (the same TABLE_ROWS estimate the
progress ticker uses). Chunk count M is bounded: `M = clamp(ceil(estRows /
threshold), 2, maxChunksPerTable)`, where the cap keeps per-chunk overhead in
check; M ≥ 2 only crosses into chunking past the threshold. A new opt-out flag
`--no-intra-table-stealing` (default off → stealing on) forces every table to a
single whole-table item for operators who want the tier-(a) behavior.

### 2. Boundaries are computed on the side metadata pool, not the snapshot

Boundary selection (`RangeBounds` for integer PK, `SampleKeysetBoundaries`
ROW_NUMBER() for keyset) runs on a FRESH connection from the reader's side
`metaDB` pool — the same collision-free pool `CountRows` already uses — never on
a pinned snapshot connection (which is actively streaming). **Boundaries are a
partition HINT, not a consistency-bearing read:** the chunk ranges tile
`(-inf, +inf]` regardless of how fresh the sampled split points are, so coverage
is complete and disjoint even if the live table differs from the frozen cut
(stale boundaries only make chunk sizes slightly uneven — a perf nuance, never a
miss/dup). The actual ROW reads happen on the snapshot connections within the
bounds.

### 3. A bounded range read on a pinned connection (the new engine surface)

New IR interface, embedding the tier-(a) one:

```go
type ChunkedWorkStealingCopyReader interface {
    WorkStealingCopyReader
    // ReadRowsRangeOn reads the half-open PK range (lowerPK, upperPK] of table
    // on the pinned connection `reader`, paging with the collation-correct SQL
    // upper-bound clip. chunkIndex disambiguates per-(table,chunk) resume state.
    ReadRowsRangeOn(ctx context.Context, table *ir.Table, lowerPK, upperPK []any, chunkIndex, reader int) (<-chan ir.Row, error)
    // RangeBoundsQuerier + KeysetSampler are surfaced so the pipeline can
    // compute boundaries without importing engine internals.
    ir.RangeBoundsQuerier
    ir.KeysetSampler
}
```

`concurrentBinlogRows` implements it. `RangeBounds`/`SampleKeysetBoundaries`
delegate to a `metaDB`-backed `RowReader` (`&RowReader{q: r.metaDB, schema:
r.dbName}`). `ReadRowsRangeOn` reuses the ADR-0111 resumable read path,
generalized to carry the range + a per-work-item cursor key (Decision 4).

### 4. Per-work-item resume keys make the ADR-0111 recovery compose

The ADR-0111 in-memory resume cursors (`r.cursors map[string]*tableCursor`) are
keyed by **work-item identity**, not table name: `table.Name` for a whole-table
item (`chunkIndex < 0`), `fmt.Sprintf("%s#%d", table.Name, chunkIndex)` for a
chunk. A table is read EITHER whole OR as chunks (never both), so the key spaces
never overlap, and **concurrent chunks of one table get distinct cursor entries**
— no collision on the shared map.

The resumable read (`streamResumable`/`readOnce`/`readKeyedPaged`) is
generalized to take `(lowerPK, upperPK, cursorKey)`:

- initial cursor `after` = `lowerPK` (was implicitly nil for whole-table);
- each page reads `ReadRowsBatchBounded(ctx, table, after, upperPK, limit)` (was
  `ReadRowsBatch` — the unbounded form is the `upperPK==nil` whole-table case);
- the cursor + `complete` marker are read/written under `cursorKey`.

The recovery is already **per-goroutine** (each pipeline, on a classified
source-read drop, coalesces the global re-snapshot via the generation counter and
then re-reads ITS OWN work item from ITS OWN cursor). With per-work-item keys and
per-item bounds threaded through, a chunk resumes exactly: re-read
`WHERE (pk) > lastPK AND (pk) <= upperPK` on the fresh snapshot. The CDC anchor
stays at the earliest P (`verifyCDCAnchorUnchanged`, unchanged) — chunking does
not touch the anchor.

### 5. Exactly-once + budget invariants (the silent-loss surface)

- **Claim:** each work item is handed to exactly one pipeline by the atomic
  fetch-add cursor (unchanged from tier (a)); total coverage = every item index
  claimed before any goroutine sees `claim >= len`.
- **Tiling:** chunk ranges are half-open `(lowerPK, upperPK]` with nil end-caps
  from the shared `chunk.go` boundary code, and the upper clip is pushed into SQL
  in the column's native collation (`ReadRowsBatchBounded`) — the Bug-74
  contract. So the M chunks of a table partition its rows with no gap and no
  overlap, by the same machinery `migrate` already pins.
- **One query per connection:** reader index i is owned by one pipeline copying
  one item at a time → at most one in-flight read per pinned connection (the
  `WorkStealingCopyReader` invariant), unchanged.
- **Budget:** only N pipelines are ever active, so source connections stay N and
  the ADR-0097 write fan-out stays N×D regardless of how many chunk items exist.
  The write fan-out (`copyTablePlainMaybeParallel`, PK-hash partition) runs
  per-item; disjoint chunks carry disjoint rows, so plain-INSERT per chunk stays
  exactly-once.
- **Seam:** the single FTWRL anchor P is independent of which pipeline/chunk read
  which range (ADR-0101 §6 / ADR-0007), unchanged.

### 6. Process-restart resume is unchanged (still re-runs the whole copy)

The concurrent copy path deliberately persists no mid-copy checkpoint (ADR-0111
§1 deferral). Chunking adds no new persistence; a process restart re-runs the
whole copy from scratch, exactly as today. Only the IN-PROCESS source-read-drop
recovery is in play, and Decision 4 makes it chunk-aware.

## Consequences

- **Win:** the cold-copy tail is bounded by a chunk, not a whole table — N-wide
  read concurrency holds down to the last `< N` chunks even on a corpus with one
  dominant blob/wide-row table. Robust to the byte/row-width skew the row-count
  partition can't see.
- **Cost:** boundary queries (one `RangeBounds` or `SampleKeysetBoundaries` per
  eligible large table) on the side pool at cold-start; a modestly larger work
  list; the resume-cursor map keyed per-item. All bounded and cheap.
- **Scope — native-MySQL only.** VStream COPY-path tier (a)+(b) and PG-path
  intra-table stealing remain roadmap-open (the per-stream Match-scoping and the
  separate PG orchestrator are the constraints). Documented in roadmap item 21.
- **Not changed:** the happy-path cold-copy correctness, the consistent-snapshot
  model, the CDC anchor, the keyless at-least-once contract (keyless tables are
  never chunked), the N×D budget, the static partition fallback (a single group
  still takes the serial loop), tier-(a) whole-table stealing (a sub-threshold or
  non-orderable-PK table still steals whole).

## Validation

- **Unit:** work-item construction (chunk-eligible large table → M chunk items
  with tiling bounds; sub-threshold / no-PK / non-orderable / sluice-injected →
  one whole item; `--no-intra-table-stealing` → all whole); the per-work-item
  cursor-key disambiguation (two chunk keys of one table never alias); chunk
  count clamp.
- **Integration (`-race` is the CI gate):** native-MySQL → {MySQL, PG} cold-copy
  of a **skewed** corpus (one huge table + several small) with N > 1 readers,
  asserting (a) src==dst exact row counts + a value-sensitive checksum (no
  miss/dup), and (b) the huge table was copied across multiple chunks (tail
  reclaimed) — observed via a dispatch observer seam mirroring
  `concurrentCopyDispatchObserver`. The exactly-once chunk-coverage pin runs
  across the **PK-family matrix** (the ADR-0096 lesson): integer PK
  (MIN/MAX/divide) and keyset PK — single non-integer (UUID/varchar/decimal/
  temporal) and composite — × {chunked, whole}, with boundary-straddling rows,
  src==dst ground-truthed on the real target.
- **Drop-recovery integration:** the ADR-0111 `concurrentDropInjector` extended
  to fire mid-chunk, asserting the chunk resumes from its cursor within its upper
  bound (no gap, no cross-chunk bleed) after a real re-snapshot.
- **`-race` (CI-only, REQUIRED before tag):** concurrency-touching (shared claim
  cursor + per-item cursor map + N goroutines reading disjoint ranges of one
  table). Push-first / tag-after per the release runbook.
