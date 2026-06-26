# ADR-0123: Budget-wide intra-table PK-range work-stealing for the `migrate` / PG fast-cold-start parallel copy

## Status

**Accepted (2026-06-26, implementation landed).** Roadmap item 21, **tier (b)** for the
`migrate` path and the Postgres `sync` fast cold-start that reuses it (ADR-0079).
Companion to [ADR-0119], which brought tier (b) to the **native-MySQL** consistent-
snapshot path. This ADR brings the same tail-reclaim to the OTHER family of cold-copy
paths — the ones built on the cross-table worker pool (ADR-0076) + the within-table
PK-range chunker (ADR-0019/ADR-0096), not on the FTWRL multi-snapshot work-stealing
queue.

**Throughput optimization, not a correctness gate** — the cross-table pool + the
within-table chunker are already correct and exactly-once; this reclaims a MEASURED
tail-taper and makes the copy width robust to the budget freed by finished peer
tables. It is, however, **exactly-once-CRITICAL and concurrency-CRITICAL in
implementation**: the change touches the single shared connection-budget chokepoint
and the within-table chunk fan-out, both on the path EVERY `migrate` + the PG `sync`
cold-start flow through. The coverage + budget invariants below are mandatory.

`-race`-before-tag concurrency chunk: the shared budget gate is a counting semaphore
drawn by N goroutines; the `-race` Integration CI gate MUST pass before any tag (per
the release runbook). This box runs CGO=0 and cannot run `-race` locally.

## Context

### The measured taper

A skewed-corpus PG→PG `migrate` (1×10M-row table + 6×50k) with
`--table-parallelism 4 --bulk-parallelism 4` (full connection budget = 16) was
measured via source `pg_stat_activity`: the small tables drained in ~1.5 s, then for
the **entire ~40 s big-table copy the reader concurrency stayed pinned at 4**
(= `--bulk-parallelism`) while **12 of 16 connection slots idled**.

Root cause (confirmed in code): the migrate path schedules on **two independent
levels** whose product is the budget but neither of which can redistribute:

- the cross-table pool (`migrate_table_pool.go`, ADR-0076) caps concurrent tables at
  `--table-parallelism` and
- each table's within-table copy (`runChunks`, `migrate_parallel.go`) split the table
  into exactly `--bulk-parallelism` chunks (`resolveChunks` →
  `computeChunkBoundaries(..., parallelism)`) behind a **per-table fixed-width gate**
  (`newCopyParallelismGate(len(chunks), …)`).

So a single large table occupies at most `--bulk-parallelism` of the budget. When the
peer tables finish and free their table-pool slots, the big table has **no mechanism**
to expand its 4 fixed chunks into the freed 12 slots — the freed budget simply idles.
This is the exact analogue, on the migrate/PG path, of the tail-taper [ADR-0119]
fixed on the native-MySQL path.

### Why the native-MySQL ADR-0119 design does not transfer verbatim

[ADR-0119] solved the native-MySQL taper with a **claim-queue work-stealing
scheduler**: flatten the disjoint partition into one ordered `[]copyWorkItem`, run
`N` pipelines that atomically claim items off a shared cursor, each reading on its own
pinned FTWRL-snapshot connection. That path could be rewritten from scratch because it
carries **none** of the migrate path's per-table machinery — it persists no mid-copy
checkpoint (ADR-0119 §6), has no resume, no index-build overlap, no per-table
source-read retry.

The `migrate` / PG-cold-start path is the opposite: it is the densely-layered shared
path. Replacing its two-level scheduler with a from-scratch claim-queue would force a
re-implementation of, and re-validation of, ALL of:

- ADR-0109 per-table source-read reconnect-and-resume retry
  (`copyTableWithSourceReadRetry`, with the per-table truncate-restart vs
  chunk-cursor strategy split),
- ADR-0077 per-table index-build overlap (`onTableCopied` fires when a whole table
  completes),
- per-table resume classification (`classifyTableForResume`) + per-chunk resume state
  (`ir.TableChunkProgress`),
- the ADR-0076 free-pair primary-reuse optimization,
- the ADR-0043 fast-loader / ADR-0078 raw-copy / ADR-0036 cursor gates, all evaluated
  per chunk.

Re-deriving every one of those on an exactly-once-critical path in a single chunk is
too broad to land safely and reviewably. So this ADR takes the **contained** design
that achieves the IDENTICAL measured outcome (budget-wide tail reclaim) while reusing
every piece of that machinery verbatim — it changes ONLY the scheduling.

## Decision

### 1. One shared connection-budget gate, drawn by every copy connection

Replace the two independent caps (table-pool cap × per-table fixed-width gate) with
**one runtime counting semaphore** — the run's single connection-budget chokepoint —
sized to the SAME product ceiling ADR-0076 enforced statically:

```
budget = tableParallelism × withinParallelism
```

Constructed once in `runBulkCopyPhases` and stored on `parallelBulkCopyDeps.copyGate`
(reusing the existing `copyParallelismGate`, which already has the ADR-0028b AIMD
shrink-on-53300 backoff). Every copy connection draws ONE token:

- each cross-table pool worker takes one token for its **base** connection (the
  primary/free-pair connection that chunk 0 / the single-reader path uses), held for
  the whole table copy;
- each within-table PK-range chunk `1..M-1` takes one token via the existing
  `acquireChunkConn` path (chunk 0 reuses the base connection's token, unchanged).

Because the budget is now a single semaphore rather than two static caps, it is
**redistributed at runtime**: when a peer table finishes and releases its base token,
an in-progress large table's surplus chunks — blocked on the same gate — steal the
freed token. The copy stays budget-wide down to the tail instead of pinned at
`--bulk-parallelism`. This is the "contained work-stealing layer": idle budget is
stolen by the big table's remaining chunks, exactly as a claim-queue would have an
idle worker steal the big table's remaining items — the effect at the tail is
identical (one big table → all `budget` tokens available → budget-wide).

This **supersedes the per-table fixed-width gate**. The per-table gate survives only
as a nil-`copyGate` fallback (serial cold-start / no-budget / unit tests), byte-
identical to the pre-ADR-0123 behaviour.

### 2. Finer, DETERMINISTIC chunking for large tables

A large table must be splittable into enough chunks to fill the *whole* budget at the
tail, not just `--bulk-parallelism`. `resolveChunks` now derives the chunk count from
the row-count estimate (mirroring [ADR-0119]'s clamp):

```
M = clamp(ceil(estRows / withinThreshold), withinParallelism, max(maxChunksPerTable, budget))
```

- `withinThreshold` is the same `resolveBulkParallelMinRows` adaptive threshold the
  chunk-eligibility gate uses (so each chunk targets ~one threshold's worth of rows);
- the floor is `withinParallelism` (the OLD fixed M) so no eligible table ever gets
  FEWER chunks than before — strictly ≥ the pre-ADR-0123 width;
- the cap is `max(maxChunksPerTable=64, budget)` so even a budget > 64 is fully
  fillable, while a per-chunk-overhead bound still holds for the common case.

Boundaries remain a **deterministic pure function of the sorted PK set**: the SAME
`computeChunkBoundaries` (integer MIN/MAX/divide, ADR-0019) / `computeKeysetChunk-
Boundaries` (sampled-keyset ROW_NUMBER, ADR-0096) functions, just fed a larger `M`.
Those functions are already pinned exactly-once across the PK-family matrix (the
Bug-74 collation clip, ADR-0096), so the finer split inherits their tiling
correctness — the half-open `(lower, upper]` tiling with nil end-caps, no gap, no
overlap, the upper bound pushed into SQL in the column's native collation.

### 3. Resume re-derives the identical chunk set (stability)

`resolveChunks` already **persists** the computed boundaries into
`state.TableProgress[t].Chunks` on the first attempt and **reloads** them unchanged on
resume (it returns `entry.Chunks` when present). So resume re-uses the byte-identical
chunk set — stronger than re-derivation. The estimate-driven `M` is computed only on a
FRESH run; a process restart mid-table reloads the persisted chunks (whatever `M` that
run chose), and each chunk resumes from its durable `chunk.LastPK` within its upper
bound, exactly as ADR-0096/ADR-0019 already do. This satisfies the ADR-0099 §5
partition-stability requirement by persistence. (A run started under the old fixed-`M`
code and resumed under this code reloads the old 4 chunks — forward-compatible.)

### 4. Budget is REDISTRIBUTED, never INFLATED

The single gate holds exactly `budget = tableParallelism × withinParallelism` tokens —
the same product ceiling ADR-0076's two static caps enforced. Every base AND chunk
connection draws one token, so the total concurrently-open copy connections never
exceeds `budget` (strictly: it is a tighter, true single chokepoint than the prior
product-of-two-caps, which could leave slots structurally unreachable by one table).
The ADR-0077 index-build pool keeps its separate `indexBudget` slice; the copy gate is
sized to the copy axes' product, which already excludes that reservation, so
`indexBudget + copy-connections ≤ measured budget` still holds. A 53300 slot-
exhaustion now shrinks the GLOBAL budget (the AIMD retire path drains the shared pool),
which is the correct run-wide response to target slot pressure.

### 5. Deadlock- and starvation-freedom (the concurrency argument)

- **No deadlock.** A table-pool worker holds its single base token while its inner
  chunk errgroup runs. Chunk 0 reuses the base connection and needs NO gate token, so
  every active table always makes forward progress on chunk 0 regardless of gate
  pressure. A chunk `1..M-1` blocked on the gate holds NO token while waiting (no
  hold-and-wait cycle). Every running chunk copies a finite range and releases; every
  table finishes and releases its base token. So free tokens always eventually appear
  and blocked chunks proceed — the system always advances. `budget ≥ tableParallelism`
  (since `withinParallelism ≥ 1`), so the initial `tableParallelism` base acquires
  never block.
- **Benign ordering nondeterminism.** A newly-admitted small table's base acquire and
  the big table's surplus-chunk acquires race for a freed token; a small table can
  therefore be delayed behind big-table chunks. This is correctness-neutral
  (exactly-once is unaffected by order) and budget-neutral (the budget stays fully
  used by *whichever* table's work wins the token), and it does not extend the
  critical path (the big table is the critical path; constraints/FKs run after all
  copy; PG index builds overlap regardless). Documented rather than engineered around.
- **Shrink vs held base tokens.** A 53300 shrink retires tokens on RELEASE; an
  in-flight table's base token is held (not retired) until the table finishes, so no
  active table is ever forcibly starved of its base connection. The retire floor keeps
  ≥1 live token, preserving forward progress.

### 6. Per-chunk invariants preserved verbatim (the silent-loss surface)

ONLY the scheduling changes (which connection copies which chunk + how many chunks a
big table has). Every per-chunk invariant is reused unchanged: exactly-once half-open
`(lower, upper]` tiling with the collation-correct SQL upper clip (ADR-0096); per-chunk
resume + `ir.TableChunkProgress` (ADR-0019); the ADR-0043 fast-loader
(`copyChunkFast`); the ADR-0078 raw-copy passthrough (`copyChunkRaw`); the ADR-0036
cursor resume; the ADR-0109 source-read retry (still per-table, with chunked tables
resuming from chunk cursors); the ADR-0110 grow-gate; the ADR-0076 cross-table
`stateMu` progress map; the ADR-0077 index-build overlap (`onTableCopied` still fires
per WHOLE table). The `copyChunk` / `copyChunkFast` / `copyChunkRaw` bodies are
untouched apart from a nil-cheap test-only lifecycle observer.

### 7. Snapshot→CDC seam (PG cold-start) unaffected

The PG `sync` fast cold-start (ADR-0079) pins every parallel reader to ONE exported
snapshot (`ImportSnapshot`), established once before the copy; `migrate` opens
independent per-connection readers (the documented ADR-0019 v1 window). In BOTH cases
the snapshot/position is established independently of WHICH worker reads WHICH chunk —
each chunk read is independently correct (migrate makes no cross-table transactional-
consistency promise; the PG cold-start's consistent_point is the snapshot, not the
schedule). Redistributing the budget across chunks does not touch the seam. The
MySQL `migrate` per-chunk read remains its own REPEATABLE-READ, unchanged.

## Consequences

- **Win:** the migrate / PG-cold-start cold-copy tail is bounded by a chunk, not by
  `--bulk-parallelism`. On the measured skewed corpus the big table now expands from
  the pinned-at-4 width toward the full budget (16), eliminating the ~12-of-16 idle
  tail.
- **Cost:** one estimate query per eligible large table (already paid by
  `shouldParallelChunk`); a larger per-table chunk fan-out (up to `max(64, budget)`
  goroutines, most blocked on the gate — cheap); the budget is now a runtime semaphore
  rather than two construction-time caps.
- **Scope:** `migrate` (all four directions) + the PG `sync` fast cold-start
  (`runColdStartParallel`, which reuses `runBulkCopyPhases`). The VStream COPY path's
  tier (b) remains **roadmap-open and BLOCKED**: each VStream is Match-scoped to its
  group at the *source*, so a single channel is not arbitrarily range-splittable —
  intra-table stealing there needs per-stream Match-scoping primitives that are a
  foundational constant-memory feature, out of scope here. Native-MySQL tier (b) is
  [ADR-0119]. Tier (a) native-MySQL is v0.99.74.
- **Not changed:** happy-path cold-copy correctness; the keyless at-least-once
  contract (keyless / non-orderable / sluice-injected-PK tables are never chunked,
  stay whole, take one base token); the N×D write fan-out budget; the ADR-0077 index
  reservation; the serial cold-start fallback (nil `copyGate`).

## Validation

- **Unit:** the chunk-count clamp (`resolveParallelChunkCount`) across est × threshold
  × budget (floor at `withinParallelism`, cap at `max(64, budget)`); that a fresh
  large table resolves to FINER chunks than `--bulk-parallelism`; that resume reloads
  the persisted chunk set unchanged; that the budget gate is shared (one gate, sized to
  the product). The half-open tiling itself stays pinned by the existing
  `chunk_test.go` / `chunk_keyset_test.go` family matrix (this ADR only feeds a larger
  `M` into the same pinned functions).
- **Integration (`-race` is the CI gate):**
  - **skewed-corpus tail-reclaim** (PG→PG migrate, 1 big + several small,
    `--table-parallelism 4 --bulk-parallelism 4`): a chunk-lifecycle dispatch observer
    records the PEAK concurrent chunk reads of the big table and asserts it expands
    **well past `--bulk-parallelism` toward the full budget** at the tail (not pinned
    at 4), AND that global concurrency never exceeds the budget — the proof the taper
    is gone, plus src==dst exact counts + value-sensitive checksum (no miss/dup).
  - **exactly-once family matrix** (integer / keyset-single non-integer / composite PK
    × {chunked, whole}): src==dst exact + checksum, ground-truthed on the real target.
  - **resume mid-big-table:** interrupt a large-table copy, resume, assert the same
    chunk set is re-derived (persisted) and the table converges exactly-once.
  - **MySQL→MySQL migrate** still correct (the shared path didn't regress).
- **`-race` (CI-only, REQUIRED before tag):** the shared budget gate is drawn by
  `tableParallelism` base goroutines + up to `max(64, budget)` chunk goroutines per
  active table. Push-first / tag-after per the release runbook.

[ADR-0019]: adr-0019-parallel-within-table-copy.md
[ADR-0076]: adr-0076-cross-table-copy-worker-pool.md
[ADR-0077]: adr-0077-overlap-index-builds-with-bulk-copy.md
[ADR-0079]: adr-0079-fast-cold-start-for-sync-path.md
[ADR-0096]: adr-0096-keyset-chunking-non-integer-pk.md
[ADR-0119]: adr-0119-intra-table-pk-range-work-stealing.md
