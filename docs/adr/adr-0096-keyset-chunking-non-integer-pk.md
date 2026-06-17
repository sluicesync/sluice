# ADR-0096: Sampled-keyset within-table chunking for non-integer and composite PKs

## Status

Accepted. Implemented in `internal/pipeline/chunk.go` (new
`keysetChunkStrategy` + boundary tuple comparison), `internal/pipeline/migrate_parallel.go`
(`filterByUpperBound` generalised to tuple comparison; `canParallelChunkTable`
relaxed), `internal/ir/interfaces.go` (new optional `KeysetSampler`
surface), and `internal/engines/{mysql,postgres}/row_reader_range.go`
(`SampleKeysetBoundaries`). CLI surface is unchanged — the existing
`--bulk-parallelism` / `--bulk-parallel-min-rows` flags now engage on a
strictly wider set of tables.

Builds on [ADR-0019](adr-0019-parallel-within-table-bulk-copy.md), which
shipped MIN/MAX/divide chunking for single integer PKs and explicitly
deferred strategies (b) OFFSET-based and (c) NTILE/sampled-keyset for
"composite PKs, UUID PKs, and badly-skewed integer PKs" to "future
iterations." This is that follow-up. It also relies on the
[ADR-0018](adr-0018-per-batch-bulk-copy-checkpointing.md)
`BatchedRowReader` row-comparison cursor contract — which is already
PK-type-agnostic — being reused unchanged per chunk.

## Context

A live experiment against a cross-region PlanetScale-MySQL target
established the motivating ground truth:

- A single-table bulk copy into PlanetScale runs ~13 GB/h. PlanetScale
  blocks `LOAD DATA LOCAL INFILE`, so sluice falls back to the
  single-connection multi-row `INSERT` batch path
  (`internal/engines/mysql/row_writer.go` `writeBatched`). That path
  pins **one** connection (it must — `SHOW WARNINGS` is session-scoped),
  and one connection cannot saturate a cross-region link: the bottleneck
  is per-connection round-trip latency, not server ingest or local CPU.
- Running N independent single-table copy streams concurrently scaled
  near-linearly: N=3 → ~128 GB/h (9.8×), N=6 → ~151 GB/h (11.6×), with
  no per-stream degradation and no ingest ceiling. Even 3–4 parallel
  INSERT streams beat PG's single-stream COPY (~43 GB/h).

The conclusion is that the gap is parallelism, and the fix is to split a
single large table into N PK-range chunks copied concurrently across N
connections — which is **exactly what ADR-0019 already built.** The
within-table parallel-copy orchestrator (`tryParallelCopyTable` →
`resolveChunks` → `runChunks` → `copyChunk`) is engine-neutral, lives in
`internal/pipeline`, opens one source reader + one target writer per
chunk, and per chunk reuses the ordinary `RowWriter` — which for a
PlanetScale target is the batched-INSERT path. So a parallel copy of a
single integer-PK table into PlanetScale **already** fans out across N
connections today.

The remaining gap is the one ADR-0019 named: a table is eligible for
that fan-out **only** when it has a single integer-typed primary key.
`canParallelChunkTable` rejects everything else, and
`computeChunkBoundaries` (MIN/MAX/divide + `coerceInt64`) only works on
integers. Tables with:

- a single **non-integer orderable** PK (UUID, string, `BINARY`,
  `DECIMAL`, temporal), or
- a **composite** PK,

fall back to the single-connection RTT-bound path. On a real migration
those tables are common (UUID surrogate keys, `(tenant_id, id)` shard
keys), and on PlanetScale every one of them copies at ~13 GB/h while the
integer-PK tables next to them run at ~128 GB/h. The last large table of
a migration, if it happens to be UUID-keyed, single-streams the tail.

The data path that consumes chunk boundaries is **already
PK-type-agnostic**. The per-chunk cursor loop uses
`ir.BatchedRowReader.ReadRowsBatch(ctx, table, after []any, limit)`,
whose contract (ADR-0018) is row-comparison over the **full PK tuple**:

```sql
WHERE (pk1, pk2, ...) > ($1, $2, ...) ORDER BY pk1, pk2, ...
```

Both engines implement it for composite and non-integer PKs already
(`row_reader_batch.go` on each). `ir.TableChunkProgress.LowerPK` /
`UpperPK` / `LastPK` are already `[]any` — multi-column, any-type. The
**only** integer-specific code in the whole parallel path is:

1. `canParallelChunkTable` — rejects non-integer / composite PKs.
2. `computeChunkBoundaries` — MIN/MAX/divide arithmetic + `coerceInt64`.
3. `filterByUpperBound` — clips a chunk's last batch at `UpperPK` using
   an `int64` comparison on a single column.

Lift those three and the existing machinery — resume, checkpointing,
the connection-budget gate, the fast-loader gate, raw-copy passthrough —
all work unchanged on any orderable PK.

## Decision

Add a **second boundary-selection strategy: sampled-keyset boundaries**,
used for tables whose PK is orderable but not a single integer. The
MIN/MAX/divide strategy stays the default for single integer PKs (it is
one cheap query and has no skew on dense integer keys). The two
strategies produce the **same** `[]chunkBoundary` shape (half-open
`(LowerPK, UpperPK]` tuples), so everything downstream is shared.

### 1. Chunk-key selection (what is now eligible, what still falls back)

`canParallelChunkTable` is relaxed to a three-way classification:

- **Single integer PK** → `strategyMinMaxDivide` (unchanged from
  ADR-0019).
- **Single non-integer *orderable* PK, or composite PK whose columns
  are all orderable** → `strategyKeysetSample` (new). "Orderable" =
  the IR type sorts deterministically in SQL `ORDER BY` and the value
  round-trips through a parameter placeholder for the row-comparison
  predicate: integer, decimal/numeric, string (`CHAR`/`VARCHAR`/`TEXT`/
  `uuid`/`inet`/`cidr`/`macaddr`), binary (`BINARY`/`VARBINARY`/`bytea`),
  temporal (`date`/`time`/`timestamp`/`timestamptz`/`datetime`). This is
  exactly the set `ReadRowsBatch` already orders and compares correctly.
- **No usable PK** (no PK, or any PK column of a non-orderable type —
  e.g. a `JSON`/`Array`/`Geometry`-typed key, which no sane schema has
  but which we refuse rather than guess) → **single-connection
  fallback**, identical to today. We never invent a chunking that could
  miss or double-copy rows.

The orderability decision is a pure function over `ir.Type`
(`isOrderablePKType`), table-unit-testable, and lives next to
`canParallelChunkTable`.

### 2. Range computation (sampled-keyset)

For the keyset strategy the orchestrator asks the source reader, via a
new optional `ir.KeysetSampler` surface, for **N-1 interior boundary
tuples** that split the table into N approximately equal **row-count**
slices:

```go
SampleKeysetBoundaries(ctx, table, pkColumns []string, n int) ([][]any, error)
```

Each engine implements it with one windowed query over the PK columns:

```sql
-- conceptually (PG; MySQL 8.0+ identical with backticks):
SELECT pk1, pk2, ...
FROM (
  SELECT pk1, pk2, ...,
         ROW_NUMBER() OVER (ORDER BY pk1, pk2, ...) AS rn,
         COUNT(*)     OVER ()                        AS total
  FROM <table>
) s
WHERE rn IN (ceil(total*1/n), ceil(total*2/n), ..., ceil(total*(n-1)/n))
ORDER BY rn;
```

This is the NTILE/row-number strategy ADR-0019 named as (c). It costs
**one** source-side index scan of the PK columns at chunk-decision time
(the PK index is covering for this query — no heap/clustered-row
fetches), which is the documented setup cost. Crucially it splits by
**actual row count**, so it is **skew-free by construction**: it does
not matter how clustered or sparse a UUID/string keyspace is, each chunk
gets ~total/N rows. This is strictly better on skew than MIN/MAX/divide,
at the cost of a scan instead of a two-aggregate `MIN/MAX` lookup. We
accept the scan for the keyset path precisely because the keyspaces it
serves (UUID, string) are the ones MIN/MAX/divide would skew on.

The returned interior tuples are assembled into `chunkBoundary` records
the same way MIN/MAX/divide assembles them: chunk 0 has `LowerPK == nil`,
chunk k>0 has `LowerPK == boundary[k-1]`, chunk k<N-1 has
`UpperPK == boundary[k]`, chunk N-1 has `UpperPK == nil`. Half-open
`(LowerPK, UpperPK]` everywhere — see the correctness argument below.

Engines that don't implement `KeysetSampler` (or a sample query that
errors / returns fewer than N-1 distinct boundaries — e.g. a tiny or
heavily-duplicate-keyed table) make the table fall back to the
single-connection path. The fan-out is a performance optimisation; its
absence is never a correctness problem.

### 3. Where the parallelism lives

**Unchanged from ADR-0019.** The orchestrator owns the N
(reader→writer) chunk-pipelines per table; the new strategy only changes
how the boundary tuples are *computed*. The IR contract stays clean: the
new `KeysetSampler` is an optional read-side surface (mirroring
`RangeBoundsQuerier`), source-specific knowledge stays in the reader,
and the existing per-chunk `RowWriter` (PlanetScale's batched INSERT) is
reused verbatim. No writer change at all.

### 4. Degree-of-parallelism control + connection budget

**Unchanged.** Same `--bulk-parallelism` (default `min(8, NumCPU)`) and
`--bulk-parallel-min-rows` knobs, the same `resolveCopyParallelismBudget`
split between the cross-table axis (ADR-0076 `--table-parallelism`) and
the within-table axis, and the same `copyParallelismGate` /
`connection_budget` machinery that bounds **total** concurrent
connections at `table × within`. A keyset-chunked table draws from the
identical budget as an integer-chunked one — it opens N reader + N writer
connections, governed by the same gate. Nothing about this change can
oversubscribe a budget the integer path respects.

The motivating experiment showed 3–4 streams already beat PG's
single-stream COPY and that PlanetScale tolerated 6 with no degradation;
the default `min(8, NumCPU)` is conservative against PlanetScale's
per-branch connection limits and is left as-is. No new default to get
zero-value-wrong (the v0.99.51 trap): this change adds **no** config
bool. The strategy selection is derived from the table's PK type at
decision time, not a flag, so there is no constructor that can get a
zero-value-wrong default — every code path (CLI, tests, broker, future
callers) classifies the same way from the `*ir.Table`.

### 5. Correctness with the idempotent-copy / resume contracts

- **Idempotent cold-copy (Bug 125 / `CopyNeedsIdempotentWriter`).** The
  VStream cold-copy path writes via upsert. Chunked parallel upserts to
  **disjoint** PK ranges never key-collide with each other, and the
  exactly-once coverage argument below guarantees disjointness regardless
  of PK type. A re-run re-copies a chunk idempotently. Composes cleanly —
  no change.
- **Resume (ADR-0072 / ADR-0019 boundary stability).** Boundaries are
  computed once on the first attempt and persisted in
  `TableChunkProgress`; a resume reads them verbatim and never
  recomputes — identical to ADR-0019. Because the boundary tuples are
  already `[]any`, the persisted JSON shape is unchanged; a keyset-chunked
  table's state row is byte-shaped exactly like an integer-chunked one
  (just with non-integer tuple values). Resume is therefore
  **fully supported** for keyset chunks — not a v1 limitation. The one
  resume subtlety: the sampled boundaries are derived from the live PK
  distribution, so a resume after large source churn would sample
  *differently* — which is **why** we persist-and-never-recompute, exactly
  as ADR-0019 does for the same reason.
- **Snapshot consistency.** Same engine-asymmetric story as ADR-0019:
  PG sources can pin all N readers to one exported snapshot via
  `SnapshotImporter`; MySQL sources get N per-connection snapshots
  (`migrate` path advises a quiesced source). The keyset *sample* query
  runs pre-stream on the decision connection / a fresh conn, never racing
  an in-flight stream — same discipline as `RangeBounds` /
  `EstimateRowCount` (ADR-0079 v1.1).

### Correctness argument — exactly-once chunk coverage (the load-bearing surface)

A row must land in **exactly one** chunk. The argument is identical for
both strategies because it rests only on a deterministic total order over
the PK tuple, not on the PK being integer.

Let `≤` be the SQL row-comparison total order `ORDER BY pk1, pk2, ...`
(the order both `ReadRowsBatch` and the sampler use). Let the persisted
interior boundaries be `b_1 ≤ b_2 ≤ ... ≤ b_{N-1}`. Chunk k owns the
**half-open** interval:

- chunk 0: `pk ≤ b_1`
- chunk k (0<k<N-1): `b_k < pk ≤ b_{k+1}`
- chunk N-1: `b_{N-1} < pk`

(`LowerPK` exclusive, `UpperPK` inclusive; chunk 0 has no lower bound,
chunk N-1 has no upper bound.)

- **Coverage (no row missed).** For any row PK `x`, exactly one of:
  `x ≤ b_1`, or `b_k < x ≤ b_{k+1}` for some k, or `b_{N-1} < x` holds —
  the intervals partition the totally-ordered domain with no gap. Open
  ends at both extremes (chunk 0 nil-lower, chunk N-1 nil-upper)
  guarantee values below `b_1` and above `b_{N-1}` are still captured,
  including any rows inserted beyond the sampled max during the copy.
- **Disjointness (no row double-copied).** The boundary `b_k` is the
  **inclusive** upper of chunk k-1 and the **exclusive** lower of chunk
  k. A row equal to `b_k` satisfies `pk ≤ b_k` (chunk k-1) and fails
  `b_k < pk` (chunk k) → it lands in chunk k-1 only. This is the single
  place a duplicate could arise, and the half-open convention closes it
  for any type with a deterministic `≤`.
- **Duplicate boundary values.** If sampling returns `b_k == b_{k+1}`
  (a heavily-duplicated key region), chunk k+1's interval
  `b_k < pk ≤ b_{k+1}` is **empty** — correct, not lost: those rows fall
  into chunk k (or earlier) under `pk ≤ b_k`. The orchestrator drops
  zero-width interior chunks (collapses N) the same way ADR-0019 collapses
  when `span < n`, so an empty chunk never costs a connection. No row is
  ever placed in two chunks or zero chunks.

The load-bearing requirement is that the **chunk upper-bound clip MUST
use the same total order the reader's `ORDER BY` / `WHERE (pk) > (...)`
cursor uses**. For string / varchar / char and decimal-as-text PKs that
order is the column's **DB collation** (PG `en_US.utf8`, MySQL's
case-/accent-insensitive `utf8mb4_0900_ai_ci`), **not** a byte order. The
first cut of this ADR enforced the upper bound in Go with a **bytewise**
tuple comparator (`comparePKTuple`) while the SQL side used the column
collation; the two **diverge** for those families, so a boundary-straddling
row could be excluded by **both** the chunk above it (Go: "past upper")
**and** the chunk below it (SQL: "≤ lower") — landing in **no chunk**, a
silent permanent loss. (Decimal carried as text is wrong for an even
simpler reason: lexical `"10" < "9"` but numeric `10 > 9`.) This was the
exact Bug-74 family-dispatch trap; the original test suite missed it
because it used **UUID** — byte-monotonic lowercase hex, accidentally
collation-safe — as the "string" representative.

The fix makes the upper bound a **SQL** predicate, enforced in the column's
native collation alongside the existing lower bound. The
`ir.BoundedBatchedRowReader` surface (`ReadRowsBatchBounded`) emits
`WHERE (pk) > ($after) AND (pk) <= ($upTo) ORDER BY pk`, so **both** bounds
and the `ORDER BY` use the same collation and the same PK index — the
partition is exactly-once for every orderable family **by construction**,
with no Go-side comparison in the coverage path. The keyset strategy
**requires** this surface (`shouldParallelChunk` routes a reader without it
to the single-reader path), so the collation-sensitive families never take
a divergent byte clip. Both shipping engines implement it; the
single-integer raw-copy lane (bare integer SQL literals) is gated to
integer single PKs so a string keyset chunk never reaches it.

`filterByUpperBound` (the byte-tuple comparator) survives **only** as the
fallback clip for a hypothetical reader that lacks the bounded surface, and
is correct there only for byte-ordered families (integer, temporal,
PG-native uuid/bytea) — never reached for string/decimal because the
strategy requires the bounded reader.

These invariants are unit-tested (the SQL bound-predicate shape — single,
composite, lower-only, upper-only, both — for each engine; the half-open
partition arithmetic across families) and integration-tested src==dst
row-count-and-checksum on real containers for the **collation-sensitive**
families specifically: PG `text`/`varchar` under default **and** explicit
`COLLATE "en_US.utf8"`, PG `numeric`, PG `char(n)`; MySQL `varchar` and
`decimal` under default `utf8mb4_0900_ai_ci` plus a composite. A further
integration pin ground-truths the Go partition against the **real DB
ORDER BY** (sample boundaries → drain each chunk via the bounded read →
assert every PK appears in exactly one chunk).

NULLs in a PK column cannot occur (PK columns are `NOT NULL` by
definition), so the comparator does not define a NULL ordering and a NULL
PK value is a loud programming-error, not a silent mis-sort.

## What this design deliberately does not do (yet)

- **OFFSET-based splitting (ADR-0019 strategy b).** Superseded by
  sampled-keyset for the cases that matter; `OFFSET` on a huge table is
  O(offset) per chunk and slower than one windowed scan. Not implemented.
- **Sampling-without-full-scan.** The windowed `ROW_NUMBER()` scans the
  PK index once. For multi-TB tables an approximate sampler
  (`TABLESAMPLE` on PG, histogram bucket bounds) would avoid the scan;
  deferred until a real workload shows the one-time PK-index scan is a
  bottleneck (it is covered by the index and runs once per table at
  decision time, overlapped with nothing on the critical path).
- **Lifting the snapshot asymmetry.** Same MySQL per-connection-snapshot
  window as ADR-0019; out of scope here.

## Consequences

**Win.** Tables with UUID / string / binary / decimal / temporal PKs and
composite PKs now get the same N-way within-table fan-out that integer-PK
tables have had since v0.5.0 — closing the PlanetScale single-connection
RTT-bound gap for the common non-integer-keyed table. The keyset path is
skew-free (row-count split), so it is robust on exactly the clustered
keyspaces MIN/MAX/divide handled poorly. No CLI change; operators already
passing `--bulk-parallelism` get the wider coverage automatically.

**Cost.** One PK-index scan per keyset-eligible table at decision time
(vs. a two-aggregate `MIN/MAX` for the integer path). Runs once,
pre-stream, on a non-critical connection. N reader + N writer connections
per parallel table, governed by the existing connection budget — no new
oversubscription surface.

**Concurrency class.** This change extends the goroutine-fan-out path
(`runChunks` / `copyChunk`) to more tables and adds a new tuple
comparator on the per-batch clip path. It is a **concurrency chunk**: the
`-race` integration gate MUST pass before the tag is cut (push-first /
tag-after), per the CLAUDE.md rule.

**Correctness surface.** The exactly-once chunk coverage now rests on a
type-family-dispatched tuple comparator rather than an int64 compare.
That comparator is the Bug-74-class risk and is pinned across every
family × shape; reviewers must re-derive the family matrix and confirm
the pins cover it (the Bug-74 reviewer corollary).

## Alternatives considered

- **Do nothing; document "use an integer PK for fast migration."**
  Rejected — non-integer/composite PKs are common and not under the
  operator's control on a source they are migrating *away* from.
- **OFFSET-based chunking (ADR-0019 b).** Rejected — O(offset) per chunk,
  slower than one windowed scan, and skews the same way on duplicate keys
  without the half-open empty-chunk safety being any simpler.
- **Hash-of-PK modulo N chunking.** Rejected — gives perfectly even
  chunks but the per-chunk predicate (`WHERE hash(pk) % N = k`) defeats
  the PK index (full scan per chunk, N scans total) and can't reuse the
  `ReadRowsBatch` cursor contract or its resume cursor. Range chunking
  reuses the entire existing data path.
- **Client-side MIN/MAX + lexicographic divide for strings.** Rejected —
  "divide the string range" is ill-defined across collations and charsets
  and would skew arbitrarily; sampling actual rows sidesteps collation
  entirely by letting the engine's own `ORDER BY` define the order.
