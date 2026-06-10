# ADR-0080: Extend index-build overlap to MySQL targets

## Status

Accepted — **shipped v0.99.30** (roadmap item 3c). Extends [ADR-0077](adr-0077-overlap-index-builds-with-bulk-copy.md) (which shipped the overlap for Postgres targets). The deferred **combined-`ALTER`** alternative (below) shipped as a measured follow-up on `main` (`591da65`) after the at-scale bench confirmed the within-table MDL serialization it dodges; see the amendment.

### Implementation notes (what landed)

- `internal/engines/mysql/schema_writer_index_overlap.go` implements `ir.IncrementalIndexBuilder` + `ir.TableIndexedNotifier` + `ir.IndexBuildBudgetSetter` on the MySQL `SchemaWriter`, mirroring the PG path. The orchestrator engages it with zero change the moment the writer satisfies `IncrementalIndexBuilder`.
- Worker sizing is the fixed-N policy (`resolveIndexBuildWorkers`): `min(4, jobCount)` clamped `[1, 8]`. The reserved budget is always 0 on MySQL (no slot prober), so it is stored for surface symmetry but not used to size the pool.
- Flavor gate: `flavor.usesVStream()` (PlanetScale/Vitess) drains the channel firing the per-table callback and defers to the post-copy `CreateIndexes` — required threading `Engine.Flavor` onto the `SchemaWriter`.
- v0.99.30 also fixed a pre-existing serial-path bug surfaced by the overlap work: SPATIAL/FULLTEXT indexes must not carry a column prefix (Error 1089) — `emitCreateIndex` now drops it for those kinds.

### Measured result (at-scale bench, `bench-mysql/`)

MySQL→MySQL, 30 tables × 1.5 M rows, **4 secondary indexes/table**, 10.7 GiB (7.55 GiB data + 3.13 GiB index ≈ 29 %), disk-bound local Docker (both DBs on one host — the realistic worst case for overlap, same regime as ADR-0077's PG bench). Comparing v0.99.30 (overlap) vs v0.99.29 (serial), all runs zero-loss + all-indexes-present:

- **−13 % median total wall (−21 % best-case run1-vs-run1).** The stable, directly-comparable signal is the serial post-copy `ALTER … ADD INDEX` tail — **~1432–1500 s and rock-steady across runs, larger than the copy itself** — which the overlap folds essentially entirely into the copy window.
- **A clear, repeatable win** — *unlike* ADR-0077's PG overlap (−60 s regression), because here the index tail is huge and steady rather than a small fraction of a copy-dominated profile. Live `SHOW PROCESSLIST` confirmed `ALTER … ADD INDEX` running concurrently with `LOAD DATA LOCAL INFILE` on other tables.
- The win is surfaced by **many indexes/table** (4) and **enough tables to keep the N=4 pool busy** (30); it shrinks toward neutral with 1 index/table or a copy-dominated corpus, and grows with more/wider indexes.

Publish the **−13 % median** as the conservative number. (The earlier "~1.27×" placeholder in the roadmap parity tracker was a pre-measurement estimate; this is the measured figure.)

## Context

Index-build overlap (ADR-0077) builds each table's secondary indexes as that table's copy lands, concurrently with the still-copying tables, instead of a separate post-copy whole-schema index phase. It is engine-NEUTRAL in concept but PG-only in implementation: the orchestrator engages it purely on `sw.(ir.IncrementalIndexBuilder)` (`internal/pipeline/migrate.go:1382`), and only the Postgres `SchemaWriter` implements that surface. A MySQL-target `migrate` (MySQL→MySQL and PG→MySQL) therefore still runs the pre-ADR-0077 order: full bulk-copy phase, then a serial whole-schema `CreateIndexes`. Implementing the surface for the MySQL writer gives every MySQL-target migrate the same tail-collapse, with **zero orchestrator change** — the moment the MySQL `SchemaWriter` satisfies `IncrementalIndexBuilder`, `runBulkCopyPhases` routes it through `runOverlappedCopyAndIndexPhase`.

Two MySQL realities shape the design, both verified in code:

- **No connection-slot budget.** MySQL implements no `ir.TargetConnectionBudgetProber`, so `resolveTargetCopyParallelism` returns a zero `CopyBudget`; `splitCopyAndIndexBudget(0, …)` yields `indexBudget=0`. The PG path would floor that to a serial single-worker build — defeating the feature. MySQL must size its index pool itself.
- **No `CREATE INDEX IF NOT EXISTS`.** MySQL's existing `CreateIndexes` already guards every index with an `indexExists` probe against `information_schema.statistics` (`schema_writer.go:618`); the overlap path reuses that probe as its idempotency/resume mechanism.

## Decision

Implement `ir.IncrementalIndexBuilder` + `ir.TableIndexedNotifier` on the MySQL `SchemaWriter`, mirroring the PG implementation (`schema_writer_index_overlap.go`): a `BuildTableIndexesFromChannel` that drains the just-copied tables off the orchestrator's channel into a bounded worker pool, each worker building one table's indexes via `ALTER TABLE … ADD INDEX` on its own connection, reusing the engine-neutral `tableIndexTracker` (register-before-queue, `onDone` outside the lock) and the `IndexesBuilt` resume callback.

**Worker sizing (the MySQL-specific decision).** Because there is no slot probe and the reserved budget is always 0, the pool sizes itself: a conservative fixed **N = 4** (pgcopydb's `--index-jobs` default, matching ADR-0077's ~0.25 fraction intent), clamped to `[1, 8]`, bounded by the table/index job count, and further capped by `--max-target-connections` when the operator sets it. This **widens** `--max-target-connections`' meaning for MySQL (its help text calls it inert for the copy axis on MySQL) to bound the index axis — recorded here as intentional. Consequence/asymmetry vs PG: there is no measured combined-connection ceiling on a MySQL target (the copy axis is already unbounded there), so the index pool's fixed N is the only bound on simultaneously-open index connections. Acceptable: MySQL operators already run the copy axis unbounded.

**Flavor gate.** PlanetScale/Vitess targets (`flavor.usesVStream()`) **decline the overlap**: `BuildTableIndexesFromChannel` drains the channel into a no-op (still firing the `IndexesBuilt` callback per table so resume accounting stays correct) and returns, letting the post-copy `CreateIndexes` run as today. Those platforms route DDL through their own online-DDL / Safe-Migrations queue; concurrent `ALTER … ADD INDEX` against vtgate fights that machinery. The gate is an internal early-return (the type still satisfies the surface), so the code path stays uniform and singly-tested. This requires threading the engine's `Flavor` onto the `SchemaWriter` (it currently isn't).

**One index per job (parallel), not a combined `ALTER`.** Each secondary index is a separate job so they parallelize across workers and the tracker counts them — matching PG. InnoDB would share one table scan across a combined `ALTER TABLE … ADD INDEX a, ADD INDEX b`, trading cross-index parallelism for scan-sharing; which wins is an at-scale question, deferred as a measured follow-up, not guessed.

## Consequences

- MySQL-target migrate's separate index phase collapses into the copy phase (the same structural win ADR-0077 gave PG). PlanetScale/Vitess targets are byte-identical to today (serial post-copy `CreateIndexes`).
- `-race` concurrency chunk (the worker pool + tracker, identical surface to PG) → the `-race` Integration gate must pass before any tag.
- The at-scale bench (above) measured **−13 % median total wall (−21 % best case)** on a disk-bound MySQL→MySQL corpus with a heavy index tail — a clear, repeatable win, unlike ADR-0077's PG −60 s regression. The honest framing held: the number is measured, not assumed, and it would shrink toward neutral on a copy-dominated / few-index corpus.

## Alternatives considered

- **Size the MySQL index pool from `splitCopyAndIndexBudget` like PG.** Rejected — MySQL's budget is always 0 (no slot prober), which floors to serial. A self-bounded fixed N is the only workable sizing without inventing a MySQL connection-slot model.
- **Run the overlap on PlanetScale/Vitess targets.** Rejected — concurrent `ALTER … ADD INDEX` fights the platform's online-DDL/Safe-Migrations queue; defer to it via the post-copy fallback.
- **Combined `ALTER TABLE … ADD INDEX a, ADD INDEX b` per table.** Originally deferred as a measured follow-up; **shipped** (`591da65`) — see the amendment below. The at-scale bench confirmed the within-table MDL serialization that motivated it.
- **Add `ADD INDEX IF NOT EXISTS` for resume.** Not portable on the MySQL versions sluice supports; the existing `indexExists` catalog probe is the idempotency mechanism (strictly an explicit check vs a server-side guard).

## Amendment — combined-`ALTER` per table (shipped `591da65`)

The v1 "one index per job" design left a measured question open: per-index jobs parallelize **across tables** but not **within** a table, because InnoDB takes a table **metadata lock** per `ALTER` — the at-scale bench observed `Waiting for table metadata lock` with one "altering table" + siblings blocked, so a table's N indexes serialize regardless of the pool width. The deferred combined-`ALTER` both dodges that MDL ping-pong and shares the single InnoDB table scan across all of a table's indexes. With the bench having confirmed the serialization empirically (and the win already large without it), the follow-up shipped.

**Grouping rule (the load-bearing constraint).** MySQL allows a comma-separated list of `ADD` clauses in one `ALTER`, but not every index kind may share the statement:

- **G1 — combine:** regular + UNIQUE BTREE/HASH secondary indexes → **one** `ALTER TABLE t ADD INDEX a (…), ADD UNIQUE INDEX b (…), …`. INPLACE-eligible, they share one scan.
- **G2 — each its own statement:** every FULLTEXT index. InnoDB permits only **one** `ADD FULLTEXT` per `ALTER` (Error 1795); the first FULLTEXT also forces an `ALGORITHM=COPY` rebuild for the hidden `FTS_DOC_ID`.
- **G3 — each its own statement:** every SPATIAL index. SPATIAL does not support `LOCK=NONE`, so folding one into G1 would downgrade the whole statement's algorithm.

Mixing G2/G3 into G1 would error (1795) or silently downgrade the combined group off its online INPLACE scan — so they stay separate. On the common all-BTREE table this collapses K statements to one.

**No explicit `ALGORITHM=`/`LOCK=` is pinned.** The target tables are sluice-created and bulk-loaded with no concurrent traffic, so `LOCK=NONE`'s only benefit (not blocking live readers/writers) doesn't apply, and pinning `ALGORITHM=INPLACE` would convert a legitimate server COPY-fallback into a hard error for zero benefit. Letting MySQL choose keeps the emit byte-compatible with the prior per-index output. The grouping rule is what secures the scan-sharing; the algorithm clause is not needed for it.

**Model change.** The job unit became one-per-**table** (`indexBuildJob{tableName, idxs []*ir.Index}`); both the serial whole-schema `CreateIndexes` and the overlapped per-table workers route through the shared `buildTableIndexes`, which filters each index through the `indexExists` probe (so a partial-resume's combined `ALTER` carries only the surviving missing indexes — no 1061 on the present ones) then emits via `emitCreateIndexesCombined`. The overlap tracker's per-table count is now 1; cross-table parallelism (the only parallelism the per-index model actually bought, given the MDL) is preserved. Flavor gate unchanged. Pinned by a unit grouped-emit matrix (all-combinable collapse, mixed-kind split, two-FULLTEXT-never-combine, single==standalone) and integration tests (all-kinds-land family guard per the Bug 74 discipline + partial-resume).

**Measured incremental (`bench-mysql/`, same corpus as above).** Comparing the combined-`ALTER` `main` build vs the per-index v0.99.30 overlap, interleaved, 2 clean runs each (all zero-loss + all-30×4-indexes): per-index median **2177 s** (2120 / 2234), combined median **1783 s** (1801 / 1764) — **−18.1 % median (−394 s), per-pair −15 % and −21 %.** A strong, consistent win *on top of* the overlap: collapsing each table's 4 builds (4 InnoDB scans + the per-`ALTER` MDL ping-pong) into one `ALTER` (1 scan, 1 MDL) compounds across all 30 tables even though cross-table parallelism already keeps the N=4 pool busy. (One combined run's harness checksum step flaked empty on a Rancher Hyper-V socket blip — a false MISMATCH on rc=0; re-verifying the target out-of-band matched the source checksum exactly, `0375a14…`. The migrate was clean; only the post-run verify CLI blipped.) Net of both bench passes: the MySQL index path went from a serial post-copy tail → overlap (−13 % median) → overlap+combined-`ALTER` (a further −18 %).
