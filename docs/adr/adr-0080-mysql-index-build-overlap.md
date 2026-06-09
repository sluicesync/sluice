# ADR-0080: Extend index-build overlap to MySQL targets

## Status

Accepted — design. Roadmap item 3c. Extends [ADR-0077](adr-0077-overlap-index-builds-with-bulk-copy.md) (which shipped the overlap for Postgres targets). Implementation notes filled in when the chunk lands.

### Implementation notes (what landed)

_(to be completed when the chunk merges)_

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
- **No throughput number is published until an at-scale `bench-pgcopydb`-style MySQL-target run measures it.** ADR-0077's PG overlap was **−60 s (a regression)** on disk-bound storage — overlapped builds contended for the saturated disk. InnoDB's online-DDL row-log + buffer-pool contention may be better or worse; the honest framing ("tail-collapse on fast storage, possibly neutral/negative when disk-bound") gets the *measured* number, not an assumed win.

## Alternatives considered

- **Size the MySQL index pool from `splitCopyAndIndexBudget` like PG.** Rejected — MySQL's budget is always 0 (no slot prober), which floors to serial. A self-bounded fixed N is the only workable sizing without inventing a MySQL connection-slot model.
- **Run the overlap on PlanetScale/Vitess targets.** Rejected — concurrent `ALTER … ADD INDEX` fights the platform's online-DDL/Safe-Migrations queue; defer to it via the post-copy fallback.
- **Combined `ALTER TABLE … ADD INDEX a, ADD INDEX b` per table.** Deferred — trades parallelism for InnoDB scan-sharing; a measured follow-up, not a v1 guess.
- **Add `ADD INDEX IF NOT EXISTS` for resume.** Not portable on the MySQL versions sluice supports; the existing `indexExists` catalog probe is the idempotency mechanism (strictly an explicit check vs a server-side guard).
