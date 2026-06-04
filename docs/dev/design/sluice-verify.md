# Design: `sluice verify` — data-integrity verification

**Status:** proto-ADR (research / design draft, not yet implementation-bound)
**Author:** main session
**Date:** 2026-05-07
**Related:** ADR-0029 (`sluice schema diff`), ADR-0017/0018/0026 (batched-apply, per-batch checkpoints, LOAD DATA writer), `docs/dev/design/logical-backups.md` (parallel research; verify is a building block for backup-restore correctness)

## Why

sluice's overarching product goal is **100% confidence that all data has been copied over and synced**. Today the tool is honest about *failure*: when something doesn't translate, it fails loud at apply time per the loud-failure tenet. But it doesn't yet have a positive-confirmation surface — an operator who completes a `sluice migrate` or runs `sync start` for a week has no built-in way to ask "is the target byte-perfect with the source?"

The closest existing surface is `sluice schema diff`, which compares schemas (ADR-0029). That handles the structural side. The missing piece is *data*: row counts, individual row content, and ideally a tunable sampling depth so the operator can spend more or less verification time depending on how confident they need to be.

The operator pain shape that motivates this design (from the user's framing): PlanetScale customers running Fivetran syncs that stop unexpectedly and aren't noticed for days. Sluice should not make that mistake easy. A regular `sluice verify` is the answer — opt-in, scriptable, exit-coded so it slots into operator monitoring.

## Decision

Add a `sluice verify` command with three verification depths that an operator picks based on their risk tolerance and the time/IO budget they want to spend.

```
sluice verify --depth count      # fastest; just compare row counts per table
sluice verify --depth sample     # default; compare row counts AND sampled-row content
sluice verify --depth full       # slowest; compare every row's content hash
```

All three modes:

- Compare source against target across the `sluice migrate` / `sync` configuration the operator already runs (same DSN/source-driver/target-driver/include-table/exclude-table flags as `migrate`, so it's a familiar surface).
- Emit a structured report with per-table pass/fail, mismatch counts, and where to look (PK ranges of mismatched rows when sample/full mode is used).
- Exit 0 on clean, 1 on mismatch detected, 2 on operational error (couldn't connect, schema drifted mid-verify, etc.) — same code shape as `schema diff`.
- Default output is text; `--format json` for machine consumption (CI gates, alerting hooks).
- `--output FILE` writes atomically; useful for nightly verification logs.

### Mode 1: row count (`--depth count`)

For each table in scope:
- Source: `SELECT COUNT(*) FROM <table>` (or engine-specific equivalent — `pg_class.reltuples` is *not* used here; this is the authoritative count).
- Target: same query against the target.
- Compare.

Cost: one query per table on each side. Fast but coarse — catches "table got truncated" or "bulk copy missed half the rows" but not "one row had a value silently changed."

Use case: cheap sanity check operators can run every minute or every hour as a continuous health probe. Good first thing to run after a migrate.

### Mode 2: sampled-row content (`--depth sample`)

For each table:
1. Run the count comparison (mode 1).
2. Pick N random PK ranges (default N=100; configurable via `--sample-rows-per-table`). For tables with a single integer PK, `TABLESAMPLE SYSTEM (...)` on PG and `WHERE id BETWEEN ... AND ...` with random offsets on MySQL. For composite-PK or non-numeric-PK tables, fall back to `ORDER BY RANDOM() LIMIT N` (slower but works everywhere).
3. For each sampled row, compute a content hash (`MD5` of the concatenated column values cast to text, or `SHA-256` if `--strict-hash` is set). Compare source-hash to target-hash.
4. Report any mismatches with the row's PK so the operator can drill in.

Cost: ~N rows per table × number-of-tables. For the default N=100 across 100 tables, that's 10,000 row reads on each side — single-digit seconds against modern hardware, no impact on production load if scheduled off-peak.

Tradeoff: sampling can miss a small number of bad rows. Coverage is statistical: with N=100 random samples per table, ~99% confidence of detecting a 5%+ corruption rate; ~50% confidence of detecting a single bad row in a million-row table. Operators wanting stronger guarantees use `--depth full`.

### Mode 3: full content hash (`--depth full`)

For each table:
1. Run the count comparison (mode 1).
2. Stream every row in PK order, computing a rolling content hash on each side. The hash function is associative-friendly (XOR-of-row-hashes, so the result is order-independent and we don't need both sides to land at exactly the same row at the same time).
3. Compare the final aggregated hash.

If the hashes differ:
4. Re-walk in PK chunks (configurable `--full-chunk-size`, default 10,000 rows), hashing each chunk to narrow the divergence to a chunk.
5. For divergent chunks, walk row-by-row and report the specific PK ranges where source and target differ.

Cost: one full-table scan on each side. For multi-TB databases this is operator-significant; the default operational answer is "run during a maintenance window," but the implementation should support `--throttle ROWS_PER_SECOND` to avoid hammering production.

Use case: post-migration cutover validation, periodic audits ("we promise our data hasn't drifted"), or forensic investigation after a suspected anomaly.

## Implementation outline

### New IR surfaces

Verification is read-mostly and engine-neutral, so it slots in as a small new IR interface set rather than expanding any existing engine interface.

```go
// In internal/ir/verify.go (new file).

// Verifier is the engine-side surface for data verification. Engines
// that don't implement it produce a "verify not supported on this
// engine" error; the verify command uses optional-interface assertion.
type Verifier interface {
    // RowCount returns the authoritative row count for the table.
    // (RowCounter from ADR-0023's --dry-run path returns approximate
    // counts; this returns exact.)
    ExactRowCount(ctx context.Context, db DBHandle, table *Table) (int64, error)

    // SampleRows yields N random rows from the table, in deterministic
    // order from a seed so source and target can sample the same set.
    // The seed handshake is engine-neutral: Verifier.SeedFor(table) on
    // both sides must agree given the same input.
    SampleRows(ctx context.Context, db DBHandle, table *Table, n int, seed int64) (RowStream, error)

    // FullScanHash streams every row in PK order, returning the
    // aggregated content hash. Caller passes the hasher implementation
    // (XOR-of-row-hashes for order-independence).
    FullScanHash(ctx context.Context, db DBHandle, table *Table, hasher RowHasher) ([]byte, error)
}
```

Engines opt-in via the optional-interface pattern (same shape as `RowCounter`, `SnapshotImporter`, `RangeBoundsQuerier`). MySQL and PG both implement straightforwardly; PlanetScale-MySQL inherits the MySQL implementation.

### New command

`internal/pipeline/verify.go` adds a `Verifier` orchestrator (mirrors `Migrator` / `Streamer` / `Differ` / `Previewer` shape):

```go
type Verifier struct {
    Source       ir.Engine
    Target       ir.Engine
    SourceDSN    string
    TargetDSN    string
    Depth        VerifyDepth          // count / sample / full
    SamplePerTable int                // sample mode only
    FullChunkSize  int                // full mode only
    Throttle       float64            // rows/sec; 0 = unlimited
    IncludeTable   []string
    ExcludeTable   []string
    Format         string             // "text" | "json"
    Out            io.Writer
}

func (v *Verifier) Run(ctx context.Context) (VerifyResult, error)
```

Same flag-loading pattern as `migrate` / `sync start`. Configuration is reusable (operator can have one `sluice.yaml` and run `verify` against it the same way they run `migrate`).

### CLI

```
$ sluice verify --help
Verify data integrity between source and target.

Compare row counts, sampled rows, or every row's content hash. Exit 0
on clean, 1 on mismatch, 2 on operational error.

Usage: sluice verify [flags]

Flags:
  --depth                  count|sample|full (default: sample)
  --sample-rows-per-table  N rows per table in sample mode (default: 100)
  --full-chunk-size        chunk size for full-mode bisection (default: 10000)
  --throttle               rows/sec to read from source/target (0 = unlimited)
  --strict-hash            use SHA-256 instead of MD5 (default: MD5)
  --include-table          glob; can be specified multiple times
  --exclude-table          glob; can be specified multiple times
  --format                 text|json (default: text)
  --output                 file path (default: stdout)
  ... + standard --source/--target/--config/--log-level
```

### Reporting shape

Text output mirrors `schema diff`'s format for operator familiarity:

```
-- sluice verify (depth=sample, sample-rows-per-table=100)
-- source: postgres (3 tables)
-- target: mysql (3 tables)
-- result: 2 tables clean, 1 table with row-content mismatches

-- ──────────── customers ────────────
row_count_source=10523  row_count_target=10523  ✓
sample_rows=100  matched=100  mismatched=0  ✓

-- ──────────── orders ────────────
row_count_source=42189  row_count_target=42189  ✓
sample_rows=100  matched=100  mismatched=0  ✓

-- ──────────── products ────────────
row_count_source=523  row_count_target=523  ✓
sample_rows=100  matched=98  mismatched=2  ✗
   pk=42 columns_differing=[updated_at]
   pk=187 columns_differing=[stock_count]

-- result: 1 table mismatched. Re-run with --depth=full for full-table verification.
```

JSON form preserves the same data with predictable field names; `VerifyResult` is the struct that drives both renderers.

## Tenet check

- **Clean elegant code.** New IR interface set is small (3 methods on `Verifier`); the orchestrator follows the existing `Migrator`/`Differ` pattern; no special-case branches polluting other engines. The XOR-of-row-hashes choice keeps the full-mode implementation clean (no global synchronization between source and target reader goroutines).
- **IR-first.** Verification operates on `ir.Row` and `ir.Table` shapes; the row-content hash is computed from the IR's typed values (not raw engine bytes), so cross-engine MySQL `TINYINT(1)` ↔ PG `BOOLEAN` translation is automatically handled. Engines just provide row streams; the hashing logic is shared.
- **Contain Postgres complexity.** Full-mode `TABLESAMPLE` on PG and `ORDER BY RANDOM()` on MySQL are both standard SQL surfaces, not extension-dependent. No `pg_stat_*` snooping required.
- **Validate end-to-end.** `verify` IS the validation surface — this proto-ADR proposes giving operators a way to run it themselves on real schemas, mirroring the CI integration tests.
- **Loud failure beats silent corruption.** The whole feature exists to surface silent corruption that would otherwise persist undetected. If `verify` itself encounters operational errors (engine doesn't implement the interface, query times out, schema drifted mid-verify), it exits with code 2 and a clear message rather than a misleading "all good."

## Consequences

- **Net new operator workflow.** Operators get a tool they didn't have. Migration verification, periodic audits, sync-health probes all become first-class. Fivetran-stops-silently style outcomes become discoverable in the cadence the operator chooses.
- **Engine surface grows by one optional interface.** Both core engines need to implement `Verifier` (small — count + sample + full-scan are mechanically straightforward). PlanetScale-MySQL inherits via its embedded MySQL engine.
- **Resource usage is opt-in.** Mode 1 is cheap; mode 2 is moderate; mode 3 is expensive. Operators choose. Throttling support keeps even mode 3 production-safe.
- **Test surface grows.** New CI integration tests boot real containers, intentionally corrupt one row on the target, run `sluice verify`, and assert the right exit code and report. Same shape as `TestDiff_*` integration tests.
- **Doesn't replace `schema diff`.** Different surfaces for different layers. `schema diff` is structural; `verify` is data. Operators run both; they're complementary.
- **Doesn't replace alerting/monitoring.** `verify` is the probe; integrating its exit code with alerting (PagerDuty, Slack, statsd) is the operator's responsibility — sluice provides the signal, not the delivery mechanism. (See parallel design-sync-health-monitoring proto-ADR for the lag/staleness side, which is closer to the alerting surface.)

## Open questions

1. **Sampling determinism on tables that mutate during verify.** If the source table is being written to during a `sluice verify --depth sample` run, the sampled rows on source might not be present on target by the time target is read (or vice versa). For the migration-then-verify case (target is read-only), this isn't an issue. For the continuous-sync case, we need a "verify against the CDC position the target has caught up to" mode. Likely shape: `verify` reads the target's `sluice_cdc_state` to find the source position the target has fully applied through, then snapshots the source at that exact LSN/GTID for the sampled reads. PG supports this via `SET TRANSACTION SNAPSHOT`; MySQL via `START TRANSACTION WITH CONSISTENT SNAPSHOT`. Worth a small ADR addendum.

2. **Cross-engine value comparison.** MySQL `TINYINT(1)=1` and PG `BOOLEAN=true` are semantically equal but render differently. The IR-typed row-hash should normalize through the same translation policy `RetargetForEngine` already applies for `schema diff`. Most cross-engine cases should "just work" once we use the same canonicalization; the corner cases (timestamp precision, decimal scale, charset/collation in TEXT) need explicit decisions analogous to ADR-0029's defaultEquivalents map.

3. **Generated columns and CHECK constraints.** Generated columns are computed from other columns; their values should match if their input columns match. CHECK constraints aren't data, just predicates. `verify` treats both as no-op for sample/full mode (skip generated columns from the row-hash; CHECKs are out of scope). Document this clearly.

4. **Bulk-load-friendliness.** `verify --depth full` on a very large table needs to use the same streaming machinery `migrate` uses for bulk copy (parallel chunked reads via `RangeBoundsQuerier`). Reuse, don't reinvent.

5. **JSON/JSONB and array columns.** Hashing structured values has edge cases (key ordering, whitespace, type coercion). The IR's value-types contract (`docs/value-types.md`) defines canonical forms; verify uses those. May surface edge cases that aren't currently pinned by tests — file as bugs if they come up.

6. **Logical-backup tie-in.** The parallel `logical-backups.md` proto-ADR notes that backup correctness depends on a verify-style restore-roundtrip check. The two designs share machinery; we should ensure the `Verifier` interface is general enough to verify "source vs. backup file" not just "source vs. live target." Likely: parameterize the `Target` side as either `Engine` (live DB) or a backup-file reader (post-MVP).

## What this is not

- **Not a continuous monitoring framework.** `verify` is a point-in-time probe. The operator runs it on a schedule (cron, k8s CronJob, GitHub Actions); sluice doesn't ship its own scheduler. (Sync-lag / staleness monitoring is a separate concern; see parallel design proto-ADR.)
- **Not a fix-up tool.** `verify` reports drift; remediation is the operator's call (`migrate --resume`, `sync start --resume` from a known-good position, manual repair). Sluice deliberately doesn't auto-heal — silent corruption that's auto-healed is hard to diagnose later.
- **Not a replacement for `schema diff`.** Different layers; both should be in the operator toolbox.
- **Not a replacement for vendor-specific verification tools.** PlanetScale's `pscale data-branching diff`, Percona's `pt-table-checksum`, etc., are still useful in their respective domains. Sluice's `verify` covers the cross-engine case those tools don't.

## Sequencing

If this lands, suggested staged delivery:

1. **MVP — `verify --depth count` only.** Smallest useful slice. Adds the `Verifier` IR interface with just `ExactRowCount`. Both engines implement. New `pipeline.Verifier` orchestrator + CLI command. CI integration test for the happy path. ~1 week.
2. **Sample mode.** Adds `SampleRows` to the interface, sample-content-hash logic to the orchestrator, mismatch-reporting machinery. ~1 week.
3. **Full mode + bisection.** Adds `FullScanHash`, chunk-bisection logic for narrowing mismatches. Throttle support. ~1 week.
4. **CDC-position-aware mode.** "Verify against the position the target has caught up to" — addresses Open Question #1. ~3 days.
5. **JSON/strict-hash polish + edge-case test coverage.** Validates against real schemas with JSON, arrays, geometry, etc. ~1 week.

Total: ~5 weeks of focused effort. The MVP slice (~1 week) is the right place to start since it gates the rest — operators can use count-mode immediately as a Fivetran-style health probe while sample/full follow.

## Recommendation

**Yes, ship the MVP slice.** The "100% confidence" / no-Fivetran-silent-stop pain shape is real and recurring; sluice has the building blocks (IR row stream, engine-pattern, schema diff infrastructure to mirror) to add this without bending its tenets. Count-mode alone closes the most common "did I lose rows?" gap and is cheap enough to leave running on a schedule. Sample and full modes follow as operators ask for stronger guarantees.

Path to no: only if a parallel surface (e.g., a vendor-specific tool the operator already runs, or `schema diff` extended to row-data verification) covers the same ground. As of v0.11.x, no such surface exists in sluice or its near peers.
