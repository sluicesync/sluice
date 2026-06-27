# sluice v0.99.145

**SQLite file imports now parallel-copy large tables. A big binary-`.db` table is split into N collation-correct keyset chunks copied concurrently into the Postgres/MySQL target, instead of streaming through a single reader — the same within-table chunking the Postgres and MySQL sources already had. Opt-in via the existing `--bulk-parallelism`; safe by construction, with temporal/decimal-PK tables deliberately kept on the single-reader path to avoid a silent-loss class.**

## Features

**Within-table parallel-copy chunking for the SQLite file source (ADR-0128).** Until now every SQLite table copied through one reader; a large table is now divided into N keyset/range chunks read concurrently and fed to the already-parallel target writer. The `sqlite` `RowReader` gained the optional orchestrator surfaces the parallel-bulk-copy path auto-detects: `BatchedRowReader`/`BoundedBatchedRowReader` (PK-cursor pages clipped to a chunk's inclusive upper bound), `RangeBoundsQuerier` (`MIN`/`MAX` for a single integer PK → divide), `KeysetSampler` (a `ROW_NUMBER()` window split-by-row-count for a non-integer or composite PK → skew-free chunks), and `RowCounter`/`RowCountEstimator` (a `COUNT(*)`, cheap on a local file). Each per-chunk reader opens its own read-only connection (with a `busy_timeout` so concurrent opens against the one file don't spuriously `SQLITE_BUSY`); SQLite permits concurrent readers and the source is never written. Tune via the existing `--bulk-parallelism` / `--bulk-parallel-min-rows` flags.

**Exactly-once partition, collation-correct by construction (the Bug-74 silent-row-loss class).** Every chunk's lower/upper bound is a row-VALUE comparison (`(pk…) > (?…) AND (pk…) <= (?…)`, SQLite 3.15+) pushed into SQL — never clipped in Go — and the `WHERE`, the `ORDER BY`, and the keyset sampler all reference the TABLE-QUALIFIED real PK columns, so they share each column's INTRINSIC collation (BINARY, NOCASE, or a declared custom collation) and no explicit `COLLATE` is ever injected. A NOCASE/custom-collation TEXT PK, a BLOB PK, and composite PKs all partition with no row landing in zero or two chunks.

## Fixed / hardened

**Temporal and decimal PRIMARY KEYs are excluded from the cursor path — closing a silent-loss class found in pre-release review.** SQLite has no native temporal/decimal storage, so such a PK decodes to a Go `time.Time` / decimal string that cannot be re-bound as the next page's `>` cursor against the column's raw INTEGER/REAL/TEXT storage. For unix-epoch/julian (numeric) storage the re-bound TEXT cursor ranks below every row and the next page returns empty (silent truncation to one page per chunk); for offset-bearing ISO text it reorders against the BINARY `ORDER BY` (silent dup/skip). A table whose PK is temporal (DATE/DATETIME/TIME) or decimal/NUMERIC is therefore copied whole-table single-reader for BOTH chunking and per-batch resume — vetoed via a new optional `ir.BatchedReadDisqualifier` surface the orchestrator's resume gate consults, with the batched readers refusing loudly if called directly. Loud failure / safe fallback, never silent loss. Integer / text (BINARY or NOCASE) / blob / composite-of-those PKs round-trip exactly and chunk as normal. (Caught by the value-fidelity review before release; pinned with multi-page per-encoding tests including the catastrophic unix-epoch INTEGER-storage case.)

## Compatibility

Additive and opt-in. The `.sql`-dump path stays single-reader (a dump materializes into a reader-owned temp DB; per-chunk re-materialization would be wasteful, so the decision surfaces report "no chunking" for it). The `d1` query-API reader (a separate type that already keyset-paginates) is unchanged. The new `ir.BatchedReadDisqualifier` is an optional surface implemented only by SQLite; the orchestrator's consult is a guarded type-assertion defaulting to "not disqualified," so the Postgres and MySQL paths are byte-for-byte unchanged. Shipped under unit pins (the exactly-once partition across each PK family × shape, the temporal/decimal disqualification, the round-trippable positive control), a multi-page cross-engine integration test, and an independent value-fidelity review (which BLOCKed the first cut over the temporal-PK silent-loss; the fix above is the result).

## Who needs this

Anyone importing a large SQLite database (a sizable binary `.db`) into Postgres or MySQL — big tables now copy concurrently instead of one reader at a time. Small databases, `.sql`-dump imports, and the `d1` reader are unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.145 · **Container:** ghcr.io/sluicesync/sluice:0.99.145
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
