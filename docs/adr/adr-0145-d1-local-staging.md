# ADR-0145: Lossless local staging for live Cloudflare D1 sources (`--stage-local`)

- Status: Accepted
- Date: 2026-06-30
- Deciders: sluice maintainers
- Relates: ADR-0132 (D1 lossless query-API reader), ADR-0144 (`--infer-types`), ADR-0129 (SQLite date/bool policy), Bug-74 (pin the class)

## Context

`--source-driver d1` reads a live Cloudflare D1 over its HTTP query API. Two limits of that API, discovered by live testing (2026-06-30), make `--infer-types` (ADR-0144) unusable against a real D1:

1. **`LIKE`/`GLOB` pattern-complexity limit (HTTP 400, code 7500 `"LIKE or GLOB pattern too complex"`).** The `--infer-types` temporal- and UUID-conformance checks use long character-class `GLOB` patterns (`isoDateTimeGlob` ≈ 79 chars; `uuidGlob` ≈ 356 chars — 32× `[0-9a-fA-F]`). D1's SQLite build caps pattern length/complexity well below that, so the validation query is rejected outright. This is **size-independent** — it fires on a 1,750-row table with pristine data, on the first `*_at`/`*_uuid` candidate.
2. **Per-query CPU ceiling (HTTP 429, code 7429).** Even where the pattern is accepted (boolean/JSON checks), an unbounded full-table validation scan over a multi-GB table trips D1's per-query CPU limit and aborts.

Both were invisible before because every unit test and the original head-to-head ran `--infer-types` against a **local** SQLite via `modernc.org/sqlite`, which uses the default (permissive) `SQLITE_MAX_LIKE_PATTERN_LENGTH` (50000) and has no CPU ceiling. Live `--source-driver d1` with a temporal/uuid candidate was never exercised — the modernc/D1 dialect divergence is exactly the Bug-162 "assert the real path, not a permissive stand-in" lesson, here applied to D1 SQL compatibility.

The conservative default mapping (`INTEGER`→`bigint`, `TEXT`→`text`) is unaffected; only the opt-in `--infer-types` validation, and any other ad-hoc full-table scan, hits these limits. The failure is loud (no data loss) but the feature's headline use case — graduating a clean D1 dataset to native Postgres types — is blocked on a live D1.

## Decision

Add **`migrate --stage-local`** (Cloudflare D1 source only): before migrating, replicate the live D1 database into a **byte-faithful** local SQLite file, then run the entire migrate — schema read, `--infer-types` validation, and bulk copy — against that local file via the existing `sqlite` engine. Local SQLite (modernc) has neither the pattern-complexity limit nor the CPU ceiling, so validation and reads run locally at full speed.

`--stage-local` **auto-engages** when `--infer-types` is set against a D1 source (the direct path is structurally broken there), unless the operator opts out with `--no-stage-local`. Set `--stage-local` explicitly to stage even without inference (e.g. for a faster local bulk read). The two flags are D1-only and mutually exclusive.

### Why staging (Strategy A) over fixing the validation SQL (alternatives below)

Staging fixes the **whole class** of D1 HTTP-query limits in one move — the GLOB-complexity limit *and* the CPU ceiling *and* the ad-hoc `COUNT`/`MAX` 429s — rather than patching one query shape at a time. Crucially, the staged file carries the **original conservative SQLite types**, so `--infer-types` sees exactly what it would have seen on D1 and makes identical decisions. And it is **lossless**, unlike `wrangler d1 export` (which rounds integers > 2^53 through a JS double — the very loss ADR-0132's reader exists to avoid).

### Byte-faithful copy (not a translating migrate)

The replica is a raw storage-class copy, NOT a `d1 → sqlite` migrate (which would apply type translation and defeat the point — `--infer-types` must see the un-translated source):

- **Schema:** each table is recreated from D1's **verbatim `sqlite_master` `CREATE TABLE` SQL** (exact declared types, PK, UNIQUE/CHECK constraints, DEFAULTs, generated columns, WITHOUT ROWID-ness). Explicit indexes are replayed from their verbatim `CREATE INDEX` SQL after the bulk load; auto-indexes from inline constraints come from the table DDL.
- **Data:** every cell is read via the same `CAST(col AS TEXT)`/`typeof(col)`/`hex()` lossless projection the D1 row reader uses, reconstructed to its exact Go storage-class value (`int64`/`float64`/`string`/`[]byte`/`nil`) by the shared `d1StorageValue`, and bound straight into the local file — so integers > 2^53, REALs, BLOBs, and NULLs are preserved exactly. **Generated columns are skipped** on insert (SQLite recomputes them from the recreated DDL). Pagination reuses the row reader's keyset plan (PK keyset, rowid fallback, OFFSET fallback). FK enforcement is off on the staging connection, so table/insert order is irrelevant.

The staged file is created under the system temp dir and removed when the migrate finishes.

## Scope / consequences

- **D1 source only.** `--stage-local` against a non-D1 source is a loud refusal; `--no-stage-local` is a no-op elsewhere.
- **Conservative default unchanged.** Without `--infer-types` (and without explicit `--stage-local`), a D1 migrate behaves exactly as before — direct streaming reads, no staging.
- **Disk + one-pass read.** Staging needs local disk for the replica and one full read of the source; the bulk copy then reads locally. Net D1 read load is unchanged (the migrate already reads every row once); the trade is local disk + a staging pass for immunity to D1's query limits.
- **Triggers/views are not staged** (a migrate doesn't copy triggers, and the inference/copy operate on tables). A D1 with views is an edge not yet covered by staging; documented, not silently dropped.
- Pinned **byte-faithful** against a modernc-backed mock D1 across storage classes, integers > 2^53, BLOBs, NULLs, generated columns, WITHOUT ROWID, composite PK, and explicit indexes. Live-validated on a real D1 (the control reproduces code 7500; `--stage-local` runs `--infer-types` to completion and lands every row byte-exact).

## Alternatives considered

- **Strategy B — chunked (rowid-windowed) validation over HTTP:** prototyped (walk the validation in `rowid`-bounded windows so each `COUNT` scan stays under the CPU budget). It addresses only the CPU ceiling, **not** the GLOB-complexity limit (chunking reuses the same long patterns), so on a live D1 the validation still aborts at code 7500 before any CPU limit is reached — confirmed by a live sweep. Parked.
- **Standalone GLOB fix** (replace the long char-class patterns with `length`/`substr` checks + a short negated-class GLOB): would fix the pattern-complexity limit but not the CPU ceiling, and would need its own real-D1 validation. A narrower fix than staging; kept as a possible follow-up if a no-staging direct path is later wanted for small D1s.
- **`wrangler d1 export` + local import:** rejected — lossy for integers > 2^53 (the JS-double rounding ADR-0132 exists to prevent).
- **Default-on staging for all D1 migrates:** rejected — a plain conservative D1 migrate works fine streaming directly; staging is only required for `--infer-types` (auto-engaged there) or when explicitly requested.
