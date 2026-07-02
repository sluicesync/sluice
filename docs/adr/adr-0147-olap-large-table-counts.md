# ADR-0147: OLAP workload mode for large-table row counts on Vitess/PlanetScale

- **Status:** Accepted (decision); implementation roadmap-tracked
- **Date:** 2026-07-02
- **Related:** ADR-0107 (PlanetScale target telemetry), the v0.99.15 "never set `workload=olap` session-wide" lesson (`internal/engines/mysql/row_reader.go` already uses OLAP on a dedicated connection for the no-PK full-scan read). Supersedes the working assumption that `verify`'s chunked-PK count exists to dodge a *row-read* cap.

## Context

`verify` needs an exact row count (`ExactRowCount`) that won't silently disagree with stored cardinality, so it pays a full count. On Vitess/PlanetScale a long-running single statement is killed by the **max-statement-execution-time** limit — MySQL **errno 3024** (`Query execution was interrupted, maximum statement execution time exceeded`) — at roughly ~900 s. A `count(*)` that scans a large **clustered** index (a wide table with no cheap secondary index to auto-narrow to) runs long enough to hit it.

`ExactRowCount` (`internal/engines/mysql/verifier.go`) dispatches on PK shape:

- **single-integer PK** → `chunkedCount`: sums `COUNT(*)` over PK ranges of `defaultCountChunkSize` (50 000) rows, one connection, sequentially. Each range query is short, so none approaches the 3024 wall.
- **anything else** (composite PK, string/UUID PK, or no PK) → `singleShotCount` = plain `SELECT COUNT(*)`. On a large table this **fails with errno 3024**. The fallback's own comment calls this a "PS row-read limit" — that diagnosis is **wrong**: it is the statement-*execution-time* wall, not a rows-returned cap (a `count(*)` returns one row).

### Empirical benchmarks (2026-07-02)

- **10 GB, real PS-10.** With a narrow secondary index, plain `count(*)` auto-narrows to it and returns in **~4 s**. Forced onto the clustered index it takes **244 s** but *completes* (small enough to stay under the wall) — which is why at this size OLAP looked unnecessary.
- **49 GB, real PS-80, PK-only (clustered scan forced):**
  - OLTP `count(*)` → **FAILS at ~661 s with errno 3024.**
  - OLAP `count(*)` (`SET workload='olap'`) → **completes in 1264 s** (streams; the OLAP path is not bound by the OLTP statement-time limit).
  - chunked-PK (240 × 50 000-row ranges) → **completes in 1264 s** — *identical* to OLAP.
- The deferred `ALTER … ADD INDEX` on the same 49 GB table independently hit errno 3024 at **~901 s** (tracked separately — see the roadmap "deferred-index errno-3024" item).

So at scale the OLTP single-count genuinely fails, and **OLAP and chunked-PK are the same speed** (both are I/O-bound on the same clustered bytes; the 240 chunk round-trips are negligible overhead). Chunking is **not** faster. The real differentiator is **generality**: chunked-PK only covers single-integer-PK tables; every other table falls to the plain `count(*)` that fails at scale.

## Decision

For a large-table exact count on Vitess/PlanetScale:

1. **Use OLAP: `SET workload='olap'` then `SELECT COUNT(*)`, on a DEDICATED connection** (never session-wide — the v0.99.15 lesson; mirror the `row_reader.go` no-PK read that already does exactly this). OLAP works for **every PK shape** at the **same speed** as chunked-PK, which closes the errno-3024 gap for the composite/string/UUID/no-PK tables that `singleShotCount` currently fails on.
2. **Tables with a usable narrow secondary index keep plain `count(*)`** — the optimizer auto-narrows to the small index, stays well under the wall, and is far faster (4 s vs 244 s at 10 GB). No reason to force those onto a heavier path.
3. **Keep the existing `chunkedCount` code in place** for now. It works, it is the same speed as OLAP, and it is well-tested; ripping it out immediately trades a proven path for an unproven one. It stays as the single-integer-PK path (or an explicit fallback) during the transition. **Follow-up (roadmap): once OLAP-count is proven in the field, evaluate removing `chunkedCount` entirely** so there is one count path. This is the operator's deliberate "swap to OLAP as primary, retire chunked later" sequencing.
4. Fix the `singleShotCount` comment: the failure mode is errno-3024 max-statement-execution-time, not a row-read cap.

`estimate-first-then-exact` (consult `information_schema` cardinality before paying the full count) remains an orthogonal fast-path lever, most valuable exactly for these large PK-only tables; not decided here.

No Postgres change: PG `count(*)` on a wide table is unaffected because the payload TOASTs out-of-line, so the heap/index scanned for the count stays small.

## Consequences

- Closes a real gap: non-single-integer-PK tables can be `verify`-counted at scale instead of failing with errno 3024.
- One workload-mode caveat: the OLAP `SET` MUST be on a dedicated connection returned to the pool clean, never leaked into the session (v0.99.15).
- Two count paths coexist until the follow-up retires `chunkedCount`; that is intentional (keep the tested path as a safety net).
