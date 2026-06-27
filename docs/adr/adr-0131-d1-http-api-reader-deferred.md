# ADR-0131: D1 ingestion paths — export-file default; HTTP-API reader deferred

## Status

**Superseded in part by an empirical finding (2026-06-27) — see the update below.** The
original decision (2026-06-26) deferred a native D1 HTTP-API reader on the premise that
the **export-file path is faithful while the query API is lossy**. A real-D1 probe
disproved the premise: the export path loses large integers too, and the query API can be
made the *higher*-fidelity path. The deferral of a *default-JSON* query reader stands, but
the reason has changed and a `CAST(... AS TEXT)` query reader is now the recommended way
to read large integers from D1.

### ★ Empirical update (2026-06-27) — premise disproven; conclusion revised

A live-D1 probe (insert `9007199254740993` = 2^53+1 and `9223372036854775807` = max
int64, then read back) found:

- **The default query API rounds them** (`…992`; max int64 off by 1,193) — bare JSON
  doubles, INTEGER/REAL indistinguishable (`1.0`→`1`), BLOB as a byte-int array. As
  documented. **But:**
- **`wrangler d1 export` ALSO rounds them** — D1's server-side export generator serializes
  through the same JS/JSON path, so the `.sql` dump contains `…992`, the exact value
  absent. So the export path is **NOT** the lossless escape this ADR assumed; it is
  *equally* lossy on > 2^53 integers, and the loss happens in D1 before sluice runs (so
  sluice cannot detect or refuse it). The values ARE stored exactly in D1 (confirmed via
  `CAST/typeof`); only these two serializers round them.
- **The query API with `CAST(col AS TEXT)` is lossless** — it returns the exact integer as
  a JSON string, and `typeof()` recovers INTEGER-vs-REAL storage class.

**Revised conclusion:** for D1 with large integers (snowflake IDs, ns timestamps, big
counters — routinely > 2^53), a **D1 query-API reader that reads INTEGER columns via
`CAST(... AS TEXT)` is the *highest*-fidelity import** — better than both the export dump
and the default query JSON. The export path remains exact for non-D1 SQLite `.db` files
and for D1 databases without > 2^53 integers (the common case), so it stays the simple
default there. The item to build is therefore the **CAST-based query-API reader** (not the
export-API automation that earlier looked best — that path is also lossy on big ints).
Evidence + the corrected comparison table live in `docs/operator/sqlite-d1-import.md`.

---

**Original decision (2026-06-26, premise now corrected above).** Roadmap item 49. Recorded
making the export-file path the recommended D1 import and deferring a native D1 HTTP-API
reader, with the (since-disproven) fidelity/perf analysis behind it. Operator-facing
guidance lives in `docs/operator/sqlite-d1-import.md`.

## Context

After the SQLite source engine (ADR-0128/0129) and the direct `.sql`-dump ingest
(ADR-0130) shipped, the open question was whether to also build a **native D1 HTTP-API
reader** — a source that queries a *live* D1 database over Cloudflare's REST API
(`POST /accounts/{id}/d1/database/{id}/query`, Bearer token) instead of importing an
export, implementing the schema/row readers over paginated HTTP-JSON. The motivation was
ergonomic: pull from live D1 in one command, no export step.

Research into the D1 API changed the calculus:

- **Integer fidelity ceiling.** The D1 query API returns results as **JSON**, and
  Cloudflare's own import/export documentation warns results are "affected by
  JavaScript's 52-bit precision for numbers." So an integer larger than 2^53 comes back
  **lossy** over the API. The export-file path reads a real SQLite file and is exact.
- **Storage-class ambiguity.** sluice's SQLite value model is *per-row storage-class
  fidelity* (INTEGER/REAL/TEXT/BLOB decoded distinctly, mismatches refused loudly).
  JSON cannot distinguish INTEGER from REAL (both are JSON numbers) and encodes BLOBs
  specially — so the HTTP path is structurally muddier than reading the `.db`.
- **Throughput + limits.** HTTP round-trips, pagination, and D1 rate limits versus a
  local file read; the export path wins bulk throughput.
- **The convenience gap shrank.** ADR-0130's `.sql`-dump ingest reduced the export path
  to a single `migrate --source dump.sql` command (no `sqlite3` CLI, no `_cf_KV`
  cleanup), so the HTTP-API reader's one real advantage — skipping the export — is now
  marginal.

## Decision

1. **The export-file path is the recommended, supported way to import D1:**
   `wrangler d1 export --remote --output dump.sql` then `sluice migrate --source-driver
   sqlite --source dump.sql`. It is exact (real SQLite storage classes), fast (local
   read), and validated end-to-end against a real D1 database.

2. **Defer the native D1 HTTP-API reader.** It would be strictly lower-fidelity (the
   JS-52-bit integer ceiling, JSON storage-class ambiguity) and slower, for an advantage
   the `.sql`-dump ingest already largely removed. Building a secondary, lower-fidelity
   path is not worth it absent concrete operator demand for live-D1 pull.

3. **If it is built later, it must preserve the loud-failure tenet:** an integer (or any
   value) that the JSON transport cannot carry faithfully — e.g. magnitude > 2^53 — must
   be **refused loudly**, never silently truncated to a JS-safe approximation. It would
   ship as an explicitly-documented lower-fidelity convenience for live/small databases,
   not as a replacement for the export path.

## Consequences

- One clear, faithful, documented D1 import story (the export path), with the tradeoff
  captured so the deferral is an informed choice, not an omission.
- No JSON-transport value-fidelity surface enters the codebase now (the per-row
  storage-class guarantee stays anchored to reading real SQLite files).
- The decision is revisitable: a real "must read live D1 without an export" need would
  reopen item 2 under the constraint of item 3.

## Alternatives considered

- **Build the HTTP-API reader now** (the original "compare the two" plan). Rejected for
  now: the comparison's outcome is structurally predetermined (export wins fidelity +
  speed; HTTP wins only "no export step," which ADR-0130 minimized), so the build cost
  isn't justified pre-demand. The analysis is captured here + in the operator guide
  instead of as a second code path.
- **Build it as the *primary* D1 path.** Rejected outright: it would make the default D1
  import lossy on large integers — a value-fidelity regression versus the export path.
