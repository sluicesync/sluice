# ADR-0132: Cloudflare D1 query-API source reader (lossless via CAST/typeof)

## Status

**Accepted (2026-06-27).** Roadmap item 49. Builds the native Cloudflare D1 source —
reading a **live** D1 database over its HTTP query API — as the **lossless** D1 import,
correcting ADR-0131's deferral (which assumed the query API was lossy and the export
faithful; the 2026-06-27 empirical probe found *both* default paths round integers > 2^53,
and that the query API with `CAST(col AS TEXT)` is the only exact path). This is the
`--source-driver d1` engine.

## Context

The empirical probe (ADR-0131 §Empirical update, evidence in
`docs/operator/sqlite-d1-import.md`) established, against a real D1 database:

- D1 stores int64 values **exactly** (confirmed via `typeof`/`CAST`).
- The **default query API** and **`wrangler d1 export`** BOTH serialize integers > 2^53
  through a JS/JSON path that rounds them (`9007199254740993` → `…992`; max int64 off by
  1,193). The export loss is server-side, before sluice runs — undetectable.
- The query API reading `CAST(col AS TEXT)` returns the **exact** integer as a JSON
  string, and `typeof(col)` recovers the storage class (the default JSON collapses
  `1.0`→`1`, losing INTEGER-vs-REAL; BLOBs return as byte-int arrays).

So the faithful way to read a D1 database is the query API with explicit per-value
exact-text extraction — not the export path, and not the default JSON. A bonus: reads do
**not** take D1 offline (only `export` does), so this reader is also operationally
gentler than the export path for a live database.

## Decision

1. **A new `d1` source engine, living in `internal/engines/sqlite`** (registered `d1`
   alongside `sqlite`), so it reuses the validated type-resolution (affinity →
   `resolveColumnType`, the ADR-0129 declared-date/bool policy) and the engine's value
   semantics. It is a migrate **source** only: `Capabilities.CDC = CDCNone`; the
   write/CDC/snapshot Open* return `ErrNotImplemented`.

2. **Transport: plain `net/http`** (no `cloudflare-go` SDK dependency — dep-light). POST
   `https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query`
   with `{"sql": ...}` and `Authorization: Bearer <token>`; parse the
   `{result:[{results:[…], meta}], success, errors}` envelope. A `success:false` /
   non-2xx / `errors[]` response is a loud error.

3. **DSN + secrets (env-first).** `--source d1://<account_id>/<database_id>` (or
   `d1://<database_id>` with the account from env). The **API token is env-only**
   (`CLOUDFLARE_API_TOKEN`, never a flag — the secrets posture); `CLOUDFLARE_ACCOUNT_ID`
   supplies the account if not in the DSN. A missing token/account is refused loudly at
   open.

4. **★ Lossless row read (the load-bearing fidelity decision).** A row SELECT must NEVER
   return a bare JSON number for a stored value, because that rounds > 2^53 integers and
   loses INTEGER-vs-REAL. Instead, for each user column `c`, the reader projects two
   expressions:
   - `typeof("c")` → the actual storage class (`integer`/`real`/`text`/`blob`/`null`),
   - `CASE typeof("c") WHEN 'blob' THEN hex("c") ELSE CAST("c" AS TEXT) END` → the value
     as **exact text** (integers + reals as their full decimal text; blobs as hex; NULL
     stays NULL).
   The decoder then maps `(typeof, text/hex)` → the IR value for the column's resolved IR
   type, applying the same loud-failure discipline as the file engine (a class that can't
   be faithfully held is refused, naming table/column + value). Big integers parse from
   the exact text to `int64`; INTEGER vs REAL is taken from `typeof`; BLOBs decode from
   hex; the ADR-0129 date/bool encoding policy applies to the text. This defeats the
   JS-52-bit ceiling entirely.

5. **Schema read over the API** runs the same catalog queries the file engine uses
   (`sqlite_master`, `PRAGMA table_info`/`foreign_key_list`/`index_list`/`index_info`)
   via the HTTP query, parses the JSON rows, and reuses `resolveColumnType` + the `_cf_*`
   / `sqlite_*` auto-skip. (PRAGMA scalar columns are small and safe as plain JSON; only
   the *data* read needs the CAST/typeof exactness.)

6. **Pagination by keyset.** Large tables are read in PK-keyset pages
   (`… WHERE pk > ? ORDER BY pk LIMIT N`, default N tuned under D1's response-size limit;
   `rowid` for an implicit-rowid table; `LIMIT/OFFSET` fallback only for a no-orderable-key
   table, with a documented caveat). This keeps each response within D1's limits and
   avoids loading a whole table into one response.

   **Amended 2026-09-02 (audit 2026-09-01 LA-1/LA-3, v0.138.0):** the `LIMIT/OFFSET` fallback is gone, not narrowed, and the rowid clause above was the defect. SQLite binds a *declared* column named `rowid`, `_rowid_` or `oid` in preference to the implicit rowid, so the reader's `SELECT rowid … LIMIT 1` probe succeeded on a PK-less table with a user column named `rowid` and the keyset paginated on that non-unique column, dropping every row that shared a page-boundary value — 2,500 source rows became 2,000 copied at exit 0 on real D1, on both the direct reader and `migrate --stage-local`. `resolveRowidName` (`internal/engines/sqlite/d1_rows.go`) now reads the declared columns from `PRAGMA table_xinfo` and keys the keyset on the first of the three names no column shadows, proven reachable by a probe; a table with neither a text-param-safe PK (a BLOB-affinity key cannot be keyset-bounded by a text param) nor an unshadowed rowid name is refused with `SLUICE-E-BULKCOPY-NO-PAGINATION-KEY` at `planPagination`, before any page is read. The former OFFSET fallback was reachable only through a failed rowid probe — exactly the path that routed the shadowed column into the keyset — so there is no third strategy any more. Every table read is also bracketed by a server-side `COUNT(*)` before and after: equal counts that differ from the delivered row count refuse with `SLUICE-E-BULKCOPY-ROW-COUNT-MISMATCH` (the reader lost or duplicated rows on a quiescent source); counts that differ from each other WARN with all three numbers (writes landed mid-read; the copy is not a point-in-time snapshot). That count is the independent expected value this lane never had, and it is what lets `sluice verify --depth count` accept a `d1` source.

7. **Date/bool encoding** reuses `--sqlite-date-encoding` / the engine's policy (ADR-0129)
   — the text from `CAST(col AS TEXT)` is exactly the ISO/unix/julian value to decode.

## Consequences

- `sluice migrate --source-driver d1 --source d1://<account>/<db>` imports a live D1
  database into Postgres or MySQL **losslessly**, including integers > 2^53 (snowflake
  IDs, ns timestamps) that the export path silently rounds — and without taking D1
  offline. The export path (`--source-driver sqlite --source dump.sql`) remains the
  simple default for D1 databases without large integers and for offline imports.
- It reuses the file engine's type/decode/date-bool logic, so the only genuinely new
  surface is the HTTP transport, the CAST/typeof projection, and keyset pagination.
- HTTP throughput, pagination, and D1 rate limits make it slower than a local file read
  for bulk; that is the accepted trade for exactness + liveness.

## Value-fidelity requirement

Pin the class on the HTTP-JSON decode path: each `typeof` (integer/real/text/blob/null) ×
the exact-text/hex representation → faithful IR value or loud refusal; specifically a
**> 2^53 integer round-trips EXACTLY** (the headline — `9007199254740993` and max int64
land exact on PG and MySQL), INTEGER vs REAL distinguished via `typeof`, BLOB exact from
hex, NULL→nil, and the date/bool policy over the text. Unit-test the decode + the SQL
projection against canned D1-JSON (`httptest`); a live-D1 validation (with a token) is the
end-to-end proof. A `value-fidelity-reviewer` pass is required before it lands.

## Alternatives considered

- **Automate `wrangler d1 export` / the export API.** Rejected as the faithful path: the
  empirical probe showed the export rounds > 2^53 integers server-side, so it cannot be
  the lossless reader (it stays the simple default for non-big-int / offline cases only).
- **Default query API (bare JSON).** Rejected: rounds big integers, loses INTEGER/REAL.
- **`quote(col)` instead of `CAST`/`typeof`.** `quote()` yields a SQL literal (exact) but
  needs literal-parsing to recover the value + type; `typeof` + `CAST`/`hex` gives the
  storage class and the value directly, which is simpler to decode faithfully.
- **`cloudflare-go` SDK.** Rejected: a heavy dependency for a few HTTP calls; `net/http`
  keeps the engine dep-light.
