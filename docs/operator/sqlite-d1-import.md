# Importing SQLite & Cloudflare D1 into Postgres or MySQL

sluice imports a SQLite database — or a Cloudflare D1 database — into Postgres or
MySQL natively, through the standard `migrate` pipeline (parallel copy, cross-engine
type translation, deferred index/constraint creation, `--dry-run`, `verify`), with no
pgloader or other external tool. The source engine is read-only and migrate-only
(no continuous sync); see ADR-0128 (engine), ADR-0129 (date/bool policy), ADR-0130
(`.sql`-dump ingest).

## SQLite file

```
sluice migrate --source-driver sqlite --source ./app.db \
  --target-driver postgres --target 'postgres://user:pass@host:5432/db?sslmode=disable'
# or --target-driver mysql --target 'user:pass@tcp(host:3306)/db'
```

`--source` may be a bare path, a `file:` URI, or a `sqlite://` URL. The file is opened
read-only.

## Cloudflare D1 (the recommended path: export → migrate)

`wrangler d1 export` emits a **`.sql` text dump**, and sluice ingests a `.sql` dump
directly (it sniffs the file's magic header, materializes the dump in-process — no
`sqlite3` CLI — and auto-skips D1's internal `_cf_*` tables). So the import is two
commands:

```
wrangler d1 export <db> --remote --output dump.sql
sluice migrate --source-driver sqlite --source dump.sql \
  --target-driver postgres --target '<pg-dsn>'
```

This path was validated end-to-end against a real Cloudflare D1 database (DATE→date,
DATETIME→timestamp, BOOLEAN→boolean/`tinyint(1)`, exact row counts, NULLs preserved,
exactly-once, into both Postgres and MySQL).

### Dates, times, booleans

SQLite/D1 have no native DATE/TIME/BOOLEAN storage. sluice maps a column whose
**declared** type names one (`DATE`/`DATETIME`/`TIMESTAMP`/`TIME`, `BOOL`/`BOOLEAN`) to
the right target type, and you tell it how the **values** are encoded with
`--sqlite-date-encoding`:

| encoding | temporal values stored as | example |
|---|---|---|
| `iso` (default) | ISO-8601 **TEXT** | `'2024-01-02 03:04:05'` |
| `unixepoch` | INTEGER unix **seconds** | `1704164645` |
| `unixmillis` | INTEGER unix **milliseconds** | `1704164645000` |
| `julian` | REAL/INTEGER **Julian day** | `2460311.6...` |

A value whose storage class doesn't match the chosen encoding (or ISO text matching no
layout, or a non-0/1/non-truthy boolean) is **refused loudly** — never a silently-wrong
date. Carry an outlier column raw with `--type-override <col>=text`. Booleans accept
INTEGER `0`/`1` and the usual truthy/falsy text.

## D1 large-integer fidelity (IMPORTANT — empirically verified 2026-06-27)

> A regular SQLite **`.db` file** is read exactly (sluice reads the int64 from the file).
> The caveat below is **specific to Cloudflare D1**.

Both of D1's *default* extraction paths **silently lose integers larger than 2^53**
(≈ 9,007,199,254,740,992) — confirmed by probing a real D1 database:

| D1 path | `9007199254740993` (2^53+1) comes back as | exact? |
|---|---|---|
| `wrangler d1 export` `.sql` dump | `9007199254740992` | **NO** — D1's export generator rounds it server-side (the exact value is absent from the dump; sluice never sees it) |
| default query API (bare JSON) | `9007199254740992` | **NO** — serialized as an IEEE-754 double; `max int64` came back off by 1,193 |
| query API with **`CAST(col AS TEXT)`** | `"9007199254740993"` | **YES** — exact, as a JSON string; `typeof()` also recovers INTEGER vs REAL |

So — correcting an earlier assumption — the **export path is NOT a lossless escape**: it
loses the same large integers as the default query API (the loss is in D1, before sluice
runs, so sluice cannot detect or refuse it). The **only lossless way to read large
integers out of D1 is the query API reading INTEGER columns via `CAST(... AS TEXT)`**.
This matters in practice for snowflake-style IDs (e.g. Discord/Twitter 64-bit IDs),
nanosecond timestamps, and large counters — all routinely > 2^53.

Also note (query API): `INTEGER 1` and `REAL 1.0` both serialize to bare `1`
(indistinguishable without `typeof()`); a `BLOB` returns as a JSON byte-int array
(`[202,254,0,255]`); `NULL` is JSON `null`. The export dump keeps BLOBs exactly
(`X'cafe00ff'`) but, again, rounds big integers.

**Practical guidance:**
- **D1 without integers > 2^53** (the common case): the export → `migrate` path is exact
  and simple — use it.
- **D1 with large integers**: neither the export nor the default query API is safe. The
  lossless path is a **D1 query-API reader that reads INTEGER columns via
  `CAST(... AS TEXT)`** (planned — ADR-0131); it is the *higher*-fidelity reader, not the
  lower-fidelity one the earlier draft assumed.

## Known limits (prototype scope)

Deferred follow-ups (ADR-0128 §): a native D1 HTTP-API reader, trigger-based continuous
CDC, within-table chunking (large single tables read as one stream), a per-column
date-encoding map (the global flag + `--type-override` cover the common and outlier
cases), and reading CHECK constraints / generated columns / expression-or-partial
indexes. SQLite is a migrate **source** only — it is never a sluice target.
