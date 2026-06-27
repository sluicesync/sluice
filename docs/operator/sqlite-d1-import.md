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

### Big tables parallel-copy

A large binary `.db` table is split into PK-range / keyset chunks copied concurrently
into the target, the same within-table parallelism the other engines use. It is on by
default and tuned by the existing flags: `--bulk-parallelism` (number of concurrent
chunk readers/writers) and `--bulk-parallel-min-rows` (the table-size threshold below
which a table stays single-reader). Each chunk opens its own read-only connection to the
file. A `.sql` **dump** source (below) stays single-reader regardless — the dump is
materialized into a temporary database that would be wasteful to rebuild per chunk; if
you want a dump's big tables to parallel-copy, materialize it to a `.db` first
(`sqlite3 app.db < dump.sql`) and migrate that.

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
- **D1 with large integers**: neither the export nor the default query API is safe. Use the
  **`d1` query-API reader** (below) — it reads every value via `CAST(... AS TEXT)` +
  `typeof()`, so integers > 2^53 round-trip exactly. It is the *higher*-fidelity reader, not
  the lower-fidelity one the earlier draft assumed.

## Cloudflare D1, the lossless way: the live query-API reader (`--source-driver d1`)

The `d1` source engine reads a **live** D1 database over D1's HTTP query API and is the
**lossless** D1 import (ADR-0132 — BUILT). Unlike the export path, it reads each value via
`CAST(col AS TEXT)` + `typeof(col)`, so it recovers integers > 2^53 EXACTLY (the export and
default-JSON paths round them, as the table above shows), distinguishes INTEGER from REAL,
and decodes BLOBs from hex. Reads do not take D1 offline (only `export` does).

```
# the API token is read from the environment ONLY (never a flag, never logged)
export CLOUDFLARE_API_TOKEN=...        # required
export CLOUDFLARE_ACCOUNT_ID=...       # optional if the account is in the DSN

sluice migrate --source-driver d1 --source d1://<account_id>/<database_id> \
  --target-driver postgres --target '<pg-dsn>'
# or the short DSN form, account from CLOUDFLARE_ACCOUNT_ID:
#   --source d1://<database_id>
```

DSN forms: `d1://<account_id>/<database_id>` or `d1://<database_id>` (account from
`CLOUDFLARE_ACCOUNT_ID`). A missing token, account, or database id is refused loudly at
startup, before any request. The same `--sqlite-date-encoding` / `sqlite_date_encoding`
date/bool policy applies to the text values, and the same loud storage-class fidelity holds
(a value that can't be faithfully held in a column's resolved type is refused, naming the
row — never silently coerced). Large tables are read in primary-key keyset pages.

**Which path for D1?** Use the export → `migrate` path (above) for a D1 database without
integers > 2^53 and for offline imports — it is simple and exact for those. Use
`--source-driver d1` when the database has large integers (snowflake IDs, nanosecond
timestamps, large counters); it is the only path that reads them losslessly.

## Schema features: generated columns, CHECK constraints, partial/expression indexes

Generated columns, CHECK constraints, and partial/expression indexes ARE carried into the
target (ADR-0133): a generated column lands as a real GENERATED column that the target
re-derives, a CHECK is enforced on the target, and a partial index keeps its `WHERE`
predicate. The expression bodies are carried **verbatim** from SQLite, so sluice emits a
one-time WARN per table/index naming what it carried — verify those expressions on the
target. Portable constructs (`a + b`, `length(x)`, `x || y`, comparisons) work directly;
a **non-portable** SQLite-only construct (e.g. `strftime(...)`) is **rejected loudly** by
the target at `CREATE` time (naming the rejected function), never silently dropped or
mistranslated. A cross-dialect *translation* of the verbatim tail is a tracked follow-up;
until then, edit the source expression or re-add the object on the target if it is rejected.
(MySQL has no partial-index support, so a SQLite partial index lands as a full index on a
MySQL target — its predicate is not representable there.)

## Known limits (prototype scope)

Deferred follow-ups (ADR-0128 §, ADR-0133): trigger-based continuous CDC, a per-column
date-encoding map (the global flag + `--type-override` cover the common and outlier
cases), and a SQLite→canonical expression translator for the verbatim generated/CHECK/
index bodies described above. (Within-table parallel-copy chunking for the binary `.db`
path now ships — see "Big tables parallel-copy" above; a `.sql`-dump source stays
single-reader by design.) SQLite/D1 is a migrate **source** only — it is never a sluice
target.
