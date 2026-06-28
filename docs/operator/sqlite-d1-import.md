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
(`sqlite3 app.db < dump.sql`) and migrate that. A table whose PRIMARY KEY is a temporal
(DATE/DATETIME/TIME) or decimal column also stays single-reader (such a value can't drive
SQLite's range cursor faithfully); it still migrates correctly, just without within-table
parallelism. Integer / text / blob / composite keys parallel-copy as normal.

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

## Continuous sync: the `sqlite-trigger` CDC source (ADR-0135)

Beyond the one-shot `migrate`, a **local SQLite file** can be a continuous-sync source —
streaming every INSERT/UPDATE/DELETE to a Postgres or MySQL target — via the
`sqlite-trigger` engine. SQLite has no logical replication or decodable change stream, so
sluice uses the proven trigger pattern (the same model as the `postgres-trigger` engine):
per-table capture triggers write each change into a `sluice_change_log` table, and a
polling reader streams them.

**1. Install the capture triggers (once):**

```
sluice trigger setup --source-driver sqlite-trigger --dsn ./app.db --tables=users,orders
```

This creates `sluice_change_log` + `sluice_change_log_meta` and three AFTER
INSERT/UPDATE/DELETE triggers per table. Each listed table must have a PRIMARY KEY (the
CDC applier identifies rows by PK); a PK-less table is refused loudly. Re-running setup is
idempotent. Use `--dry-run` to print the DDL without applying it.

**2. Start the sync (cold-start snapshot, then continuous CDC):**

```
sluice sync start --source-driver sqlite-trigger --source ./app.db \
  --target-driver postgres --target 'postgres://user:pass@host:5432/db?sslmode=disable'
```

The cold-start bulk-copies existing rows (reusing the validated `sqlite` reader, including
its date/bool policy and big-table chunking), then hands off gap-free to the change-log
poller. Resume is exactly-once from a durable per-row watermark; a hard stop + restart
re-streams only the un-applied tail. Big integers (> 2^53) and BLOB columns round-trip
**exactly** through capture and CDC — the trigger encodes each column as a `(typeof,
text/hex)` pair (the same faithful encoding as the `--source-driver d1` reader), never a
lossy JSON number.

**3. Remove the triggers when done:**

```
sluice trigger teardown --source-driver sqlite-trigger --dsn ./app.db --yes
# add --keep-data to retain sluice_change_log for inspection
```

**WAL is recommended.** Enable `PRAGMA journal_mode=WAL` on the source so the poller's
reads never block the application's writes (sluice does not change your journal mode for
you). **Source-write overhead:** every captured INSERT/UPDATE/DELETE fires a trigger that
writes one change-log row (the standard trigger-CDC cost). **Schema changes need
re-setup:** SQLite has no DDL triggers, so a source `ALTER TABLE` is not auto-captured —
run `trigger teardown` then `trigger setup` again after a schema change. To keep this from
silently dropping data (an `ADD COLUMN` whose values a stale trigger would never capture),
`trigger setup` records each table's captured column set, and **`sluice sync start` refuses
loudly at stream start** if any replicated table's live columns differ from what the
triggers were built against (in either direction — add, drop, or rename — or a dropped
table), naming the table and pointing you to re-run `trigger setup`. Until you re-setup
after a schema change, the stream will not start (it never silently mis-captures). The
change-log, meta, and column-fingerprint tables are auto-skipped by the schema reader, so
they are never themselves migrated or captured.

## Continuous sync from a LIVE Cloudflare D1: the `d1-trigger` CDC source (ADR-0136)

The `sqlite-trigger` design above runs unchanged against a **live Cloudflare D1 database**
over D1's HTTP query API — the `d1-trigger` engine (ADR-0136). It is the same trigger +
change-log + polling design (the setup DDL, the trigger bodies, the change-log/meta/
fingerprint schema, the poll, the watermark, the `MAX(id)` snapshot anchor, and the
schema-drift refusal are all identical); only the transport differs (the D1 `/query` API
instead of a local connection). So a live D1 can stream continuous logical CDC to Postgres
or MySQL.

**Setup over the API.** Credentials are resolved exactly as for `--source-driver d1`: the
DSN is `d1://<account_id>/<database_id>` (or `d1://<database_id>` with
`CLOUDFLARE_ACCOUNT_ID`), and the API token is read from `CLOUDFLARE_API_TOKEN` ONLY (never
a flag, never logged). Installing the triggers MODIFIES your D1 database, so use a real/test
D1 you control:

```sh
export CLOUDFLARE_API_TOKEN=...   # a D1:Edit token (CREATE TABLE/TRIGGER over /query)
sluice trigger setup --source-driver d1-trigger \
  --dsn d1://<account_id>/<database_id> --tables=users,orders

sluice sync start --source-driver d1-trigger \
  --source d1://<account_id>/<database_id> \
  --target-driver postgres --target 'postgres://user:pass@host:5432/db?sslmode=require'

# remove every artifact when done (drops the triggers + change-log; --keep-data to retain):
sluice trigger teardown --source-driver d1-trigger \
  --dsn d1://<account_id>/<database_id> --yes
```

The cold-start snapshot reuses the lossless `d1` query-API reader (the CAST/typeof
projection, so integers > 2^53 survive), and the CDC tail polls the change-log over the same
transport — big integers and blobs round-trip **exact**, identical to the local engine.

**Primary-consistency (load-bearing).** The poll uses D1's DEFAULT primary
(strongly-consistent) query path, NOT D1's Sessions / read-replica routing. The exactly-once
`id > watermark` guarantee rests on commit-order = id-order, which holds at the
write-serialised primary but can wobble against a lagging replica; a replica-aware mode would
have to re-introduce a safety-lag (ADR-0136 §4).

**Caveats specific to D1.** (1) **Write amplification + billing:** every D1 write fires a
trigger that writes a change-log row — on D1 that is billable rows-written and storage, and
the change-log grows unbounded until the change-log-retention follow-up. (2) **Polling
latency + API rate limits:** CDC latency is the poll interval (default 1s) + HTTP round-trip;
keep the cadence within D1's request-rate limits. (3) Schema changes need a re-setup, with the
same loud schema-drift refusal as the local engine (above). The token needs D1:Edit (the
setup runs `CREATE TABLE`/`CREATE TRIGGER` over `/query`).

## Known limits (prototype scope)

Deferred follow-ups: trigger-CDC **Phase 3** (schema-change forwarding, capture-payload
trimming, change-log retention/pruning — more urgent on D1 because writes/storage are
billable — and a replica-aware poll mode; ADR-0135 / ADR-0136), a per-column date-encoding
map (the global flag + `--type-override` cover the common and outlier cases — ADR-0129), and
a SQLite→canonical expression translator for the verbatim generated/CHECK/index bodies
described above (ADR-0133).
(Within-table parallel-copy chunking for the binary `.db` path now ships — see "Big tables
parallel-copy" above; a `.sql`-dump source stays single-reader by design. Continuous CDC
from a local SQLite file now ships — see "Continuous sync" above — as does continuous CDC
from a live Cloudflare D1 database over the HTTP query API — see "Continuous sync from a
LIVE Cloudflare D1" above.)
