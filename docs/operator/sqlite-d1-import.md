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

## Why not read live D1 over its HTTP API?

A native D1 HTTP-API reader (query the live database over Cloudflare's REST API instead
of exporting) was considered and **deliberately deferred** (ADR-0131). The export path
is both more faithful and faster:

| | export → `migrate` (recommended) | live HTTP-API read (deferred) |
|---|---|---|
| **Integer fidelity** | exact (real SQLite file) | **lossy > 2^53** — the API returns JSON, subject to JavaScript's 52-bit number precision (Cloudflare's documented caveat) |
| **Storage-class fidelity** | exact INTEGER/REAL/TEXT/BLOB | ambiguous — JSON can't distinguish INTEGER vs REAL; BLOBs encode specially |
| **Throughput** | local file read after one export | HTTP round-trips + pagination + rate limits |
| **Steps** | two commands (export, migrate) | one command, but needs an API token |
| **Auth** | wrangler login once | a Cloudflare API token per run |

Since the `.sql`-dump ingest reduced the export path to a single `migrate` command, the
HTTP-API reader's only real advantage (skipping the export) is small, while its fidelity
ceiling is real. The recommendation is the export path; the HTTP-API reader remains a
documented future option for operators who specifically need to pull from a live D1
without an export, and would ship with a **loud refusal on out-of-precision integers**
(never a silent loss) if demand surfaces.

## Known limits (prototype scope)

Deferred follow-ups (ADR-0128 §): a native D1 HTTP-API reader, trigger-based continuous
CDC, within-table chunking (large single tables read as one stream), a per-column
date-encoding map (the global flag + `--type-override` cover the common and outlier
cases), and reading CHECK constraints / generated columns / expression-or-partial
indexes. SQLite is a migrate **source** only — it is never a sluice target.
