# sluice v0.99.142

**The Cloudflare D1 import is now ONE command: `sluice migrate --source-driver sqlite --source dump.sql` ingests a `wrangler d1 export` `.sql` dump directly — no `sqlite3` CLI, no `_cf_KV` cleanup (ADR-0130). Plus a documented decision (ADR-0131): the export path is the recommended D1 import, and a native D1 HTTP-API reader is deferred (it would be lower-fidelity). Opt-in, additive; fully drop-in over v0.99.141.**

## Features

**Direct `.sql`-dump ingest for the SQLite source (ADR-0130).** `wrangler d1 export` emits a `.sql` text dump, not a binary file — so v0.99.141 needed a manual `sqlite3 app.db < dump.sql` materialize step. Now the `sqlite` source accepts a `.sql` dump **directly**: it sniffs the file's magic header (`"SQLite format 3\0"`), routes a real binary `.db` to the existing read path, and for anything else **materializes the dump in-process** into a temp SQLite database via the pure-Go `modernc.org/sqlite` (a single multi-statement `Exec`, with a quote/comment-aware statement-split fallback), reads that, and removes the temp file on close. A malformed dump fails loudly (naming the dump) before any data moves, with no temp-file leak. So a Cloudflare D1 import is two commands:

```
wrangler d1 export <db> --remote --output dump.sql
sluice migrate --source-driver sqlite --source dump.sql --target-driver postgres --target '<pg-dsn>'
```

**`_cf_*` internal tables auto-skipped.** D1's internal `_cf_*` tables (e.g. `_cf_KV`) are now excluded by the schema reader alongside SQLite's own `sqlite_*` tables — so no `--exclude-table _cf_KV` is needed. The exclusion uses an escaped `LIKE` so a user table merely containing "cf" is never silently dropped (`--exclude-table` remains available for anything else).

## Changed

**D1 ingestion decision recorded (ADR-0131): export path default, HTTP-API reader deferred.** A native D1 HTTP-API reader (querying a live D1 over Cloudflare's REST API instead of importing an export) was analyzed and deliberately **not built**: the D1 query API returns JSON, so it is structurally lower-fidelity — Cloudflare's own docs note results are subject to "JavaScript's 52-bit precision for numbers" (integers > 2^53 come back lossy), and JSON can't distinguish INTEGER vs REAL storage class — and it's slower (HTTP round-trips + pagination + rate limits) than reading a real SQLite file. Since the `.sql`-dump ingest above reduced the export path to a single `migrate` command, the HTTP reader's only advantage (skipping the export) is marginal. The export path is the recommended, validated D1 import; the HTTP reader stays a documented future option that, if built, must refuse out-of-precision values loudly. A new operator guide — **`docs/operator/sqlite-d1-import.md`** — has the import how-to + the full fidelity/perf comparison table.

## Compatibility

Purely additive and opt-in: a binary SQLite `.db` source is unchanged (same magic-header path), the new behavior only affects a non-`.db` (dump) `--source`, and no flag defaults move. It's a transport convenience — the materialized dump is read through the exact same validated affinity / date-bool / storage-class decode as a native SQLite file (ADR-0128/0129), so there's no value-path change. Fully drop-in over v0.99.141.

## Who needs this

Anyone importing a **Cloudflare D1** database: `wrangler d1 export` then a single `sluice migrate --source dump.sql` — no intermediate tooling. Plain SQLite-file users are unaffected. See `docs/operator/sqlite-d1-import.md` for the recipe and the export-vs-HTTP-API tradeoff.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.142 · **Container:** ghcr.io/sluicesync/sluice:0.99.142
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
