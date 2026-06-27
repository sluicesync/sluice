# sluice v0.99.141

**New: a SQLite / Cloudflare D1 migrate source — `sluice migrate --source-driver sqlite --source app.db` imports a SQLite (or D1-exported) database into Postgres OR MySQL natively, no pgloader (ADR-0128/ADR-0129, roadmap item 49). Declared `DATE`/`DATETIME`/`BOOLEAN` columns map to real target temporal/boolean types, with an explicit value-encoding policy that refuses loudly rather than guess a wrong date. Opt-in, additive; fully drop-in over v0.99.140.**

## Features

**SQLite / Cloudflare D1 migrate source engine (ADR-0128).** A new read-only `sqlite` source engine imports a SQLite database file — and, by extension, a Cloudflare D1 database — into Postgres OR MySQL through the existing `sluice migrate` pipeline, reusing parallel cold-copy, cross-engine type translation, deferred index/constraint creation, `--dry-run`, and `verify` for free, with **no pgloader dependency**. It uses the pure-Go `modernc.org/sqlite` driver (no CGO — sluice's `CGO_ENABLED=0` posture is preserved) and declares `Capabilities.CDC = CDCNone` (migrate source only; SQLite cannot be a sluice target or a CDC source). Invoke with:

```
sluice migrate --source-driver sqlite --source ./app.db --target-driver postgres --target <pg-dsn>
# or --target-driver mysql
```

**Cloudflare D1 note:** `wrangler d1 export` emits a **`.sql` TEXT dump**, not a binary SQLite file, so the D1 flow is: export to `dump.sql`, materialize a file with `sqlite3 app.db < dump.sql` (exclude D1's internal `_cf_KV` table, or `--exclude-table _cf_KV`), then point sluice at `app.db`. Accepting a `.sql` dump directly (materialized in-process) is a deferred ergonomic follow-up.

**Value fidelity — refuse, never coerce (the load-bearing piece).** SQLite is dynamically typed: a column's declared-type *affinity* (INTEGER / TEXT / BLOB / REAL / NUMERIC) maps to an IR type, but each row's value carries its own storage class independent of that affinity. The row reader decodes every cell by its **actual** storage class and **refuses loudly** — naming the table, column, rowid, and offending storage class — when a value cannot be faithfully represented in the column's resolved IR type, rather than silently coercing it to a wrong-but-plausible value. Pinned by the full affinity × storage-class matrix (incl. the real-driver behavior) and proven by a cross-engine round-trip into both Postgres and MySQL; an independent value-fidelity review confirmed no silent-loss path.

**Declared date/time/boolean interpretation policy (ADR-0129) — what makes it usable on real databases.** SQLite has no native DATE/TIME/BOOLEAN storage (apps store dates as ISO TEXT, unix INTEGER, or Julian REAL; booleans as 0/1), which under raw affinity made a column declared `DATE`/`DATETIME`/`BOOLEAN` map to NUMERIC→Decimal and a typical ISO-text date hard-fail. The `sqlite` source now resolves a column whose **declared** type names a temporal/bool shape to the right IR type (case-insensitive precedence: `DATETIME`/`TIMESTAMP` → timestamp [tz-naive], else `DATE` → date, else `TIME` → time, `BOOL`/`BOOLEAN` → boolean — an `INTEGER` 0/1 column is **not** guessed as a bool). The **value encoding is an explicit operator choice** via the new `--sqlite-date-encoding` flag (or the per-source `sqlite_date_encoding` DSN param, which wins): `iso` (default; ISO-8601 TEXT), `unixepoch` / `unixmillis` (INTEGER/REAL unix seconds/milliseconds), or `julian` (REAL/INTEGER Julian day). A value whose storage class doesn't match the chosen encoding — or ISO text matching no layout, or a non-0/1/non-truthy boolean — is **refused loudly** (naming table/column/rowid + the value), never a silently-wrong date; carry an outlier raw with `--type-override <col>=text`. So real SQLite / Cloudflare D1 databases (ISO-text dates, 0/1 bools) migrate cleanly into Postgres/MySQL with proper date/timestamp/boolean target columns instead of numeric.

## Compatibility

Purely additive and opt-in: a new source engine + a new `--sqlite-date-encoding` flag (default `iso`); nothing else changes, and no existing flag default moves. It is a value-path addition shipped under the full Bug-74 family matrix (affinity × storage-class, and the temporal family × encoding × storage-class + the boolean matrix, on both the SQLite reader boundary and the cross-engine round-trip into Postgres and MySQL) plus an independent value-fidelity review. Fully drop-in over v0.99.140.

## Who needs this

Anyone wanting to import a **SQLite** database or a **Cloudflare D1** export into Postgres or MySQL — natively, with sluice's parallel copy, type translation, `--dry-run`, and `verify`, and no external tool. Everyone else is unaffected (the engine is inert unless `--source-driver sqlite` is used). Known prototype limits (documented, deferred): a native D1 HTTP-API reader, trigger-based continuous CDC, within-table chunking, and direct `.sql`-dump ingestion.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.141 · **Container:** ghcr.io/sluicesync/sluice:0.99.141
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
