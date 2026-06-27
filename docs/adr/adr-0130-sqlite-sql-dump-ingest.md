# ADR-0130: SQLite source — direct `.sql` dump ingest + `_cf_*` auto-skip

## Status

**Accepted (2026-06-26).** Roadmap item 49 follow-up; extends the SQLite source
engine (ADR-0128/0129). Surfaced by the real-Cloudflare-D1 validation: `wrangler d1
export` emits a `.sql` TEXT dump, but the engine reads a SQLite *file*, so the D1 path
required a manual `sqlite3 app.db < dump.sql` materialize step (and no `sqlite3` CLI is
guaranteed present). This makes the `sqlite` source accept a `.sql` dump **directly**,
materializing it in-process, so the D1 import is a single command.

## Context

The real-D1 end-to-end validation (v0.99.141) confirmed the engine works against
genuine D1 output, but the flow was: `wrangler d1 export --remote --output dump.sql` →
`sqlite3 app.db < dump.sql` → `sluice migrate --source-driver sqlite --source app.db`.
The middle step needs the `sqlite3` CLI (absent on the validation machine; the run used
a Go+modernc shim instead) and an operator who knows to strip D1's internal `_cf_KV`
table. Both are friction sluice can remove: `modernc.org/sqlite` can execute a
multi-statement dump in one `Exec` (the validation proved a single `db.Exec(dump)`
loads a real D1 export), and the schema reader already filters `sqlite_%` internal
tables, so adding `_cf_*` is one clause.

Fidelity note: materializing a dump reproduces the original storage classes —
re-inserting the dump's values under the same `CREATE TABLE` applies the same SQLite
affinity coercion the source applied — so the validated affinity / date-bool /
storage-class decode (ADR-0128/0129) is unchanged. This is a *transport* convenience,
not a value-path change.

## Decision

1. **Auto-detect input format by the SQLite file magic header.** On open, sniff the
   first 16 bytes of `--source`: if they equal `"SQLite format 3\000"` it is a binary
   SQLite database, opened read-only exactly as today. Otherwise the input is treated
   as a **SQL text dump** and materialized (below). Magic-header sniffing is reliable
   and independent of file extension (a `.sql` from `d1 export`, a `.dump`, or a
   `sqlite3 .dump` all work; a binary `.db` named `.sql` still opens correctly).

2. **Materialize a dump in-process into a temp SQLite database.** Create a temp file
   under `os.TempDir()`, open it via the pure-Go `modernc.org/sqlite`, and `Exec` the
   dump (modernc runs a multi-statement script in one call; if a single `Exec` fails,
   fall back to statement-split). The engine then reads that temp database read-only via
   the existing path. No external `sqlite3` CLI is required.

3. **Temp-file lifecycle is owned by the reader.** The materialized temp database path
   is tracked on the `SchemaReader` / `RowReader` and **removed on `Close()`** (and on
   any materialize error before the reader is returned). A migrate run opens a schema
   reader and a row reader, so the temp DB is materialized per-open; for a single-pass
   `migrate` that is fine (a future optimization could cache one temp DB per run if
   profiling shows the double-materialize matters — noted, not built).

4. **Auto-skip `_cf_*` internal tables** in the schema reader's table list, alongside
   the existing `sqlite_%` exclusion — so an operator needn't know `--exclude-table
   _cf_KV`. (Current `wrangler d1 export` already omits `_cf_KV`, but older/other
   exports include it; the auto-skip is the friendly default and a harmless no-op when
   absent.) `--exclude-table` remains available for anything else.

5. **A malformed dump fails loudly** at materialize time (the `Exec` error names the
   failure), before any data moves — the loud-failure posture.

## Consequences

- The Cloudflare D1 import becomes one command: `wrangler d1 export --remote --output
  dump.sql` then `sluice migrate --source-driver sqlite --source dump.sql --target-driver
  postgres --target <dsn>` — no `sqlite3` CLI, no `_cf_KV` cleanup. A plain SQLite `.db`
  is unchanged.
- No value-path change: the decode runs on a materialized SQLite database identical in
  format to a native one, reusing the ADR-0128/0129 logic (and its pins). The added
  surface is the sniff + materialize + temp-file lifecycle.
- A dump is materialized to disk (temp), so a very large dump needs temp space and a
  load pass; the export-file path's throughput is bounded by that load + the local read
  (still fast). This is one of the two D1 paths the ADR-0131 HTTP-API reader will be
  benchmarked against.

## Alternatives considered

- **Require the operator to materialize with `sqlite3`** (status quo). Rejected: needs a
  CLI that isn't always present and an undocumented `_cf_KV` cleanup; the in-process
  materialize is strictly friendlier and reuses a dependency already vendored.
- **A separate `--source-driver sqlite-dump`.** Rejected: the magic-header sniff makes
  one `sqlite` driver handle both forms transparently — fewer concepts, no wrong-driver
  errors.
- **Stream the dump without materializing** (parse SQL → IR directly). Rejected: a SQL
  parser is a large, fragile surface; materializing into SQLite reuses the real engine's
  exact affinity/storage-class semantics (the fidelity guarantee) for free.
