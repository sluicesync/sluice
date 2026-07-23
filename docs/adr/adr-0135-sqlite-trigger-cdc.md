# ADR-0135: SQLite trigger-based CDC source engine (`sqlite-trigger`)

## Status

**Accepted — shipped v0.99.148** (the `sqlite-trigger` engine, `internal/engines/sqlite-trigger`;
the index row carried the shipped status while this header still said Proposed — caught by the
G-17 status-parity gate, audit 2026-07-23 DOC-3). Roadmap item 49 follow-up (#5 of the SQLite
queue). Phase 1 scope: continuous logical CDC from a **local SQLite file** to Postgres/MySQL via
triggers + a change-log table + a polling reader, mirroring the `pgtrigger` engine
(ADR-0066). D1-over-HTTP landed as the `d1-trigger` engine (ADR-0136); schema-change
forwarding and capture-payload trimming remain explicit deferred follow-ups.

Prior status: Proposed (2026-06-27).

## Context

SQLite has no logical replication and no usable change stream: its WAL is a physical
page log (crash-recovery / reader-writer concurrency), not a decodable logical feed, and
Litestream ships raw pages (same-engine physical replication). The ONLY way to get
logical row changes out of SQLite is the trigger pattern — exactly what `pgtrigger`
already does for managed Postgres without replication slots (ADR-0066): per-table AFTER
INSERT/UPDATE/DELETE triggers write before/after images into a `sluice_change_log` table,
and a CDC reader polls that table on a cadence, emitting `ir.Change` events with a
monotonic-id watermark for exactly-once resume. This applies to plain SQLite AND to
Cloudflare D1 (D1 is SQLite and supports triggers), though D1's reader would poll over
the HTTP query API (deferred to a follow-up — see Phase 2).

The `sqlite` source engine (ADR-0128/0129) is migrate-only (`CDC = CDCNone`). This adds a
sibling `sqlite-trigger` engine that composes it (delegation, like `pgtrigger` composes
`postgres`) and adds the CDC surfaces, so `sluice sync start --source-driver sqlite-trigger
--source ./app.db` streams a local SQLite file's changes to a PG/MySQL target.

### The value-fidelity crux (the load-bearing decision)

The capture trigger must record each changed row's values **without losing storage-class
fidelity** — and SQLite's obvious tool, `json_object(...)`, does NOT: it serializes an
INTEGER as a JSON number (IEEE-754 double → silently rounds any integer > 2^53, the exact
defect the D1 reader hit, ADR-0132) and cannot represent a BLOB at all. A naive JSON
capture would silently corrupt big integers (snowflake IDs, ns timestamps) and blobs —
the cardinal sin.

**Decision: the capture trigger encodes each column as a `(typeof, text)` pair using the
SAME proven encoding as the D1 reader** — `typeof(col)` for the storage class and
`CASE typeof(col) WHEN 'blob' THEN hex(col) WHEN 'real' THEN format('%.17g', col) ELSE
CAST(col AS TEXT) END` for the value — built into a `json_object` of per-column
`{t, v}` entries (or two parallel json_objects). The CDC reader reconstructs the exact
`int64` / `float64` / text / `[]byte` from the `(typeof, text/hex)` pair and feeds it to
the SAME storage-class-faithful decode path (`decodeCell`) the `sqlite`/`d1` readers use,
inheriting their refuse-not-coerce loud-failure contract. Big integers are exact (text,
not JSON number), REAL is `%.17g` round-trip-exact, BLOBs come back from hex. This reuses
`d1_decode.go`'s reconstruction, keeping one faithful-capture implementation.

## Decision

1. **New engine `internal/engines/sqlite-trigger`**, registered as `sqlite-trigger`,
   composing `sqlite.Engine` by delegation for `OpenSchemaReader`/`OpenRowReader` (the
   cold-start snapshot reuses the validated `sqlite` reader, incl. within-table chunking
   and the ADR-0129 date/bool policy) and declaring `Capabilities.CDC = CDCTrigger` (or
   the existing trigger-CDC capability value pgtrigger uses). Write/target surfaces stay
   not-implemented (CDC source only).

2. **Source-side artifacts** (names mirror pgtrigger §2 for operator familiarity):
   `sluice_change_log` (`id INTEGER PRIMARY KEY AUTOINCREMENT` — the monotonic watermark —
   `op TEXT`, `tbl TEXT`, `before TEXT` JSON, `after TEXT` JSON, `captured_at`), a
   `sluice_change_log_meta` (schema-version pin), and per-table AFTER INSERT/UPDATE/DELETE
   triggers whose body builds the faithful `(typeof, text/hex)` before/after JSON (§crux)
   and inserts one change-log row. `sluice setup` / `sluice trigger` subcommands create +
   tear them down (mirroring the pgtrigger CLI); the change-log + meta + triggers are
   auto-skipped by the schema reader (like `_cf_*` / `sqlite_*`).

3. **CDC reader** polls `SELECT id, op, tbl, before, after FROM sluice_change_log WHERE
   id > ? ORDER BY id LIMIT batch` at a cadence (default 1s, batch 10k — pgtrigger
   defaults), reconstructs values via the D1 decode, emits `ir.Change`, and advances the
   watermark to the last `id`. One reader → one stream; the poll goroutine owns the pool.
   Exactly-once on resume is the durable watermark (last applied `id`), same contract as
   pgtrigger.

   **No safety-lag predicate — and the invariant it rests on.** Unlike pgtrigger (whose
   `bigserial` id is allocated at INSERT but NOT in commit order, forcing an `xmin`
   safety-lag + contiguous-committed-prefix anchor), SQLite's `id` is gap-free-correct under
   a plain `id > watermark` scan because **standard SQLite serializes writers**: only one
   write transaction is in flight at a time, so the `INTEGER PRIMARY KEY AUTOINCREMENT` id is
   allocated in *commit* order. **Caveat (load-bearing if the driver ever changes):** this
   holds for the standard modernc/`sqlite3` engine. A future swap to a *concurrent-writer*
   SQLite variant — `BEGIN CONCURRENT`, `wal2`, or the HC-tree branch — would break commit-
   order = id-order (two writers could commit out of id order, opening a silent-gap window),
   and the reader would then have to re-introduce a safety-lag / contiguous-prefix anchor
   exactly as pgtrigger does. Do not adopt such a driver for the CDC source without that
   change.

   **Schema-drift guard (Phase 1, no DDL triggers).** Because SQLite cannot capture DDL,
   `trigger setup` records each table's exact non-generated column set in a
   `sluice_change_log_columns` fingerprint table, and the CDC reader compares it to the live
   schema at stream START — refusing loudly on any difference in either direction. This
   closes the silent `ADD COLUMN` direction (a stale trigger captures the OLD set, so every
   captured column is still present and a per-row check would never fire, yet the new
   column's values would vanish): the operator must re-run `trigger setup` after a schema
   change.

4. **Snapshot → CDC handoff:** `OpenSnapshotStream` captures `MAX(id)` from
   `sluice_change_log` as the start watermark, runs the cold-start snapshot via the
   delegated `sqlite` reader, then streams changes with `id > watermark` — no gap, no
   double-apply (a row changed during the snapshot is re-applied idempotently on the PK).

5. **Concurrency / WAL:** the change-log is written by the app's own triggers (in the
   app's write transactions) and read by sluice on a separate read-only connection;
   enabling WAL mode (`PRAGMA journal_mode=WAL`) on the source is recommended so the
   poller doesn't block writers (documented, not forced — sluice does not silently change
   the operator's journal mode). The change-log grows unbounded; a `sluice trigger prune`
   / retention follow-up is noted (Phase 2).

## Consequences

- A local SQLite file gains continuous logical CDC to PG/MySQL, reusing the validated
  cold-start reader and the D1 faithful-decode — big integers and blobs in captured
  changes are exact, not silently rounded/dropped.
- Source-write overhead: every INSERT/UPDATE/DELETE fires a trigger that writes a
  change-log row (the standard trigger-CDC cost; capture-payload trimming to reduce it is
  a Phase 2 follow-up, as in pgtrigger ADR-0068).
- New concurrency surface (poll loop, watermark, exactly-once) → the `-race` integration
  gate MUST pass before any tag (push-first/tag-after; the project's concurrency-chunk
  rule).

## Deferred (Phase 2 / follow-ups)

- **D1 variant:** poll `sluice_change_log` over the D1 HTTP query API (the `d1` reader's
  transport), inheriting its fidelity (already solved) + polling latency. The trigger
  setup runs the same CREATE TRIGGER over the query API.
- **Schema-change forwarding:** SQLite has no DDL triggers, so a source `ALTER TABLE` is
  not auto-captured; Phase 1 documents this limitation (a schema change requires
  re-setup). pgtrigger's DDL-trigger forwarding has no SQLite equivalent.
- **Capture-payload modes** (full/changed/minimal, ADR-0068) and **change-log retention/
  pruning**.

## Alternatives considered

- **`json_object()` capture (the obvious path).** Rejected: silently rounds integers
  > 2^53 and cannot represent BLOBs — the cardinal silent-loss sin. The `(typeof, text/
  hex)` encoding is the faithful alternative, already proven by the D1 reader.
- **Per-column typed change-log columns.** Rejected: a generic change-log can't have
  columns matching every source table's schema.
- **WAL decoding.** Rejected: SQLite's WAL is physical pages with no logical decoder.
- **Litestream.** Rejected: physical same-engine replication, not cross-engine logical CDC.
