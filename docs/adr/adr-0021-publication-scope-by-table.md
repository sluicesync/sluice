# ADR-0021: Postgres publication scope by table list

## Status

Accepted. Implemented in `internal/engines/postgres/cdc_reader.go::ensurePublication` (creates / migrates the publication's table set), `internal/engines/postgres/engine.go::EnsurePublication` (the engine-side entry point), `internal/pipeline/streamer.go::coldStart` (calls `EnsurePublication` after schema-read + filter), and `internal/engines/postgres/change_applier.go::dispatch` (defence-in-depth skip-with-warning for unknown tables).

## Context

Pre-v0.5.0, sluice's Postgres CDC reader created the source-side publication with `CREATE PUBLICATION sluice_pub FOR ALL TABLES`. The shape was simple and idempotent: the publication captures every base table in the database; new tables on the source automatically join the WAL stream.

The "automatically join" part is the bug. When a developer runs `CREATE TABLE foo` on a source database that has an active sluice stream:

1. The publication picks up `foo` immediately (FOR ALL TABLES has no opt-in).
2. The first `INSERT INTO foo` produces a CDC event for an unknown-on-target table.
3. The applier's `dispatch` calls `colTypesFor("public", "foo")`, which queries `information_schema.columns` against the *destination* and gets zero rows.
4. Pre-v0.5.0, the applier errored loudly: `postgres: applier: table public.foo has no columns (does it exist?)` and exited.

This was Bug 13 in the v0.4.0 soak. Recovery required either creating the same table on dst by hand, or pruning the publication's table list, or restarting sluice with the new table somehow accommodated. None of those are operationally tolerable for "a developer ran a routine DDL."

## Decision

Two changes, layered:

1. **Primary fix: scope the publication to the source-side table list** the streamer reads at cold-start time. `CREATE PUBLICATION sluice_pub FOR TABLE schema.t1, schema.t2, …` instead of `FOR ALL TABLES`. New tables on the source stay out of the WAL stream until the operator explicitly re-runs sluice with the schema-rescan path (a future `sync rescan` or a fresh cold-start). The decision to include a new table on the source must be deliberate; auto-inclusion has no audit trail and no path to handle the "table exists on source but not on target" gap.

2. **Defence in depth: skip-with-warning on unknown table.** The applier's `dispatch` now recognises the "table has no columns" error path (returned by `colTypesFor` as `errUnknownTable`) and skips the event with a single WARN line. This handles the drift case where someone manually altered the publication, or where a v0.4.0-era publication still has FOR ALL TABLES until the next sluice run rescopes it. The stream stays alive instead of crashing.

The Postgres `Engine` exposes `EnsurePublication(ctx, dsn, tables)` — engine-side because schema is an engine concept (PG namespaces vs. MySQL databases). The streamer's `coldStart` calls it after `applyTableFilter` so the filter-excluded tables don't end up in the publication scope. MySQL doesn't implement the interface; the streamer's structural-interface check skips the call gracefully.

### Migration path from `FOR ALL TABLES`

`ensurePublication` checks `pg_publication.puballtables`. When a v0.4.0 publication exists with `puballtables = true` and the streamer is now passing a scoped list, the helper drops and recreates the publication. `ALTER PUBLICATION ... SET TABLE` cannot demote `FOR ALL TABLES` directly — Postgres rejects with "publication ... is defined as FOR ALL TABLES." Drop-and-recreate is safe because the publication is metadata; replication slots reference WAL by LSN, not by publication binding, so the in-flight slot keeps streaming the same WAL records. The brief window between the DROP and CREATE is on the order of milliseconds.

For an existing scoped publication where the table set differs from what the streamer wants (e.g., the operator added a `--exclude-table` flag on a re-run), `ALTER PUBLICATION ... SET TABLE` replaces the set atomically.

## Consequences

- **Mid-stream `CREATE TABLE` no longer crashes the applier.** The new table simply doesn't appear in the WAL stream. Operators who want the new table replicated re-run sluice with a schema rescan (e.g., a `sluice migrate` in resume mode that picks up the new table, then continue the existing sync).

- **Defence-in-depth absorbs publication drift.** Even if the publication's table set somehow includes a table that doesn't exist on dst (a manual `ALTER PUBLICATION ... ADD TABLE`, or a stale v0.4.0 setup that hasn't been rescoped yet), the applier logs a WARN and continues. The stream stays alive. Operators see the WARN and can fix the publication or migrate the table.

- **Schema rescans on warm-resume are NOT a new requirement.** Warm resume continues to use the publication scope established at cold start; we don't re-read the schema or re-call `EnsurePublication`. The defence-in-depth path covers the rare case where a publication needs rescoping mid-stream (operator-driven via `ALTER PUBLICATION` outside of sluice).

- **PG-only.** MySQL has no analogous concept (binlog doesn't filter by table at the source); MySQL's CDC reader filters at the per-event dispatch level via the `tableMap` schema check. The streamer's `publicationEnsurer` interface probe is opt-in; engines without publications simply omit the method.

## Why not auto-translate-and-create

A "detect the new table, run schema-translate, CREATE TABLE on dst, then apply" approach was considered. It mirrors what vanilla logical replication subscriptions do (CREATE SUBSCRIPTION optionally creates target schemas).

Reasons we don't:

- **DDL during runtime is risky.** The cross-engine schema translator (ADR-0016) is a static-time pass; running it mid-stream against a single new table would land a target table without indexes, FKs, or check constraints — silently wrong if the new table joins existing FKs.
- **Schema drift is an operator-visible event.** sluice's "Contain Postgres complexity" tenet says explicit operator action is preferred over silent auto-handling for anything that touches schema topology.
- **Bulk-copy of the new table can't run from inside the CDC stream.** A new table needs a fresh snapshot to seed dst before the CDC events for it would land cleanly. That requires a different orchestration shape (mid-stream snapshot capture) which we'd want to design carefully — not bolt onto a hot fix.

The right path for new-table-on-source is a deliberate `sluice migrate --add-table foo` invocation that handles snapshot + bulk-copy + insertion-into-publication-scope as one operation. That's a future feature, not a v0.5.0 fix.

## Verification

The integration test lives in `internal/pipeline/streamer_bug13_integration_test.go::TestStreamer_PostgresToPostgres_NewTableOnSourceIgnored`. The shape:

1. Cold-start with two tables (customers, products).
2. Verify CDC works on the original tables.
3. `CREATE TABLE ignored_table` + INSERT on the source.
4. Drive another change on customers AFTER the new table's events.
5. Assert the customers change lands AND the new table doesn't exist on dst.

Pre-fix, step 4 would time out because the applier crashed on step 3's events. Post-fix, the test passes in seconds.
