# Prep: production ChangeApplier on each engine

Roadmap reference: implicit prerequisite for [docs/dev/roadmap.md §5](../roadmap.md) (position persistence). Related: [ADR-0007: position persistence](../../adr/adr-0007-position-persistence.md), [prep-snapshot-cdc-handoff.md](prep-snapshot-cdc-handoff.md) (which left this gap explicitly open).

## Goal

Implement `OpenChangeApplier` on the MySQL and Postgres engines so that the `ir.Change` events the snapshot+CDC stream produces actually land on the target. Today the engines return `ErrNotImplemented` for this method; the integration test for §4 supplies a tiny stub applier. With this chunk shipped, `pipeline.Streamer` can drive a real continuous-sync migration end-to-end on either engine, and §5 (position persistence) has somewhere to commit the position alongside the data it reflects.

Out of scope for this chunk:

- **Position persistence** (control table writes). That's roadmap §5 with its own scope. The applier's interface lets §5 plug in later by adding a hook between the data-write and the transaction commit.
- **Cross-engine appliers**. Same-engine on each side first. Cross-engine apply (MySQL change → Postgres apply, or vice versa) is the snapshot+CDC equivalent of roadmap §1's cross-engine bulk migration and lands as a follow-up once same-engine is solid.
- **Transaction batching across CDC events**. v1 applies one change per target transaction. Slow on high-write workloads but unambiguously correct; batching is a v1.5 optimization once we have benchmarks.

## What the applier actually does

For each `ir.Change` arriving on the channel:

- `ir.Insert{Schema, Table, Row}` → `INSERT INTO schema.table (col1, col2, ...) VALUES (?, ?, ...)` with the row map flattened into positional parameters.
- `ir.Update{Schema, Table, Before, After}` → `UPDATE schema.table SET <After-cols> WHERE <Before-cols>`. The Before image is the row identifier; using Before instead of a separately-cached PK avoids a target-schema lookup and is robust to PK-column updates.
- `ir.Delete{Schema, Table, Before}` → `DELETE FROM schema.table WHERE <Before-cols>`.
- `ir.Truncate{Schema, Table}` → `TRUNCATE TABLE schema.table` (engine-specific identifier quoting).

The Before-image-as-WHERE-clause approach has one hard requirement: the Before image must contain enough columns to uniquely identify the row. On Postgres that means `REPLICA IDENTITY FULL` for tables without a usable PK (already surfaced in [prep-postgres-cdc.md](prep-postgres-cdc.md) §6 as a startup warning to add). On MySQL with `binlog_row_image=FULL` (which the integration tests already set), the full Before image is always present in row events.

## Library surface

No new dependencies. Both engines already import `database/sql` and the engine-native driver. The applier holds an open `*sql.DB` for the target and runs each change in its own short transaction.

## Files to add / touch

New files:

- `internal/engines/mysql/change_applier.go` — `ChangeApplier` struct, `Apply` method, per-change SQL generation.
- `internal/engines/postgres/change_applier.go` — same shape on the Postgres side.
- `internal/engines/mysql/change_applier_test.go` and the postgres equivalent — unit tests for the SQL-generation helpers (no Docker needed; assert the produced SQL string + args).
- `internal/engines/mysql/change_applier_integration_test.go` and postgres equivalent — testcontainers-backed end-to-end: feed Change events through the applier, assert target rows match.

Modify:

- `internal/engines/mysql/engine.go` and `internal/engines/postgres/engine.go` — replace the `OpenChangeApplier` stub with the real implementation.
- The `pipeline/streamer_integration_test.go` we just shipped uses a stub applier; once production appliers exist, add a parallel test that wires through the real applier and asserts the target table reaches the expected state. (Keep the stub-based test too — it's useful coverage for the Streamer's lifecycle.)

## Data flow sketch

```
[CDC stream]
  ch ← ir.Change events (Insert / Update / Delete / Truncate)
    │
    ▼
[ChangeApplier.Apply]
  for change := range ch:
      switch change.(type):
          ir.Insert:    INSERT INTO ...
          ir.Update:    UPDATE ... WHERE <Before>
          ir.Delete:    DELETE FROM ... WHERE <Before>
          ir.Truncate:  TRUNCATE ...
      target.BeginTx()
        execute the SQL
        (future §5 hook: UPSERT INTO sluice_cdc_state (stream_id, position) VALUES (...))
      target.Commit()
    │
    ▼
[target database]
```

The §5 hook is mentioned for completeness — it doesn't ship in this chunk. The applier's transaction shape (one tx per change, including position update when §5 ships) is the part that future-proofs the design.

## Transaction shape

v1: **one source change → one target transaction**. Every `ir.Change` becomes:

```
BEGIN;
  <single INSERT / UPDATE / DELETE / TRUNCATE>;
COMMIT;
```

This is the simplest correct shape. Properties:

- **Atomic with future position update.** §5 will add `UPDATE sluice_cdc_state ...` inside the same `BEGIN/COMMIT`, satisfying ADR-0007's "progress and data can never diverge."
- **Idempotent on retry.** A restart re-applies events from the persisted position. If the applier crashed mid-transaction, the source change wasn't committed on the target and will be re-emitted from the slot. If the applier crashed after the commit but before the position update made it back to the source's confirmed_flush, the next stream replays the change — and the upsert/delete-then-insert idempotency handling (below) absorbs it.
- **Slow on high write rates.** A 10K events/sec source is 10K target transactions/sec, which neither engine handles gracefully. That's the v1.5 optimization: batch N events per target transaction with a max-time-to-commit. Punted, but the interface is structured to allow it without behavior change.

## Conflict semantics

For `ir.Insert`, two cases produce a "row already exists" conflict on the target:

1. **Resume duplicate.** A change was already applied before the previous run died, but the position update didn't make it. The replayed change must be a no-op rather than fail.
2. **Pre-existing row.** Bulk-copy already loaded a row with the same key, and a subsequent CDC event re-inserts it. Same handling.

Recommendation: **upsert semantics for Insert.**

- MySQL: `INSERT ... ON DUPLICATE KEY UPDATE col1=VALUES(col1), col2=VALUES(col2), ...`. Uses the table's PK (or unique key) for collision detection; sets all non-key columns to the new values.
- Postgres: `INSERT ... ON CONFLICT (<pk>) DO UPDATE SET col1=EXCLUDED.col1, ...`. Same shape.

Both require knowing the PK column list — which the applier reads from the target schema at startup (one `information_schema` round-trip per table on first sight, cached). This is the only schema-side dependency the applier has on the target.

For `ir.Update` and `ir.Delete`, "row doesn't exist" surfaces as `RowsAffected() == 0`. That's almost always benign on resume (we already deleted/updated this row in the previous run). For v1: log at debug level and continue. A strict mode that errors out is a future flag.

## Idempotency

The combination of upsert-Insert + tolerant-Update/Delete (zero-affected-rows is OK) gives full idempotency: replaying any prefix of the change stream is safe. This is the property that makes resume work without a separate dedup mechanism.

## Open questions for the user

1. **One tx per change vs batch.** Above, v1 is one-per-change for simplicity + atomicity with future position writes. *Recommendation:* one-per-change for v1; batch as a follow-up once benchmarks reveal the pain. Confirm?
2. **Insert collision policy.** Upsert (above) vs strict-error vs configurable. *Recommendation:* upsert in v1 because resume idempotency is non-negotiable for continuous sync. A strict mode is a future flag if a real use case needs to detect double-applies. Confirm?
3. **Update/Delete miss policy.** Tolerate (zero-affected-rows is fine) vs error. *Recommendation:* tolerate in v1 — same reasoning as above. Operators wanting strict-mode get a future flag.
4. **Schema reads on applier startup.** Read target schema eagerly at construction (one round-trip), or lazily on first sight of each table. *Recommendation:* lazy, cached. Tables that aren't touched never get queried; tables that are touched get a one-time `information_schema` lookup that the cache reuses.
5. **TRUNCATE handling.** PG sends explicit TRUNCATE messages via pgoutput; MySQL embeds TRUNCATE inside QUERY_EVENT and the standalone CDC reader's prep doc punted on emitting `ir.Truncate` for MySQL. The applier should handle `ir.Truncate` (PG path) but won't see it from MySQL until that's wired. *Recommendation:* implement TRUNCATE handling on both appliers; cross-engine the MySQL CDC TRUNCATE emission can land later without applier changes. Confirm?
6. **Identity / sequence handling on Insert.** PG `GENERATED BY DEFAULT AS IDENTITY` accepts user-supplied values (which is what we need — the source row's `id` should land on the target verbatim) but doesn't auto-bump the sequence. The next user-driven insert collides. Same issue exists in simple-mode (roadmap §7 calls it out). *Recommendation:* punt — same fix lands in §7 across both modes. Note in docs.

## Anticipated rough edges

- **Update/Delete WHERE clauses with NULL columns.** SQL `WHERE col = NULL` doesn't match anything; needs `WHERE col IS NULL`. The clause builder has to special-case nil row values.
- **Floating-point equality in WHERE.** `WHERE price = 19.95` is unreliable for exact-match in some scenarios. Real-world impact is small (most CDC sources don't have row-image floating-point ambiguity), but worth a comment in the WHERE-builder.
- **Postgres's quoted vs lowercase identifiers.** Always quote target identifiers; matches the existing schema writer.
- **MySQL's case-sensitivity quirks.** Table names are case-sensitive on Linux but case-insensitive on macOS/Windows. The schema reader already lowercases incoming names; the applier should do the same on outgoing identifiers.
- **Unsupported value types.** Most types round-trip cleanly via `database/sql` parameter binding (the bulk-copy path already exercises this). Bytea / JSONB / arrays are the main risk; the integration test should cover a representative sample.
- **Order preservation across the CDC channel.** The CDC reader emits events in source-binlog/WAL order. The applier must preserve that order — single-goroutine consumption from the channel is the simplest way to guarantee it. No parallelism in v1.

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/notes/prep-change-applier.md, and ADR-0007. Propose the design before writing code: (1) the exact ChangeApplier struct shape on each engine, (2) the SQL-generation helpers (insert/update/delete/truncate), (3) the conflict-handling SQL (UPSERT on Insert, tolerant on Update/Delete), (4) the integration test shape that proves end-to-end apply + idempotency. Note any deviation from the prep doc with a why. Stop after the design for review."
