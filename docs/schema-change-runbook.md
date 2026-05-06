# Schema-change runbook

A short operator reference for changing schemas on a source / target pair while sluice is running. Sluice doesn't itself manage schema migrations — it preserves streaming continuity through them — so this doc is about coordination, not about replacing tools like Atlas / sqitch / liquibase.

The three operations covered here are the ones v0.8.0 stretch-testing pinned the behaviour of: `ADD COLUMN`, `DROP COLUMN`, and `MODIFY` (type / size widening). Each section names what sluice does when a change happens mid-sync, and the workflow that gets the schema change applied cleanly to both sides.

## TL;DR — the standard workflow

```bash
sluice sync stop --wait \
    --target-driver postgres \
    --target "$TARGET_DSN" \
    --stream-id default

# Apply ALTER on source and target as appropriate
psql "$SOURCE_DSN" -c 'ALTER TABLE accounts ADD COLUMN ...'
psql "$TARGET_DSN" -c 'ALTER TABLE accounts ADD COLUMN ...'

sluice sync start \
    --source-driver mysql --source "$SOURCE_DSN" \
    --target-driver postgres --target "$TARGET_DSN" \
    --resume \
    --stream-id default
```

The `--wait` flag (v0.9.0+) blocks until the streamer confirms graceful-drain completion; without it the CLI returns immediately and the operator has no clean handle for "drain is done; safe to ALTER now". See `docs/adr/adr-0025-graceful-drain-stop.md` for the mechanism.

The `--resume` flag picks up the persisted CDC position. Without `--resume` sluice pre-flight-refuses to bulk-copy into a populated target.

## What sluice does on each schema-change class

### `ADD COLUMN` + `INSERT` referencing the new column

**Behaviour:** fail-loud. The applier hits `column "X" does not exist (SQLSTATE 42703)` on the first `INSERT` that uses the new column; the open batch rolls back and sluice exits non-zero.

**Why:** sluice's CDC schema cache is built from the source's initial column list. Mid-stream `CREATE TABLE` and `ADD COLUMN` are deliberately punted (ADR-0021) — the alternative is silently dropping events for unknown columns, which violates the loud-failure tenet.

**Workflow:** `sync stop --wait` → `ALTER` source → `ALTER` target → `sync start --resume`. The CDC position is preserved, so the pre-stop events apply cleanly and the post-start events see the new column on both sides.

### `DROP COLUMN` + `INSERT` not referencing the dropped column

**Behaviour:** graceful. Sluice's applier compares the source row's columns against the target's column list; columns missing on the target map to NULL when nullable, or fail loudly when NOT NULL. The drop on the source doesn't surface until the first event after the drop, and only matters if the column's value mattered.

**Why:** dropping a column is a one-sided operation in the IR — the source stops sending the column, and the target's column either stays (with NULLs accumulating) or gets dropped on its own schedule.

**Workflow:** the operator can either:

- **Eager:** `sync stop --wait` → `DROP COLUMN` on source → `DROP COLUMN` on target → `sync start --resume`.
- **Lazy:** `DROP COLUMN` on source while sluice runs; clean up the target column on the next maintenance window. New rows accumulate NULLs in the target column.

The lazy path is fine when the target column is nullable and no application is reading it. The eager path keeps the schemas in lock-step.

### `MODIFY` (type widening) + `INSERT` of a value the target can't hold

**Behaviour:** fail-loud. The applier hits `value too long for type ... (SQLSTATE 22001)` (or analogous overflow / out-of-range error) on the first `INSERT` of the wider value; the open batch rolls back and sluice exits non-zero.

**Why:** sluice's value-shaping helpers preserve the source value bit-for-bit; the target's stricter constraint is what rejects. Loud failure here is a deliberate choice — the alternative is truncating the value, which is silent corruption.

**Workflow:** `sync stop --wait` → `ALTER COLUMN` on **target** (widen first) → `sync start --resume`. The source `ALTER` is optional — the target only needs to accept the wider value. If you're widening on the source side too, do that first, then target, then start.

A narrower-on-target shape is the recovery path operators most commonly want; sluice's `--type-override` flag (per-column source → target type mapping) is the right tool when the operator deliberately wants the target type to differ from sluice's auto-emit choice.

## Planning ahead with `sluice schema diff`

`sluice schema diff` (ADR-0029) runs the source schema through sluice's translation pipeline and compares the result against the target's actual schema. Use it to:

1. **Pre-flight a planned ALTER.** Apply the ALTER on the source first, then run `sluice schema diff --source-driver ... --source ... --target-driver ... --target ...`. Drift surfaces in the output as missing-on-target columns / type mismatches; the suggested `ALTER` statements give a starting point for the target-side change.
2. **Verify post-ALTER alignment.** Run after applying both sides; expect "in sync" + exit code 0. Useful in CI as a gate.
3. **Catch unintended drift.** Operators sometimes run hand-edits on the target and forget to mirror them on the source; the diff surfaces those before they cause CDC failures.

The suggested ALTER statements are starting points, not verified migration scripts. Review them; the diff doesn't know about your data volume, lock duration, or downstream consumers.

## When to reach for Atlas / sqitch / liquibase instead

Sluice's role is preserving streaming continuity through schema changes. It is not a schema-migration tool. If you need:

- **Version-controlled schema migrations** with an audit log of who applied what when.
- **Multi-environment promotion** (dev → staging → prod) with the same migration applied identically.
- **Down-migrations** (rolling back to a previous schema version).
- **Schema-as-code** integrated into your application's build process.

…use Atlas, sqitch, liquibase, Flyway, or your framework's built-in migration tool. Sluice runs alongside those — apply the migration with your tool of choice, and use the workflow above to keep sluice's stream aligned.

## CDC + position implications

The `sync stop --wait` flow guarantees:

- The applier's in-flight batch is committed (no rolled-back events).
- The CDC position is persisted past the last applied event.
- The stop signal is cleared, signalling clean drain to the CLI.

After ALTER, `sync start --resume`:

- Reads the persisted position (the source LSN / GTID set / VStream cursor).
- Picks up CDC events from immediately after the last applied one.
- The first `INSERT` / `UPDATE` after the ALTER carries the new schema; sluice's CDC schema cache is rebuilt from RELATION messages (Postgres) or TABLE_MAP_EVENT entries (MySQL), so the new column is recognised on the first event that uses it.

Edge case: if the source's schema-change DDL is itself committed during the ALTER window, it surfaces as a DDL event in the binlog / WAL. Sluice's CDC reader filters DDL events (per ADR-0021); the schema-cache rebuild on the next row event picks up the change. So the order "stop → ALTER source → ALTER target → start" is robust regardless of which side commits the DDL first, as long as both sides have the new shape before `sync start` runs.

## See also

- `docs/adr/adr-0025-graceful-drain-stop.md` — the `sync stop --wait` mechanism.
- `docs/adr/adr-0029-schema-diff.md` — the `sluice schema diff` design.
- `docs/architecture.md` — the IR / engine pattern context.
- `docs/throughput-tuning.md` — knobs for the bulk-copy rerun if a schema change requires `--reset-target-data`.
