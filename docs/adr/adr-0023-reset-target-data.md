# ADR-0023: `--reset-target-data` flag for explicit destructive recovery

## Status

Accepted. To be implemented in `cmd/sluice/cli.go` (flag), `internal/ir/interfaces.go` (`TableDropper` optional surface), `internal/engines/{postgres,mysql}/schema_writer.go` (or row_writer.go — wherever fits), `internal/pipeline/reset.go` (new — orchestrates the cleanup), and `internal/pipeline/{migrate.go,streamer.go}` (call into the cleanup before the existing pre-flight).

## Context

Item F's v0.4.0 testing report noted that even after v0.5.2's slot-missing fall-through landed, recovery from `wal_status='lost'` still requires the operator to manually drop dest tables (or pass `--force-cold-start`) because Bug 9's pre-flight refusal of populated dest fires regardless of how the operator arrived at the cold-start path.

The remaining manual recovery steps after v0.5.2:

1. `sluice slot drop ...` (already required — operator-initiated destructive action).
2. **`DROP TABLE foo, bar, baz`** on the target — manual SQL.
3. Re-run `sluice sync start ...`.

Step 2 is the friction. It's also error-prone:

- Operators have to enumerate which tables sluice manages — easy to miss one if the source schema has many tables, or include too many if the target hosts other applications.
- Foreign-key cascades require ordering or explicit `CASCADE` — wrong order produces "cannot drop table X because Y depends on it" errors that can leave half the schema dropped.
- It's destructive in a way that's not easily undone if executed on the wrong target. Bare `DROP TABLE` has no confirmation gate.

`--force-cold-start` exists (since v0.4.0 Bug 9 fix) and bypasses the pre-flight refusal but does *not* clean up — it tries to bulk-copy into a populated table and hits PRIMARY KEY collisions. It's documented as "use with caution"; it's not a recovery path, it's an opt-out for the rare legitimate "bulk-copy into pre-existing data" case.

This ADR adds the explicit destructive recovery path: `--reset-target-data`.

## Decision

A new `--reset-target-data` flag on both `sluice migrate` and `sluice sync start`. When set, sluice:

1. **Confirms intent** via a destructive-action prompt: "This will DROP tables on the target. Type 'reset' to confirm." The companion `--yes` flag (already in the CLI vocabulary from `slot drop`) skips the prompt for non-interactive use.
2. **Clears the bookkeeping row**: `sluice_cdc_state` row (sync start) or `sluice_migrate_state` row (migrate). DELETE WHERE stream_id/migration_id matches.
3. **Drops dest tables** that match the source-side schema (after filter). One `DROP TABLE IF EXISTS` per table via the engine's `TableDropper` surface; PG uses `CASCADE` to handle FK dependencies, MySQL relies on InnoDB's referential cascade.
4. **Skips the pre-flight refusal** (no populated dest after the drops; nothing to refuse).
5. **Proceeds with cold-start** normally — the schema-write phase will `CREATE TABLE` everything from scratch.

### Engine surface

```go
// In internal/ir/interfaces.go
type TableDropper interface {
    DropTable(ctx context.Context, table *Table) error
}
```

Mirrors `TableTruncator`. Implementations on `SchemaWriter` (or `RowWriter` — wherever the connection pool lives most naturally per engine). Engines that don't implement it cause `--reset-target-data` to error clearly: `engine "X" does not support --reset-target-data; drop dest tables manually before re-running`.

### Mutual exclusion with `--resume`

`--resume` says "pick up where I left off"; `--reset-target-data` says "wipe and start fresh". They're semantically incompatible. The CLI rejects the combination at parse time with a clear error.

### Implies `--force-cold-start`

After the drop loop runs, the dest is empty (tables don't exist). The pre-flight refusal would not fire even without `--force-cold-start` (its `IsTableEmpty` would either return "missing table" → empty, or the schema-write phase recreates the table fresh). For clarity, the implementation explicitly skips the pre-flight when `--reset-target-data` is set, avoiding any race between drop and the next probe.

### Scope of dropped tables

The drop loop iterates `schema.Tables` after `applyTableFilter` — the same set the schema-write phase uses. Tables on dest that aren't in the source schema are NOT touched. This is the load-bearing safety property: an operator running `--reset-target-data` against a target shared with other applications drops only sluice-managed tables, not everything in the database.

`sluice_cdc_state` and `sluice_migrate_state` are explicitly excluded from the schema (already filtered by the schema readers per ADR-0015 + v0.3.0 work). They're cleared via targeted `DELETE` instead.

### Confirmation prompt design

The prompt is interactive on stdin/stdout. The user types `reset` (not `y`/`yes`/`Y`) — a small friction step that prevents muscle-memory enter-key acceptance. Anything else, including bare Enter, aborts.

Non-interactive flows pass `--yes`. CI scripts, container orchestrators, and automation set the flag explicitly. There's no `--prompt=auto` middle ground; presence of `--reset-target-data` is itself the destructive-action acknowledgement, and the typed-confirmation prompt is the safety net for human terminals.

## Consequences

- **Item F's recovery path collapses to one command.** `sluice sync start --reset-target-data --yes ...` after `sluice slot drop`. No more enumerating tables for `DROP TABLE`.

- **`--force-cold-start` semantics unchanged.** It remains the opt-out for "bulk-copy into populated dest on purpose" — a different and rarer use case than recovery.

- **Engine boundaries kept thin.** A new optional surface (`TableDropper`) in the IR; the engines opt in by exposing the method. No engine-specific code in the pipeline.

- **Audit trail in logs.** Each dropped table emits an INFO line. The reset-action summary fires before the cold-start phase logs, so operators can correlate "I asked for reset, here's what got dropped" against the subsequent migration.

- **No state-row write before drops.** The CDC state row is DELETEd before the table drops; if the drop loop fails partway through, the state row is gone but some tables remain. This is intentional — the next run with `--reset-target-data` finishes the cleanup. The alternative (drop tables first, then state row) leaves a stale state row pointing at deleted tables, which is more confusing.

- **Idempotent on partial failures.** `DROP TABLE IF EXISTS` is idempotent; re-running `--reset-target-data` after a partial failure proceeds cleanly.

## Why not auto-reset on slot-missing fall-through

A natural-feeling alternative: when ADR-0022's slot-missing fall-through fires, also drop the dest tables. "The persisted position is invalid → wipe and redo".

Reasons we don't:

- **Different blast radius.** Position fall-through changes pipeline state; table drop changes operator data. Conflating them removes the operator's last gate before destructive action.
- **Bug 9's deliberate refusal stays useful.** A populated dest *might* be intentional (someone manually loaded a backup). The pre-flight refusal lets the operator notice and decide. Auto-drop on fall-through would silently destroy work in that case.
- **Explicit > implicit.** `--reset-target-data` is the operator saying "I want destruction." Slot-missing fall-through is the operator saying "I dropped a slot." These are different intents; the flag separates them cleanly.

## Why not `--reset` (shorter)

`--reset` is ambiguous — reset what? Position? Tables? Indexes? `--reset-target-data` says exactly what it does: data on the target gets reset. Length is not the bottleneck; the flag is typed once per recovery run.

## Verification

Integration tests:

- `TestMigrate_ResetTargetData_HappyPath` — populate dest manually, run migrate with `--reset-target-data --yes`, assert dest is wiped + re-bulk-copied.
- `TestMigrate_ResetTargetData_RejectsResume` — flag combo errors at parse.
- `TestStreamer_ResetTargetData_RecoversFromSlotMissing` — combine with the ADR-0022 fall-through scenario; assert one-command recovery.
- Unit tests on the confirmation prompt behaviour (mocked stdin).

CLI help-text snapshot updated; docs/postgres-source-prep.md gets a one-line cross-reference in the recovery section.
