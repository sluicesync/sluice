# Prep: position persistence (control table)

> **Status: SHIPPED in v0.1.0.** Canonical reference: [ADR-0007](../../adr/adr-0007-position-persistence.md). The control table is the `sluice_cdc_state` referenced across all CDC features.

Roadmap reference: [docs/dev/roadmap.md §5](../roadmap.md). Related: [ADR-0007: position persistence on the target](../../adr/adr-0007-position-persistence.md), [prep-change-applier.md](prep-change-applier.md) (which built the per-change transaction shape this chunk plugs into).

## Goal

Make CDC streams resumable across process restarts by persisting the source position on the target database, in the same transaction as each data change. ADR-0007 picked the design (a control table on the target, position committed atomically with data) — this chunk implements it.

The two load-bearing properties:

- **No-divergence.** Progress and applied data can never diverge. Either the target row was committed AND the position update was committed, or neither. There is no intermediate state.
- **No-gap resume.** A restart picks up exactly where the previous run committed last. Combined with the applier's idempotency (upsert-on-Insert + tolerant Update/Delete), replaying any change that *was* applied but whose position update *didn't* commit is safe.

Out of scope:

- **Configurable table prefix.** Roadmap §10. v1 hardcodes `sluice_cdc_state`.
- **Concurrent-streamer locking.** Two processes sharing a `stream_id` will clobber each other's position. v1 documents this; explicit locking is a future hardening pass.
- **Position-write batching.** v1 inlines the position UPDATE in every per-change transaction. Batching would only help if we batched data writes too — that's the v1.5 perf chunk and they belong together.

## Schema

A single table per target database. ADR-0007 named it; v1 honors the name verbatim:

```sql
CREATE TABLE IF NOT EXISTS sluice_cdc_state (
    stream_id       VARCHAR(255) NOT NULL,
    source_position TEXT         NOT NULL,
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (stream_id)
);
```

Engine-specific tweaks:

- **MySQL**: `TEXT` for source_position is fine (binlog positions and GTID sets fit comfortably in MySQL TEXT's 64KB limit). `VARCHAR(255)` for stream_id matches conventional MySQL key-column lengths.
- **Postgres**: `TEXT` for both source_position and stream_id (PG has no real length advantage to `VARCHAR(N)` and TEXT is the conventional choice). `TIMESTAMP` is `TIMESTAMP WITHOUT TIME ZONE`; `now()` works as a default.

The schema-init runs at Streamer startup, before opening any source connection. `CREATE TABLE IF NOT EXISTS` is idempotent — second-and-later runs are no-ops.

## Files to add / touch

New files:

- `internal/engines/mysql/control_table.go` — `EnsureControlTable`, `ReadPosition`, and the per-tx `WritePositionTx` helper that the applier calls inside its existing `BEGIN/COMMIT`.
- `internal/engines/postgres/control_table.go` — same shape with PG SQL.
- `internal/engines/mysql/control_table_test.go` and PG equivalent — unit tests for the SQL builders (no Docker).
- `internal/engines/mysql/control_table_integration_test.go` and PG equivalent — testcontainers-backed: ensure-table-idempotency, write-then-read round-trip, multi-stream-id isolation.

Modify:

- `internal/engines/mysql/change_applier.go` and PG equivalent — `Apply` loop accepts a `streamID` + the latest `Position` per change, and inlines the position write inside the data-change transaction.
- `internal/ir/interfaces.go` — `ChangeApplier.Apply` signature gains a `streamID` parameter, OR the applier gets a separate `SetStream(streamID string)` setup call. *Open question* — see below.
- `internal/pipeline/streamer.go` — at startup: ensure control table exists → look up persisted position by stream_id → if found, jump straight to CDC from that position (skip snapshot+bulk-copy); if not found, fall through to the existing cold-start flow that does snapshot+bulk-copy. Warm-resume requires the engine's `OpenCDCReader.StreamChanges(ctx, persistedPosition)` to work — already the case from §3.
- `internal/pipeline/streamer.go` — pass stream_id through to the applier.

## Stream lifecycle: cold start vs warm resume

This is the load-bearing change in `Streamer.Run`. Today it always:

1. reads source schema → captures snapshot → bulk-copies → streams CDC

After §5 ships:

```
Streamer.Run(ctx):
    ensure target.sluice_cdc_state exists
    persisted, ok = read position for stream_id

    if ok:
        # WARM RESUME — skip snapshot+bulk-copy, jump straight to CDC.
        cdc = source.OpenCDCReader(ctx, dsn)
        changes = cdc.StreamChanges(ctx, persisted)
    else:
        # COLD START — same as today.
        stream = source.OpenSnapshotStream(ctx, dsn)
        runBulkCopy(...)
        changes = stream.Changes.StreamChanges(ctx, stream.Position)

    applier.Apply(ctx, streamID, changes)
```

Single, well-bounded branch at startup. The applier's tx-per-change loop is identical for both paths.

## Position write integration

Inside the applier's per-change transaction:

```
BEGIN;
  <Insert / Update / Delete / Truncate SQL>;
  INSERT INTO sluice_cdc_state (stream_id, source_position, updated_at)
    VALUES ($1, $2, NOW())
    ON CONFLICT (stream_id) DO UPDATE SET
        source_position = EXCLUDED.source_position,
        updated_at      = EXCLUDED.updated_at;
COMMIT;
```

MySQL gets the row-alias UPDATE form (matching the applier's INSERT shape). Both engines: the position write is the *second* statement in the transaction, after the data change. If the data change errors, we roll back — no position update either, so resume re-applies. If the data change succeeds and the position write errors (extremely unlikely — same connection, same tx), we roll back — same outcome.

The position passed to `WritePositionTx` is the change's `Pos()` value. CDC events emit positions that point *after* the event in the source log, so resuming from that position skips the just-applied event — exactly what we want.

## stream_id generation

The operator picks the stream_id, or sluice generates one. ADR-0007 suggested `source-driver + source-host + target-driver + target-host`. Mechanically:

```go
streamID = fmt.Sprintf("%s://%s -> %s://%s",
    source.Name(), redactedHost(sourceDSN),
    target.Name(), redactedHost(targetDSN))
```

`redactedHost` strips passwords and parses out host:port from either URI- or KV-style DSNs. Length-bounded to 255 chars to fit `VARCHAR(255)` on MySQL.

For v1, generate it; expose a `Streamer.StreamID string` field that overrides the auto-generation when non-empty. Operator-supplied IDs take precedence. Default is the auto-generated form.

## Open questions for the user

1. **ChangeApplier.Apply signature change.** Two options for plumbing the stream_id in: (a) add a `streamID string` parameter — clean but breaks the existing `ir.ChangeApplier` interface. (b) keep `Apply(ctx, changes)` and add a `SetStream(streamID, db)` setup method. *Recommendation:* (a). The interface is internal (no external users) and the parameter is unambiguous. Confirm?

2. **Position write on Truncate.** Truncate is a metadata-ish event with no row-level cost on the target — but the position should still advance through it. *Recommendation:* yes, write the position alongside the TRUNCATE in the same tx. Same shape as data changes. Confirm?

3. **Failure mode when control table DDL is blocked.** Some target environments restrict DDL on the application database (read-only replicas used as sync targets, locked-down RDS instances). Right now this would error at Streamer startup. ADR-0007 mentioned "state DSN" pointing to a different database as a future escape hatch. *Recommendation:* clear startup error in v1; YAGNI on the alternate-DSN until a real case appears. Confirm?

4. **`updated_at` clock skew.** The control table uses `NOW()` on the target. If the target is a read replica with replication lag (or has a very different system clock), `updated_at` could mislead operators. *Recommendation:* ignore in v1. The column is operator-readable diagnostic info, not load-bearing for correctness — `source_position` is what matters.

5. **Schema isolation on Postgres targets.** PG's namespaced schemas mean `sluice_cdc_state` lives in the schema named in the DSN's `schema` query parameter (default `public`). MySQL's flat namespace puts it in the configured database. *Recommendation:* schema-qualify the table name on PG; on MySQL we already operate against a specific database. Confirm?

6. **Eager schema-init in Streamer vs lazy in Applier.** Both work. *Recommendation:* eager in Streamer — the control table is a Streamer-level concern (drives cold/warm choice), so the Streamer should own its lifecycle.

## Anticipated rough edges

- **DDL inside a transaction.** Some MySQL versions (and storage engines) implicit-commit when running CREATE TABLE. We run the CREATE outside any application transaction, at Streamer startup, before any data writes — so this isn't an issue, but worth a comment in the code.
- **TIMESTAMP precision.** MySQL's default TIMESTAMP is second-precision; Postgres's is microsecond. The `updated_at` column is for human reading, not for correctness comparisons, so this is fine — but operators reading the table might notice the difference.
- **Concurrent appliers writing the same stream_id.** Two processes with the same stream_id will see lost updates (last-writer-wins for the position). v1 documents this; explicit advisory locking (PG `pg_advisory_lock`, MySQL `GET_LOCK`) is a future hardening pass.
- **Resume across schema changes.** The persisted position is a binlog/LSN; if the source schema changed between runs, the CDC reader's relations cache rebuilds itself from the next RelationMessage / TABLE_MAP_EVENT. No special handling needed here.
- **Cleanup on stream end.** `DROP TABLE sluice_cdc_state` on the target. v1 ships no automated cleanup; it's a manual step the operator runs once the migration is fully cut over. Document.

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/notes/prep-position-persistence.md, and ADR-0007. Propose the design before writing code: (1) the exact control_table.go API on each engine (EnsureControlTable, ReadPosition, WritePositionTx), (2) the ChangeApplier.Apply signature change to thread stream_id through, (3) the Streamer cold-start vs warm-resume branch shape, (4) the integration test that proves restart-resume works (kill mid-stream, restart, assert the surviving target state has every event exactly once). Note any deviation from the prep doc with a why. Stop after the design for review."
