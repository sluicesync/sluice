# ADR-0007: CDC position persistence on the target

## Status

Accepted. Implementation in `internal/engines/{mysql,postgres}/control_table.go` and the per-tx position write inlined into the `ChangeApplier` for each engine.

## Context

Continuous-sync mode streams changes from a source's binlog (MySQL) or WAL (Postgres) and applies them to a target. To resume after a restart — planned or otherwise — sluice has to remember where the stream left off. The "position" is engine-specific (binlog file+pos, GTID, or LSN) but the bookkeeping problem is identical.

There are three plausible places to keep the position:

1. **A sidecar state file on the local filesystem.** Simple to implement, but couples sluice to a single host. Failover, container restarts, and shared deployments all hit edge cases. Backup discipline is now sluice's problem.
2. **A separate state store** (etcd, Redis, S3, etc.). Reliable but adds an operational dependency that's heavy for a tool whose core promise is "two DSNs and a CLI".
3. **A control table on the target database.** The same database the user is already operating; backups already cover it; no new dependency.

Option 3 has another quiet virtue. Since changes are applied to the target in the same transaction context the position update can use, sluice can commit data and position together — `INSERT INTO orders ...; UPDATE sluice_cdc_state SET source_position = ...; COMMIT;`. Progress and applied data can therefore *never* diverge. With options 1 or 2 they can.

## Decision

CDC position is persisted in a control table on the target database, named `sluice_cdc_state` by default (configurable prefix). Schema:

```sql
CREATE TABLE sluice_cdc_state (
    stream_id       TEXT       PRIMARY KEY,
    source_position TEXT       NOT NULL,
    updated_at      TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

`stream_id` is a user-facing identifier the operator picks (or sluice generates from `source-driver + source-host + target-driver + target-host`). `source_position` is engine-opaque; the engine that wrote it is the same engine that reads it, so no cross-engine compatibility is required. Position updates are committed in the same transaction as the data changes they reflect. The actual schema shipped uses `VARCHAR(255)` for `stream_id` rather than `TEXT` for MySQL primary-key compatibility — the only deviation from the block above.

## Consequences

Resume after a restart is a single `SELECT` against the target. No sidecar files; no extra services; no clock skew between progress and data. Cleanup is `DROP TABLE sluice_cdc_state` on the target.

The cost is a small permanent table on the target the user didn't ask for. Surface it in docs and make the prefix configurable so users with strong naming conventions aren't surprised. For targets where DDL on the user's database is genuinely off-limits (read-only replicas used as sync targets — rare but possible), the design can grow a "state DSN" to point the control table at a different database, but YAGNI until a real case appears.
