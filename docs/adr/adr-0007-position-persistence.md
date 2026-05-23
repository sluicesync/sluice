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

## Durability hardening for Postgres targets (F7)

The "commit position and data together" guarantee depends on the COMMIT actually being durable when PG acknowledges it. PG's `synchronous_commit` GUC governs this: with the default `on`, COMMIT waits for the WAL to be flushed and fsynced before ACKing; with `off` (asynchronous commit, documented in *The Internals of PostgreSQL* Ch 9.5), the ACK can return BEFORE the WAL is durably written, so a crash between ACK and flush silently loses the most recent committed transactions despite the client having moved on.

`synchronous_commit` is inheritable via PG's parameter-precedence chain (Ch 11.2): `ALTER ROLE name SET synchronous_commit = off` or `ALTER DATABASE name SET synchronous_commit = off` pre-applies the value on every login from that role or to that database. An operator with the most innocuous of motivations — "make the bulk-load app go faster" — can therefore silently undermine sluice's ADR-0007 guarantee without sluice ever seeing the configuration change.

The PG applier hardens against this by emitting `SET LOCAL synchronous_commit = on` as the first statement on every apply transaction (`internal/engines/postgres/change_applier.go: ChangeApplier.forceSynchronousCommitOn`, called from `applyOne`, `applyOneBatch`, and `WritePosition` immediately after `BeginTx`). `SET LOCAL` scope reverts automatically at tx end so non-sluice sessions on the same role are unaffected, and a session that already has `synchronous_commit = on` (the PG default) sees no behaviour change. The pin lives in `change_applier_synccommit_test.go` (unit, recording-driver SQL-emission check) and `change_applier_synccommit_integration_test.go` (integration, `ALTER ROLE … SET synchronous_commit = off` end-to-end). This closes severity-A finding F7 from the 2026-05-22 PG-internals research run.

The MySQL applier does not need an analogous hardening: MySQL's `sync_binlog` + `innodb_flush_log_at_trx_commit` settings are not inheritable per-role in the same way (they are global / per-connection only by explicit SET), and their failure mode is operator-visible rather than silent-inheritance.

## Related PG-internals research

Two durable research artifacts inform this ADR's PG-side guarantees:

- `sluice-pg-internals-research-2026-05-22.md` (Ch 12 logical-replication chapter) findings **F1** (pgoutput protocol-version audit — affects whether new pgoutput messages affect position semantics on protocol upgrade) and **F3** (`confirmed_flush_lsn` pin test — invariant that the slot's confirmed-flush never advances past the target-committed LSN) are direct corollaries of the position-persistence design. The Phase 4 follow-up tasks for F1/F3 are tracked in the backlog and reference this ADR.
- `sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md` finding **F7** is the inheritance hazard documented in the "Durability hardening" section above. Finding **F5** (source-identity pinning via `IDENTIFY_SYSTEM`) is in ADR-0051; both close silent-loss-class hazards that this ADR's "position + data lands durably together" guarantee silently depended on.
