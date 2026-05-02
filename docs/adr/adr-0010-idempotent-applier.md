# ADR-0010: Idempotent applier semantics

## Status

Accepted. Implemented in `internal/engines/{mysql,postgres}/change_applier.go`.

## Context

CDC resume works by replaying changes from the last persisted position. If a process crashes between a target data write and the position update commit (within the same transaction — see [ADR-0007](adr-0007-position-persistence.md)), both roll back and the change re-streams cleanly. But if the crash happens *after* both commit but *before* the source acknowledges the position advance, the change re-streams once more on restart.

The applier's behavior on that replay determines whether resume is safe. Three options:

1. **Strict identity matching.** Insert errors on duplicate-key, Update/Delete errors on zero-affected-rows. Safe only if the stream is exactly-once at the source — which logical replication doesn't guarantee.
2. **Track per-change application IDs separately.** A second bookkeeping table to remember "this change was applied". Solves exactly-once at the cost of a second commit per change.
3. **Idempotent semantics.** Insert upserts on PK; Update and Delete tolerate zero-affected-rows. Replay of any prefix of the change stream produces the same final target state.

## Decision

Option 3. Per-engine implementations:

- **Insert with PK**: `INSERT ... ON DUPLICATE KEY UPDATE col = new.col` (MySQL row-alias form, 8.0.20+) or `INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col` (Postgres).
- **Insert without PK**: plain `INSERT`. Best-effort idempotency (replays produce duplicates); the applier package comment names this as a documented limitation. Tables without PKs are not recommended for continuous sync.
- **Update / Delete**: `RowsAffected() == 0` is silently absorbed. A future strict-mode flag could error for operators who want loud failure on data drift; punted until a real case appears.

## Consequences

Resume after a partial-apply is safe by construction; sluice does not need a second tracking table. The cost is target-side write amplification on replay (every replayed Insert is a write, even when the row is unchanged), which matters only if replay volume is significant — and if it is, the operator probably has bigger problems.

Tables without primary keys are quietly best-effort. The applier package comment surfaces this prominently so operators are not surprised; the long-term fix is to require a PK on the source for sluice's continuous-sync mode, which is a documentation question not a code one.
