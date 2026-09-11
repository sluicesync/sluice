# ADR-0012: Bypass `pglogrepl` to send `CREATE_REPLICATION_SLOT FAILOVER true`

## Status

Accepted. Implemented in v0.2.0 / v0.2.1 (`internal/engines/postgres/slot_create.go`).

## Context

PostgreSQL 17 added a `FAILOVER` option to the `CREATE_REPLICATION_SLOT` replication-protocol command. Slots created with `FAILOVER true` survive switchover/failover events when the cluster's permanent-slots config (Patroni `slots:`, PlanetScale "Logical slot name", or PG 17 native slot sync, which needs `sync_replication_slots` **and** `hot_standby_feedback` together) lists them. Without the flag, slots are primary-local and silently lost on failover — the headline CDC failure mode for PlanetScale Postgres customers.

Sluice's CDC reader uses `github.com/jackc/pglogrepl` to send replication-protocol commands. The library's `CreateReplicationSlotOptions` struct exposes `Temporary`, `SnapshotAction`, and `Mode` — but **not** `FAILOVER`. Adding it requires either (a) waiting for upstream to land support, (b) forking the library, or (c) bypassing pglogrepl's constructor for this one call and sending the raw protocol command via `pgconn.PgConn.Exec`.

Option (a) ties slot HA to a third-party release schedule. Option (b) inherits maintenance forever. Option (c) is small and self-contained.

## Decision

Bypass pglogrepl for slot creation when `FAILOVER true` is wanted. Use `pgconn.PgConn.Exec` directly with a hand-built command string:

```
CREATE_REPLICATION_SLOT "slotname" LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER true)
```

Parse the result rows the same way pglogrepl does internally (slot_name, consistent_point, snapshot_name, output_plugin). On PG ≤ 16 (where the option doesn't exist), fall back to `pglogrepl.CreateReplicationSlot` and emit a one-time stderr warning that the slot won't survive failover without manual permanent-slots config.

## Consequences

Slot HA on PG 17+ doesn't depend on pglogrepl's release cadence. The bypass is small (~50 LOC plus tests) and lives in one helper file with thorough documentation of the protocol command shape. When pglogrepl eventually exposes `FAILOVER` in `CreateReplicationSlotOptions`, the bypass can be replaced with the library call and the helper deleted — the test pinning the command shape will catch any drift.

**The flag is necessary and not sufficient, and v0.151.0 added the runtime check that says so.** This ADR's framing — set `FAILOVER true` and the slot survives — is only half the requirement: the flag marks a slot *eligible* for synchronization, and something on the cluster still has to do the synchronizing. A slot flagged FAILOVER on a cluster that never syncs it is silently primary-local, which is the failure this ADR exists to prevent. `preflightSlotFailover` (`internal/pipeline/slot_failover_preflight.go`) now reads both GUCs at `sync` cold start and WARNs when it cannot confirm either mechanism. It never refuses: Patroni permanent slots preserve slots without touching `pg_settings`, so a correctly-configured cluster reads identically to an unprotected one.

**And the hazard is not only promotion.** This ADR says "switchover/failover events". Measured 2026-09-10 on PlanetScale Postgres 18.6, **single-node, zero replicas**: a slot carrying `failover=t, synced=f` was destroyed by a routine PS-10 → PS-20 **resize**. A cluster change replaces the node, which strands a primary-local slot exactly as a promotion would. The failover-only framing is precisely what made "single-node instances are exempt" sound obvious, and the wrong version of that advisory shipped twice before the measurement corrected it.

The cost is one place where we maintain protocol-shape knowledge that should belong upstream. The PG 17 syntax mismatch we hit in v0.2.0 (sending pre-PG-17 `EXPORT_SNAPSHOT` keyword inside the PG 17 option-list grammar, fixed in v0.2.1) is the canonical example of why this kind of bypass needs a defensive unit test pinning the exact command string sent. We have it.
