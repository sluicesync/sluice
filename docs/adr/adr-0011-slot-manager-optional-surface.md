# ADR-0011: SlotManager as an optional engine surface

## Status

Accepted. Implemented in v0.2.0 (`internal/ir/interfaces.go`, `internal/engines/postgres/slot_manager.go`, `cmd/sluice/slot.go`).

## Context

Logical replication slots are a Postgres-specific concept. MySQL CDC uses binlog coordinates and has no equivalent server-side resource that operators need to inspect or drop manually. After v0.1.0 testing surfaced a recurring class of operator pain — abandoned slots from failed setup attempts, slot invalidation under load, slots lost across PlanetScale failover — `sluice slot list` and `sluice slot drop` became necessary commands.

The straightforward implementation would be to add `OpenSlotManager` to the `ir.Engine` interface alongside `OpenSchemaReader` etc. But that forces every engine to implement (or stub) a method that's meaningless on its surface. MySQL's stub would either return a useful "not supported" error (better than silence but still surface area for nothing) or — worse — return an empty list and accept Drop calls as no-ops, which is silently misleading.

Two alternatives considered: (1) a separate top-level `Engine` interface variant; (2) a runtime probe via type assertion on the existing `ir.Engine`.

## Decision

Add slot management as an **optional interface** (`ir.SlotManagerOpener`) that engines implement only when meaningful. The CLI checks for the interface via Go type assertion:

```go
opener, ok := eng.(ir.SlotManagerOpener)
if !ok {
    return fmt.Errorf("engine %q does not support replication-slot management", driver)
}
mgr, err := opener.OpenSlotManager(ctx, dsn)
```

Postgres implements `OpenSlotManager`; MySQL doesn't. Engines that grow slot-equivalent primitives later (e.g. Vitess named tablet vstream cursors) can opt in by adding the method without touching the core `Engine` interface.

## Consequences

The `ir.Engine` interface stays narrow and engine-neutral — engines only declare what they actually support. Operators get a clear runtime error when invoking a CLI surface against an engine that doesn't support it, rather than a silently wrong result. Adding more optional engine surfaces (e.g. a future `BackupManager`, `MetricsExporter`) follows the same pattern with no churn to existing engines.

The cost is one extra type assertion at each CLI dispatch site, and the discipline of remembering that optional surfaces aren't visible in the core `Engine` interface — anyone reading `internal/ir/interfaces.go` cold won't immediately know `SlotManager` exists. The package-level doc comment names the optional surfaces to mitigate.
