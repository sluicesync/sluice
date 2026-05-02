# ADR-0008: go-mysql for MySQL binlog parsing

## Status

Accepted. Implemented in `internal/engines/mysql/cdc_reader.go`.

## Context

MySQL's row-based binary log is a documented but non-trivial wire protocol. Parsing it requires handling table-map events, multiple versions of write/update/delete row events, GTID and binlog-file position tracking, schema-cache invalidation on DDL, and per-type value decoding through driver-version-dependent shapes.

Three options for the Go implementation:

1. **Roll our own parser.** Full control, no third-party dependency. Months of work. Decoding edge cases (TIME, DECIMAL, JSON) accumulate as bugs against real MySQL versions for years.
2. **`github.com/go-mysql-org/go-mysql/replication`.** Mature, used by canal (Alibaba), TiDB DM (PingCAP), and other production CDC tooling. Direct event-loop interface; we own the schema cache and DDL handling.
3. **`github.com/go-mysql-org/go-mysql/canal`.** A higher-level wrapper around `replication` that owns the schema cache and exposes event callbacks. Less control over the message loop.

## Decision

Use the lower-level `replication` package directly. Sluice owns the event dispatch loop, the schema cache (refreshed via the existing `SchemaReader` on DDL events), and the position bookkeeping. Both binlog file/pos and GTID-set positions are supported; the reader auto-detects from the source's `gtid_mode`.

## Consequences

`go-mysql` is a real top-level dependency in `go.mod` (not a sub-package of an existing dep). Mature library with stable API; the risk of upstream churn is low. Sluice retains control of the event shape — every binlog event maps to a typed `ir.Change` variant or is consumed internally — so future changes to the IR don't require upstream coordination.

Symmetric with [ADR-0006](adr-0006-pgoutput.md) on the Postgres side: in both cases, sluice depends on the canonical Go library for the engine's logical-decoding output and produces engine-neutral IR events from there.
