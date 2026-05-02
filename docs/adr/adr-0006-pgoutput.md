# ADR-0006: pgoutput over wal2json for Postgres CDC

## Status

Accepted. Implemented in `internal/engines/postgres/cdc_reader.go` using `github.com/jackc/pglogrepl` for the binary protocol parser.

## Context

Postgres logical replication streams changes from the WAL through a *logical decoding plugin*. Two plugins are commonly used:

- **`pgoutput`** is built into Postgres 10+ and ships in every supported server distribution. It emits a binary replication protocol that pgx can decode natively. No server-side install step.
- **`wal2json`** is a third-party extension that emits JSON for each change. JSON is easier to read in logs and easier to parse with hand-rolled tooling, but it requires the extension to be compiled and installed on the source server, and on managed Postgres services it's only available where the cloud provider has chosen to enable it.

The third option — using a different replication mechanism entirely (Bottled Water, Debezium connectors, etc.) — adds an operational dependency the project tenets explicitly want to avoid.

## Decision

Sluice's Postgres CDC reader will use `pgoutput`. Decoding will go through pgx's logical replication helpers (`pgconn.ReceiveMessage` plus the typed `pglogrepl` helpers), producing IR `Change` events per row event and per relation event.

Sluice will *not* attempt to install or detect `wal2json`. If a user's environment requires a different plugin, that's a future extension point on the engine's `Capabilities` (`CDC` could grow a sub-mode), not a runtime fallback.

## Consequences

Sluice works against any Postgres 10+ server — including every major managed service — without asking the user (or their DBA) to install an extension. This is a direct application of the "Contain Postgres complexity" tenet ([CLAUDE.md](../../CLAUDE.md)): we surface what we need (logical replication enabled, replication slot privileges, `wal_level=logical`) and stop there.

The cost is debuggability. `pgoutput`'s wire format is binary; raw inspection in production requires the streaming session itself rather than a tail-of-logs glance. We mitigate this with structured logging at the engine boundary — every decoded change emits a one-line summary at debug level, so operators can see *what* without needing to decode the wire bytes themselves.

If a future use case genuinely needs `wal2json` (richer change metadata, easier on-the-wire diagnostics), it can be added behind a capability flag without disturbing the default path.
