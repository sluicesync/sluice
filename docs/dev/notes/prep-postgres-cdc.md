# Prep: Postgres CDC reader

Roadmap reference: [docs/dev/roadmap.md §3](../roadmap.md). Related: [ADR-0006: pgoutput over wal2json](../../adr/adr-0006-pgoutput.md), [ADR-0007: position persistence](../../adr/adr-0007-position-persistence.md).

## Goal

Implement `OpenCDCReader` on the Postgres `Engine` so that source-side row changes flow as `ir.Change` events to a downstream applier. Initial scope: streaming from "now" (no snapshot/CDC handoff yet — that's roadmap §4). Uses the built-in `pgoutput` plugin, no extension install.

## Library

pgx already lives in `go.mod` (we use `pgx/v5/stdlib` for the row reader and writer). For CDC we need **the native pgx interface**, specifically:

- `github.com/jackc/pgx/v5/pgconn` — for the underlying replication connection (cannot be done through `database/sql`; the replication protocol bypasses it).
- `github.com/jackc/pglogrepl` — pgx's helper package for parsing the `pgoutput` binary protocol into typed messages (`InsertMessage`, `UpdateMessage`, `DeleteMessage`, `RelationMessage`, `BeginMessage`, `CommitMessage`).

No new top-level dependency; both are already in pgx's module space. Verify with `go list -m all | grep pglogrepl` once we add the import.

## Files to add / touch

New files in `internal/engines/postgres/`:

- `cdc_reader.go` — `CDCReader` struct, `StreamChanges`, the message-loop.
- `cdc_position.go` — encode/decode `ir.Position.Token` for an LSN plus a slot name.
- `cdc_relations.go` — relation cache populated from `RelationMessage` events; maps OIDs to IR types via the existing `internal/engines/postgres/types.go` translator.
- `cdc_reader_test.go` — unit tests for position encoding, OID-to-IR mapping, message dispatch.
- `cdc_reader_integration_test.go` — `//go:build integration`, testcontainers Postgres started with `wal_level=logical`, exercises end-to-end change capture.

Modify:

- `engine.go` — replace the `OpenCDCReader` stub with the real implementation. Add a precondition check that reads `SHOW wal_level` and surfaces a clear error if it's not `logical`.
- `capabilities.go` (or wherever `capabilities` is declared in the postgres package) — declare `CDC: ir.CDCLogical` (may need to add the `CDCLogical` enum value to `internal/ir/capabilities.go`; currently the enum has `CDCNone`, `CDCBinlog`, `CDCWalLogical?`, `CDCTriggers` — verify the exact name and add if missing).

## Data flow sketch

```
[CDC user]
  StreamChanges(ctx, ir.Position{Engine:"postgres", Token:"<slot>/<lsn>"})
    │
    ▼
[CDCReader]
  decode token → connect via pgconn with replication=database
  ensure publication exists (CREATE PUBLICATION sluice_pub FOR ALL TABLES — idempotent)
  ensure replication slot exists (CREATE_REPLICATION_SLOT, captures snapshot+LSN)
  START_REPLICATION SLOT sluice_slot LOGICAL <start_lsn> ("proto_version" '2', "publication_names" 'sluice_pub')
    │
    ▼ (XLogData messages)
  for msg in pglogrepl.ParseXLogData(...):
      switch msg.(type):
          *RelationMessage   → populate relations cache (OID → schema/table/column types)
          *BeginMessage      → start tx scope (capture begin LSN)
          *InsertMessage     → emit ir.Insert (look up relation by OID, decode tuple)
          *UpdateMessage     → emit ir.Update (Before optional based on REPLICA IDENTITY)
          *DeleteMessage     → emit ir.Delete (same)
          *CommitMessage     → end tx scope; advance the durable position
          *TruncateMessage   → emit ir.Truncate
          *TypeMessage       → custom type registration (rare)
    │
    ▼
[out chan ir.Change]
  + periodic StandbyStatusUpdate to acknowledge progress and keep the slot from blocking WAL recycling
```

The `pglogrepl.SendStandbyStatusUpdate` keepalive is **load-bearing** — without it the slot holds WAL forever and the disk fills. Send it on a ticker (e.g. every 10s) and after every committed batch.

## Position encoding

Postgres CDC needs both a slot name and a confirmed-flush LSN to resume cleanly:

```go
type pgPos struct {
    Slot string  // "sluice_slot" by default
    LSN  pglogrepl.LSN  // last confirmed-applied LSN
}
// encodePGPos / decodePGPos via ir.Position.Token. Slot/LSN format like "sluice_slot/0/16B7350".
```

The slot has to exist on the server for the LSN to be usable; resume should detect a missing slot and surface a clear error rather than silently re-creating one (which would skip changes).

## Preconditions to surface

Before any streaming starts, the reader should verify and clearly report on:

1. `wal_level = logical` (server config; needs server restart to change).
2. Connecting role has the `REPLICATION` privilege.
3. `max_replication_slots > 0` and there's room for one more slot.
4. (Soft) The publication and slot exist or can be created.

Each precondition failure should produce a one-line error naming the specific GUC/role and what to fix. The "Contain Postgres complexity" tenet says we surface, not hide.

## Open questions for the user

1. **Publication scope.** "FOR ALL TABLES" vs explicit table list. *Recommendation:* default to ALL TABLES; expose a config field for an explicit list (later).
2. **Slot naming.** Default `sluice_slot` is fine; expose an override for environments running multiple sluice streams against the same source.
3. **Slot lifecycle.** Always leave the slot in place across restarts (resume works), or drop on clean shutdown? *Recommendation:* leave by default (resume), expose `--drop-slot-on-exit` for one-off use cases.
4. **REPLICA IDENTITY policy.** For tables without a primary key, `REPLICA IDENTITY FULL` is required to get useful Before images on UPDATE/DELETE. Sluice could verify this at startup and warn, or just stream what it gets. *Recommendation:* warn loudly at startup for any table without a PK and without `REPLICA IDENTITY FULL`; do not silently miss data.
5. **TOAST values.** Unchanged TOASTed columns come through as a sentinel marker, not the actual value. Sluice's IR contract for `Update` doesn't model this today. *Recommendation:* require `REPLICA IDENTITY FULL` if TOAST handling matters; otherwise treat unchanged-toast columns as "preserve target's existing value" which the applier handles via partial-row UPDATE.
6. **Protocol version.** `pgoutput` v2 is widely supported (Postgres 14+); v1 is a fallback. *Recommendation:* v2; require Postgres 14+ for CDC (document this).

## Anticipated rough edges

- **Slot creation requires `replication=database` mode** — a different connection string flag. Two connections will be open at once: one in normal mode (for one-time `CREATE PUBLICATION` etc.), one in replication mode (for `START_REPLICATION`).
- **Custom types** (`CREATE TYPE ... AS ENUM`, composite types) trigger `TypeMessage` events. Decoding requires either a server-side lookup or assuming the type is one we already know. v1 can ignore unknown types and emit a warning per type.
- **`pgx`'s `database/sql` connection cannot be repurposed** for replication — the replication protocol takes over the connection. Open a new `pgconn.PgConn` directly.
- **Long-running transactions on the source** delay slot advancement and can pin WAL. This is operational rather than a bug; surface in docs.

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/roadmap.md §3, docs/adr/adr-0006-pgoutput.md, docs/adr/adr-0007-position-persistence.md, and docs/dev/notes/prep-postgres-cdc.md. Before writing code, verify pglogrepl is available in pgx v5 and propose: (1) the CDCReader struct shape, (2) the position encoding format, (3) the relations-cache structure mapping OIDs to ir.Type, (4) the keepalive and acknowledgment cadence. Note any deviation from the prep doc with a why. Stop after the design for review."
