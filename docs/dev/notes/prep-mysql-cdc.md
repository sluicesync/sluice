# Prep: MySQL CDC reader

Roadmap reference: [docs/dev/roadmap.md §2](../roadmap.md). Related: [ADR-0007: position persistence](../../adr/adr-0007-position-persistence.md).

## Goal

Implement `OpenCDCReader` on the MySQL `Engine` so that source-side row changes (INSERT/UPDATE/DELETE) flow as `ir.Change` events to a downstream applier. Initial scope: streaming from "now" (no snapshot/CDC handoff yet — that's roadmap §4). DDL events are surfaced but ignorable by the applier for v1.

## Library choice

**`github.com/go-mysql-org/go-mysql`** (formerly siddontang/go-mysql). It's the mature Go binlog client used by canal, TiDB DM, and most Go CDC tooling. The relevant sub-packages:

- `github.com/go-mysql-org/go-mysql/replication` — the binlog event syncer.
- `github.com/go-mysql-org/go-mysql/canal` — a higher-level wrapper that includes a schema cache and event-callback hooks. Worth evaluating, but it's opinionated about how it manages the schema cache; we may prefer the lower-level `replication` package for finer control.

Recommendation: start with the lower-level `replication.BinlogSyncer`. The event types we care about (`RowsEvent`, `TableMapEvent`, `QueryEvent` for DDL, `XIDEvent` for transaction commits) are clearly typed there.

## Files to add / touch

New files in `internal/engines/mysql/`:

- `cdc_reader.go` — `CDCReader` struct, the `StreamChanges` method, the binlog→IR event loop.
- `cdc_position.go` — encode/decode `ir.Position.Token` for binlog file+pos and GTID.
- `cdc_reader_test.go` — unit tests for position encoding and event-mapping helpers (no Docker).
- `cdc_reader_integration_test.go` — `//go:build integration`, testcontainers MySQL, exercises end-to-end change capture (INSERT/UPDATE/DELETE on a table, assert events arrive).

Modify:

- `engine.go` — replace the `OpenCDCReader` stub returning `ErrNotImplemented` with the real implementation.
- `flavor.go` — `FlavorVanilla` already declares `CDC: ir.CDCBinlog`; no change. `FlavorPlanetScale` stays `CDCNone` (PlanetScale doesn't expose binlog directly).
- `go.mod` / `go.sum` — `go get github.com/go-mysql-org/go-mysql@latest`.

## Data flow sketch

```
[CDC user]
  StreamChanges(ctx, ir.Position{Engine:"mysql", Token:"<file>/<pos>" or "<gtid set>"})
    │
    ▼
[CDCReader]
  decode token → BinlogSyncer.StartSync(...) (file/pos) or StartSyncGTID(...)
    │
    ▼ (events)
  for ev in syncer.GetEvent(ctx):
      switch ev.Header.EventType:
          ROTATE / FORMAT_DESCRIPTION  → update internal state (file name, etc.)
          TABLE_MAP_EVENT              → cache TableMap by table_id
          WRITE_ROWS_EVENTv1/v2        → emit ir.Insert per row
          UPDATE_ROWS_EVENTv1/v2       → emit ir.Update per row pair
          DELETE_ROWS_EVENTv1/v2       → emit ir.Delete per row
          QUERY_EVENT (BEGIN/COMMIT/DDL) → ignore BEGIN/COMMIT; surface DDL via TODO hook
          XID_EVENT                     → transaction boundary marker
    │
    ▼
[out chan ir.Change]
```

Per row, mapping the binlog row (which is `[]any` indexed by column position) to `ir.Row` (keyed by column name) requires the column names — that comes from a schema cache, which we populate by calling the existing `SchemaReader` once at start and refreshing on DDL events.

## Position encoding

`ir.Position.Token` is opaque to the IR. For MySQL we propose a small typed wrapper internal to the engine:

```go
type binlogPos struct {
    File   string  // "mysql-bin.000123"
    Pos    uint32  // byte offset
    GTIDs  string  // optional; preferred when source has GTIDs enabled
}
// encodeBinlogPos / decodeBinlogPos round-trip via ir.Position.Token.
// Use a small JSON or custom delimited form; keep it stable so resume across versions works.
```

Recommendation: prefer GTID when the source has `gtid_mode = ON`; fall back to file/pos otherwise. Surface which mode is active at `StreamChanges` start so logs are clear.

## Open questions for the user

These need a decision before significant code lands. Each has a recommended default.

1. **GTID vs file/pos as primary.** GTIDs survive failover cleanly but require server-side configuration. *Recommendation:* support both; auto-detect at start via `SHOW VARIABLES LIKE 'gtid_mode'`.
2. **Schema cache invalidation strategy.** Easiest: on every DDL event, re-read the affected table from `information_schema`. More accurate: parse the DDL. *Recommendation:* re-read; parsing DDL is regex-over-strings territory which the project tenets explicitly avoid.
3. **DDL handling.** For v1, emit a `TODO` `Change` variant or just log and continue? *Recommendation:* log at info level, do not emit a `Change` variant yet; revisit when we have a real "apply DDL on the target" use case (post snapshot+CDC).
4. **Filtering surface.** `--include-table foo,bar` flag now or later? *Recommendation:* later (roadmap §10). For v1 the reader emits everything; the applier can filter if needed.
5. **PlanetScale's change feed.** Out of scope here — `FlavorPlanetScale` declares `CDCNone` and that's the right answer for now.

## Anticipated rough edges

- **JSON column events** in MySQL binlog are encoded with a partial-update protocol that go-mysql decodes opaquely. Confirm the decoded value lands as `[]byte` matching the IR JSON contract.
- **DECIMAL** types in binlog use a packed binary form; go-mysql handles decoding but the resulting Go value type may need an extra step to match the IR's string-form decimal contract.
- **Time zones for TIMESTAMP** — MySQL stores TIMESTAMP in UTC and the connection's `time_zone` setting affects display, not storage. Binlog events should give us the UTC value; verify in the integration test.
- **Sub-second precision** (`TIMESTAMP(6)`) — go-mysql exposes fractional seconds; confirm round-trip.
- **`REPLICATION SLAVE` / `REPLICATION CLIENT`** privileges are required on the source. Surface this as a startup precondition check; current readers assume `SELECT` is enough.

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/roadmap.md §2, docs/adr/adr-0007-position-persistence.md, and docs/dev/notes/prep-mysql-cdc.md. Propose a design before writing code: (1) the exact CDCReader struct shape and its field doc-comments, (2) the position encoding format, (3) the event-loop dispatch table from binlog event types to ir.Change variants, (4) the schema-cache invalidation flow on DDL events. Stop after the design; I'll review and approve before you implement."
