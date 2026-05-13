# Prep: Snapshot-to-CDC handoff

> **Status: SHIPPED in v0.1.0** — gapless cutover via `START TRANSACTION WITH CONSISTENT SNAPSHOT` (MySQL) and `EXPORT_SNAPSHOT` + `SET TRANSACTION SNAPSHOT` (PG).

Roadmap reference: [docs/dev/roadmap.md §4](../roadmap.md). Related: [ADR-0007: position persistence](../../adr/adr-0007-position-persistence.md), prior CDC work in [prep-mysql-cdc.md](prep-mysql-cdc.md) and [prep-postgres-cdc.md](prep-postgres-cdc.md).

## Goal

Close the seam between simple-mode bulk copy (roadmap §0–§1, shipped) and CDC streaming (§2–§3, shipped). Today the two run independently, so a continuous-sync migration has a *gap window* between the snapshot's `SELECT` and the CDC stream's start in which writes are silently lost. This chunk eliminates the gap by making the bulk copy and CDC stream share a single capture point: the bulk-copy reads run *as of* a known source position, and CDC resumes from *exactly* that position. Zero gap, zero duplicates, no manual reconciliation. This is the killer feature for low/zero-downtime migrations.

Out of scope for v1: change application to the target (`ir.ChangeApplier`). The integration test asserts that the emitted change stream has the right shape (no gap, no overlap) but does not actually apply the events. Wiring the applier and choosing conflict semantics is a separate chunk.

## Engine primitives

Both engines already expose the building blocks; this chunk wires them up.

**MySQL.** Inside one connection: `START TRANSACTION WITH CONSISTENT SNAPSHOT` (InnoDB opens a REPEATABLE-READ view), then immediately `SHOW BINARY LOG STATUS` (8.4+) or `SHOW MASTER STATUS` (older) for the file/pos. With GTIDs enabled, the same status query also returns `executed_gtid_set`. The transaction stays open for the lifetime of the bulk copy; SELECTs on this connection see the snapshot. CDC starts from the captured position on a *separate* binlog connection.

**Postgres.** `CREATE_REPLICATION_SLOT <name> LOGICAL pgoutput` is atomic — it creates the slot and exports a snapshot in one operation, returning `(slot_name, consistent_point LSN, snapshot_name)`. The replication-mode connection that ran the create *must remain open* for the snapshot name to be valid. A *second* (regular SQL) connection opens a transaction with `SET TRANSACTION SNAPSHOT '<snapshot_name>'` and runs the bulk-copy SELECTs. CDC streams on the replication connection from the consistent_point LSN.

Connection topologies differ — flagged here so readers don't expect symmetry:

```
MySQL                                    Postgres
─────                                    ────────
conn A (transaction):                    conn A (replication mode):
  START TX W/ CONSISTENT SNAPSHOT          CREATE_REPLICATION_SLOT
  SHOW BINARY LOG STATUS  → pos            → (slot, lsn, snapshot_name)
  SELECT ... (bulk copy)                   START_REPLICATION ... (CDC)

conn B (binlog protocol):                conn B (regular SQL):
  START_BINLOG_DUMP from pos               BEGIN
                                           SET TRANSACTION SNAPSHOT 'name'
                                           SELECT ... (bulk copy)
```

Notice the asymmetry: MySQL pairs (snapshot+SELECT) on one conn and CDC on another; Postgres pairs (slot+CDC) on one conn and SELECT on another. The IR shouldn't care — engines own this internally.

## IR surface

A new struct returned by a new engine method:

```go
// SnapshotStream pairs a snapshot-pinned RowReader with a CDCReader
// whose start position is the snapshot's logical capture point. The
// orchestrator runs the bulk-copy phase using Rows, then starts the
// continuous-sync phase using Changes — guaranteed no gap, no overlap.
//
// The two readers share an engine-internal connection lifecycle. Close
// releases everything; no other Close calls are needed.
type SnapshotStream struct {
    // Position is the source position the snapshot was taken at.
    // Surfaced for logging, position persistence (ADR-0007), and
    // operational sanity ("we resumed at LSN X").
    Position ir.Position

    // Rows reads from the source as it appeared at Position. The
    // implementation pins the read view via engine-specific means
    // (REPEATABLE READ tx + consistent snapshot for MySQL,
    // SET TRANSACTION SNAPSHOT '<name>' for Postgres).
    Rows ir.RowReader

    // Changes streams events that occurred *after* Position. Combined
    // with Rows, the union is the complete state plus the diff to
    // current — every row exactly once.
    Changes ir.CDCReader
}
```

```go
// New method on ir.Engine. Returns ErrNotImplemented for engines
// without CDC support. Closing the stream releases all resources.
type Engine interface {
    // ... existing methods ...
    OpenSnapshotStream(ctx context.Context, dsn string) (*SnapshotStream, io.Closer, error)
}
```

The `io.Closer` is returned alongside the struct so the orchestrator owns the cleanup explicitly. Returning a method on `*SnapshotStream` would also work; the io.Closer separation is a style choice — call out for review.

## Files to add / touch

New files:

- `internal/engines/mysql/cdc_snapshot.go` — wraps `START TRANSACTION WITH CONSISTENT SNAPSHOT` + status query + connection ownership; produces a `SnapshotStream` whose Rows read in the snapshot tx and Changes use a fresh binlog connection.
- `internal/engines/postgres/cdc_snapshot.go` — wraps `CREATE_REPLICATION_SLOT` + `SET TRANSACTION SNAPSHOT '<name>'`; produces a `SnapshotStream` whose Rows read in the snapshot tx (separate conn) and Changes stream on the replication conn.
- `internal/pipeline/streamer.go` (or extend `migrate.go` with a Mode flag — see open questions) — the new orchestrator phase: snapshot → bulk-copy via Rows → CDC via Changes → run until cancel.
- `internal/engines/mysql/cdc_snapshot_integration_test.go` and `internal/engines/postgres/cdc_snapshot_integration_test.go` — the no-gap proof tests (see below).
- `internal/pipeline/streamer_integration_test.go` — end-to-end same-engine snapshot+CDC test.

Modify:

- `internal/ir/interfaces.go` — add `OpenSnapshotStream` to `Engine`.
- `internal/engines/mysql/engine.go` and `internal/engines/postgres/engine.go` — implement the new method.
- `internal/engines/mysql/row_reader.go` — accept an externally-supplied `*sql.Tx` (or equivalent) so the snapshot path can pin reads to the existing transaction. Currently `RowReader` opens its own `*sql.DB` query; we'd factor that out.
- `internal/engines/postgres/row_reader.go` — same: accept an externally-supplied snapshot transaction.

The row-reader refactor is the largest secondary change; it's a small extraction of the "where does the SELECT run?" decision into a constructor parameter.

## Data flow sketch

```
[CDC user / orchestrator]
  eng.OpenSnapshotStream(ctx, dsn) → (stream, closer, err)
    │
    ▼
[engine internals]
  open snapshot conn
  ┌─ MySQL ──────────────┐    ┌─ Postgres ───────────────────┐
  │ START TX W/ CONSIST  │    │ pgconn.Connect(repl=database)│
  │ SHOW BINLOG STATUS   │    │ CREATE_REPLICATION_SLOT      │
  │ → pos                │    │ → (slot, lsn, snapshot_name) │
  │                      │    │ open second SQL conn         │
  │                      │    │ BEGIN; SET TX SNAPSHOT       │
  └──────────────────────┘    └──────────────────────────────┘
  return SnapshotStream{
      Position:  pos / lsn,
      Rows:      RowReader pinned to snapshot conn,
      Changes:   CDCReader configured to start from Position,
  }
    │
    ▼
[orchestrator]
  schema-apply (target)
  bulk-copy via stream.Rows.ReadRows(...)
  schema-apply phase 2 + 3 (target)
  for change := range stream.Changes.StreamChanges(ctx, stream.Position):
      → (future: ChangeApplier; v1: assertion-only)
  ...until ctx cancellation or user "stop"
  closer.Close()
```

## Position handoff semantics

The Position the SnapshotStream returns *is* a valid resume point for the CDCReader on its own. That's load-bearing because it lets the orchestrator persist the position (ADR-0007) the moment the snapshot is captured — before bulk-copy even starts. If the process dies during bulk copy, restart resumes from the persisted position by re-running the entire bulk-copy from scratch (same snapshot can't be re-acquired) and resuming CDC from the persisted position.

A subtler resume question: if bulk copy is partially done and we restart, do we resume bulk copy or restart it? *Restart it* — the snapshot's logical clock means re-running the bulk copy is idempotent at the row level (same tuples, same values), and the CDC stream will catch up any subsequent changes. Resumable bulk copy is roadmap §10. Not in scope here.

## Open questions for the user

1. **Orchestrator shape.** A new `pipeline.Streamer` type (separate from `Migrator`) vs. a `Mode` field on `Migrator` with `ModeSimple` / `ModeSnapshotPlusCDC`. *Recommendation:* `Mode` field — schema-apply + bulk-copy phases are shared, the only delta is "what runs after." Keeps one orchestrator path. Counter-argument: lifecycle differs (one-shot vs long-running), and `Migrator` taking a `ChangeApplier` complicates its constructor for the simple case. Lean Mode but flag for review.

2. **Cutover semantics.** When does CDC streaming stop? Options: (a) user runs `sluice sync stop` (separate command, requires per-process state). (b) ctx cancellation from CLI signal handling. (c) configurable max-events or max-duration. *Recommendation:* (b) for v1 — simplest, matches the existing `Migrator.Run(ctx)` shape. Operator hits Ctrl-C, the Streamer flushes its position, and exits. Per-stream state file is post-v1.

3. **Position persistence.** ADR-0007 mandates a control table on the target. v1 of this chunk could (a) skip persistence entirely and rely on stdout-printed positions, or (b) ship the control table as part of this chunk. *Recommendation:* (a) for this chunk — the control table is roadmap §5 with its own scope. The Streamer should print the captured Position prominently so a user can re-pass it manually for resume. §5 wires the persistent path.

4. **MySQL GTID vs file/pos at handoff.** The MySQL CDCReader supports both. The handoff captures whichever the source advertises. *Recommendation:* same auto-detection as the standalone CDC reader — GTID when `gtid_mode = ON`, file/pos otherwise. Caller doesn't need to know.

5. **Postgres: pre-existing slot.** If the slot already exists (from a previous run that wasn't cleanly torn down), `CREATE_REPLICATION_SLOT` errors. Should the snapshot path detect-and-reuse the existing slot, drop-and-recreate, or error? *Recommendation:* error with a clear message naming the slot — silent reuse loses the original consistent_point and silent drop is destructive. Operator deals with leftover slots explicitly. (The standalone PG CDC reader does the same thing today, by design.)

6. **The `io.Closer` separation** in `OpenSnapshotStream`'s return tuple. Returning `(*SnapshotStream, io.Closer, error)` keeps the closer explicit; alternative is `*SnapshotStream` with a `Close()` method. *Recommendation:* `Close()` method on the struct. Simpler caller code; mirrors how RowReader/CDCReader already behave.

## Anticipated rough edges

- **Snapshot lifetime on Postgres.** The exported snapshot is only valid for the duration of the connection that exported it, AND only valid for transactions that haven't yet seen any data. So `SET TRANSACTION SNAPSHOT` must happen as the very first thing in the bulk-copy connection's transaction. Document; verify in test.
- **MySQL transaction longevity.** A long-running `WITH CONSISTENT SNAPSHOT` transaction on a busy source pins the undo log and can stress the source (similar to a slow consumer). For the integration test this is fine; for production it's an operational consideration to surface in docs.
- **Postgres VACUUM and snapshot retention.** Same family of issue: while the snapshot exists, autovacuum can't reclaim tuples newer than the snapshot. Surface as a known limitation; not a v1 fix.
- **Bulk-copy connection pooling.** `RowReader` currently opens its own `*sql.DB` (pooled). Snapshot mode needs to bypass the pool and use the specific snapshot-bearing connection. The factor-out of "where does the query run" is the load-bearing refactor; mock it with a `Querier` interface so existing tests stay clean.
- **Error during bulk copy.** If bulk copy fails, the snapshot is still open (and the CDC slot still exists). Cleanup needs to release both. SnapshotStream.Close handles this, but the Streamer must call it on every error path.
- **Test coverage for "no gap" specifically.** The integration test must seed N rows, open the snapshot, insert row N+1 *while the snapshot is held*, then assert: bulk-copy emits 1..N (not N+1), CDC emits N+1 (not 1..N), and the union is exactly the right set. This is the test that proves the feature works as advertised.

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/roadmap.md §4, docs/adr/adr-0007-position-persistence.md, and docs/dev/notes/prep-snapshot-cdc-handoff.md. Propose the design before writing code: (1) the exact SnapshotStream struct and engine method signatures, (2) the row-reader refactor needed to accept an external transaction, (3) the orchestrator integration (Mode flag on Migrator vs separate Streamer), (4) the no-gap integration test shape on each engine. Note any deviation from the prep doc with a why. Stop after the design for review."
