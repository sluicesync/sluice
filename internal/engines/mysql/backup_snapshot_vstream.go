// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// # PlanetScale backup snapshot via VStream COPY mode (GitHub issue #16)
//
// Before v0.44.0, [Engine.OpenBackupSnapshot] used a single shared
// path for all MySQL flavors: open a `*sql.Conn`, run
// `START TRANSACTION WITH CONSISTENT SNAPSHOT`, capture the position
// via `@@global.gtid_executed`, encode as [binlogPos] shape
// (`{"mode":"gtid","gtid_set":"..."}`). For vanilla MySQL this is
// correct; for PlanetScale / Vitess sources, two things go wrong:
//
//  1. **Wrong position encoding.** The live continuous-sync path uses
//     the VStream CDC reader which only understands [shardGtid] slice
//     positions (`[{"keyspace":"...","shard":"-","gtid":"..."}]`).
//     The binlog-shape position the backup wrote can't be decoded by
//     the VStream reader — `sluice backup stream run` and
//     `sluice backup incremental` failed immediately with
//     "json: cannot unmarshal object into Go value of type
//     []mysql.shardGtid" when trying to resume from the manifest's
//     end position. Chain-resume was completely broken on PS sources.
//  2. **Snapshot semantics that don't generalise.** vtgate routes
//     `START TRANSACTION WITH CONSISTENT SNAPSHOT` to a single tablet.
//     For an unsharded keyspace this happens to give a per-tablet
//     snapshot. For a sharded keyspace the snapshot covers only one
//     shard's view — the other shards' rows are read from their own
//     unsynchronised local clocks, breaking cross-shard consistency.
//
// v0.44.0's fix delegates the PlanetScale path to VStream COPY mode.
// VStream COPY runs an internal table-copy phase across all shards
// with the same gRPC stream sluice's live-sync path already uses,
// captures the terminal vgtid (multi-shard-aware), and exposes
// table-by-table reads from an in-memory row buffer via the same
// [vstreamSnapshotRows] reader the cold-start path uses.
//
// Architecture:
//
//   - Engine.OpenBackupSnapshot branches on flavor at v0.44.0.
//     PlanetScale flavor delegates here; vanilla MySQL keeps the
//     pre-existing pinned-conn + START TRANSACTION path.
//   - This function calls [Engine.openVStreamSnapshotStream] — the
//     same code path the live-sync orchestrator uses. The returned
//     SnapshotStream's Position, Rows, and CloseFn are everything
//     [irbackup.Snapshot] needs; the Changes channel is ignored
//     (backup doesn't consume CDC, it just records the position so
//     a downstream incremental can resume from there).
//   - The gRPC stream stays open until BackupSnapshot.Close fires
//     (mapped to vstreamSnapshotStream.close), at which point it's
//     cancelled and the underlying conn closed. No leak.
//
// Memory: VStream COPY buffers all rows for all tables in memory
// before returning. This matches the live-sync's existing trade-off
// (see openVStreamSnapshotStream's docstring) — sluice's v1 simple-
// mode workloads fit well in memory; sharded / very large tables
// would need a streaming variant in a future revision.
//
// Multi-shard consistency: VStream COPY captures all shards under a
// single logical stream; the terminal vgtid encodes per-shard
// positions. Incrementals resume from this vgtid and pick up each
// shard's CDC from its own captured position — exactly the contract
// the live-sync path established (and the contract Vitess RFC
// vitessio/vitess#6277 documents).

// openBackupSnapshotVStream is the PlanetScale-flavor implementation
// of [Engine.OpenBackupSnapshot] / [Engine.OpenBackupSnapshotForTables].
// Opens a VStream COPY-mode snapshot stream (the same mechanism the
// live-sync coldStart path uses) and wraps the result in an
// [irbackup.Snapshot] for the backup orchestrator.
//
// The COPY phase is drained CONCURRENTLY, not synchronously: ADR-0071
// made the COPY pump a goroutine that finalises the snapshot's terminal
// VGTID only once the backup row sweep has consumed every table's rows.
// So the anchor position is NOT known at constructor return — it is the
// zero value there. The orchestrator recovers the real anchor after the
// sweep via [irbackup.Snapshot.FinalizePositionFn], which joins the COPY
// completion barrier before reading the finalised Position (the empty-
// EndPosition chain-root bug; same race #243 the cold-start handoff
// fixed, overlooked on the backup path).
//
// There is no slotName parameter — VStream doesn't expose a slot concept
// to clients (the underlying binlog position is in the captured vgtid),
// so the callers' slotName is dropped before reaching here.
//
// The returned BackupSnapshot.Position is encoded as a VStream
// [shardGtid] slice (`[{"keyspace":"...","shard":"-","gtid":"..."}]`),
// which is the format incremental backups + `sluice backup stream run`
// expect for chain-resume on PlanetScale sources.
//
// Errors from the COPY phase surface as the snapshot-open error.
// Resource cleanup on error path: snapshot is closed before return,
// so no goroutine or connection leaks. Caller closes the returned
// snapshot to commit lifecycle (closes the gRPC stream + cancels
// the stream context).
//
// tables scopes the VStream COPY filter (vstreamCopyFilterRules):
// empty/nil copies every table in the keyspace; a non-empty allowlist
// restricts vtgate's COPY to those unqualified table names so a large
// unrelated table in the same keyspace is never streamed/buffered (the
// ADR-0071 multi-table-interleaving overflow). This is the backup-path
// counterpart to the cold-start [Engine.OpenSnapshotStreamForTables]
// scope — both seed a fresh scoped snapshot via
// [openVStreamSnapshotStreamFrom] with a nil start cursor.
func (e Engine) openBackupSnapshotVStream(ctx context.Context, dsn string, tables []string) (*irbackup.Snapshot, error) {
	if len(tables) > 0 {
		slog.InfoContext(ctx, "mysql/vstream: backup snapshot: scoping COPY to included tables",
			slog.Int("table_count", len(tables)))
	}
	// A nil start cursor produces a fresh from-beginning snapshot; the
	// tables arg scopes the COPY filter exactly as the cold-start
	// OpenSnapshotStreamForTables path does.
	snap, err := e.openVStreamSnapshotStreamFrom(ctx, dsn, nil, tables)
	if err != nil {
		return nil, fmt.Errorf("mysql: backup snapshot (vstream): %w", err)
	}

	// SnapshotStream.Changes is the CDC-phase pump that the backup
	// orchestrator doesn't consume — we drop the reference. The
	// underlying gRPC stream stays open (the pump goroutine never
	// starts since startPump is only called via Changes.StreamChanges),
	// and CloseFn cleans it up when BackupSnapshot.Close fires.
	//
	// snap.Position is the ZERO value here: the concurrent COPY pump
	// (ADR-0071) finalises the terminal VGTID only after the backup sweep
	// drains every row. FinalizePositionFn recovers it post-sweep — it
	// joins the copy-completion barrier (copyDone), which the pump closes
	// only AFTER writing the finalised Position under mu (#243 / ADR-0095),
	// so the join happens-before our Position read. The finalised value is
	// the encodeVStreamPos VGTID (unsharded finishCopy / sharded
	// finishCopyAutoShard stitched-min) — the encoding
	// incremental / `backup stream run` chain-resume decodes, NOT the
	// binlog-shape CaptureBackupPosition fallback (GitHub #16).
	return &irbackup.Snapshot{
		Position: snap.Position,
		Rows:     snap.Rows,
		CloseFn:  snap.CloseFn,
		FinalizePositionFn: func(ctx context.Context) (ir.Position, error) {
			if err := snap.WaitCopyComplete(ctx); err != nil {
				return ir.Position{}, fmt.Errorf("mysql: backup snapshot (vstream): finalize position: %w", err)
			}
			return snap.Position, nil
		},
	}, nil
}
