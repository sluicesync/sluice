// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// SnapshotStream pairs a snapshot-pinned [RowReader] with a [CDCReader]
// whose start position is the snapshot's logical capture point. The
// orchestrator runs the bulk-copy phase using Rows, then starts the
// continuous-sync phase by calling Changes.StreamChanges(ctx, Position).
//
// Lifecycle: the engine that produced the stream owns the underlying
// connections and the snapshot transaction. Callers MUST call Close
// once Rows has been fully drained and CDC is either complete or no
// longer needed. Calling Close on Rows or Changes individually is a
// no-op — the stream's Close is the operative cleanup.
//
// Optional early release: when bulk-copy completes long before CDC
// does, callers SHOULD call ReleaseRows to commit the snapshot
// transaction and close the import-side connections immediately. The
// CDC reader runs on its own connection (and the slot's logical
// position is independent of the exporting transaction), so the
// snapshot tx is no longer load-bearing once Rows has been fully
// drained. Without this, on Postgres the snapshot tx sits as `idle in
// transaction` for the entire CDC lifetime — holding AccessShareLock
// on every snapshotted table and blocking ALTER on the source
// (Bug 21). Close is still required after CDC ends; ReleaseRows just
// frees the import-side resources earlier. Both methods are
// idempotent.
type SnapshotStream struct {
	// Position is the source position at which the snapshot was
	// captured. Surfaced for logging and as a manual resume point
	// until persistent position storage lands (roadmap §5).
	Position Position

	// Rows reads the source as it appeared at Position. The
	// implementation pins the read view via engine-specific means
	// (REPEATABLE-READ tx + WITH CONSISTENT SNAPSHOT for MySQL,
	// SET TRANSACTION SNAPSHOT '<name>' for Postgres).
	Rows RowReader

	// Changes streams events that occurred *after* Position. Combined
	// with Rows, the union covers every row exactly once: bulk-copy
	// captures pre-snapshot state, CDC captures post-snapshot deltas.
	Changes CDCReader

	// CloseFn is the engine-supplied cleanup closure. Set by the
	// engine's OpenSnapshotStream. Callers invoke it via the Close
	// method; direct access exists only so engines can populate it.
	CloseFn func() error

	// ReleaseRowsFn is the engine-supplied cleanup closure for the
	// import-side connections (snapshot tx + pinned read conn +
	// slot-creation replication conn on Postgres). Optional —
	// engines without a separable bulk-read lifecycle can leave it
	// nil and rely on CloseFn alone. Engines that set it MUST make
	// the closure idempotent so a CloseFn that internally calls
	// it as a safety net doesn't double-rollback.
	ReleaseRowsFn func() error
}

// Close releases the snapshot transaction and the underlying
// connections. After Close, Rows and Changes are unusable.
func (s *SnapshotStream) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
}

// ReleaseRows commits the snapshot transaction and closes the
// import-side connections, leaving the CDC reader to run on its own
// connection. Call this after bulk-copy completes and Rows is fully
// drained but BEFORE CDC streaming starts (or anytime CDC is still
// running). On engines without a separable bulk-read lifecycle this
// is a no-op. Idempotent — safe to call before Close, or to call
// twice.
func (s *SnapshotStream) ReleaseRows() error {
	if s == nil || s.ReleaseRowsFn == nil {
		return nil
	}
	return s.ReleaseRowsFn()
}

// BackupSnapshot is the lighter-weight cousin of [SnapshotStream] used
// by the full-backup orchestrator (v0.18.0, "snapshot-anchored
// EndPosition"). Whereas [SnapshotStream] pairs a snapshot-pinned
// RowReader with a CDCReader for the migrate / sync-start cold-start
// path, [BackupSnapshot] only provides the snapshot-pinned RowReader
// — backups don't open CDC, they just need consistent reads across
// every table plus the snapshot's anchor position.
//
// Position is the source position at which the snapshot was captured.
// For Postgres this is the `consistent_point` LSN returned by
// CREATE_REPLICATION_SLOT EXPORT_SNAPSHOT; for MySQL it's
// `@@global.gtid_executed` (or `(file, pos)` in non-GTID mode)
// captured inside the snapshot transaction. The full-backup
// orchestrator records this on [Manifest.EndPosition] — a Phase 3
// incremental chained off the manifest opens CDC at this position,
// and because the snapshot read covers everything up to the same
// boundary, no source writes during the backup window are silently
// dropped.
//
// Rows reads the source as it appeared at Position. The
// implementation pins the read view via engine-specific means:
//   - Postgres: a separate temporary replication slot creates an
//     exported snapshot; one or more pinned `*sql.Conn` values
//     `SET TRANSACTION SNAPSHOT '<name>'` to import the same view.
//     N-conn parallel reads are possible here in a future revision.
//   - MySQL: a single pinned `*sql.Conn` running
//     `START TRANSACTION WITH CONSISTENT SNAPSHOT`. All table reads
//     run on this one connection sequentially — MySQL's snapshot is
//     per-session and not shareable, so multi-conn parallel reads
//     are not available on this path.
//
// CloseFn is the engine-supplied cleanup closure: drops the
// temporary slot (PG), commits the snapshot tx, closes the pinned
// conn(s), and closes the underlying DB pool. The orchestrator calls
// it via Close once the row sweep completes.
type BackupSnapshot struct {
	Position Position
	Rows     RowReader
	CloseFn  func() error
}

// Close releases the snapshot transaction and the underlying
// connections. After Close, Rows is unusable.
func (s *BackupSnapshot) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
}

// BackupSnapshotOpener is the optional engine surface for opening a
// backup-scoped consistent snapshot. The full-backup orchestrator
// type-asserts on this method when starting a run; engines that
// implement it get cross-table snapshot consistency plus a
// snapshot-anchored [Manifest.EndPosition], engines that don't fall
// back to the v0.17.x shape (per-table independent reads + end-of-
// backup [BackupPositionCapturer] capture) with a soft warning about
// the during-backup write-window gap.
//
// Lives in the [ir] package so engine packages can implement it
// without forming an import cycle through the pipeline package's
// integration tests. The full-backup orchestrator's only direct
// dependency on the engine surface is this interface — engines wire
// the implementation onto whichever value is convenient (today: the
// engine struct itself, parallel to [Engine.OpenSnapshotStream]).
//
// The slotName argument is honoured by engines with a slot concept
// (Postgres: the temporary slot used to anchor the snapshot is
// distinct from the chain-handoff slot; the slot name is recorded on
// the manifest's EndPosition so a Phase 3 incremental against this
// manifest opens CDC against the correct chain-handoff slot, even
// though the temporary backup slot has long been dropped). MySQL
// ignores it — the binlog stream is the slot.
type BackupSnapshotOpener interface {
	OpenBackupSnapshot(ctx context.Context, dsn, slotName string) (*BackupSnapshot, error)
}
