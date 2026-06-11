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

	// SnapshotName is the engine's SHAREABLE exported-snapshot name —
	// the handle other connections pass to the engine's
	// [SnapshotImporter] to observe the EXACT same consistent view as
	// Rows. Postgres populates it from CREATE_REPLICATION_SLOT …
	// EXPORT_SNAPSHOT (the `snapshot_name`); MySQL and the Vitess
	// VStream leave it empty because their snapshots are per-session /
	// single-stream and not shareable across connections.
	//
	// It is the capability gate for the fast parallel cold-start
	// (ADR-0079): an empty value means "not shareable → the cold-start
	// copy stays serial". Treated like Position — an additive, optional
	// field the engine fills in; readers that don't recognise it ignore
	// it.
	SnapshotName string

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

	// CommitFn, when non-nil, is called by the orchestrator exactly
	// once after the backup's final manifest commit succeeds — i.e.
	// the moment the backup becomes a durable, complete chain root.
	// Engines use it to persist run-scoped resources that must
	// outlive a SUCCESSFUL run but not a failed one: the Postgres
	// --chain-slot path keeps the persistent chain slot (anchored at
	// Position) instead of dropping it in Close. After Commit, Close
	// must still be called; it releases connections but skips
	// dropping committed resources. nil when the engine has nothing
	// to persist (the default temporary-anchor shape).
	CommitFn func(ctx context.Context) error
}

// Close releases the snapshot transaction and the underlying
// connections. After Close, Rows is unusable.
func (s *BackupSnapshot) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
}

// Commit persists run-scoped resources that should outlive a
// successful backup (see CommitFn). Safe to call when no CommitFn is
// set (no-op).
func (s *BackupSnapshot) Commit(ctx context.Context) error {
	if s == nil || s.CommitFn == nil {
		return nil
	}
	return s.CommitFn(ctx)
}

// PositionMonotonicChecker is the OPTIONAL engine surface the
// inline-rotation FSM (ADR-0046 §2) uses to enforce its load-bearing
// `S >= P_N` hard-fail assertion: the freshly-opened next segment's
// snapshot anchor S must be at or after the prior segment's last
// committed incremental position P_N, or writes in (S, P_N) would be
// silently gapped out of the new segment.
//
// The invariant holds BY CONSTRUCTION when the snapshot is opened on
// the same in-flight source handle as the CDC pump (the source is
// position-monotonic). This interface is the DEFENSIVE assertion
// against a buggy / lying snapshot opener: PrecedesOrEqual reports
// whether a <= b in the engine's native position order (PG LSN
// numeric order; MySQL GTID-set containment). The FSM hard-aborts the
// rotation — staying on the still-open prior segment, never gapping —
// when the engine implements this and reports the anchor regressed.
//
// Engines that don't implement it fall back to the structural
// same-handle guarantee plus the FSM's non-empty / engine-match
// assertion (both still loud refusals). PG implements it via its LSN
// comparator.
type PositionMonotonicChecker interface {
	// PrecedesOrEqual reports whether position a is at or before
	// position b in this engine's native change-stream order. Both
	// positions must have been produced by this engine. A malformed /
	// cross-engine position is a non-nil error (the FSM treats an
	// error as "cannot prove monotonic → hard-abort", loud-failure).
	PrecedesOrEqual(a, b Position) (bool, error)
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
// The options' SlotName is honoured by engines with a slot concept
// (Postgres: by default a temporary anchor slot — distinct from the
// chain-handoff slot — pins the snapshot; the chain slot name is
// recorded on the manifest's EndPosition so a Phase 3 incremental
// against this manifest opens CDC against the correct chain-handoff
// slot, even though the temporary backup slot has long been dropped).
// MySQL ignores it — the binlog stream is the slot.
type BackupSnapshotOpener interface {
	OpenBackupSnapshot(ctx context.Context, dsn string, opts BackupSnapshotOptions) (*BackupSnapshot, error)
}

// BackupSnapshotOptions carries the engine-facing knobs for opening a
// backup snapshot. A zero value preserves the historical defaults
// (engine default slot name, temporary anchor slot dropped at Close).
type BackupSnapshotOptions struct {
	// SlotName is the chain-handoff replication-slot name recorded on
	// the snapshot Position for engines with a slot concept. Empty
	// falls back to the engine default (`sluice_slot` on Postgres).
	// Engines without slots (MySQL) ignore it.
	SlotName string

	// PersistChainSlot, when true on engines with a slot concept,
	// anchors the snapshot on the PERSISTENT chain slot itself
	// (named SlotName) instead of a temporary anchor slot, and keeps
	// it once the backup commits ([BackupSnapshot.CommitFn]). The
	// slot's consistent point then IS the manifest's EndPosition, so
	// `backup incremental` chains with zero gap by construction —
	// without it, the operator must create and maintain the chain
	// slot themselves BEFORE the full backup (a late-created slot
	// cannot serve the WAL between the full's anchor and its own
	// creation; see [ChainResumePreflighter]).
	//
	// The cost is source-side WAL retention: an unconsumed slot
	// retains WAL until the next incremental advances it. Engines
	// without a slot concept log a loud no-op and ignore the field.
	PersistChainSlot bool
}

// ChainResumePreflighter is the OPTIONAL engine surface the
// incremental-backup orchestrator uses to verify, BEFORE opening CDC,
// that the engine can actually serve the chain from the parent
// manifest's terminal position. Engines with server-side consumer
// state (Postgres replication slots) implement it; engines whose
// resume needs no server-side cursor (MySQL: binlog position is
// client-side, pruning is detected loudly at stream open) omit it.
//
// The Postgres implementation refuses loudly when:
//   - the slot named in `from` does not exist (it may never have been
//     created — the chain anchor records where the next incremental
//     must start, but only a standing slot retains the WAL to serve
//     it), or
//   - the slot's confirmed_flush_lsn is AHEAD of `from` (a slot
//     created or advanced after the parent backup: the WAL in
//     between is not retained, and PostgreSQL would silently
//     fast-forward the stream past it — the silent-loss shape this
//     preflight converts into a loud refusal).
type ChainResumePreflighter interface {
	PreflightChainResume(ctx context.Context, dsn string, from Position) error
}

// TableScopedBackupSnapshotOpener is the OPTIONAL engine surface for
// opening a backup-scoped consistent snapshot whose snapshot COPY is
// restricted to a table allowlist. It is to [BackupSnapshotOpener] what
// [TableScopedSnapshotOpener] is to [Engine.OpenSnapshotStream]: the same
// snapshot contract, but with the COPY scoped to the included tables.
//
// An empty/nil tables slice means "all tables" — identical behaviour to
// [BackupSnapshotOpener.OpenBackupSnapshot]. A non-empty slice scopes the
// backup snapshot's VStream COPY filter to those UNQUALIFIED table names,
// so a large unrelated table in the same keyspace is never streamed or
// buffered (the ADR-0071 multi-table-interleaving buffer overflow). The
// semantics match [TableScopedSnapshotOpener.OpenSnapshotStreamForTables]
// exactly.
//
// Only engines that over-stream by default need to implement it (today:
// the MySQL PlanetScale/VStream flavor). The full-backup orchestrator
// type-asserts on this surface and prefers it when a table scope is in
// effect; engines that don't implement it fall back to the base
// [BackupSnapshotOpener.OpenBackupSnapshot] (Postgres and vanilla MySQL
// read per-table and never over-stream, so the scope is a no-op there).
type TableScopedBackupSnapshotOpener interface {
	OpenBackupSnapshotForTables(ctx context.Context, dsn string, opts BackupSnapshotOptions, tables []string) (*BackupSnapshot, error)
}
