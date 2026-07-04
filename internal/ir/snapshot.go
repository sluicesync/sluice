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

	// AbandonFn is the OPTIONAL engine-supplied closure for the
	// "this cold start will never consume the stream" exit: it must
	// release everything CloseFn releases AND discard any DURABLE
	// server-side artifact the open created (the Postgres logical
	// replication slot — which Close deliberately leaves alive
	// because a consumed stream's slot is the CDC resume anchor).
	//
	// Callers invoke it via Abandon, and only on exits where no CDC
	// anchor position has been durably persisted for this stream —
	// preflight refusals, pre-copy setup failures, and handoff
	// failures BEFORE the anchor write (Bug 177: a target-not-empty
	// refusal after slot creation orphaned the slot, pinning source
	// WAL and breaking the refusal's own recovery hint on "slot
	// already exists"). Once the anchor IS persisted, warm-resume
	// depends on the slot and callers must use Close instead.
	// Engines whose opens create no durable artifact (MySQL binlog,
	// VStream) leave it nil; Abandon then falls back to Close.
	AbandonFn func() error

	// WaitCopyCompleteFn is the OPTIONAL engine-supplied barrier the
	// cold-start handoff joins after bulk-copy drains but BEFORE it
	// reads Position to start CDC. It blocks until the engine has
	// finished recording the snapshot's CDC-resume Position.
	//
	// Why it exists: on most engines, draining Rows to EOF already
	// establishes the happens-before edge to the Position write — the
	// row channel closes only after the producer has recorded Position.
	// The MySQL VStream auto-shard / concurrent COPY paths (ADR-0095 /
	// ADR-0099) break that edge: each table's Rows channel closes on a
	// PER-TABLE completion signal, so the LAST ReadRows can return
	// BEFORE the producer goroutine stitches and writes the
	// stitched-minimum Position. Reading Position at the handoff then
	// races the producer's write and can observe an EMPTY/stale
	// Position — the wrong CDC start position, a potential silent gap.
	//
	// Engines that set this MUST close their completion signal (the
	// VStream copyDone channel) only AFTER the Position write under the
	// same mutex the read side uses, so the chain is: producer writes
	// Position under mu → producer signals completion → handoff waits on
	// completion → handoff reads Position under mu. The closure is
	// ctx-cancellable so a shutdown mid-wait unwedges, and idempotent
	// (joining an already-closed signal returns immediately). Engines
	// without a separable COPY producer leave it nil; the handoff treats
	// a nil hook as "no barrier needed" (the Rows drain already ordered
	// the Position read).
	WaitCopyCompleteFn func(ctx context.Context) error
}

// Close releases the snapshot transaction and the underlying
// connections. After Close, Rows and Changes are unusable. Any
// durable artifact the open created (the Postgres replication slot)
// stays alive — it is the CDC resume anchor for a consumed stream.
func (s *SnapshotStream) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
}

// Abandon is the exit for a cold start that will never consume this
// stream: it releases everything Close releases AND discards any
// durable artifact the open created (see AbandonFn). Callable ONLY
// while no CDC anchor position has been durably persisted for the
// stream; after the anchor write, use Close. Falls back to Close on
// engines that set no AbandonFn (their opens create nothing durable).
func (s *SnapshotStream) Abandon() error {
	if s == nil {
		return nil
	}
	if s.AbandonFn == nil {
		return s.Close()
	}
	return s.AbandonFn()
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

// WaitCopyComplete blocks until the engine has finished recording the
// snapshot's CDC-resume Position, establishing the happens-before edge
// the cold-start handoff needs before it reads Position. Call this after
// bulk-copy drains and BEFORE reading Position to start CDC. On engines
// that drain Rows in a way that already orders the Position write (every
// non-VStream-auto-shard path) the hook is nil and this is a no-op.
// Cancellable via ctx; idempotent — safe to call once the barrier has
// already passed. See [SnapshotStream.WaitCopyCompleteFn].
func (s *SnapshotStream) WaitCopyComplete(ctx context.Context) error {
	if s == nil || s.WaitCopyCompleteFn == nil {
		return nil
	}
	return s.WaitCopyCompleteFn(ctx)
}

// ExportedSnapshot is the handle to a plain-SQL exported source
// snapshot (perf research delta 1 — the migrate-path sibling of
// [SnapshotStream]): a pinned [RowReader] observing the exporting
// transaction's consistent view, plus the SHAREABLE snapshot name other
// connections pass to the engine's [SnapshotImporter] to observe the
// EXACT same view. Unlike [SnapshotStream] it involves NO replication
// machinery — on Postgres it is a REPEATABLE READ transaction plus
// pg_export_snapshot(), so it needs no wal_level=logical, no
// replication privilege, and creates no slot. `sluice migrate` uses it
// to pin ALL of its parallel bulk-copy readers (cross-table pool +
// within-table chunks) to one MVCC view.
//
// Lifecycle mirrors [SnapshotStream]'s Close/ReleaseRows split:
//
//   - Release commits the exporting transaction as soon as the copy
//     phase has drained, so the snapshot stops holding back source
//     vacuum during the (potentially long) index/constraint phases.
//     Rows stays USABLE afterward — its queries simply observe fresh
//     per-statement views, which is exactly what the post-copy
//     reconciliation re-reads want.
//   - Close tears down the connection resources. Required exactly once;
//     both closures are engine-supplied and MUST be idempotent.
type ExportedSnapshot struct {
	// Name is the engine's shareable exported-snapshot handle
	// (Postgres: the pg_export_snapshot() return). Valid for import
	// only while the exporting transaction is open — i.e. until
	// Release/Close.
	Name string

	// Rows reads the source as of the exported snapshot, on the
	// exporting transaction's own pinned connection. After Release it
	// keeps working with fresh per-query views.
	Rows RowReader

	// ReleaseFn commits the exporting transaction (see Release).
	// Engine-supplied; idempotent.
	ReleaseFn func() error

	// CloseFn releases the pinned connection and pool (see Close).
	// Engine-supplied; idempotent, and implies Release when it hasn't
	// run yet.
	CloseFn func() error
}

// Release ends the snapshot's vacuum-pinning exporting transaction
// while keeping Rows usable (fresh views). Call once the bulk-copy
// phase has fully drained. Idempotent; nil-safe.
func (s *ExportedSnapshot) Release() error {
	if s == nil || s.ReleaseFn == nil {
		return nil
	}
	return s.ReleaseFn()
}

// Close releases every underlying connection resource. After Close,
// Rows is unusable. Idempotent; nil-safe.
func (s *ExportedSnapshot) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
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
