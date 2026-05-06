package ir

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
