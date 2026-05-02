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
}

// Close releases the snapshot transaction and the underlying
// connections. After Close, Rows and Changes are unusable.
func (s *SnapshotStream) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
}
