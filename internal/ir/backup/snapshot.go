// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"

	"sluicesync.dev/sluice/internal/ir"
)

// Snapshot is the lighter-weight cousin of [ir.SnapshotStream] used
// by the full-backup orchestrator (v0.18.0, "snapshot-anchored
// EndPosition"). Whereas [ir.SnapshotStream] pairs a snapshot-pinned
// RowReader with a CDCReader for the migrate / sync-start cold-start
// path, [Snapshot] only provides the snapshot-pinned RowReader
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
//     N-conn parallel reads ride [ir.SnapshotImporter] against the
//     exported SnapshotName (ADR-0084).
//   - MySQL: a single pinned `*sql.Conn` running
//     `START TRANSACTION WITH CONSISTENT SNAPSHOT`. MySQL's snapshot
//     is per-session and not shareable across connections, so it
//     cannot be lazily IMPORTED the way PG's exported snapshot is.
//     But N independent snapshots can be made to COINCIDE under a
//     brief `FLUSH TABLES WITH READ LOCK` window (mydumper's
//     mechanism): when [SnapshotOptions.ReaderParallelism] > 1 on a
//     vanilla source, the engine opens N reader transactions whose
//     read views are byte-identical and returns the extras on
//     [Snapshot.ExtraReaders] (ADR-0088). With ReaderParallelism <= 1,
//     or on FTWRL-denied / Vitess sources, all table reads run on the
//     one pinned connection sequentially.
//
// CloseFn is the engine-supplied cleanup closure: drops the
// temporary slot (PG), commits the snapshot tx, closes the pinned
// conn(s) — INCLUDING every reader handed back on ExtraReaders — and
// closes the underlying DB pool. The orchestrator calls it via Close
// once the row sweep completes.
type Snapshot struct {
	Position ir.Position
	Rows     ir.RowReader
	CloseFn  func() error

	// ExtraReaders are the N-1 additional snapshot-pinned readers a
	// MySQL coordinated parallel backup opens (ADR-0088): each is a
	// dedicated connection whose `START TRANSACTION WITH CONSISTENT
	// SNAPSHOT` ran inside the same `FLUSH TABLES WITH READ LOCK`
	// window as Rows, so every reader observes the byte-identical
	// consistent view at Position. The orchestrator seeds them into the
	// cross-table backup pool (the EAGER counterpart to PG's LAZY
	// [ir.SnapshotImporter] minting) — when non-empty, it is the
	// presence-driven parallel-eligibility signal for
	// `backupParallelEligible`, just as a non-empty SnapshotName is for
	// the PG path.
	//
	// nil for PG (lazy import via SnapshotName), for serial MySQL
	// (ReaderParallelism <= 1 / FTWRL-denied / coordinated-open
	// failure), and for the Vitess/PlanetScale VStream path. Additive,
	// optional — readers that don't recognise it ignore it.
	//
	// Ownership: these readers are owned by the Snapshot lifecycle —
	// CloseFn closes every one. The pool that pops them MUST NOT close a
	// popped reader (mirrors the PG importer-reader ownership note).
	ExtraReaders []ir.RowReader

	// SnapshotName is the engine's SHAREABLE exported-snapshot name —
	// the handle other connections pass to the engine's
	// [ir.SnapshotImporter] to observe the EXACT same consistent view as
	// Rows. Postgres populates it from CREATE_REPLICATION_SLOT …
	// EXPORT_SNAPSHOT (the `snapshot_name`); MySQL leaves it empty
	// because its snapshot is per-session and not shareable across
	// connections.
	//
	// It is the capability gate for the parallel per-table backup
	// reads (ADR-0084, mirroring [ir.SnapshotStream.SnapshotName]'s role
	// in ADR-0079): an empty value means "not shareable → the backup
	// row sweep stays serial". Additive, optional — readers that don't
	// recognise it ignore it.
	SnapshotName string

	// CommitFn, when non-nil, is called by the orchestrator exactly
	// once, the moment the run's in-progress manifest durably records
	// the chain anchor (task #42, ADR-0085; previously: after the
	// final manifest commit). Engines use it to persist run-scoped
	// resources that must outlive the run once anything durable
	// references them: the Postgres --chain-slot path keeps the
	// persistent chain slot (anchored at Position) instead of
	// dropping it in Close — including across a LATER failure of the
	// same run, because the slot is the WAL-retention guarantee a
	// resumed run adopts. A run that fails before the manifest is
	// durable still drops the slot via the uncommitted Close. After
	// Commit, Close must still be called; it releases connections but
	// skips dropping committed resources. nil when the engine has
	// nothing to persist (the default temporary-anchor shape).
	CommitFn func(ctx context.Context) error
}

// Close releases the snapshot transaction and the underlying
// connections. After Close, Rows is unusable.
func (s *Snapshot) Close() error {
	if s == nil || s.CloseFn == nil {
		return nil
	}
	return s.CloseFn()
}

// Commit persists run-scoped resources that should outlive a
// successful backup (see CommitFn). Safe to call when no CommitFn is
// set (no-op).
func (s *Snapshot) Commit(ctx context.Context) error {
	if s == nil || s.CommitFn == nil {
		return nil
	}
	return s.CommitFn(ctx)
}

// SnapshotOpener is the optional engine surface for opening a
// backup-scoped consistent snapshot. The full-backup orchestrator
// type-asserts on this method when starting a run; engines that
// implement it get cross-table snapshot consistency plus a
// snapshot-anchored [Manifest.EndPosition], engines that don't fall
// back to the v0.17.x shape (per-table independent reads + end-of-
// backup [PositionCapturer] capture) with a soft warning about
// the during-backup write-window gap.
//
// Lives in the ir/backup package so engine packages can implement it
// without forming an import cycle through the pipeline package's
// integration tests. The full-backup orchestrator's only direct
// dependency on the engine surface is this interface — engines wire
// the implementation onto whichever value is convenient (today: the
// engine struct itself, parallel to [ir.Engine.OpenSnapshotStream]).
//
// The options' SlotName is honoured by engines with a slot concept
// (Postgres: by default a temporary anchor slot — distinct from the
// chain-handoff slot — pins the snapshot; the chain slot name is
// recorded on the manifest's EndPosition so a Phase 3 incremental
// against this manifest opens CDC against the correct chain-handoff
// slot, even though the temporary backup slot has long been dropped).
// MySQL ignores it — the binlog stream is the slot.
type SnapshotOpener interface {
	OpenBackupSnapshot(ctx context.Context, dsn string, opts SnapshotOptions) (*Snapshot, error)
}

// SnapshotOptions carries the engine-facing knobs for opening a
// backup snapshot. A zero value preserves the historical defaults
// (engine default slot name, temporary anchor slot dropped at Close).
type SnapshotOptions struct {
	// SlotName is the chain-handoff replication-slot name recorded on
	// the snapshot Position for engines with a slot concept. Empty
	// falls back to the engine default (`sluice_slot` on Postgres).
	// Engines without slots (MySQL) ignore it.
	SlotName string

	// PersistChainSlot, when true on engines with a slot concept,
	// anchors the snapshot on the PERSISTENT chain slot itself
	// (named SlotName) instead of a temporary anchor slot, and keeps
	// it once the backup commits ([Snapshot.CommitFn]). The
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

	// ReaderParallelism is the cross-table fan-out the orchestrator
	// wants snapshot-pinned readers for — the already-resolved
	// (budget-bounded) requested parallelism, computed BEFORE the
	// snapshot opens (ADR-0088). It lets an engine that can open
	// multiple coincident readers do so eagerly, at snapshot-open time,
	// while it still holds whatever short-lived coordination its
	// consistency mechanism requires.
	//
	// MySQL vanilla honours it: with ReaderParallelism > 1 it opens N
	// reader transactions under a brief `FLUSH TABLES WITH READ LOCK`
	// window and returns the N-1 extras on [Snapshot.ExtraReaders]. PG
	// ignores it — its readers are minted LAZILY from the exported
	// SnapshotName via [ir.SnapshotImporter], so there is nothing to
	// pre-open. The Vitess/PlanetScale VStream path ignores it too.
	//
	// nil/0 (and 1) preserve today's single-reader behaviour on every
	// engine — the field is purely additive.
	ReaderParallelism int
}

// AnchorSweeper is the OPTIONAL engine surface the full-backup
// orchestrator invokes when it RESUMES an in-progress backup, to
// clean up anchor-slot debris a previously crashed run may have left
// on the source (Bug 137).
//
// New binaries create the backup anchor slot protocol-TEMPORARY, so
// a hard-killed run leaks nothing — but backups crashed under
// pre-fix binaries left a PERSISTENT timestamped anchor slot behind,
// each one silently pinning WAL at its restart_lsn until the source
// disk fills. The resume run is the natural moment to sweep those:
// it proves a prior run died mid-flight, and the operator is already
// watching the logs.
//
// Implementations must be conservative — drop only slots they can
// prove are sluice backup anchors that no live run could still be
// using (Postgres: the `sluice_backup_anchor_<unixnano>` name shape,
// inactive, non-temporary, older than a safety margin) and WARN-log
// each slot dropped (and each suspected orphan deliberately left
// alone) so the operator sees the slot lifecycle explicitly.
//
// The sweep is hygiene, not correctness: the orchestrator calls it
// best-effort and a sweep failure must not fail the resume. Engines
// without server-side slot state (MySQL) simply don't implement it.
type AnchorSweeper interface {
	SweepOrphanedBackupAnchors(ctx context.Context, dsn string) error
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
	PreflightChainResume(ctx context.Context, dsn string, from ir.Position) error
}

// TableScopedBackupSnapshotOpener is the OPTIONAL engine surface for
// opening a backup-scoped consistent snapshot whose snapshot COPY is
// restricted to a table allowlist. It is to [SnapshotOpener] what
// [ir.TableScopedSnapshotOpener] is to [ir.Engine.OpenSnapshotStream]: the same
// snapshot contract, but with the COPY scoped to the included tables.
//
// An empty/nil tables slice means "all tables" — identical behaviour to
// [SnapshotOpener.OpenBackupSnapshot]. A non-empty slice scopes the
// backup snapshot's VStream COPY filter to those UNQUALIFIED table names,
// so a large unrelated table in the same keyspace is never streamed or
// buffered (the ADR-0071 multi-table-interleaving buffer overflow). The
// semantics match [ir.TableScopedSnapshotOpener.OpenSnapshotStreamForTables]
// exactly.
//
// Only engines that over-stream by default need to implement it (today:
// the MySQL PlanetScale/VStream flavor). The full-backup orchestrator
// type-asserts on this surface and prefers it when a table scope is in
// effect; engines that don't implement it fall back to the base
// [SnapshotOpener.OpenBackupSnapshot] (Postgres and vanilla MySQL
// read per-table and never over-stream, so the scope is a no-op there).
type TableScopedBackupSnapshotOpener interface {
	OpenBackupSnapshotForTables(ctx context.Context, dsn string, opts SnapshotOptions, tables []string) (*Snapshot, error)
}
