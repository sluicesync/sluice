// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Shared exported snapshot for the migrate bulk-copy (perf research
// delta 1, perf-parity matrix row 15). Before this, only the sync
// cold-start pinned its parallel readers to ONE exported snapshot
// (ADR-0079 via the replication slot's EXPORT_SNAPSHOT); migrate's
// cross-table pool and within-table chunks each opened an independent
// connection observing its own MVCC view — the documented ADR-0019 v1
// consistency window. pgcopydb always pins.
//
// This file closes that gap with the SAME importer machinery: when the
// SOURCE engine implements BOTH [ir.SnapshotExporter] (a plain-SQL
// exported snapshot — no replication prerequisites) and
// [ir.SnapshotImporterOpener], the migrate primary reader IS the
// exporting transaction's pinned reader and every additional
// table/chunk reader is minted through the importer pinned to the same
// snapshot name. Capability-gated and fallback-loud: engines without
// the surfaces (MySQL — per-session snapshots, its consistency story is
// FTWRL/binlog-based; SQLite) are untouched, and an export failure
// (e.g. a hot-standby source, where pg_export_snapshot() cannot run)
// WARNs and falls back to the pre-existing independent readers rather
// than failing a previously-working migrate.
//
// Release discipline (the long-pin source-bloat lesson, ADR-0079 /
// Bug 21 class): the snapshot is released the moment the bulk-copy
// phase drains — see [parallelBulkCopyDeps.releaseSharedSnapshot] and
// its call in [runBulkCopyTablePool] — NOT held through the index /
// constraint phases, where a pinned transaction would hold back source
// vacuum for the (potentially long) DDL tail.

package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
)

// migrateSharedSnapshotExportedObserver is a TEST-ONLY seam (nil in
// production — a single nil check): it fires right after the shared
// snapshot is exported and its importer opened, BEFORE any row is read.
// The consistency integration test uses it to mutate the source at a
// deterministic point (rows inserted here must be invisible to every
// copy reader). Mirrors the coldStartDispatchObserver disposition.
var migrateSharedSnapshotExportedObserver func()

// migrateSharedSnapshotReleasedObserver is the TEST-ONLY sibling: it
// fires after the shared snapshot's release has run (exporting tx
// committed, importer closed). The integration test asserts release
// happened at copy-phase end — with no lingering idle-in-transaction
// snapshot session on the source — rather than at run teardown.
var migrateSharedSnapshotReleasedObserver func()

// sharedSourceSnapshot bundles the migrate run's exported snapshot with
// the importer that mints its pinned peer readers, plus the once-only
// release/teardown discipline. Constructed by
// [Migrator.openSharedSourceSnapshot]; nil means "not engaged" and every
// consumer keeps the pre-existing independent-reader behaviour.
type sharedSourceSnapshot struct {
	snap           *ir.ExportedSnapshot
	importer       ir.SnapshotImporter
	maxBufferBytes int64

	// releaseOnce guards release(): it is invoked from the copy pool's
	// completion point (which, under the ADR-0077 overlapped copy+index
	// phase, runs on the copy producer goroutine) and again defensively
	// from close() on every exit path.
	releaseOnce sync.Once
}

// openSharedSourceSnapshot engages the shared-snapshot machinery when
// the source engine supports it. Returns nil — with the caller falling
// back to an independent [ir.Engine.OpenRowReader] — when the engine
// lacks either surface (the quiet, by-design case) or when the export
// itself fails (the LOUD case: the operator is told the run proceeds
// with per-connection snapshots, the documented ADR-0019 window).
func (m *Migrator) openSharedSourceSnapshot(ctx context.Context) *sharedSourceSnapshot {
	exporter, ok := m.Source.(ir.SnapshotExporter)
	if !ok {
		return nil
	}
	opener, ok := m.Source.(ir.SnapshotImporterOpener)
	if !ok {
		return nil
	}
	snap, err := exporter.ExportSnapshot(ctx, m.SourceDSN)
	if err != nil {
		slog.WarnContext(ctx, "migrate: shared source snapshot unavailable; falling back to independent per-connection readers (rows committed on the source mid-copy may be observed by some readers and not others — quiesce the source or retry against a primary for a fully consistent copy)",
			slog.String("err", err.Error()))
		return nil
	}
	if snap.Name == "" || snap.Rows == nil {
		// Defensive: an engine returning a nil-reader / unnamed snapshot
		// can't be pinned against. Same loud fallback as an export error.
		_ = snap.Close()
		slog.WarnContext(ctx, "migrate: source engine returned an unusable exported snapshot; falling back to independent per-connection readers")
		return nil
	}
	importer, err := opener.OpenSnapshotImporter(ctx, m.SourceDSN)
	if err != nil {
		_ = snap.Close()
		slog.WarnContext(ctx, "migrate: shared-snapshot importer unavailable; falling back to independent per-connection readers",
			slog.String("err", err.Error()))
		return nil
	}
	slog.InfoContext(ctx, "migrate: parallel readers pinned to one shared exported snapshot",
		slog.String("snapshot", snap.Name))
	if obs := migrateSharedSnapshotExportedObserver; obs != nil {
		obs()
	}
	return &sharedSourceSnapshot{snap: snap, importer: importer, maxBufferBytes: m.MaxBufferBytes}
}

// readerFactory returns the [parallelBulkCopyDeps.chunkReaderFactory]
// that mints one snapshot-pinned reader per call — the exact shape the
// sync cold-start wires (streamer_coldstart_parallel.go): each reader is
// a fresh connection inside its own REPEATABLE READ tx pinned to the
// shared snapshot name, safe for concurrent calls from peer chunk/table
// goroutines, its lifecycle owned by the caller's closeIf release path.
func (s *sharedSourceSnapshot) readerFactory() func(ctx context.Context) (ir.RowReader, error) {
	snapshotName := s.snap.Name
	maxBuffer := s.maxBufferBytes
	importer := s.importer
	return func(ctx context.Context) (ir.RowReader, error) {
		readers, err := importer.ImportSnapshot(ctx, snapshotName, 1)
		if err != nil {
			return nil, err
		}
		applyMaxBufferBytes(readers[0], maxBuffer)
		return readers[0], nil
	}
}

// release ends the snapshot's vacuum pin: it commits the exporting
// transaction and closes the importer pool. Invoked at copy-phase end
// ([runBulkCopyTablePool]) so the pin never spans the index/constraint
// phases; close() re-invokes it defensively on error unwinds. The
// primary reader (snap.Rows) stays USABLE afterward with fresh
// per-query views — exactly what the ADR-0141 reparent reconciliation,
// which re-reads the CURRENT source after the index phase, wants.
// Once-guarded: peer paths may race the deferred teardown.
func (s *sharedSourceSnapshot) release(ctx context.Context) {
	s.releaseOnce.Do(func() {
		if err := s.snap.Release(); err != nil {
			slog.WarnContext(ctx, "migrate: shared snapshot release failed; the exporting transaction may pin source vacuum until run end",
				slog.String("err", err.Error()))
		}
		if c, ok := s.importer.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		if obs := migrateSharedSnapshotReleasedObserver; obs != nil {
			obs()
		}
	})
}

// close is the run-teardown cleanup (deferred in runSingleDatabase): it
// makes sure release ran — covering error unwinds where the copy pool
// never reached its release point — then closes the exporting
// connection resources. Idempotent via the engine's Close contract.
func (s *sharedSourceSnapshot) close() {
	s.release(context.Background())
	_ = s.snap.Close()
}
