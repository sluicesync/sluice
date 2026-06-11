// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table worker pool for the full-backup row sweep (ADR-0084).
//
// Before ADR-0084 the backup orchestrator streamed tables strictly
// serially — `for _, table := range schema.Tables { b.backupTable(...) }`
// on the snapshot's ONE pinned reader. On a many-table corpus that left
// the source's cores and the store's upload bandwidth idle between
// tables (the 2026-06-10 benchmark: 133 GB / 43 tables, `sluice backup
// full` 2367 s vs `pg_dump -j8` 232 s — ~3.4× of that gap is pure
// cross-table parallelism). This file adds the missing axis: a bounded
// pool that sweeps up to tableParallelism tables CONCURRENTLY, every
// reader pinned to the SAME exported snapshot.
//
// The shape deliberately mirrors migrate_table_pool.go (ADR-0076) and
// the ADR-0079 sync cold-start:
//
//   - Capability gate ([backupParallelEligible]) is field-/interface-
//     presence-driven, never an engine-name check (the IR-first
//     tenet). PG qualifies (exported snapshot + [ir.SnapshotImporter]);
//     MySQL's per-session snapshot and the v0.17.x non-snapshot
//     fallback don't and stay serial with a loud INFO naming the
//     reason. Not-eligible collapses to tableParallelism=1 through the
//     SAME pool function — one code path, like runBulkCopyTablePool.
//   - Free-reader 1-slot channel: the snapshot's already-pinned reader
//     is claimed by one in-flight table; peers mint dedicated readers
//     via a readerFactory ([ir.SnapshotImporter.ImportSnapshot] with
//     the exported SnapshotName), the table-granularity twin of the
//     migrate pool's free pair.
//   - Manifest writes are serialized through [manifestCommitter]: all
//     table entries are PRE-STAGED into manifest.Tables in schema order
//     before the pool starts, so the manifest's table order equals
//     schema order regardless of completion order, and every entry
//     mutation + marshal + same-key Put happens under one mutex.
//
// Crash/resume semantics under concurrency: a crashed parallel backup
// leaves at most tableParallelism tables with Partial=true AND a
// per-chunk-accurate chunk list (the in-flight workers), plus the
// pre-staged not-yet-started entries (Partial=true, zero chunks). The
// existing resume classifier ([tableManifestFullyComplete]) already
// handles all three states — Partial=true forces the per-chunk resume
// path, and a zero-chunk entry simply re-streams from scratch.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// backupDispatchObserver is a TEST-ONLY seam: when non-nil it fires
// with the resolved cross-table dispatch decision the moment
// [Backup.resolveBackupTableParallelism] chooses — tableParallelism > 1
// means the parallel sweep engaged; reason carries the not-engaged
// clause ("" when engaged). It lets the MySQL / fallback serial
// integration tests assert the SERIAL path was taken without inferring
// it from timing — a green zero-loss test alone can't distinguish the
// two paths. nil in production (a single nil check). Mirrors
// [coldStartDispatchObserver].
var backupDispatchObserver func(tableParallelism int, reason string)

// backupParallelEligible is the ADR-0084 capability gate: it decides
// whether the backup row sweep may fan out across tables. It returns
// (true, "") only when EVERY precondition holds; otherwise
// (false, reason) where reason is a single operator-facing clause for
// the loud INFO log. Mirrors [coldStartFastEligible] (ADR-0079) —
// presence-driven, never an engine-name string.
//
// The three predicates, in order:
//   - snapshotName != "" — the source surfaced a SHAREABLE exported
//     snapshot (Postgres does; MySQL and the v0.17.x non-snapshot
//     fallback leave it empty).
//   - source implements [ir.SnapshotImporterOpener] — the engine can
//     mint additional readers pinned to that snapshot.
//   - tableParallelism > 1 — the operator didn't ask for serial.
//
// Pure and table-unit-testable: no I/O, no state mutation.
func backupParallelEligible(snapshotName string, source ir.Engine, tableParallelism int) (ok bool, reason string) {
	if snapshotName == "" {
		return false, "source snapshot is not shareable (per-session / single-stream / non-snapshot fallback)"
	}
	if _, ok := source.(ir.SnapshotImporterOpener); !ok {
		return false, "source engine has no snapshot importer"
	}
	if tableParallelism <= 1 {
		return false, "cross-table parallelism disabled (--table-parallelism=1)"
	}
	return true, ""
}

// backupTableTask is one table's unit of work for the pool: the table
// and its pre-staged manifest entry (already appended to
// manifest.Tables in schema order by [Backup.stageBackupTables]).
// There is deliberately NO prior-run chunk state here: partially-
// written tables re-stream from scratch on resume (Bug 135 — see
// [Backup.backupTable]'s doc comment).
type backupTableTask struct {
	table *ir.Table
	entry *irbackup.TableManifest
}

// backupReaderFactory mints one additional snapshot-pinned
// [ir.RowReader]. nil when the pool runs serial (tableParallelism=1)
// — the single worker always wins the free reader, so the factory is
// never called there.
type backupReaderFactory func(ctx context.Context) (ir.RowReader, error)

// runBackupTablePool sweeps tasks through a bounded cross-table worker
// pool (ADR-0084). tableParallelism caps how many tables stream
// concurrently; 1 collapses to the pre-ADR-0084 serial behaviour (one
// goroutine, reusing the snapshot's pinned reader for every table in
// turn).
//
// primary is the orchestrator's already-open reader (the snapshot's
// pinned conn, or the v0.17.x fallback reader) — the "free reader".
// Exactly one running table uses it at a time (claimed via a 1-slot
// channel); peers mint their own via factory and close it when done.
// The free reader is NOT closed here (the caller owns its lifecycle
// through the deferred snapshot/reader cleanup).
//
// The errgroup's derived ctx cancels on the first table's error so
// peers unwind promptly; tg.Wait returns the first error.
func (b *Backup) runBackupTablePool(
	ctx context.Context,
	tasks []backupTableTask,
	primary ir.RowReader,
	factory backupReaderFactory,
	tableParallelism int,
	chunkRows int,
	committer *manifestCommitter,
	chainCEK []byte,
) error {
	limit := tableParallelism
	if limit < 1 {
		limit = 1
	}

	// freeReader is a 1-slot pool holding the orchestrator's pinned
	// reader. A table goroutine tries a non-blocking receive; the winner
	// reuses the free reader (and returns it on completion so a later
	// table can claim it), every other concurrent table mints its own.
	// This mirrors the migrate pool's free-pair channel at the
	// reader-only granularity backups need (no writer side).
	freeReader := make(chan ir.RowReader, 1)
	freeReader <- primary

	tg, tctx := errgroup.WithContext(ctx)
	tg.SetLimit(limit)
	for _, task := range tasks {
		task := task
		tg.Go(func() error {
			rr, release, err := acquireBackupReader(tctx, freeReader, factory)
			if err != nil {
				return err
			}
			defer release()
			if err := b.backupTable(tctx, rr, task, chunkRows, committer, chainCEK); err != nil {
				return wrapWithHint(PhaseBulkCopy, fmt.Errorf("backup: table %q: %w", task.table.Name, err))
			}
			return nil
		})
	}
	return tg.Wait()
}

// acquireBackupReader returns the reader a table goroutine should
// stream through, plus a release function the caller defers. It first
// tries to claim the free reader (non-blocking); if another table
// already holds it, it mints a dedicated snapshot-pinned reader via
// factory.
//
// The release function returns the free reader to the pool (so a later
// table can reuse it) or closes a dedicated one. It never closes the
// free reader — the orchestrator owns that lifecycle. Mirrors
// [acquireTablePair].
func acquireBackupReader(
	ctx context.Context,
	freeReader chan ir.RowReader,
	factory backupReaderFactory,
) (ir.RowReader, func(), error) {
	select {
	case r := <-freeReader:
		// Won the free reader; return it to the pool on release.
		return r, func() { freeReader <- r }, nil
	default:
		// Free reader is in use by a peer table; mint a dedicated one.
		if factory == nil {
			// Unreachable: a nil factory means the pool runs serial
			// (limit 1), where the free reader is always available. Loud
			// rather than a silent nil-func call.
			return nil, func() {}, errBackupPoolNoFactory
		}
		r, err := factory(ctx)
		if err != nil {
			return nil, func() {}, err
		}
		return r, func() { closeIf(r) }, nil
	}
}

// errBackupPoolNoFactory is the loud precondition guard for
// [acquireBackupReader]: the dedicated-reader branch is only reachable
// when the pool runs with tableParallelism > 1, which the orchestrator
// only configures together with a reader factory (the gate asserted
// [ir.SnapshotImporterOpener]). Reaching it with a nil factory is a
// programming error, surfaced rather than silently deref'd.
var errBackupPoolNoFactory = errors.New("pipeline: backup table pool: dedicated reader needed but no reader factory configured (gate bypassed)")

// stageBackupTables walks schema.Tables in order and stages one
// manifest entry per table through the committer: a prior run's
// fully-complete entry verbatim (whole-table resume skip — sound: the
// chunk SET is whole and order-independent), or a fresh Partial=true
// placeholder paired into a [backupTableTask] for the pool. Returns
// the tasks (the tables that still need streaming) in schema order.
//
// A prior run's PARTIAL table is deliberately re-streamed from
// scratch — its chunks are NOT reused. The per-chunk reuse this
// replaces (Bug 34b) silently corrupted resumed backups (duplicate +
// missing rows, exit 0) because it assumed repeatable scan order
// across runs, which the reader has never guaranteed (Bug 135; see
// [Backup.backupTable]). The fresh placeholder REPLACES the prior
// entry in the new manifest, so the corrupt-prone chunk list never
// survives into the resumed manifest; the prior chunk FILES are
// overwritten index-by-index as the table re-streams (a byte-identical
// chunk skips its upload via flush's content-addressed SHA comparison
// — sound, order-independent).
func (b *Backup) stageBackupTables(
	ctx context.Context,
	committer *manifestCommitter,
	schema *ir.Schema,
	priorTables map[string]*irbackup.TableManifest,
) ([]backupTableTask, error) {
	tasks := make([]backupTableTask, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		key := manifestTableKey(table.Schema, table.Name)
		if existing, ok := priorTables[key]; ok {
			full, err := tableManifestFullyComplete(ctx, b.Store, existing)
			if err != nil {
				return nil, fmt.Errorf("backup: re-validate prior table %q: %w", table.Name, err)
			}
			if full {
				slog.InfoContext(
					ctx, "skipping table — already complete in partial backup",
					slog.String("table", table.Name),
					slog.Int64("rows", existing.RowCount),
					slog.Int("chunks", len(existing.Chunks)),
				)
				committer.stageTable(existing)
				continue
			}
			slog.InfoContext(
				ctx, "re-streaming partially-backed-up table from scratch — prior partial chunks are not reusable (scan order is not repeatable across runs; Bug 135)",
				slog.String("table", table.Name),
				slog.Int("discarded_prior_chunks", len(existing.Chunks)),
			)
		}
		entry := &irbackup.TableManifest{
			Schema:  table.Schema,
			Name:    table.Name,
			Partial: true, // flips to false on natural EOF; checkpoints persist it as true until then
		}
		committer.stageTable(entry)
		tasks = append(tasks, backupTableTask{table: table, entry: entry})
	}
	return tasks, nil
}

// resolveBackupTableParallelism resolves the effective cross-table
// fan-out for the row sweep: the operator's requested value (0 = auto
// = 4, [resolveTableParallelism]) gated by [backupParallelEligible]
// and bounded by the SOURCE's measured connection budget. Not-eligible
// (or ≤1 tables to sweep) collapses to 1 — the same pool runs serial —
// with a loud INFO naming the reason (the ADR-0079 disposition: a
// silent fallback would leave operators wondering why the knob did
// nothing).
//
// The budget chokepoint reuses [resolveTargetCopyParallelism] against
// the SOURCE DSN (backups open reader connections there; the prober is
// engine-optional — MySQL never reaches this, its gate fails first),
// then RESERVES one slot for the snapshot's slot-creation replication
// conn, which stays open for the whole sweep (the ADR-0079 CDC-conn
// reservation pattern).
func (b *Backup) resolveBackupTableParallelism(ctx context.Context, snapshotName string, taskCount int) (int, error) {
	requested := resolveTableParallelism(b.TableParallelism)
	if requested > taskCount {
		requested = taskCount // never fan out wider than there are tables to sweep
	}
	ok, reason := backupParallelEligible(snapshotName, b.Source, requested)
	if !ok {
		if taskCount <= 1 {
			reason = "at most one table to sweep"
		}
		slog.InfoContext(
			ctx, "backup: cross-table parallel reads not engaged; sweeping tables serially",
			slog.String("reason", reason),
			slog.Int("requested_table_parallelism", requested),
		)
		if backupDispatchObserver != nil {
			backupDispatchObserver(1, reason)
		}
		return 1, nil
	}

	effective, report, err := resolveTargetCopyParallelism(ctx, b.Source, b.SourceDSN, requested, 0)
	if err != nil {
		return 0, err
	}
	if report.CopyBudget >= 1 {
		budget := report.CopyBudget - 1 // reserve the replication conn's slot
		if budget < 1 {
			// A budget of exactly 1 left only the repl-conn slot; the
			// sweep still needs at least one reader. Floor at 1 — the
			// loud refusal in resolveTargetCopyParallelism already fired
			// if the source had truly zero free slots.
			budget = 1
		}
		if effective > budget {
			effective = budget
		}
	}
	if effective < 1 {
		effective = 1
	}
	slog.InfoContext(
		ctx, "backup: cross-table parallel reads engaged (ADR-0084)",
		slog.Int("table_parallelism", effective),
		slog.String("snapshot", snapshotName),
	)
	if backupDispatchObserver != nil {
		backupDispatchObserver(effective, "")
	}
	return effective, nil
}

// openBackupReaderFactory opens the source's [ir.SnapshotImporter] and
// returns a [backupReaderFactory] that mints one snapshot-pinned
// reader per call, plus a cleanup closing the importer once the pool
// has drained (the minted readers are closed individually by the
// pool's release path). The factory is the ADR-0079 shape: every
// reader runs `SET TRANSACTION SNAPSHOT '<name>'` inside its own
// REPEATABLE READ tx, so it observes the EXACT view the snapshot's
// pinned primary reader does — valid for as long as the slot-creation
// replication conn lives (closed by the orchestrator's deferred
// snapshot cleanup, after the pool).
//
// Importer-minted readers are single-schema, bound to the DSN's
// schema — the SAME binding the snapshot's primary reader carries
// (`&RowReader{q: conn, schema: cfg.schema}`), so the parallel sweep
// reads exactly what the serial sweep would.
//
// tableParallelism <= 1 returns a nil factory with a no-op cleanup:
// the serial pool's single worker always wins the free reader, so no
// importer is needed (and the v0.17.x fallback path has none to open).
func (b *Backup) openBackupReaderFactory(ctx context.Context, snapshotName string, tableParallelism int) (backupReaderFactory, func(), error) {
	if tableParallelism <= 1 {
		return nil, func() {}, nil
	}
	opener, ok := b.Source.(ir.SnapshotImporterOpener)
	if !ok {
		// Unreachable: backupParallelEligible already asserted this.
		// Loud rather than a silent serial degrade of an engaged gate.
		return nil, func() {}, errBackupPoolNoImporter
	}
	importer, err := opener.OpenSnapshotImporter(ctx, b.SourceDSN)
	if err != nil {
		return nil, func() {}, wrapWithHint(PhaseConnect, fmt.Errorf("backup: open snapshot importer: %w", err))
	}
	cleanup := func() {
		if c, ok := importer.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	factory := func(rctx context.Context) (ir.RowReader, error) {
		readers, err := importer.ImportSnapshot(rctx, snapshotName, 1)
		if err != nil {
			return nil, err
		}
		return readers[0], nil
	}
	return factory, cleanup, nil
}

// errBackupPoolNoImporter mirrors [errColdStartNoImporter] for the
// backup pool's factory-open precondition.
var errBackupPoolNoImporter = errors.New("pipeline: backup table pool: source engine has no snapshot importer (gate bypassed)")

// manifestCommitter serializes every in-flight-manifest mutation and
// store commit for the backup row sweep (ADR-0084). With peer tables
// checkpointing concurrently, both the manifest's Go structures (each
// worker mutating its own *irbackup.TableManifest while a peer marshals the
// WHOLE manifest) and the same-key `manifest.json` Puts (load-bearing
// on stores without atomic rename) need one serialization point — this
// mutex is it. The data-plane chunk Puts (distinct keys) stay outside.
type manifestCommitter struct {
	mu       sync.Mutex
	store    irbackup.BackupStore
	manifest *irbackup.Manifest
}

// stageTable appends one table's entry to manifest.Tables. The
// orchestrator calls it once per schema table, in schema order, BEFORE
// the pool starts — so the manifest's table order is deterministic
// (== schema order) regardless of which worker finishes first. The
// entry is either a prior run's fully-complete entry staged verbatim
// (the whole-table resume skip) or a fresh Partial=true placeholder
// its worker fills in.
//
// Called single-threaded (pre-pool); no lock strictly needed, but
// taking it keeps the invariant trivially auditable: every manifest
// mutation in this file happens under mu.
func (c *manifestCommitter) stageTable(entry *irbackup.TableManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manifest.Tables = append(c.manifest.Tables, entry)
}

// appendChunk records one finished chunk on entry and commits the
// manifest so a mid-table crash leaves an up-to-date record of exactly
// which chunks completed — progress observability, plus it keeps
// flush's content-addressed same-path upload skip effective across a
// re-run. Resume never REUSES partial chunk lists (Bug 135).
func (c *manifestCommitter) appendChunk(ctx context.Context, entry *irbackup.TableManifest, ci *irbackup.ChunkInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.Chunks = append(entry.Chunks, ci)
	return c.commitLocked(ctx)
}

// finishTable flips entry to its terminal complete state (natural row
// EOF) and commits the manifest — the per-table checkpoint. Empty
// tables and tables whose row count is an exact chunk multiple rely on
// this commit (their last appendChunk checkpoint doesn't carry the
// Partial=false flip).
func (c *manifestCommitter) finishTable(ctx context.Context, entry *irbackup.TableManifest, rowCount int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.RowCount = rowCount
	entry.Partial = false
	return c.commitLocked(ctx)
}

// commit marshals + writes the manifest under the mutex. Used by the
// orchestrator for the pre-pool and post-pool manifest writes (resume
// staging, EndPosition, the final complete flip) so every
// `manifest.json` Put in a backup run flows through one code path.
func (c *manifestCommitter) commit(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitLocked(ctx)
}

// commitLocked is the marshal+Put core. Callers hold mu — the marshal
// MUST happen under the lock because it reads every worker's entry.
func (c *manifestCommitter) commitLocked(ctx context.Context) error {
	b, err := json.MarshalIndent(c.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return c.store.Put(ctx, ManifestFileName, bytes.NewReader(b))
}
