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

package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// BackupDispatchObserver is a TEST-ONLY seam: when non-nil it fires
// with the resolved cross-table dispatch decision the moment
// [Backup.resolveBackupTableParallelism] chooses — tableParallelism > 1
// means the parallel sweep engaged; reason carries the not-engaged
// clause ("" when engaged). It lets the MySQL / fallback serial
// integration tests assert the SERIAL path was taken without inferring
// it from timing — a green zero-loss test alone can't distinguish the
// two paths. nil in production (a single nil check). Mirrors
// [coldStartDispatchObserver].
var BackupDispatchObserver func(tableParallelism int, reason string)

// backupParallelEligible is the ADR-0084 / ADR-0088 capability gate: it
// decides whether the backup row sweep may fan out across tables. It
// returns (true, "") only when the parallelism request holds AND the
// snapshot supplies a way to read N tables concurrently; otherwise
// (false, reason) where reason is a single operator-facing clause for
// the loud INFO log. Mirrors [coldStartFastEligible] (ADR-0079) —
// presence-driven, never an engine-name string.
//
// Two ways the snapshot can supply parallel readers, EITHER suffices:
//   - LAZY (Postgres, ADR-0084): a SHAREABLE exported snapshot
//     (snap.SnapshotName != "") plus a source that implements
//     [ir.SnapshotImporterOpener] to mint additional readers pinned to
//     it. The v0.17.x non-snapshot fallback has neither.
//   - EAGER (MySQL vanilla, ADR-0088): the snapshot already opened N
//     coincident readers under a FTWRL window and handed back the
//     extras on snap.ExtraReaders. FTWRL-denied / serial MySQL leaves
//     it empty.
//
// tableParallelism > 1 is always required (the operator didn't ask for
// serial). Pure and table-unit-testable: no I/O, no state mutation.
func backupParallelEligible(snap *irbackup.Snapshot, source ir.Engine, tableParallelism int) (ok bool, reason string) {
	if tableParallelism <= 1 {
		return false, "cross-table parallelism disabled (--table-parallelism=1)"
	}
	if snap != nil && len(snap.ExtraReaders) > 0 {
		return true, "" // eager coordinated readers (MySQL FTWRL-aligned)
	}
	snapshotName := ""
	if snap != nil {
		snapshotName = snap.SnapshotName
	}
	if snapshotName == "" {
		return false, "source snapshot is not shareable (per-session / single-stream / non-snapshot fallback)"
	}
	if _, ok := source.(ir.SnapshotImporterOpener); !ok {
		return false, "source engine has no snapshot importer"
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
// within, when non-nil, enables ADR-0149 within-table read chunking:
// each worker routes its table through [Backup.backupTableDispatch]
// (chunk-eligible tables fan out into PK-range readers drawn from the
// SAME free-reader channel + factory), and draws one BASE token from
// the shared reader-budget gate so table + range readers together
// never exceed the tableParallelism × bulk-parallelism budget. nil ⇒
// the pre-ADR-0149 behaviour, byte-identical.
//
// primary is the orchestrator's already-open reader (the snapshot's
// pinned conn, or the v0.17.x fallback reader). peers are the EAGER
// pre-opened coordinated readers (MySQL ADR-0088; nil for the PG lazy
// path and the serial/fallback paths). primary + peers form a reusable
// free-reader pool: a running table claims one (non-blocking) and
// returns it on completion; a table that finds the pool empty mints its
// own via factory (the PG lazy importer path) and closes it when done.
// None of the seeded readers (primary or peers) are closed here — the
// caller owns their lifecycle through the deferred snapshot cleanup
// (for MySQL coordinated, the snapshot's CloseFn commits/closes every
// reader conn exactly once).
//
// The errgroup's derived ctx cancels on the first table's error so
// peers unwind promptly; tg.Wait returns the first error.
func (b *Backup) runBackupTablePool(
	ctx context.Context,
	tasks []backupTableTask,
	primary ir.RowReader,
	peers []ir.RowReader,
	factory backupReaderFactory,
	tableParallelism int,
	chunkRows int,
	committer *manifestCommitter,
	chainCEK []byte,
	within *backupWithinTable,
) error {
	limit := tableParallelism
	if limit < 1 {
		limit = 1
	}

	// freeReader is the reusable reader pool: the orchestrator's pinned
	// primary plus any eager pre-opened peers (MySQL coordinated). A
	// table goroutine tries a non-blocking receive; the winner reuses the
	// reader (and returns it on completion so a later table can claim
	// it), every other concurrent table mints its own via factory (PG
	// lazy importer). This mirrors the migrate pool's free-pair channel at
	// the reader-only granularity backups need (no writer side).
	freeReader := make(chan ir.RowReader, 1+len(peers))
	freeReader <- primary
	for _, p := range peers {
		freeReader <- p
	}
	if within != nil {
		// The ADR-0149 range workers draw from the SAME reader supply as
		// cross-table peers — one acquisition path, one budget.
		within.freeReader = freeReader
		within.factory = factory
	}

	tg, tctx := errgroup.WithContext(ctx)
	tg.SetLimit(limit)
	for _, task := range tasks {
		task := task
		tg.Go(func() error {
			if within != nil {
				// Base token (the ADR-0123 copyGate discipline): the
				// worker's own held reader counts against the same
				// table × within read budget the extra range readers draw
				// from, so the product ceiling holds by construction.
				if err := within.gate.Acquire(tctx); err != nil {
					return err
				}
				defer within.gate.Release()
			}
			rr, release, err := acquireBackupReader(tctx, freeReader, factory)
			if err != nil {
				return err
			}
			defer release()
			if err := b.backupTableDispatch(tctx, rr, task, chunkRows, committer, chainCEK, within); err != nil {
				return migcore.WrapWithHint(migcore.PhaseBulkCopy, fmt.Errorf("backup: table %q: %w", task.table.Name, err))
			}
			return nil
		})
	}
	return tg.Wait()
}

// acquireBackupReader returns the reader a table goroutine should
// stream through, plus a release function the caller defers. It first
// tries to claim a reader from the free-reader pool (non-blocking); if
// every pooled reader is in use, it mints a dedicated snapshot-pinned
// reader via factory (the PG lazy importer path).
//
// The release function returns a pooled reader to the pool (so a later
// table can reuse it) or closes a dedicated minted one. It never closes
// a pooled reader — the orchestrator owns those lifecycles (the MySQL
// coordinated readers and the primary are committed/closed exactly once
// by the snapshot's CloseFn). Mirrors [acquireTablePair].
func acquireBackupReader(
	ctx context.Context,
	freeReader chan ir.RowReader,
	factory backupReaderFactory,
) (ir.RowReader, func(), error) {
	select {
	case r := <-freeReader:
		// Won a pooled reader; return it to the pool on release.
		return r, func() { freeReader <- r }, nil
	default:
		// Every pooled reader is in use by a peer table; mint a dedicated
		// one (PG lazy importer). For the serial path (limit 1) and the
		// MySQL eager path (pool sized to limit) this branch is
		// unreachable — the pool always has a free reader.
		if factory == nil {
			// Unreachable: a nil factory means the pool runs serial
			// (limit 1) or all readers are pooled (MySQL eager); a free
			// reader is always available. Loud
			// rather than a silent nil-func call.
			return nil, func() {}, errBackupPoolNoFactory
		}
		r, err := factory(ctx)
		if err != nil {
			return nil, func() {}, err
		}
		return r, func() { migcore.CloseIf(r) }, nil
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

// resolveRequestedReaderParallelism is the pre-snapshot-open half of
// the parallelism resolution (ADR-0088): the operator's requested
// value (0 = auto = 4, [migcore.ResolveTableParallelism]) bounded by the table
// count and by the SOURCE's measured connection budget — but WITHOUT
// the [backupParallelEligible] gate and WITHOUT firing the dispatch
// observer. The orchestrator calls it before the snapshot opens so an
// engine that opens coincident readers eagerly (MySQL vanilla, under a
// FTWRL window) knows how many readers to open; the gated, observed
// decision is made post-staging by [resolveBackupTableParallelism].
// Only the cross-table axis matters here — the eager reader supply is
// per-table — so the within factor is discarded.
func (b *Backup) resolveRequestedReaderParallelism(ctx context.Context, taskCount int) (int, error) {
	tableP, _, err := b.resolveBackupReadParallelism(ctx, taskCount)
	return tableP, err
}

// resolveBackupReadParallelism resolves BOTH read axes against the
// SOURCE's measured connection budget: the cross-table fan-out
// (ADR-0084/0088) and the ADR-0149 within-table range-reader factor
// (--bulk-parallelism, 0 = auto = min(8, NumCPU) via
// [migcore.ResolveBulkParallelism]).
//
// The budget chokepoint reuses [migcore.ResolveTargetCopyParallelism] against
// the SOURCE DSN (backups open reader connections there; the prober is
// engine-optional — MySQL has none, so both requests stand unbounded by
// this step), then RESERVES one slot for the coordinator /
// slot-creation conn that stays open during the open window (the
// ADR-0079 CDC-conn reservation pattern), then splits the remainder
// across the two axes via [migcore.ResolveCopyParallelismBudget] with the axes
// SWAPPED relative to migrate: backup satisfies the shipped
// CROSS-TABLE axis first (its cell of the parity matrix predates
// within-table chunking, and a default flip that shrank it would
// regress many-table corpora), and the within axis gets whatever whole
// multiples remain — which on a single-huge-table corpus (tableP
// clamped to the task count = 1) is the entire remaining budget, the
// ADR-0149 headline case. The product tableP × withinP never exceeds
// the reserved-slot-adjusted budget.
func (b *Backup) resolveBackupReadParallelism(ctx context.Context, taskCount int) (tableP, withinP int, err error) {
	if taskCount == 0 {
		return 0, 1, nil // nothing to sweep; no probe needed
	}
	tableP = migcore.ResolveTableParallelism(b.TableParallelism)
	if tableP > taskCount {
		tableP = taskCount // never fan out wider than there are tables to sweep
	}
	withinP = migcore.ResolveBulkParallelism(b.BulkParallelism, runtime.NumCPU())
	if withinP < 1 {
		withinP = 1
	}
	if tableP <= 1 && withinP <= 1 {
		return tableP, withinP, nil
	}
	probeWidth := tableP
	if probeWidth < 1 {
		probeWidth = 1
	}
	effective, report, err := migcore.ResolveTargetCopyParallelism(ctx, b.Source, b.SourceDSN, probeWidth, 0)
	if err != nil {
		return 0, 0, err
	}
	budget := 0
	if report.CopyBudget >= 1 {
		budget = report.CopyBudget - 1 // reserve the coordinator/repl conn's slot
		if budget < 1 {
			// A budget of exactly 1 left only the reserved slot; the sweep
			// still needs at least one reader. Floor at 1 — the loud
			// refusal in migcore.ResolveTargetCopyParallelism already fired if the
			// source had truly zero free slots.
			budget = 1
		}
		if effective > budget {
			effective = budget
		}
	}
	if effective < 1 {
		effective = 1
	}
	// Axis-swapped budget split (see the doc comment): the helper
	// satisfies its FIRST argument first and clamps the second to whole
	// multiples of it, so passing (table, within) yields
	// (withinClamped, tableUnchanged).
	withinP, tableP = migcore.ResolveCopyParallelismBudget(effective, withinP, budget, 0)
	return tableP, withinP, nil
}

// resolveBackupTableParallelism resolves the EFFECTIVE cross-table
// fan-out for the row sweep, post-staging: the requested value
// ([resolveBackupReadParallelism]) gated by [backupParallelEligible]
// against the actual snapshot. Not-eligible (or ≤1 tables to sweep)
// collapses to 1 — the same pool runs serial — with a loud INFO naming
// the reason (the ADR-0079 disposition: a silent fallback would leave
// operators wondering why the knob did nothing). The dispatch observer
// (test seam) fires with the decision.
//
// The second return is the budget-split within-table factor
// (--bulk-parallelism, ADR-0149) — NOT yet mode-gated; the caller
// hands it to [Backup.resolveBackupWithinTable], whose own gate + INFO
// decide whether within-table chunking engages.
//
// For the EAGER MySQL path the snapshot has already opened
// len(snap.ExtraReaders)+1 readers at the requested count, so the
// effective value here is bounded by what was opened — it never
// exceeds 1+len(ExtraReaders) because both were derived from the same
// requested number bounded by the (≥ this run's) schema table count.
func (b *Backup) resolveBackupTableParallelism(ctx context.Context, snap *irbackup.Snapshot, taskCount int) (tableParallelism, withinParallelism int, err error) {
	effective, withinParallelism, err := b.resolveBackupReadParallelism(ctx, taskCount)
	if err != nil {
		return 0, 0, err
	}
	ok, reason := backupParallelEligible(snap, b.Source, effective)
	if !ok {
		if taskCount <= 1 {
			reason = "at most one table to sweep"
		}
		slog.InfoContext(
			ctx, "backup: cross-table parallel reads not engaged; sweeping tables serially",
			slog.String("reason", reason),
			slog.Int("requested_table_parallelism", effective),
		)
		if BackupDispatchObserver != nil {
			BackupDispatchObserver(1, reason)
		}
		return 1, withinParallelism, nil
	}
	// Never fan out wider than the eager readers actually opened (the
	// gate proved at least 1 extra when SnapshotName is empty).
	if snap != nil && len(snap.ExtraReaders) > 0 && effective > len(snap.ExtraReaders)+1 {
		effective = len(snap.ExtraReaders) + 1
	}
	slog.InfoContext(
		ctx, "backup: cross-table parallel reads engaged (ADR-0084/ADR-0088)",
		slog.Int("table_parallelism", effective),
		slog.Bool("eager_readers", snap != nil && len(snap.ExtraReaders) > 0),
	)
	if BackupDispatchObserver != nil {
		BackupDispatchObserver(effective, "")
	}
	return effective, withinParallelism, nil
}

// openBackupReaderFactory returns a [backupReaderFactory] that mints
// one ADDITIONAL peer reader per call, plus a cleanup the orchestrator
// defers. It is the LAZY (Postgres, ADR-0084) path only: the EAGER
// (MySQL vanilla, ADR-0088) path opens its peers up front under the
// FTWRL window and the orchestrator seeds them straight into the pool's
// reusable free-reader channel (see [Backup.runBackupTablePool]'s peers
// argument), so this returns a nil factory for it — there is nothing to
// mint on demand.
//
// LAZY shape: open the source's [ir.SnapshotImporter] and mint one
// reader per call via `SET TRANSACTION SNAPSHOT '<name>'` in its own
// REPEATABLE READ tx (the ADR-0079 shape) — same view as the snapshot's
// primary, valid as long as the slot-creation conn lives. Each minted
// reader is owned by the pool and closed on its release path. The minted
// reader is single-schema, bound to the DSN's schema — the SAME binding
// the primary reader carries — so the parallel sweep reads exactly what
// the serial sweep would.
//
// tableParallelism <= 1 with within-table chunking not engaged
// (withinParallelism <= 1), or an eager snapshot, returns a nil
// factory with a no-op cleanup: the serial pool's single worker always
// wins the free reader, and the eager pool's peers all live in the
// free-reader channel, so no on-demand minting is ever needed.
// withinParallelism > 1 (ADR-0149 — the caller only passes it when the
// within gate held, which asserted the same importer surface) needs
// the factory even on a serial table sweep: one huge table's range
// workers mint their readers here.
func (b *Backup) openBackupReaderFactory(ctx context.Context, snap *irbackup.Snapshot, tableParallelism, withinParallelism int) (backupReaderFactory, func(), error) {
	if tableParallelism <= 1 && withinParallelism <= 1 {
		return nil, func() {}, nil
	}
	// Eager (MySQL coordinated) readers are seeded into the pool's
	// free-reader channel by the orchestrator; no factory is needed.
	if snap != nil && len(snap.ExtraReaders) > 0 {
		return nil, func() {}, nil
	}

	opener, ok := b.Source.(ir.SnapshotImporterOpener)
	if !ok {
		// Unreachable: backupParallelEligible / backupWithinChunkingEligible
		// already asserted this. Loud rather than a silent serial degrade
		// of an engaged gate.
		return nil, func() {}, errBackupPoolNoImporter
	}
	importer, err := opener.OpenSnapshotImporter(ctx, b.SourceDSN)
	if err != nil {
		return nil, func() {}, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("backup: open snapshot importer: %w", err))
	}
	cleanup := func() {
		if c, ok := importer.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	snapshotName := snap.SnapshotName
	factory := func(rctx context.Context) (ir.RowReader, error) {
		readers, err := importer.ImportSnapshot(rctx, snapshotName, 1)
		if err != nil {
			return nil, err
		}
		// ADR-0149: a minted reader may serve a table worker whose chunk
		// DECISION probes EstimateRowCount — opt it into the exact-COUNT
		// never-ANALYZEd fallback so a fresh source doesn't silently
		// report 0 and lose chunking (the 59c55e27 class). No-op for
		// engines without the surface; sync cold-start minting does NOT
		// share this path (ADR-0079 v1.1 unchanged).
		applyExactCountEstimate(readers[0])
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
//
// Checkpoint cost (ADR-0086, task #54): when the store implements
// [irbackup.Appender], per-chunk / per-table checkpoints append one
// JSON line to the `manifest.progress.jsonl` sidecar — O(1) per event
// — instead of re-marshaling the whole manifest (schema included) per
// checkpoint, which made the row sweep quadratic in table count
// (~78 h of pure manifest rewriting at 100k tables, per the #38 scale
// probe). The base manifest is written once by [commitBase] (stamped
// [irbackup.FormatVersionProgressSidecar] so OLDER binaries refuse the
// layout loudly instead of resuming off an under-reporting base), and
// [finalize] folds everything back into the one self-contained final
// `manifest.json` (re-stamped to the schema's own format version) and
// deletes the sidecar — finalized backups keep the pre-ADR shape.
//
// Stores without the append capability (object stores) keep the
// legacy full-rewrite checkpoints byte-for-byte — a named wart: the
// cost grows with table count, and [commitBase] says so loudly on
// large corpora.
type manifestCommitter struct {
	mu       sync.Mutex
	store    irbackup.Store
	manifest *irbackup.Manifest

	// Sidecar mode (ADR-0086). appender == nil means legacy mode:
	// every checkpoint rewrites the full manifest, exactly the
	// pre-ADR-0086 behaviour.
	appender    irbackup.Appender
	sidecarPath string
	attemptID   string

	// finalVersion is the manifest's schema-appropriate format version
	// ([irbackup.FormatVersionFor]), displaced while in progress by the
	// sidecar-layout stamp and restored by [finalize].
	finalVersion int
}

// newManifestCommitter builds the committer for one backup run. When
// store can append (the [irbackup.Appender] capability), the manifest
// is switched into the sidecar-checkpoint layout: stamped
// [irbackup.FormatVersionProgressSidecar] and given a
// [irbackup.ProgressSidecarRef] with a fresh random attempt ID (the
// stale-sidecar guard — see the ref's doc). Otherwise the committer
// runs in legacy full-rewrite mode and the manifest is untouched.
func newManifestCommitter(store irbackup.Store, manifest *irbackup.Manifest) (*manifestCommitter, error) {
	c := &manifestCommitter{store: store, manifest: manifest}
	appender, ok := store.(irbackup.Appender)
	if !ok {
		return c, nil
	}
	attemptID, err := newBackupAttemptID()
	if err != nil {
		return nil, fmt.Errorf("backup: mint progress attempt id: %w", err)
	}
	c.appender = appender
	c.sidecarPath = ManifestProgressFileName
	c.attemptID = attemptID
	c.finalVersion = manifest.FormatVersion
	// The in-progress stamp must never DOWNGRADE the schema-derived
	// version: a schema carrying standalone sequences already sits
	// above the sidecar tier, and re-stamping it lower would let an
	// older (sidecar-capable, sequence-unaware) binary resume the run
	// and finalize a manifest with the Sequences field silently
	// dropped — the Bug 116 class through the resume door.
	manifest.FormatVersion = max(irbackup.FormatVersionProgressSidecar, c.finalVersion)
	manifest.ProgressSidecar = &irbackup.ProgressSidecarRef{
		File:      c.sidecarPath,
		AttemptID: attemptID,
	}
	return c, nil
}

// newBackupAttemptID mints the random per-attempt token sidecar lines
// are stamped with. 8 random bytes hex-encoded — collision across the
// handful of attempts a single backup directory ever sees is not a
// realistic event.
func newBackupAttemptID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
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

// appendChunk records one finished chunk on entry and checkpoints it
// so a mid-table crash leaves an up-to-date record of exactly which
// chunks completed — progress observability, plus it keeps flush's
// content-addressed same-path upload skip effective across a re-run.
// Resume never REUSES partial chunk lists (Bug 135). Sidecar mode
// appends one delta line (O(1)); legacy mode rewrites the manifest.
func (c *manifestCommitter) appendChunk(ctx context.Context, entry *irbackup.TableManifest, ci *irbackup.ChunkInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.Chunks = append(entry.Chunks, ci)
	if c.appender == nil {
		return c.commitLocked(ctx)
	}
	return c.appendEventLocked(ctx, &irbackup.ProgressEvent{
		AttemptID: c.attemptID,
		Event:     irbackup.ProgressEventChunk,
		Schema:    entry.Schema,
		Table:     entry.Name,
		Chunk:     ci,
	})
}

// finishTable flips entry to its terminal complete state (natural row
// EOF) and checkpoints it — the per-table checkpoint. Empty tables and
// tables whose row count is an exact chunk multiple rely on this
// checkpoint (their last appendChunk checkpoint doesn't carry the
// Partial=false flip).
func (c *manifestCommitter) finishTable(ctx context.Context, entry *irbackup.TableManifest, rowCount int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.RowCount = rowCount
	entry.Partial = false
	if c.appender == nil {
		return c.commitLocked(ctx)
	}
	return c.appendEventLocked(ctx, &irbackup.ProgressEvent{
		AttemptID: c.attemptID,
		Event:     irbackup.ProgressEventTableComplete,
		Schema:    entry.Schema,
		Table:     entry.Name,
		RowCount:  rowCount,
	})
}

// appendEventLocked marshals one progress event and appends it to the
// sidecar. Callers hold mu — appends to the same path must be
// serialized (the [irbackup.Appender] contract), and the event reads
// entry fields peers may otherwise mutate.
func (c *manifestCommitter) appendEventLocked(ctx context.Context, ev *irbackup.ProgressEvent) error {
	line, err := ev.MarshalLine()
	if err != nil {
		return err
	}
	if err := c.appender.Append(ctx, c.sidecarPath, bytes.NewReader(line)); err != nil {
		return fmt.Errorf("append progress sidecar event: %w", err)
	}
	return nil
}

// commitBase writes the pre-sweep in-progress manifest — the ONE base
// write the sidecar's deltas accrue on (ADR-0086) — and resets any
// stale sidecar left by a previous attempt's crash window. The base
// carries the schema, the ADR-0085 anchor stamp, and every pre-staged
// table entry; its durability is what allows BackupSnapshot.Commit to
// fire (the crashed-chain-slot adoption contract — do not reorder).
//
// Legacy mode (no appender) is the plain pre-ADR-0086 manifest write,
// with the quadratic per-checkpoint cost named loudly when the corpus
// is large enough for it to hurt.
func (c *manifestCommitter) commitBase(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.commitLocked(ctx); err != nil {
		return err
	}
	if c.appender == nil {
		logf := slog.DebugContext
		if len(c.manifest.Tables) >= legacyCheckpointWarnTables {
			logf = slog.WarnContext
		}
		logf(
			ctx, "backup: store does not support appends; per-chunk checkpoints rewrite the full manifest (cost grows with table count)",
			slog.Int("tables", len(c.manifest.Tables)),
		)
		return nil
	}
	// Reset the sidecar AFTER the base write: a stale sidecar surviving
	// a crash in between is harmless (its lines carry the previous
	// attempt's ID and replay skips them), whereas deleting first would
	// widen the window where the PRIOR attempt's progress is lost.
	if err := c.store.Delete(ctx, c.sidecarPath); err != nil {
		return fmt.Errorf("reset progress sidecar %q: %w", c.sidecarPath, err)
	}
	return nil
}

// legacyCheckpointWarnTables is the corpus size above which commitBase
// WARNs (vs DEBUGs) about legacy-mode quadratic checkpoint cost.
const legacyCheckpointWarnTables = 1000

// finalize writes the final self-contained manifest after the pool has
// drained: sidecar mode restores the schema-appropriate format version
// and drops the sidecar reference first, so the finalized manifest is
// byte-shape-identical to the pre-ADR-0086 contract (older binaries
// read it; restore/verify/chain tooling unaffected), then deletes the
// now-redundant sidecar. The delete is best-effort: with the reference
// cleared the file is inert (replay is gated on the manifest ref), and
// failing a finished backup over cleanup would be disproportionate.
//
// The caller has already set PartialState/BackupID/EndPosition; this
// is single-threaded (post-pool) but takes the mutex anyway — every
// manifest mutation in this file happens under mu.
func (c *manifestCommitter) finalize(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.appender != nil {
		c.manifest.FormatVersion = c.finalVersion
		c.manifest.ProgressSidecar = nil
	}
	if err := c.commitLocked(ctx); err != nil {
		return err
	}
	if c.appender == nil {
		return nil
	}
	if err := c.store.Delete(ctx, c.sidecarPath); err != nil {
		slog.WarnContext(
			ctx, "backup: progress sidecar cleanup failed; the file is inert (the final manifest no longer references it) but remains on the store",
			slog.String("path", c.sidecarPath),
			slog.String("err", err.Error()),
		)
	}
	return nil
}

// commitLocked is the marshal+Put core. Callers hold mu — the marshal
// MUST happen under the lock because it reads every worker's entry.
func (c *manifestCommitter) commitLocked(ctx context.Context) error {
	b, err := json.MarshalIndent(c.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return c.store.Put(ctx, lineage.ManifestFileName, bytes.NewReader(b))
}
