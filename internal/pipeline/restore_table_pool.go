// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-table worker pool for the restore bulk-apply phase (ADR-0084,
// restore side).
//
// Before this file the restore orchestrator applied tables strictly
// serially — `for _, table := range schema.Tables { r.restoreTable(...) }`
// through the ONE row writer Run opened. On a many-table corpus that
// left the target's cores and fsync bandwidth idle between tables (the
// 2026-06-10 benchmark: 133 GB / 43 tables, `sluice restore` projected
// ~3 h vs `pg_restore -j8` 917 s — and restore wall time IS the
// operator's recovery-time objective). This file adds the missing
// axis: a bounded pool that applies up to tableParallelism tables
// CONCURRENTLY, each through its own row-writer connection.
//
// The shape deliberately mirrors backup_table_pool.go (the read-side
// twin) and migrate_table_pool.go (ADR-0076), with one structural
// simplification: restore needs NO capability gate. The backup side
// gates on a shareable exported snapshot because parallel READERS must
// observe one consistent view; parallel WRITERS have no such
// constraint — each table's rows land through an independent
// connection and tables are independent (constraints/indexes are later
// phases). So restore parallelism is engine-generic: it engages for
// every target (PG and MySQL alike), bounded only by the TARGET's
// measured connection budget.
//
//   - Free-writer 1-slot channel: the orchestrator's already-open row
//     writer is claimed by one in-flight table; peers open dedicated
//     writers via a restoreWriterFactory ([Restore.openTargetRowWriter]
//     — the SAME construction path as the primary, so buffer cap +
//     target-schema routing can never drift) and close them on
//     release. The primary's lifecycle stays with the orchestrator.
//   - Per-table chunk ordering is preserved by construction:
//     [Restore.restoreTable] is called UNCHANGED — its producer
//     goroutine streams the table's chunks IN ORDER through one
//     channel into one WriteRows call, and the Bug-40b
//     cancel-on-writer-error shape inside it is per-call-local.
//   - DataOnly segments parallelize identically: the idempotent-writer
//     selection inside restoreTable type-asserts its OWN writer, so
//     each worker independently routes through WriteRowsIdempotent.
//
// Read-side shared state under concurrency (verified, not assumed):
//
//   - r.chainCEK is set once by preflightEncryption BEFORE the pool
//     and read-only after (chunkCEK only reads it).
//   - r.segCodec is set pre-pool (Run step 1 / ChainRestore.applyFull)
//     and read-only after.
//   - Per-chunk-mode Envelope.UnwrapCEK runs concurrently across peer
//     tables: all four implementations (Passphrase, AWS/GCP/Azure KMS)
//     are read-only on envelope state (e.kek / SDK clients, which are
//     goroutine-safe) and the AES-GCM open is per-call — the exact
//     mirror of the WrapCEK audit on the backup side (ADR-0084 §4).
//   - chunkReader is per-chunk-local inside each worker; store Gets
//     are concurrent-safe on every BackupStore (os.Open / blob SDKs).

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
)

// restoreDispatchObserver is a TEST-ONLY seam: when non-nil it fires
// with the resolved cross-table dispatch decision the moment
// [Restore.resolveRestoreTableParallelism] chooses — tableParallelism
// > 1 means the parallel apply engaged; reason carries the not-engaged
// clause ("" when engaged). nil in production (a single nil check).
// Mirrors [backupDispatchObserver].
var restoreDispatchObserver func(tableParallelism int, reason string)

// restoreTableTask is one table's unit of work for the pool: the
// (retargeted) schema table and its manifest entry. Tables absent from
// the manifest are filtered out before the tasks are built (with the
// historical "skipping bulk-copy" INFO).
type restoreTableTask struct {
	table *ir.Table
	entry *ir.TableManifest
}

// restoreWriterFactory opens one additional fully-configured
// [ir.RowWriter] against the target. nil when the pool runs serial
// (tableParallelism=1) — the single worker always wins the free
// writer, so the factory is never called there.
type restoreWriterFactory func(ctx context.Context) (ir.RowWriter, error)

// runRestoreTablePool applies tasks through a bounded cross-table
// worker pool (ADR-0084 restore side). tableParallelism caps how many
// tables bulk-apply concurrently; 1 collapses to the pre-ADR-0084
// serial behaviour (one goroutine, reusing the orchestrator's writer
// for every table in turn) through the SAME code path.
//
// primary is the orchestrator's already-open row writer — the "free
// writer". Exactly one running table uses it at a time (claimed via a
// 1-slot channel); peers open their own via factory and close it on
// release. The free writer is NOT closed here (the caller owns its
// lifecycle through Run's deferred closeIf).
//
// The errgroup's derived ctx cancels on the first table's error so
// peers unwind promptly (each through restoreTable's own Bug-40b
// producer-cancel path); tg.Wait returns the first error.
func (r *Restore) runRestoreTablePool(
	ctx context.Context,
	tasks []restoreTableTask,
	primary ir.RowWriter,
	factory restoreWriterFactory,
	tableParallelism int,
) error {
	limit := tableParallelism
	if limit < 1 {
		limit = 1
	}

	// freeWriter is a 1-slot pool holding the orchestrator's writer. A
	// table goroutine tries a non-blocking receive; the winner reuses
	// the free writer (and returns it on completion so a later table
	// can claim it), every other concurrent table opens its own. This
	// mirrors the backup pool's free-reader channel at the writer-only
	// granularity restores need (no reader side).
	freeWriter := make(chan ir.RowWriter, 1)
	freeWriter <- primary

	tg, tctx := errgroup.WithContext(ctx)
	tg.SetLimit(limit)
	for _, task := range tasks {
		task := task
		tg.Go(func() error {
			rw, release, err := acquireRestoreWriter(tctx, freeWriter, factory)
			if err != nil {
				return err
			}
			defer release()
			if err := r.restoreTable(tctx, rw, task.table, task.entry); err != nil {
				return wrapWithHint(PhaseBulkCopy, fmt.Errorf("restore: table %q: %w", task.table.Name, err))
			}
			return nil
		})
	}
	return tg.Wait()
}

// acquireRestoreWriter returns the writer a table goroutine should
// apply through, plus a release function the caller defers. It first
// tries to claim the free writer (non-blocking); if another table
// already holds it, it opens a dedicated writer via factory.
//
// The release function returns the free writer to the pool (so a
// later table can reuse it) or closes a dedicated one. It never
// closes the free writer — the orchestrator owns that lifecycle.
// Mirrors [acquireBackupReader] / [acquireTablePair].
func acquireRestoreWriter(
	ctx context.Context,
	freeWriter chan ir.RowWriter,
	factory restoreWriterFactory,
) (ir.RowWriter, func(), error) {
	select {
	case w := <-freeWriter:
		// Won the free writer; return it to the pool on release.
		return w, func() { freeWriter <- w }, nil
	default:
		// Free writer is in use by a peer table; open a dedicated one.
		if factory == nil {
			// Unreachable: a nil factory means the pool runs serial
			// (limit 1), where the free writer is always available. Loud
			// rather than a silent nil-func call.
			return nil, func() {}, errRestorePoolNoFactory
		}
		w, err := factory(ctx)
		if err != nil {
			return nil, func() {}, err
		}
		return w, func() { closeIf(w) }, nil
	}
}

// errRestorePoolNoFactory is the loud precondition guard for
// [acquireRestoreWriter]: the dedicated-writer branch is only
// reachable when the pool runs with tableParallelism > 1, which the
// orchestrator only configures together with a writer factory.
// Reaching it with a nil factory is a programming error, surfaced
// rather than silently deref'd. Mirrors [errBackupPoolNoFactory].
var errRestorePoolNoFactory = errors.New("pipeline: restore table pool: dedicated writer needed but no writer factory configured")

// resolveRestoreTableParallelism resolves the effective cross-table
// fan-out for the restore bulk-apply: the operator's requested value
// (0 = auto = 4, [resolveTableParallelism]) clamped to the number of
// tables actually being applied and bounded by the TARGET's measured
// connection budget. Unlike the backup side there is NO capability
// gate — parallel writers need no shared snapshot, so every target
// engine is eligible. ≤1 collapses to serial — the same pool runs
// with one worker — with a loud INFO naming the reason (the ADR-0079
// disposition: a silent fallback would leave operators wondering why
// the knob did nothing).
//
// The budget chokepoint reuses [resolveTargetCopyParallelism] against
// the TARGET DSN. Restore opens exactly ONE writer connection per
// concurrent table (no within-table axis), so the budget-capped value
// IS the table parallelism directly. Targets without a prober (MySQL)
// pass through unclamped, the same contract as migrate. The schema
// writer's single long-lived connection rides in the prober's
// existing headroom, exactly as on the migrate path.
func (r *Restore) resolveRestoreTableParallelism(ctx context.Context, taskCount int) (int, error) {
	requested := resolveTableParallelism(r.TableParallelism)
	if requested > taskCount {
		requested = taskCount // never fan out wider than there are tables to apply
	}
	serialReason := func(reason string) int {
		slog.InfoContext(
			ctx, "restore: cross-table parallel apply not engaged; applying tables serially",
			slog.String("reason", reason),
			slog.Int("requested_table_parallelism", requested),
		)
		if restoreDispatchObserver != nil {
			restoreDispatchObserver(1, reason)
		}
		return 1
	}
	if requested <= 1 {
		reason := "cross-table parallelism disabled (--table-parallelism=1)"
		if taskCount <= 1 {
			reason = "at most one table to apply"
		}
		return serialReason(reason), nil
	}

	effective, _, err := resolveTargetCopyParallelism(ctx, r.Target, r.TargetDSN, requested, 0)
	if err != nil {
		return 0, err
	}
	if effective <= 1 {
		return serialReason("target connection budget allows only one writer"), nil
	}
	slog.InfoContext(
		ctx, "restore: cross-table parallel apply engaged (ADR-0084)",
		slog.Int("table_parallelism", effective),
	)
	if restoreDispatchObserver != nil {
		restoreDispatchObserver(effective, "")
	}
	return effective, nil
}
