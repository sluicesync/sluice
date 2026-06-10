// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Overlapped copy + index-build phase (ADR-0077, roadmap item 3b(a)).
//
// Before ADR-0077 the migrate ran a full cross-table copy phase
// (runBulkCopyTablePool, ADR-0076), THEN a separate whole-schema index
// phase (sw.CreateIndexes) only after every copy connection had closed —
// so copy and index connections never coexisted. At scale that index
// phase was a sequential ~457 s tail (29% of total wall on a 110 GB /
// 43-table corpus) that pgcopydb hides by building each table's indexes as
// its data lands.
//
// This file overlaps the two: the copy pool and the engine's
// per-table index builder run as two cooperating pools under ONE errgroup.
// The copy pool's per-table success callback forwards each just-copied
// table onto a buffered channel; the index builder
// (ir.IncrementalIndexBuilder) drains it and builds that table's secondary
// indexes concurrently with the still-copying tables, sized from the
// connection slice splitCopyAndIndexBudget reserved for the index axis
// (so copy + index connections held simultaneously never exceed the
// measured budget). A copy error cancels the index pool via the shared
// ctx and vice versa.
//
// Constraints/FKs + identity-sync stay AFTER this combined phase
// (unchanged ordering — FK validation needs all data + indexes). This is
// the migrate path only; the sync cold-start path (runBulkCopyWithOpts)
// stays serial by design.

package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// onTableCopiedObserver is a TEST-ONLY observability seam (ADR-0077): when
// non-nil it fires with each table's name at the moment its copy completes,
// so the overlap integration test can record max(copyComplete) and assert
// min(indexBuildStart) < max(copyComplete) — i.e. that an index build
// genuinely STARTED before the last copy finished. nil in production (no
// overhead beyond a nil check). The postgres engine has the symmetric
// per-table index-build-start seam. Without these the chunk could silently
// regress to sequential and still pass a zero-loss test.
var onTableCopiedObserver func(tableName string)

// runOverlappedCopyAndIndexPhase runs Phase 2 (cross-table copy) and Phase
// 4 (secondary-index builds) concurrently (ADR-0077). It is only entered
// when the target engine implements [ir.IncrementalIndexBuilder] (PG); the
// orchestrator runs the sequential fallback otherwise.
//
// The two pools cooperate over completedTables (buffered to #tables so a
// copy goroutine never blocks handing off): the copy pool pushes each
// just-copied table (filtering out tables already IndexesBuilt on resume),
// closes the channel when the pool finishes, and the index builder drains
// it. Both run under one errgroup so a failure on either axis cancels the
// other.
//
// Per-table IndexesBuilt accounting: the index builder fires the
// registered TableIndexedNotifier callback once a table's last index
// lands; the callback flips state.TableProgress[name].IndexesBuilt = true
// through the same clone-under-lock setTableProgressAndWrite helper the
// copy pool uses (ADR-0076 stateMu discipline), so a resume short-circuits
// fully-indexed tables.
func runOverlappedCopyAndIndexPhase(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	schema *ir.Schema,
	rows ir.RowReader,
	sw ir.SchemaWriter,
	rw ir.RowWriter,
	indexBuilder ir.IncrementalIndexBuilder,
	resuming bool,
	bulkBatchSize int,
	parallel *parallelBulkCopyDeps,
	tableParallelism int,
	redactor *redact.Registry,
	shard ShardColumnSpec,
) error {
	if err := markPhase(ctx, rc, state, ir.MigrationPhaseBulkCopy); err != nil {
		_ = err
	}
	if state.TableProgress == nil {
		state.TableProgress = map[string]ir.TableProgress{}
	}

	// completedTables carries each copied table to the index builder.
	// Buffered to the table count so a copy goroutine's onTableCopied push
	// never blocks on a busy index pool (it must return promptly — index
	// builds must not run on copy goroutines, or they'd starve copy slots).
	completedTables := make(chan *ir.Table, len(schema.Tables))

	// Register the per-table IndexesBuilt callback. The builder invokes it
	// from a build worker once a table's last index finishes; we flip the
	// table's IndexesBuilt under stateMu so resume can fully skip it. nil
	// builder-notifier (an engine without the optional surface) still
	// builds indexes — IndexesBuilt just isn't recorded, which only means
	// a future resume re-feeds the table (a no-op under IF NOT EXISTS).
	if notifier, ok := sw.(ir.TableIndexedNotifier); ok {
		notifier.SetTableIndexedCallback(func(table *ir.Table) {
			markTableIndexesBuilt(ctx, rc, state, stateMu, table.Name)
		})
	}

	g, gctx := errgroup.WithContext(ctx)

	// Producer: the cross-table copy pool. Its onTableCopied fires for
	// every table whose copy returned nil — which on resume INCLUDES the
	// resumeActionSkip path (a completed table returns nil without
	// re-copying). We forward such a table to the index builder ONLY when
	// its indexes are not yet built, so:
	//   - fresh copy            → IndexesBuilt false → fed (builds indexes);
	//   - resume, copied-not-indexed → IndexesBuilt false → fed (finishes);
	//   - resume, fully-indexed → IndexesBuilt true  → NOT fed (skipped).
	// The channel is closed once the pool finishes so the builder drains.
	g.Go(func() error {
		defer close(completedTables)
		return runBulkCopyTablePool(
			gctx, rc, state, stateMu, schema, rows, rw,
			resuming, bulkBatchSize, parallel, tableParallelism, redactor, shard,
			func(table *ir.Table) {
				if hook := onTableCopiedObserver; hook != nil {
					hook(table.Name)
				}
				if alreadyIndexed(state, stateMu, table.Name) {
					return
				}
				select {
				case completedTables <- table:
				case <-gctx.Done():
				}
			},
		)
	})

	// Consumer: the engine's per-table index builder. Returns nil once
	// completedTables closes and every queued build finishes, or the first
	// build error (which cancels the copy pool via gctx).
	g.Go(func() error {
		return indexBuilder.BuildTableIndexesFromChannel(gctx, schema, completedTables)
	})

	if err := g.Wait(); err != nil {
		// Attribute the failure to whichever phase is in flight. Both axes
		// share the bulk-copy/index window; the bulk-copy phase mark is the
		// in-flight one (index builds piggyback on it), so wrap as indexes
		// only if the error came from the build side is hard to tell here —
		// use the bulk-copy hint, the conservative attribution, and persist.
		return wrapWithHint(PhaseBulkCopy, markFailedLocked(ctx, rc, state, stateMu, ir.MigrationPhaseBulkCopy, err))
	}
	return nil
}

// markTableIndexesBuilt flips one table's IndexesBuilt flag to true and
// persists THAT table's progress row, taking stateMu for the in-memory
// map mutation and cloning the entry under the lock before the
// JSON-encoding write (ADR-0076 / ADR-0077 resume-under-concurrency
// discipline; per-table persistence per ADR-0082). It preserves the
// table's existing State (complete) and other fields; only IndexesBuilt
// changes. A write error is logged at WARN and swallowed — the index
// build itself is the load-bearing work, the breadcrumb is best-effort,
// mirroring setTableProgressAndWrite.
func markTableIndexesBuilt(
	ctx context.Context,
	rc resumeContext,
	state *ir.MigrationState,
	stateMu *sync.Mutex,
	tableName string,
) {
	stateMu.Lock()
	entry := state.TableProgress[tableName]
	entry.IndexesBuilt = true
	state.TableProgress[tableName] = entry
	entryCopy := cloneTableProgressForWrite(entry)
	stateMu.Unlock()
	if err := writeTableProgress(ctx, rc, tableName, entryCopy); err != nil {
		warnStateWriteFailed(ctx, tableName, err)
	}
	slog.DebugContext(ctx, "migration: table indexes built",
		slog.String("table", tableName))
}

// alreadyIndexed reports whether the table's IndexesBuilt flag is set,
// reading under stateMu (peer copy/index goroutines mutate the shared
// map concurrently). Used by the copy pool's onTableCopied to avoid
// re-feeding a fully-indexed table to the index builder on resume.
func alreadyIndexed(state *ir.MigrationState, stateMu *sync.Mutex, tableName string) bool {
	stateMu.Lock()
	defer stateMu.Unlock()
	return state.TableProgress[tableName].IndexesBuilt
}
