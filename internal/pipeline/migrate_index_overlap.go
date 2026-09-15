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
// connection slice migcore.SplitCopyAndIndexBudget reserved for the index axis
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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
)

// indexAxisError tags an error as having come from the INDEX-BUILD axis of
// the overlapped phase rather than the copy axis, so
// [runOverlappedCopyAndIndexPhase] can attribute the failure to the phase
// that actually produced it instead of guessing.
//
// The guess used to be "bulk-copy, conservatively", and it cost operators
// the single most valuable sentence sluice can say at that moment. A
// PlanetScale index build walled at errno 3024 after a 122 GB copy had
// ALREADY completed surfaced as a bare
// `mysql: create indexes on "events": Error 3024 … maximum statement
// execution time exceeded` — no `pipeline:` prefix, no `hint:` line, no
// SLUICE-E- code — because the errno-3024 hint is registered under
// [migcore.PhaseIndexes] and this call site handed the error to
// [migcore.PhaseBulkCopy], whose entries ("does not exist" / "doesn't
// exist" / "pipeline: copy table") the walled text matches none of. The
// registry was right the whole time; the wiring was not. The remedy the
// operator never saw is that the data is already copied and `--resume`
// finishes just the indexes with NO re-copy — so the natural reaction to a
// bare timeout, starting over, re-copies 122 GB for nothing.
//
// The tag is applied inside the index goroutine and read after
// [errgroup.Group.Wait], which returns the FIRST error handed to it: a copy
// failure that merely CANCELS the index axis therefore still reports as
// bulk-copy (the index axis's ctx.Err() arrives second and loses), which is
// the correct attribution.
//
// That last sentence was true only for a failure ORIGINATING in one of the
// two axes, and it is the whole reason
// [attributeOverlappedFailure] exists — read it before trusting the
// first-to-return ordering. When the cancellation arrives from outside the
// group both axes see it at once, the index axis returns first, and the tag
// then names the wrong phase.
type indexAxisError struct{ err error }

func (e indexAxisError) Error() string { return e.err.Error() }
func (e indexAxisError) Unwrap() error { return e.err }

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

	// copyErr and indexJobsQueued are the two inputs
	// [attributeOverlappedFailure] needs that errgroup cannot give it:
	// which error the COPY axis produced (Wait returns only the first
	// error any axis produced), and how many tables the index axis was
	// ever handed. Both are written inside the group and read after
	// Wait, which establishes the happens-before.
	var (
		copyErr         error
		indexJobsQueued atomic.Int64
	)

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
		copyErr = runBulkCopyTablePool(
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
					indexJobsQueued.Add(1)
				case <-gctx.Done():
				}
			},
		)
		return copyErr
	})

	// Consumer: the engine's per-table index builder. Returns nil once
	// completedTables closes and every queued build finishes, or the first
	// build error (which cancels the copy pool via gctx).
	g.Go(func() error {
		if err := indexBuilder.BuildTableIndexesFromChannel(gctx, schema, completedTables); err != nil {
			return indexAxisError{err: err}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		// Attribute the failure to the axis that produced it, not to
		// whichever phase mark happens to be in flight, and not to
		// whichever axis merely returned FIRST. See indexAxisError for what
		// the old "bulk-copy, conservatively" guess cost, and
		// attributeOverlappedFailure for what "first to return" costs.
		attr := attributeOverlappedFailure(err, copyErr, indexJobsQueued.Load())
		return migcore.WrapWithHint(attr.hint,
			markFailedLocked(ctx, rc, state, stateMu, attr.phase, attr.err))
	}
	return nil
}

// overlapFailureAttribution names the axis a failed overlapped phase is
// reported against: the hint registry's phase, the phase persisted into the
// migration-state row, and the error the operator sees.
type overlapFailureAttribution struct {
	hint  string
	phase ir.MigrationPhase
	err   error
}

// attributeOverlappedFailure decides which axis names the failure, given
// the error [errgroup.Group.Wait] returned, the error the copy axis itself
// produced, and how many tables the index axis was ever handed.
//
// It exists because Wait returns whichever goroutine returned an error
// FIRST, which is an attribution only when one axis is the thing that
// failed. [indexAxisError]'s doc argues exactly that: a copy failure that
// merely CANCELS the index axis still reports as bulk-copy because "the
// index axis's ctx.Err() arrives second and loses". That holds for a copy
// failure. It does not hold when the cancellation comes from OUTSIDE the
// group — a deadline, a Ctrl-C, a test's stall watchdog — because then both
// axes observe the cancellation at the same instant and the index axis is
// enormously quicker to return. With zero index jobs in the schema (a
// single-integer-PK fixture has none: the PK is constraint-backed and
// skipped), the PG builder's drain loop returns a bare ctx.Err() on its
// very next select — see the totalJobs == 0 branch of
// internal/engines/postgres/schema_writer_index_overlap.go — while the copy
// axis is still unwinding real database calls. The index axis wins errOnce
// essentially every time, so EVERY externally cancelled overlapped migrate
// was labelled `create indexes` regardless of where it was blocked.
// TESTFRAGILE-3's wedged CI run was reported that way, and the label sent
// the investigation at an index phase that had nothing queued.
//
// So an index axis that was handed nothing and returned nothing but the
// shared cancellation does not get to name the failure. Scope, stated
// plainly rather than implied: the only case this reclassifies is
// `indexJobsQueued == 0` AND the index error is context.Canceled /
// DeadlineExceeded. An index axis that was handed work and then hit the
// cancellation still names the failure — it may well have been mid-build —
// and every non-context index error is untouched, so the errno-3024 hint
// path this attribution was built for is unaffected.
//
// indexJobsQueued counts what the pipeline HANDED the index axis, not what
// the engine built: the pipeline cannot see inside a builder. Zero is
// therefore proof the axis had nothing to do; non-zero is not proof it did
// any of it.
func attributeOverlappedFailure(groupErr, copyErr error, indexJobsQueued int64) overlapFailureAttribution {
	var axis indexAxisError
	if !errors.As(groupErr, &axis) {
		return overlapFailureAttribution{
			hint:  migcore.PhaseBulkCopy,
			phase: ir.MigrationPhaseBulkCopy,
			err:   groupErr,
		}
	}

	if !idleIndexAxisCancellation(axis.err, indexJobsQueued) {
		// Same prefix the sequential index phase uses, so the registry's
		// PhaseIndexes entries and the operator's grep both see the shape
		// they see on every other index-phase path.
		//
		// The persisted phase moves to indexes with it. Resume is
		// unaffected: every resume decision reads per-table
		// state.TableProgress, and state.Phase is only ever compared
		// against MigrationPhaseComplete (see loadOrInitState) — so this
		// names what failed without changing what a --resume re-copies.
		return overlapFailureAttribution{
			hint:  migcore.PhaseIndexes,
			phase: ir.MigrationPhaseIndexes,
			err:   fmt.Errorf("pipeline: create indexes: %w", axis.err),
		}
	}

	// The index axis reported nothing but the shared cancellation, with
	// nothing ever queued to it. Prefer the copy axis's own error; when the
	// copy axis has none, keep the cancellation but say plainly that the
	// index axis was idle, so the sentence cannot be read as "the index
	// build hung".
	if copyErr != nil {
		return overlapFailureAttribution{
			hint:  migcore.PhaseBulkCopy,
			phase: ir.MigrationPhaseBulkCopy,
			err:   copyErr,
		}
	}
	return overlapFailureAttribution{
		hint:  migcore.PhaseBulkCopy,
		phase: ir.MigrationPhaseBulkCopy,
		err: fmt.Errorf("pipeline: copy phase cancelled with the index axis idle "+
			"(no index jobs were ever queued): %w", axis.err),
	}
}

// idleIndexAxisCancellation reports whether an index-axis error is nothing
// but the cancellation the copy axis also saw, on an axis that was never
// handed a table.
func idleIndexAxisCancellation(err error, indexJobsQueued int64) bool {
	if indexJobsQueued > 0 {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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

// verifyBuiltIndexes runs the post-index-phase loud-failure safety net
// (SLUICE-E-INDEX-MISSING): if the target writer implements
// [ir.IndexVerifier], every secondary index the build phase was supposed to
// create must now exist on the target, or the phase fails loudly naming the
// missing table.index list.
//
// This guards the silent-schema-loss CLASS — a build path that silently
// no-ops (the v0.99.x VStream miss, where a MySQL/PlanetScale target created
// NO secondary indexes yet reported success) — so it can never recur
// unnoticed even if a future refactor re-breaks the build path. It is invoked
// on every index-building path: runBulkCopyPhases (migrate + fast-parallel
// sync cold-start) and runBulkCopyWithOpts (serial sync cold-start). Writers
// without the surface are a no-op.
func verifyBuiltIndexes(ctx context.Context, sw ir.SchemaWriter, schema *ir.Schema) error {
	verifier, ok := sw.(ir.IndexVerifier)
	if !ok {
		return nil
	}
	return verifier.VerifyIndexes(ctx, schema)
}
