// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sync cold start's progress-recording LIFECYCLE, in one place for
// every cold-start lane (audit A0909-P2b).
//
// # What this file exists to stop happening again
//
// v0.148.0 gave a cold start a phase row and per-table progress rows so
// `sync status` could report on a run whose `sluice_cdc_state` row is
// not written until the very end. It was built INSIDE
// [Streamer.runColdStartParallel] — the ADR-0079 fast lane, whose gate
// ([coldStartFastEligible]) admits only a source that exports a
// SHAREABLE snapshot. That is PostgreSQL and nothing else. Every serial
// cold start reached [runBulkCopyWithOpts] instead and recorded nothing
// at all:
//
//   - MySQL / MariaDB binlog, VStream (PlanetScale / Vitess) — no
//     shareable snapshot;
//   - pgtrigger, sqlite-trigger, d1-trigger — likewise;
//   - `--schema-already-applied`, an ADR-0072 resumable copy, and the A0
//     client-copy fallback — excluded by the gate's other three
//     predicates even on Postgres;
//   - every `--databases` / `--schemas` fan-out
//     ([Streamer.coldStartMultiDatabase]), which is a separate entry
//     point that never reaches the single-stream dispatch at all.
//
// The operator field report that motivated the whole feature
// (docs/operator/cross-region-migration.md) was an AWS→GCP move of a
// PlanetScale MySQL database — a serial lane. The feature did not cover
// its own motivating case.
//
// So the lifecycle lives here and both lanes call it, rather than the
// fast lane owning a copy of it: [Streamer.coldStartRunCopy] opens the
// recording once and hands it to whichever lane it dispatches to.
//
// # The roster: every path that copies rows, recorded or exempt
//
// Written out rather than promised, because "which sibling did this
// miss" is this project's most expensive recurring question:
//
//   - [Streamer.runColdStartParallel] (ADR-0079 fast lane, PG) —
//     RECORDED. Phases + per-table rows via [runBulkCopyPhases], plus
//     the snapshot anchor. Unchanged in substance; it now takes the
//     context from its caller instead of building one.
//   - [runBulkCopyWithOpts] via [Streamer.coldStartRunCopy] (every
//     serial single-stream cold start) — RECORDED, no anchor.
//   - [runBulkCopyWithOpts] via [Streamer.coldStartCopyOneDatabase]
//     (the `--databases` / `--schemas` fan-out) — RECORDED, no anchor,
//     progress keys namespace-qualified. It is a DISTINCT entry point
//     that never reaches coldStartRunCopy, which is why it is listed
//     and wired separately rather than assumed covered.
//   - [Streamer.resumeStoppedColdStart] — already RECORDED before this
//     change (its own [newSyncRecordingContext]); untouched.
//   - [runBulkCopyForAddTable] (`sync add-table`) — EXEMPT: not a cold
//     start. The stream already has its `sluice_cdc_state` row, so
//     `sync status` reports it through the ordinary stream surface and
//     never had the blackout this file addresses.
//   - [Migrator.Run] → [runBulkCopyPhases] (`sluice migrate`) — EXEMPT:
//     it has its own resume context, keyed by [deriveMigrationID], and
//     records and RESUMES from it. Nothing here touches it; the
//     zero-value [bulkCopyOpts.Recording] is what keeps that true, and
//     TestBulkCopyRecordingZeroValueTouchesNoStore is what checks it.
//
// # The one deliberate asymmetry: the SNAPSHOT ANCHOR
//
// Only the fast lane records an anchor, and that is a scope decision
// rather than an oversight. The recorded anchor is what
// [Streamer.resumeStoppedColdStart] stands on to SKIP a copy, and that
// resume ladder was designed, measured and pinned against the fast
// lane's shape only — it re-runs the post-copy DDL phases, which is
// wrong under `--schema-already-applied`, and it has never been
// exercised against a mid-COPY ADR-0072 resume or the A0 client-copy
// fallback. Recording an anchor here would silently widen that door.
//
// The mechanism that keeps it shut is [gradeRecordedColdStartHeader]:
// it declines any header whose `snapshot_anchor` is empty, and
// [beginRecordedColdStart] writes no anchor when handed an empty
// record. TestSerialColdStartRecordsNoSnapshotAnchor is the test that
// fails if that stops being true — this comment is a hypothesis without
// it.

package pipeline

import (
	"context"
	"log/slog"
	"sync"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// openColdStartRecording opens the target's progress store and returns
// the record-but-never-resume context this cold start writes through,
// plus the closer the caller defers.
//
// rec is the run's snapshot anchor + copy-shape fingerprint, or the
// zero value for a lane that deliberately records no anchor (see the
// file comment). Either way [beginRecordedColdStart] first CLEARS any
// previous run's rows, so what a reader finds describes THIS run.
//
// Every failure degrades to an inert context with a WARN, never an
// error: this is observability, and refusing to migrate because a
// progress row could not be written would be strictly worse than the
// blackout it replaces. The returned closer is always safe to call.
func (s *Streamer) openColdStartRecording(
	ctx context.Context,
	streamID string,
	rec ir.SnapshotAnchorRecord,
) (rc resumeContext, closeStore func()) {
	store, err := openMigrationStateStore(ctx, s.Target, s.TargetDSN, s.TargetSchema)
	if err != nil {
		slog.WarnContext(ctx, "pipeline: cold start could not open the progress store; `sync status` will report "+
			"this stream as absent until the copy finishes and the CDC anchor is written",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()))
		store = nil
	}
	rc = newSyncRecordingContext(ctx, store, streamID)
	beginRecordedColdStart(ctx, rc, rec)
	if store == nil {
		return rc, func() {}
	}
	// Close the store we opened whether or not the context ended up
	// recording: the degrade paths inside newSyncRecordingContext (an
	// over-long stream id, tables that could not be ensured) leave rc
	// inert with the pool still open — one leak per cold start.
	return rc, func() { migcore.CloseIf(store) }
}

// markRecordedColdStartComplete flips the recorded run TERMINAL.
//
// Without it `sync status` prints "cold start in progress" above the
// live stream row forever, with a LAST PROGRESS WRITE age that only
// climbs — which is the exact signal that section tells operators means
// the run is DEAD (audit A0909-P3).
//
// Only the header identity is load-bearing: [markComplete] writes
// [headerOnly] of what it is given, and sets Phase + LastError itself,
// so a header carrying just the migration id is byte-equivalent to
// handing it the run's whole accumulated state. Inert on a
// non-recording context.
func markRecordedColdStartComplete(ctx context.Context, rc resumeContext) {
	markComplete(ctx, rc, ir.MigrationState{MigrationID: rc.migrationID})
}

// tableProgressRecorder writes the per-table progress rows for a copy
// driven by [runBulkCopyWithOpts] — the serial cold start, the ADR-0100
// concurrent-group copy, and the ADR-0119 work-stealing copy.
//
// It persists through the SAME three primitives migrate's cross-table
// pool persists through — [writeTableProgress] (the ADR-0082 per-row
// upsert, and the throttle), [cloneTableProgressForWrite] and
// [warnStateWriteFailed] — so a change to HOW a progress row is written
// reaches both lanes. It does not call [setTableProgressAndWrite]
// itself, and that is deliberate rather than an oversight: that helper
// REPLACES a table's entry, which is right when one goroutine owns a
// table start-to-finish, and wrong here, where a table can be copied as
// M work-stealing chunks by M different pipelines and the running row
// count has to survive each of them. The read-modify-write below has to
// be atomic with its own lock, which a helper that takes the lock for
// you cannot give.
//
// What it adds over the pool's shape is two things: a namespace
// qualifier for the multi-database fan-out (N databases under ONE
// migration id), and a per-table outstanding-item count so a table
// split into chunks is marked complete when its LAST chunk lands rather
// than when its first does.
//
// A nil recorder is inert, so every non-recording caller — `migrate`,
// every test, any future caller that has nothing to record — passes nil
// and pays one nil check per table.
type tableProgressRecorder struct {
	rc        resumeContext
	state     *ir.MigrationState
	mu        sync.Mutex
	namespace string

	// outstanding counts the work items still to land per table, for the
	// work-stealing lane where one table is copied as M chunks claimed by
	// different pipelines. Nil (every other lane) means one item per
	// table: the first completion is the terminal one.
	//
	// Guarded by mu, which also guards state.TableProgress — one lock for
	// one invariant ("what this table's row says") rather than two that
	// could disagree.
	outstanding map[string]int
}

// newTableProgressRecorder returns the recorder for a run, or nil when
// the context records nothing — so the copy paths' nil check is the
// single gate and no caller has to ask `rc.writes()` itself.
func newTableProgressRecorder(rc resumeContext, namespace string) *tableProgressRecorder {
	if !rc.writes() {
		return nil
	}
	return &tableProgressRecorder{
		rc:        rc,
		state:     &ir.MigrationState{MigrationID: rc.migrationID, TableProgress: map[string]ir.TableProgress{}},
		namespace: namespace,
	}
}

// key is the progress row's key: the bare table name (what the parallel
// lane and `migrate` both write, so the two lanes stay comparable), or
// "<namespace>.<table>" on the multi-database fan-out where the bare
// name is not unique within one migration id.
func (r *tableProgressRecorder) key(table *ir.Table) string {
	return qualifiedTableName(r.namespace, table.Name)
}

// expectItems declares how many work items a table's copy is split into
// on the work-stealing lane. Called once per table before any pipeline
// spawns; anything not declared is treated as a single item.
func (r *tableProgressRecorder) expectItems(table *ir.Table, n int) {
	if r == nil || n <= 1 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outstanding == nil {
		r.outstanding = map[string]int{}
	}
	r.outstanding[r.key(table)] = n
}

// started records the in-progress breadcrumb for a table. The first
// write for a table is never throttled ([progressThrottle]), so a table
// shows up in the recorded state as soon as its copy begins. Repeated
// calls (one per work-stealing chunk) are harmless: the row is an
// upsert and the state it carries is the same.
func (r *tableProgressRecorder) started(ctx context.Context, table *ir.Table) {
	if r == nil {
		return
	}
	key := r.key(table)
	r.mu.Lock()
	if _, seen := r.state.TableProgress[key]; seen {
		// A peer work item already opened this table's row. Re-writing
		// the bare in-progress breadcrumb here would zero the running
		// count that item has been accumulating.
		r.mu.Unlock()
		return
	}
	entry := ir.TableProgress{State: ir.TableProgressInProgress}
	r.state.TableProgress[key] = entry
	entryCopy := cloneTableProgressForWrite(entry)
	r.mu.Unlock()
	if err := writeTableProgress(ctx, r.rc, key, entryCopy); err != nil {
		warnStateWriteFailed(ctx, key, err)
	}
}

// completed records rows against a table and marks it complete once
// every one of its work items has landed. Until then the row stays
// in_progress carrying the running total, which is the truth about a
// half-copied table — marking it complete on the first chunk would
// record a finished copy that has not happened.
func (r *tableProgressRecorder) completed(ctx context.Context, table *ir.Table, rows int64) {
	if r == nil {
		return
	}
	key := r.key(table)
	r.mu.Lock()
	entry := r.state.TableProgress[key]
	entry.RowsCopied += rows
	entry.State = ir.TableProgressInProgress
	remaining := 0
	if r.outstanding != nil {
		if n, ok := r.outstanding[key]; ok {
			remaining = n - 1
			r.outstanding[key] = remaining
		}
	}
	if remaining <= 0 {
		entry.State = ir.TableProgressComplete
	}
	r.state.TableProgress[key] = entry
	entryCopy := cloneTableProgressForWrite(entry)
	r.mu.Unlock()
	if err := writeTableProgress(ctx, r.rc, key, entryCopy); err != nil {
		warnStateWriteFailed(ctx, key, err)
	}
}
