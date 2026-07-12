// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Resumable simple-mode migrations.
//
// Today, if `sluice migrate` fails partway, the operator has to drop
// the target and start over. For multi-hour migrations of large tables
// that's a real operational pain point. The resume path picks up where
// the failure left off, skips what's already done, and only redoes
// what's actually needed.
//
// State is persisted per target — parallel to (but deliberately
// separate from) the streamer's `sluice_cdc_state` — keyed by
// --migration-id: a header row in `sluice_migrate_state` (phase,
// timestamps, last error) plus one `sluice_migrate_table_progress`
// row per table (ADR-0082). The pipeline writes the header at every
// phase transition and the touched table's progress row at every
// per-table bulk-copy boundary; on restart with --resume it reads
// the merged state and branches:
//
//   - phase `tables` → re-run CreateTablesWithoutConstraints
//     (idempotent via CREATE TABLE IF NOT EXISTS).
//   - phase `bulk_copy` → walk the schema, skipping tables marked
//     `complete`, TRUNCATE-and-redo tables marked `in_progress`,
//     starting fresh on tables missing from the JSON map.
//   - phases `identity_sync`, `indexes`, `constraints` → re-run.
//     Idempotency is best-effort here (CREATE INDEX with a clashing
//     name will fail) but in practice the v1 contract is that a
//     resume from these phases means the failed phase is the latest
//     work — pre-existing indexes/constraints from a clean prior run
//     are absent.
//   - phase `complete` → log "already complete; nothing to do" and
//     exit cleanly.
//   - phase `failed` → if --resume, treat like the last running
//     phase recorded; if not, refuse with "drop the row or pass
//     --resume".
//
// The truncate-and-redo decision for in-progress tables is the load-
// bearing trade-off: per-batch checkpointing would let us resume
// mid-table, but it adds significant complexity (per-batch state
// writes, handling multi-row INSERT atomicity, dealing with
// COPY-protocol's all-or-nothing commit). v1 punts this to a future
// enhancement — the operator pays the cost of re-copying one
// in-progress table, not the entire migration.
//
// Failure handling never masks the original error. When a phase
// errors, we attempt one final state Write recording the phase and
// truncated message; if that secondary write also fails, we join it
// with errors.Join so the operator sees both — the primary cause
// remains the head of the chain.

package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/progress"
)

// lastErrorMaxLen caps the size of the persisted last_error column.
// 1 KiB is enough to capture the wrapped phase prefix plus a typical
// driver error message; longer messages are truncated with an
// ellipsis. The cap exists because:
//
//   - Some database errors (PG verbose error contexts, MySQL
//     prepared-statement dumps) can run kilobytes long, which clutters
//     `psql` output.
//   - The state row is intended for ad-hoc inspection; oversize values
//     hurt readability without adding diagnostic value.
const lastErrorMaxLen = 1024

// resumeContext bundles the state the orchestrator needs to thread
// through every phase. Constructing it once at Run start lets each
// phase helper take a single argument rather than the full Migrator.
type resumeContext struct {
	store       ir.MigrationStateStore
	migrationID string
	enabled     bool // store != nil (i.e. target engine supports MigrationStateStore)
}

// openMigrationStateStore type-asserts the target engine for the
// optional [ir.MigrationStateStoreOpener]. Engines that don't
// implement it (none today) cause the Migrator to fall back to the
// non-resumable path: state is silently not persisted, --resume
// errors clearly, and a fresh migration runs as it did before this
// chunk landed.
func openMigrationStateStore(ctx context.Context, target ir.Engine, dsn, targetSchema string) (ir.MigrationStateStore, error) {
	opener, ok := target.(ir.MigrationStateStoreOpener)
	if !ok {
		return nil, nil
	}
	store, err := opener.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pipeline: open migration-state store: %w", err)
	}
	// The migration-state table itself is **not** moved into the
	// per-source target schema — control tables stay in the DSN's
	// default schema (typically `public`) so a single sluice instance
	// can manage state for multiple target schemas without spamming
	// `sluice_migrate_state` rows across every namespace. Migration IDs
	// already disambiguate across overlapping target schemas.
	//
	// We deliberately do NOT call migcore.ApplyTargetSchema(store, targetSchema)
	// here; the parameter is accepted for symmetry with the streamer's
	// applier path (which also keeps `sluice_cdc_state` in the default
	// schema). The unused parameter is documented rather than removed
	// so a future shape that does want per-schema state can flow
	// through cleanly.
	_ = targetSchema
	return store, nil
}

// deriveMigrationID auto-generates a stable migration_id from
// source/target engine names plus DSN host info plus the operator-
// supplied target schema. Mirrors Streamer.resolveStreamID's approach:
// stable across restarts on the same (host pair, target schema),
// distinct for any change in any of those, length-bounded for the
// VARCHAR(255) PK.
//
// `targetSchema` is included in the hash so v0.25.0's multi-source
// pattern (`sluice migrate --target-schema=customer_svc` then again
// with `--target-schema=billing_svc` against the same target host)
// produces distinct migration ids per stream — without this each
// auto-derived id would collide on the second invocation, the
// "migration already complete" guard would fire, and the operator
// would have to manually supply --migration-id every time. Empty
// targetSchema (the pre-v0.25.0 default behavior) hashes the same way
// as before — operators not using --target-schema see no auto-derived
// id change after upgrade.
//
// Note: source-DSN database name is NOT included today; two migrations
// from different databases on the same host will still collide on
// auto-derived id. Documented as a known limitation; operators hitting
// this should supply --migration-id explicitly. Could be addressed in
// a follow-up if real demand surfaces — would need an upgrade story
// for operators who already have the colliding ids persisted.
//
// We hash the input rather than embedding the host directly so the
// PK column stays compact and predictable. Operators who need a
// stable identity across DSN changes (e.g., DNS round-robin
// rotating the host) should pass --migration-id explicitly.
func deriveMigrationID(sourceEngine, sourceDSN, targetEngine, targetDSN, targetSchema string) string {
	// Empty targetSchema (operators not using --target-schema) hashes
	// the same way as pre-v0.25.0 so auto-derived ids on existing
	// migrations don't shift after upgrade. Non-empty targetSchema
	// appends a discriminator so v0.25.0's multi-source pattern
	// produces distinct ids per stream.
	in := fmt.Sprintf("%s://%s -> %s://%s",
		sourceEngine, redactedHost(sourceDSN),
		targetEngine, redactedHost(targetDSN))
	if targetSchema != "" {
		in += " schema=" + targetSchema
	}
	sum := sha256.Sum256([]byte(in))
	// 16 hex chars (8 bytes) is enough collision-resistance for the
	// realistic population of source/target host pairs an operator
	// will run, and stays human-friendly in `psql` output.
	return "auto-" + hex.EncodeToString(sum[:8])
}

// truncateLastError clamps msg to lastErrorMaxLen bytes, appending an
// ellipsis when truncated. Keeps the persisted column short and ad-
// hoc-inspect-friendly without dropping the head of the message
// (which carries the phase prefix).
//
// The ellipsis ("…") is three bytes in UTF-8, so the head slice
// reserves three bytes to keep the total under the byte budget.
func truncateLastError(msg string) string {
	if len(msg) <= lastErrorMaxLen {
		return msg
	}
	const ellipsis = "…"
	return msg[:lastErrorMaxLen-len(ellipsis)] + ellipsis
}

// loadOrInitState resolves the pre-run state. Branches:
//
//   - !enabled → return zero state, ok=false
//   - row missing, --resume=true → error: nothing to resume
//   - row missing, --resume=false → fresh state, write it
//   - row found, phase=complete, --resume=true → log + exit cleanly
//   - row found, phase=complete, --resume=false → error: re-use ID
//   - row found, phase!=complete, --resume=true → return for resume
//   - row found, phase!=complete, --resume=false → error: drop or
//     pass --resume
//
// When resetting=true, the "row already exists" refusal branches are
// bypassed: the caller (Migrator with --reset-target-data) intends to
// DELETE the row and start fresh, so the existence of an old row is
// expected. A pending row is still written so subsequent phase
// transitions have somewhere to land.
//
// The boolean second return signals "exit cleanly with no further
// work" — i.e., the already-complete-resume case. Callers branch on
// that to short-circuit Migrator.Run.
func loadOrInitState(ctx context.Context, rc resumeContext, resume, resetting bool) (ir.MigrationState, bool, error) {
	if !rc.enabled {
		// No store available — fall back to non-resumable behaviour.
		// --resume requested without a store is a clear caller error
		// (engines that support it self-register).
		if resume {
			return ir.MigrationState{}, false, errors.New("pipeline: --resume requested but target engine does not support resumable migrations")
		}
		return ir.MigrationState{}, false, nil
	}

	if err := rc.store.EnsureControlTable(ctx); err != nil {
		return ir.MigrationState{}, false, fmt.Errorf("pipeline: ensure migrate-state table: %w", err)
	}

	state, found, err := rc.store.Read(ctx, rc.migrationID)
	if err != nil {
		return ir.MigrationState{}, false, fmt.Errorf("pipeline: read migrate-state: %w", err)
	}

	if resetting {
		// --reset-target-data: ignore any existing row's phase; the
		// reset path will DELETE it shortly. Return a pending state
		// so the rest of Run treats this as a fresh migration.
		fresh := ir.MigrationState{
			MigrationID:   rc.migrationID,
			Phase:         ir.MigrationPhasePending,
			TableProgress: nil,
		}
		return fresh, false, nil
	}

	switch {
	case !found && resume:
		return ir.MigrationState{}, false, fmt.Errorf("pipeline: --resume: no migration found for migration_id %q; run without --resume to start a fresh one", rc.migrationID)

	case !found && !resume:
		// Fresh migration: write the initial pending row. Subsequent
		// phase boundaries flip the phase forward.
		fresh := ir.MigrationState{
			MigrationID:   rc.migrationID,
			Phase:         ir.MigrationPhasePending,
			TableProgress: nil,
		}
		if err := rc.store.Write(ctx, fresh); err != nil {
			return ir.MigrationState{}, false, fmt.Errorf("pipeline: write initial migrate-state: %w", err)
		}
		return fresh, false, nil

	case found && state.Phase == ir.MigrationPhaseComplete && resume:
		slog.InfoContext(
			ctx, "migration: already complete; nothing to do",
			slog.String("migration_id", rc.migrationID),
		)
		return state, true, nil

	case found && state.Phase == ir.MigrationPhaseComplete && !resume:
		return ir.MigrationState{}, false, fmt.Errorf("pipeline: migration_id %q is already complete; drop the target tables to redo, or use a different --migration-id", rc.migrationID)

	case found && !resume:
		return ir.MigrationState{}, false, fmt.Errorf("pipeline: a partial migration is already recorded for migration_id %q (phase=%s); pass --resume to continue or drop the row to start fresh", rc.migrationID, state.Phase)

	case found && resume:
		return state, false, nil
	}
	// unreachable; the switch is exhaustive on the (found, resume,
	// phase==complete) tuple.
	return state, false, nil
}

// writeState persists `state` (with updated_at refreshed by the
// store), wrapping store errors with a phase-tagged prefix so the
// operator can tell where the failure happened. A nil store (resume
// disabled / engine doesn't support it) is a no-op so non-resumable
// migrations don't pay any extra round-trip.
func writeState(ctx context.Context, rc resumeContext, state ir.MigrationState) error {
	if !rc.enabled {
		return nil
	}
	if err := rc.store.Write(ctx, state); err != nil {
		return fmt.Errorf("pipeline: write migrate-state: %w", err)
	}
	return nil
}

// writeTableProgress persists ONE table's progress entry via the
// store's per-row upsert (ADR-0082) — the O(1) hot-path counterpart
// of writeState. Per-table breadcrumbs, per-batch resume cursors, and
// per-chunk checkpoints all land through here so a 10k-table schema
// never re-encodes the whole progress map per checkpoint. Same nil-
// store no-op contract as writeState.
func writeTableProgress(ctx context.Context, rc resumeContext, tableName string, entry ir.TableProgress) error {
	if !rc.enabled {
		return nil
	}
	if err := rc.store.WriteTableProgress(ctx, rc.migrationID, tableName, entry); err != nil {
		return fmt.Errorf("pipeline: write table progress: %w", err)
	}
	return nil
}

// headerOnly strips the TableProgress map off a state value before a
// phase-transition / failure-mark Write. Per-table progress is
// already persisted incrementally by writeTableProgress, and
// [ir.MigrationStateStore.Write] never deletes absent entries — so
// shipping the map again would re-upsert every row (O(N) per phase
// transition at the ADR-0076 10k-table scale) for no information
// gain. The caller keeps its in-memory map; only the written copy is
// stripped.
func headerOnly(state ir.MigrationState) ir.MigrationState {
	state.TableProgress = nil
	return state
}

// markPhase updates the persisted state to the given phase, clears
// last_error (we only get here on phase entry/success), and emits a
// log line. Errors from the state write are returned but logged so
// the caller can decide whether to fail-fast (typical) or continue
// (the resume path's tolerance for older state rows).
func markPhase(ctx context.Context, rc resumeContext, state *ir.MigrationState, phase ir.MigrationPhase) error {
	// ADR-0155: announce phase entry to the presentation sink (no-op on
	// the structured-log sink, so byte-identical on every non-TTY path).
	// markPhase is the single choke point every phase transition passes
	// through, so the checklist's "in progress" mark is driven from here.
	progress.FromContext(ctx).PhaseStarted(phase)
	state.Phase = phase
	state.LastError = ""
	if err := writeState(ctx, rc, headerOnly(*state)); err != nil {
		// Non-fatal in production: the migration's data work is the
		// load-bearing thing. Surface as warn rather than swallowing
		// silently — operators inspecting the state table see the
		// stale phase and know the bookkeeping lagged.
		slog.WarnContext(
			ctx, "migration: phase mark failed; continuing",
			slog.String("phase", string(phase)),
			slog.String("err", err.Error()),
		)
		return err
	}
	return nil
}

// markFailed records a failed phase + truncated error message.
// Persists `phase` (the in-flight phase) in the state row so resume
// knows where to re-enter; the design doc reserves the literal
// `failed` value as a future signal for "no resumption attempted yet"
// — v1 keeps the in-flight phase on disk because that's what a
// re-entry needs to read.
//
// Best-effort: a state-write failure here is joined with the primary
// err via errors.Join so the operator sees both. The primary error
// stays the head of the chain, preserving any phase-hint the caller
// already attached.
func markFailed(ctx context.Context, rc resumeContext, state ir.MigrationState, phase ir.MigrationPhase, err error) error {
	if !rc.enabled {
		return err
	}
	state.Phase = phase
	state.LastError = truncateLastError(err.Error())
	if writeErr := rc.store.Write(ctx, headerOnly(state)); writeErr != nil {
		slog.WarnContext(
			ctx, "migration: state write on failure also failed; joining",
			slog.String("phase", string(phase)),
			slog.String("primary_err", err.Error()),
			slog.String("state_write_err", writeErr.Error()),
		)
		return errors.Join(err, fmt.Errorf("pipeline: state-write on failure: %w", writeErr))
	}
	return err
}

// markComplete flips the persisted phase to complete and clears
// last_error. Best-effort write — the migration's data work is done
// either way; a state-write failure here is logged at warn but not
// returned, so the operator sees a clean exit instead of a "succeeded
// but couldn't bookkeep" tail.
func markComplete(ctx context.Context, rc resumeContext, state ir.MigrationState) {
	if !rc.enabled {
		return
	}
	state.Phase = ir.MigrationPhaseComplete
	state.LastError = ""
	if err := rc.store.Write(ctx, headerOnly(state)); err != nil {
		slog.WarnContext(
			ctx, "migration: failed to mark complete; data is safe but state row is stale",
			slog.String("migration_id", state.MigrationID),
			slog.String("err", err.Error()),
		)
	}
}

// resumeBulkCopyAction is the per-table action the bulk-copy phase
// takes during resume. Five values cover the resume cases:
// skip a completed table, truncate-and-redo an in-progress no-PK
// table (or a v0.3.0-shape row), resume mid-table from a recorded
// single-chunk cursor (v0.4.0), resume mid-table from per-chunk
// cursors (v0.5.0), start fresh on a missing-from-progress table.
type resumeBulkCopyAction int

const (
	resumeActionFresh            resumeBulkCopyAction = iota // not in progress map → start fresh
	resumeActionSkip                                         // state=complete → skip
	resumeActionTruncate                                     // state=in_progress without cursor, or state=no_pk_truncate_and_redo → truncate and redo
	resumeActionResumeFromCursor                             // state=in_progress with non-empty LastPK → resume mid-table (single-chunk)
	resumeActionResumeChunked                                // state=in_progress with non-empty Chunks → resume mid-table (parallel)
)

// classifyTableForResume picks the action for a table during a
// resume run. When resume is disabled (fresh run or store-less
// engine), every table is "fresh" — the orchestrator's normal
// behaviour.
//
// During a resume, the action depends on the persisted state plus the
// presence of a cursor:
//
//   - State `complete`  → skip (no work to do).
//   - State `in_progress` with a non-nil LastPK (v0.4.0 cursor-bearing
//     row) → caller should resume mid-table from the cursor.
//   - State `in_progress` with nil LastPK (v0.3.0 row, or v0.4.0 row
//     written before any batch landed) → truncate-and-redo.
//   - State `no_pk_truncate_and_redo` → truncate-and-redo (sticky
//     fallback for tables without a primary key).
//   - Missing key → fresh start.
func classifyTableForResume(state ir.MigrationState, tableName string, resuming bool) resumeBulkCopyAction {
	if !resuming {
		return resumeActionFresh
	}
	entry, ok := state.TableProgress[tableName]
	if !ok {
		return resumeActionFresh
	}
	switch entry.State {
	case ir.TableProgressComplete:
		return resumeActionSkip
	case ir.TableProgressNoPKTruncateAndRedo:
		return resumeActionTruncate
	case ir.TableProgressInProgress:
		// v0.5.0 parallel-copy progress: per-chunk cursors live in
		// Chunks. Even if Chunks has only chunk 0 with no cursor,
		// each chunk is independently resumable so we hand off to
		// the parallel path rather than truncate-and-redo.
		if len(entry.Chunks) > 0 {
			return resumeActionResumeChunked
		}
		if len(entry.LastPK) > 0 {
			return resumeActionResumeFromCursor
		}
		// v0.3.0-shape row, or a v0.4.0 row that failed before any
		// batch committed: no cursor to resume from. Fall back to
		// truncate-and-redo.
		return resumeActionTruncate
	}
	return resumeActionFresh
}

// truncateForResume invokes the optional [ir.TableTruncator] surface
// on the row writer. Falls back to an error when the writer doesn't
// implement it — a target without TRUNCATE support is unusable for
// resume, so the operator gets a clear refusal rather than an opaque
// duplicate-row error during the re-copy.
func truncateForResume(ctx context.Context, rw ir.RowWriter, table *ir.Table) error {
	t, ok := rw.(ir.TableTruncator)
	if !ok {
		return fmt.Errorf("pipeline: resume: row writer for table %q does not support TRUNCATE; cannot resume in-progress table", table.Name)
	}
	return t.TruncateTable(ctx, table)
}

// summariseTableProgress is a small helper for the resume-start log
// line. Returns counts of complete/in-progress/missing tables so the
// operator gets a one-glance view of what resume is about to do.
func summariseTableProgress(schema *ir.Schema, state ir.MigrationState) (complete, inProgress, missing int) {
	for _, t := range schema.Tables {
		entry, ok := state.TableProgress[t.Name]
		if !ok {
			missing++
			continue
		}
		switch entry.State {
		case ir.TableProgressComplete:
			complete++
		case ir.TableProgressInProgress, ir.TableProgressNoPKTruncateAndRedo:
			inProgress++
		default:
			missing++
		}
	}
	return
}

// updatedAtNow returns the wall-clock time used for log lines that
// want a "decision made at" stamp. The store overwrites updated_at
// on its own; this is purely for human-readable logging so resume
// runs are easy to correlate against the corresponding stored row.
//
// Using a function lets tests stub out the clock when assertions
// need stable output.
var updatedAtNow = time.Now

// logResumeStart emits a single Info line at the top of a resume
// run. Carrying the table-progress summary up front means an
// operator looking at the head of a multi-hour resume can decide in
// one glance whether the resume target matches what they expected.
func logResumeStart(ctx context.Context, state ir.MigrationState, schema *ir.Schema) {
	complete, inProgress, missing := summariseTableProgress(schema, state)
	slog.InfoContext(
		ctx, "migration: resuming",
		slog.String("migration_id", state.MigrationID),
		slog.String("phase", string(state.Phase)),
		slog.Int("tables_complete", complete),
		slog.Int("tables_in_progress", inProgress),
		slog.Int("tables_pending", missing),
		slog.String("started_at", state.StartedAt.Format(time.RFC3339)),
		slog.String("now", updatedAtNow().Format(time.RFC3339)),
	)
	if state.LastError != "" {
		slog.InfoContext(
			ctx, "migration: prior failure recorded",
			slog.String("migration_id", state.MigrationID),
			slog.String("last_error", state.LastError),
		)
	}
}
