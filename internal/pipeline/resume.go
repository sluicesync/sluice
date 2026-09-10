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
	"sync"
	"time"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/progress"
	"sluicesync.dev/sluice/internal/sluicecode"
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

	// noResume refuses the READ half while leaving the WRITE half alone.
	//
	// `enabled` is store AVAILABILITY and nothing more — its own comment
	// always said so. Two unrelated questions sat on top of it because
	// they happen to share a table:
	//
	//   - RECORD: writeState / writeTableProgress / markFailed /
	//     markComplete write where this run has got to. Pure
	//     OBSERVABILITY, and safe on a run that can never resume.
	//   - RESUME: loadOrInitState reads prior state to continue FROM it.
	//     A correctness decision, and the one the sync cold start must
	//     never take (its fast path is fresh-cold-start-only; resume
	//     stays serial behind [coldStartFastEligible]).
	//
	// Collapsing them cost the sync cold start its entire progress
	// surface, because giving it no store was the only way to say "do not
	// resume" (see [newSyncRecordingContext]).
	//
	// The polarity is deliberate and is the v0.99.51 zero-value rule:
	// this field is spelled as an OPT-OUT so its zero value is the
	// existing behaviour. Every construction that predates it — `migrate`
	// and every test — keeps recording AND resuming exactly as before,
	// with no edit. The first cut of this change spelled it `recording`,
	// defaulting off, and TestMarkFailedJoinsStateError caught it
	// immediately: a field named for the on-behaviour silently inverts to
	// off for every caller that does not know to set it.
	noResume bool

	// throttle rate-limits INTERMEDIATE per-table progress writes. nil
	// (the zero value, so `migrate` is untouched) means every write goes
	// through.
	//
	// The same write serves two purposes with very different granularity
	// requirements, and that is the whole reason this exists:
	//
	//   - For `migrate`, a progress row is a RESUME cursor. It must be
	//     fine-grained, because whatever it last recorded is what a
	//     --resume re-copies from; throttling it would widen the replay
	//     window on every interrupted migration.
	//   - For a sync cold start, it is a STATUS heartbeat. Nobody resumes
	//     from it (loadOrInitState refuses), and a human polling `sync
	//     status` cannot perceive sub-second freshness.
	//
	// writeTableProgress is called PER BATCH inside the copy loop, so
	// without this the sync cold start pays one synchronous control-table
	// round trip per batch per table — on the cross-region, RTT-bound
	// copy that motivated this whole feature, that is the exact cost the
	// operator had just finished tuning away.
	//
	// Pointer so every copy of the context shares one throttle.
	throttle *progressThrottle
}

// progressThrottleInterval is how stale a sync cold start's per-table
// progress row may get. Two seconds is far finer than a human polling
// `sync status` can perceive and bounds the write rate regardless of how
// small the batches are.
const progressThrottleInterval = 2 * time.Second

// progressThrottle rate-limits intermediate progress writes per table.
//
// TERMINAL states are never throttled — a table that finished must say so
// immediately, or `sync status` would show it stuck at its last
// intermediate write until some later table happened to flush. The FIRST
// write for a table is never throttled either, so a table appears in
// status as soon as it starts rather than up to an interval later.
type progressThrottle struct {
	mu   sync.Mutex
	last map[string]time.Time
}

// allow reports whether this table's write should go through now.
func (p *progressThrottle) allow(tableName string, terminal bool, now time.Time) bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.last == nil {
		p.last = map[string]time.Time{}
	}
	prev, seen := p.last[tableName]
	if terminal || !seen || now.Sub(prev) >= progressThrottleInterval {
		p.last[tableName] = now
		return true
	}
	return false
}

// newSyncRecordingContext builds the record-but-never-resume context the
// sync cold start uses, keyed on the STREAM ID.
//
// # Why this exists
//
// The sync cold start used to pass a zero-value resumeContext, so every
// state write no-op'd. The stated reason was about RESUME — the fast path
// is fresh-cold-start-only, so a resumable store is inert — and that
// reasoning is correct as far as it goes. But one flag disabled two
// unrelated things, and nobody decided the second: the cold start also
// lost every progress row it would have written.
//
// The consequence was not subtle. `WritePosition` at the end of
// coldStartBeginCDC is the ONLY row the cold-start path writes, so for
// the whole of schema apply, bulk copy, index build and the FLOAT
// re-read — hours on a real migration — `sync status`, `sync health` and
// `verify` all reported the stream as absent, which is indistinguishable
// from a dead process. The 2026-09-08 user report surfaced it at the
// tail, where they expected to be done; the blackout covered everything
// before that too.
//
// # Why the stream ID is load-bearing, and not just a collision fix
//
// Keying on the stream ID keeps two streams against one target apart,
// which is the obvious reason. The important one is that it keeps sync's
// rows out of the namespace `migrate --resume` derives.
// [deriveMigrationID] hashes (source, target, targetSchema); if a sync
// cold start wrote under that same id, a later `migrate --resume`
// against the same pair would find state describing a copy IT did not
// perform and resume from it. Sync rows live under a "sync-" prefix that
// no migrate run can derive, so the two populations cannot alias.
//
// The returned context is deliberately NOT resumable: nothing calls
// loadOrInitState with it, and if something ever does, `enabled` alone
// no longer implies "you may resume from this".
func newSyncRecordingContext(ctx context.Context, store ir.MigrationStateStore, streamID string) resumeContext {
	// The id has to FIT, and a stream id that does not must cost the
	// status surface rather than the migration.
	//
	// Both control tables declare migration_id VARCHAR(255) (characters,
	// not bytes, on MySQL utf8mb4 and on PG). `--stream-id` has no length
	// validation anywhere, and sluice_cdc_state.stream_id is also
	// VARCHAR(255) — so a 251-to-255-character stream id is legal today
	// and worked fine before this feature existed. Prefixing it pushes
	// the migration id past the column: strict-mode MySQL (the default)
	// refuses with Error 1406 and would have failed the whole cold start,
	// and a non-strict server would truncate silently, which is worse —
	// two long stream ids sharing a 250-character prefix would collapse
	// onto one row and attribute one migration's progress to the other.
	//
	// Recording is OBSERVABILITY. Losing it is a degraded status surface;
	// failing the copy for it would be a regression on a configuration
	// that used to work. Same call as the unopenable-store path below.
	if n := utf8.RuneCountInString(syncMigrationID(streamID)); n > migrationIDMaxRunes {
		slog.WarnContext(ctx, "pipeline: cold-start progress recording is DISABLED for this stream: its id is "+
			"too long for the progress table, so `sync status` will report the stream as absent until the copy "+
			"finishes and the CDC anchor is written. The migration itself is unaffected",
			slog.String("stream_id", streamID),
			slog.Int("migration_id_runes", n),
			slog.Int("limit", migrationIDMaxRunes))
		return resumeContext{}
	}
	if store == nil || streamID == "" {
		// A target whose engine implements no MigrationStateStore records
		// nothing, and that is a real gap rather than a silent nicety:
		// `sync status` on such a target still reports the stream as
		// absent during a cold start. Stated here rather than implied,
		// because a reader who sees the recording context wired in could
		// otherwise reasonably assume every target gets it.
		return resumeContext{}
	}

	// The control tables have to EXIST, and on this path nothing else
	// creates them (audit A0909-P2).
	//
	// EnsureControlTable is called from exactly one place in the pipeline:
	// loadOrInitState — which is the resume read this context refuses by
	// construction. So splitting record-from-resume left the table
	// creation on the far side of the door: on a target that had never run
	// `migrate` (the ORDINARY `sync start` target) every progress write
	// failed with SQLSTATE 42P01, and `sync status` showed nothing for the
	// whole cold start. That is precisely the blackout the feature was
	// built to end, so the feature was a no-op in its common case.
	//
	// A failure here degrades to inert rather than failing the copy, for
	// the same reason the unopenable-store path does: this is
	// observability, and refusing to migrate because a progress table
	// could not be created would be the wrong trade in the wrong
	// direction.
	if err := store.EnsureControlTable(ctx); err != nil {
		slog.WarnContext(ctx, "pipeline: cold-start progress recording is DISABLED for this stream: the "+
			"progress tables could not be created, so `sync status` will report the stream as absent until "+
			"the copy finishes and the CDC anchor is written. The migration itself is unaffected",
			slog.String("stream_id", streamID),
			slog.String("error", err.Error()))
		return resumeContext{}
	}

	return resumeContext{
		store:       store,
		migrationID: syncMigrationID(streamID),
		enabled:     true,
		noResume:    true,
		throttle:    &progressThrottle{},
	}
}

// beginRecordedColdStart makes the recorded state describe THIS cold
// start and nothing else, then records the run's snapshot anchor.
//
// # Why the reset comes first, and why it is not tidiness
//
// The rows under `sync-<stream-id>` are per-RUN evidence: the
// stopped-cold-start resume gate reads them to decide whether a copy
// finished, and a copy is a property of one run. Leaving a previous
// run's rows in place makes them look like this run's the moment this
// run writes anything of its own. The concrete loss: run 1 finishes its
// copy (every table row `complete`, phase `indexes`) and is stopped;
// the operator re-runs with --reset-target-data, which DROPS the target
// tables; run 2 records its new anchor and is killed in the window
// before its first phase mark. Run 3 would then see run 1's
// "copy finished" evidence beside run 2's anchor and a slot that
// matches it exactly — and skip the copy onto an empty target, at exit
// 0. Clearing here makes that window carry no evidence at all, which is
// the truth about it.
//
// # The record
//
// Recorded at the START of the copy rather than at its end because the
// point is to survive an interrupt; a token written only on success
// would be missing in exactly the case it exists for. Recording it
// early is safe because the anchor alone licenses nothing — the gate
// additionally requires the per-table evidence that the copy finished,
// the copy-shape fingerprint recorded beside it, the target's own
// rows, AND the source's confirmation that the slot still stands
// there.
//
// Both steps are best-effort with a WARN: this is the same trade the
// rest of the recording context takes (see [newSyncRecordingContext]).
// Failing them degrades a stopped run to today's re-copy; failing the
// COPY over them would break a configuration that works.
func beginRecordedColdStart(ctx context.Context, rc resumeContext, rec ir.SnapshotAnchorRecord) {
	if !rc.writes() {
		return
	}
	if err := rc.store.ClearMigration(ctx, rc.migrationID); err != nil {
		// What actually survives this branch: the PREVIOUS run's rows,
		// anchor and shape, all of them stale. That is fail-safe rather
		// than fail-open, and for a reason worth stating precisely — the
		// stale anchor names the slot the previous run created, and this
		// run creates a NEW slot at a new consistent point, so the
		// source-side verification cannot match and the resume refuses.
		// The honest cost is a stale `sync status` and a stopped run
		// that will not resume.
		slog.WarnContext(ctx, "pipeline: cold start could not clear the previous run's recorded progress; "+
			"`sync status` may show stale per-table rows for this stream, and if this run is STOPPED before "+
			"its CDC anchor is written the resume will refuse (the recorded anchor is the PREVIOUS run's and "+
			"cannot match this run's replication slot), leaving drop-the-slot-and-re-copy as the way out",
			slog.String("migration_id", rc.migrationID),
			slog.String("error", err.Error()))
		return
	}
	if rec.Anchor == "" {
		// A source whose snapshot carries no position token records no
		// anchor. Stated rather than silent: such a cold start is not
		// resumable after a stop, and the gate will say so by finding
		// nothing.
		return
	}
	recorder, ok := rc.store.(ir.SnapshotAnchorRecorder)
	if !ok {
		return
	}
	if err := recorder.WriteSnapshotAnchor(ctx, rc.migrationID, rec); err != nil {
		slog.WarnContext(ctx, "pipeline: cold start could not record its snapshot anchor; if this run is "+
			"STOPPED after the copy but before the CDC anchor is written, it will not be resumable and the "+
			"only way forward will be to drop the replication slot and copy again",
			slog.String("migration_id", rc.migrationID),
			slog.String("error", err.Error()))
	}
}

// syncMigrationID namespaces a sync cold start's progress rows so they
// can never be mistaken for a resumable `migrate` state. See
// [newSyncRecordingContext] for why that separation is a safety property
// and not just tidiness.
func syncMigrationID(streamID string) string { return "sync-" + streamID }

// migrationIDMaxRunes is the width both engines declare for
// migration_id (VARCHAR(255) — characters, not bytes, on MySQL utf8mb4
// and on PostgreSQL).
//
// Held here rather than derived from the DDL because the DDL lives
// engine-side and this package must not import engines;
// TestMigrationIDWidthMatchesTheEngineDDL greps both CREATE TABLE
// statements so a widened column cannot leave this constant behind.
//
// Note the ASYMMETRY with `migrate`, which is NOT fixed here and does
// not need to be: an over-long `--migration-id` fails migrate loudly,
// and the error names a value the operator typed themselves. A sync
// operator types a STREAM id and would get an error about a
// migration_id column they have never heard of, for a table that exists
// only to make status prettier — so this path degrades instead.
const migrationIDMaxRunes = 255

// writes reports whether this context should PERSIST progress. Every
// state writer gates on it; the resume readers deliberately do not, so
// the two questions stay separable (see [resumeContext.recording]).
//
// `migrate` sets recording alongside enabled, so its behaviour is
// byte-identical to before the split.
func (rc resumeContext) writes() bool { return rc.enabled }

// migrateRaiseRecorder resolves the ADR-0182 crash-safe recorder for the
// query-timeout raise from the migrate resume context: the resumable
// migrate-state store when it can record raises (the MySQL/Vitess target), else
// nil. The shared query-timeout helpers ([autoRevertDanglingQueryTimeoutRaise],
// [maybeRaiseQueryTimeout]) treat a nil recorder as "this target can't record a
// raise crash-safely" — a no-op for the auto-revert, a loud refusal when the
// raise is armed. Mirrors the pre-item-111 `!rc.enabled` / store-type-assert
// gate the *Migrator methods carried inline.
func migrateRaiseRecorder(rc resumeContext) ir.QueryTimeoutRaiseRecorder {
	if !rc.enabled {
		return nil
	}
	recorder, _ := rc.store.(ir.QueryTimeoutRaiseRecorder)
	return recorder
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
	// A record-only context must never be resumed from. Nothing calls
	// this with one today — the sync cold start writes progress and never
	// reads it back — so this is a door held shut ahead of the first
	// caller rather than a live guard. It refuses LOUDLY rather than
	// degrading to a fresh state, because a silent fresh-start here would
	// look identical to a successful resume and re-copy the whole
	// database (audit-style reasoning: the failure mode of the quiet
	// branch is the expensive one).
	if rc.noResume {
		return ir.MigrationState{}, false, fmt.Errorf(
			"pipeline: migration state %q is record-only and cannot be resumed from: it is written by a sync "+
				"cold start purely so `sync status` has something to report, and the cold-start fast path is "+
				"fresh-start-only by construction. Resuming an interrupted sync cold start is `sync start` "+
				"again, which takes the serial resumable path", rc.migrationID,
		)
	}
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
	// Throttled only for a sync cold start, whose rows are a status
	// heartbeat rather than a resume cursor; nil throttle on every
	// `migrate` path, which needs the fine granularity. Terminal states
	// always pass — see [progressThrottle].
	if !rc.throttle.allow(tableName, entry.State != ir.TableProgressInProgress, time.Now()) {
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
	progress.FromContext(ctx).PhaseStarted(migPhase(phase))
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

// createTablesRedundantOnResume reports whether a resume attempt can
// skip the create-tables phase outright: every table the phase would
// CREATE is already recorded `complete` in the SAME persisted state the
// bulk-copy phase classifies from ([classifyTableForResume]). One
// predicate, one source of truth — a table the copy would skip is
// exactly a table whose DDL is redundant.
//
// This is not only an optimization; it is what makes a documented
// PlanetScale recovery path exist at all. On a safe-migrations branch
// every direct DDL statement is refused (errno 1105 "direct DDL is
// disabled"), so re-issuing the idempotent `CREATE TABLE` for a table
// that already holds 20M rows killed the run in its FIRST phase —
// before the index phase, where the ADR-0148 deploy-request fallback
// lives. Both index-phase hints (errno 3024, and the safe-migrations
// 1105) tell the operator to re-run with `--resume` (plus a service
// token) to finish just the indexes with no re-copy; measured at
// 122 GB against real PlanetScale, that advice dead-ended here after
// one second with zero rows copied and nothing achieved. The two fit
// together precisely: those hints fire from the index phase, which
// only runs once EVERY table's copy has completed, so the state they
// leave behind is exactly the all-complete shape this predicate
// matches.
//
// Deliberately narrow — the boundary is the load-bearing part:
//
//   - Not resuming → false. A fresh run always creates; its
//     equivalent is the ADR-0166 pre-create gate, which VALIDATES the
//     target's column shape rather than trusting a recorded row.
//   - A table absent from the recorded state (added to the source, or
//     to --include-table, since the last attempt) → false: it
//     genuinely needs its DDL.
//   - A table recorded in-progress / no-PK-truncate-and-redo → false:
//     the phase runs exactly as before, for the whole create set.
//   - An empty create set → false: with no table to reason about there
//     is no evidence the prior attempt created anything, and a
//     table-less schema can still carry schema-level objects the phase
//     emits (PG standalone sequences, the target schema itself).
//
// One consequence is deliberate and worth naming: when a table is
// recorded complete but has since been DROPPED on the target, the
// skipped CREATE means the later index/identity/constraint phases fail
// loudly on the missing relation — where before the fix the phase
// silently re-created it EMPTY, the copy skipped it ("skipping
// completed table"), and the run reported success. Loud is the
// direction we want.
func createTablesRedundantOnResume(createSchema *ir.Schema, state ir.MigrationState, resuming bool) bool {
	if !resuming || createSchema == nil || len(createSchema.Tables) == 0 {
		return false
	}
	for _, t := range createSchema.Tables {
		if t == nil {
			return false
		}
		if classifyTableForResume(state, t.Name, resuming) != resumeActionSkip {
			return false
		}
	}
	return true
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

// refuseResumedFreshTableThatHasRows is the independent check on the
// invariant the whole fresh-start disposition rests on: a table with NO
// persisted progress row was never copied, so the resume can start it at
// PK > nil WITHOUT truncating.
//
// [classifyTableForResume] cannot verify that; it only sees the absence
// of a row, and an absence has two causes — never copied, or copied and
// the row never landed. The target itself is the independent evidence,
// and it is one bounded existence probe.
//
// When the invariant does hold (the ordinary case: the previous attempt
// died before reaching this table) the target is empty and this costs a
// probe that answers immediately. When it does not hold, the alternative
// is a second copy of every row appended to the first, at whatever exit
// code the original failure produced — measured 2026-09-10 on a sharded
// PlanetScale Neki target whose control tables could not accept an
// INSERT (neki-issues/NEKI-009): a 40-row keyless table came back from
// the resume holding 80.
//
// # Why it refuses rather than truncating
//
// Truncating would be right for the case this exists to catch — those
// rows are ours, from this migration. It would be catastrophic for the
// other way a resumed fresh table can hold rows: an operator who passed
// --force-cold-start over a deliberately pre-populated target. The
// pipeline cannot tell those apart from here, and only one of the two
// mistakes is recoverable, so it names both remedies and lets the
// operator choose.
//
// # Why it is not narrowed to keyless tables
//
// The measured harm is keyless: with a primary key (or a usable non-null
// UNIQUE index) the writer upserts and the second copy heals. That
// healing is a property of the engine's upsert-key choice, which is not
// visible from here — so a refusal scoped to it would be scoped by
// something the pipeline cannot actually see, and would go quiet on
// exactly the engines that stop resolving a key. The contradiction is
// real for every table shape; a keyed table merely survives it. Saying
// so is worth one refusal an operator clears with a flag.
//
// Engines that do not expose [ir.TableEmptyChecker] are skipped, as
// everywhere else this surface is used — the check is opportunistic, and
// the pre-existing behaviour is what they keep.
func refuseResumedFreshTableThatHasRows(ctx context.Context, rw ir.RowWriter, table *ir.Table, resuming bool) error {
	if !resuming {
		return nil
	}
	checker, ok := rw.(ir.TableEmptyChecker)
	if !ok {
		return nil
	}
	empty, err := checker.IsTableEmpty(ctx, table)
	if err != nil {
		return fmt.Errorf("pipeline: resume: probe whether target table %q is empty: %w", table.Name, err)
	}
	if empty {
		return nil
	}
	return &sluicecode.CodedError{
		Code: sluicecode.CodeResumeFreshTableNotEmpty,
		// The remedy must be RUNNABLE. This refusal fires only on a --resume
		// run, and --resume and --reset-target-data are mutually exclusive
		// (cmd/sluice/cli.go), so telling the operator to add the latter
		// without telling them to drop the former hands them a command the
		// CLI rejects. Found by the pre-tag docs-drift pass.
		Hint: "re-run WITHOUT --resume and WITH --reset-target-data (the two are mutually exclusive) to clear " +
			"the in-scope target tables and re-copy, or keep --resume and add --exclude-table=" + table.Name +
			" to leave this table alone",
		Err: fmt.Errorf(
			"pipeline: resume: target table %q holds rows but this migration recorded no progress for it"+
				"\nresuming would start the table from scratch WITHOUT truncating, so a table with no primary "+
				"key would end up holding every row twice"+
				"\nthe usual cause is that an earlier attempt copied the table and could not persist its "+
				"progress row (check that run's logs for state-write failures); the other is a target that "+
				"was already populated when the migration started", table.Name,
		),
	}
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
