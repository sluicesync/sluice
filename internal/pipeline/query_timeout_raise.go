// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Opt-in PlanetScale query-timeout raise — the orchestration half (ADR-0182)
//
// The engine-neutral pipeline owns the WORKFLOW around the injected
// [ir.QueryTimeoutController]: the size gate, recording the previous value in
// migrate-state BEFORE the raise takes effect, the defer-style revert that
// runs on every exit path (success or abort), and the crash-recovery
// auto-revert that reverts a dangling raise a prior run left behind before
// anything else touches the target. The control-plane HOW (keyspace
// resolution, config-change staging, rollout polling) lives entirely behind
// the controller — the pipeline never imports the PlanetScale API.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// queryTimeoutRaiseMinRows is the size floor below which the ADR-0182 raise is
// SKIPPED: two rolling tablet restarts (~5–12m total, and disruptive to
// everyone else on the keyspace) cost more than the ~900s statement wall a
// small migration would never approach. Deliberately conservative — the
// measured wall-hitting scale is ~153M rows, so a 1M-row floor never skips a
// migration that could plausibly wall while still sparing a genuinely small
// one. Row count (via the source [ir.RowCountEstimator]) is an imperfect proxy
// for wall risk — a narrow-but-huge or wide-but-short table is judged only by
// its estimated rows — but it is the estimate the migrate path already has
// pre-copy; see ADR-0182's honest-scope note.
const queryTimeoutRaiseMinRows int64 = 1_000_000

// revertQueryTimeoutTimeout bounds the teardown revert. The revert runs on an
// uncancellable context (the reason a migration is aborting must not be the
// reason its keyspace is left raised) but still needs a ceiling so a wedged
// control plane can't hang the process forever.
const revertQueryTimeoutTimeout = 25 * time.Minute

// autoRevertDanglingQueryTimeoutRaise reverts a query-timeout raise a prior
// crashed run recorded in migrate-state, BEFORE anything else in the run
// touches the target. It runs whenever the store can record raises (MySQL
// target); a dangling record with no controller to revert it is a LOUD
// refusal, not a silent skip — the keyspace is in a modified state and sluice
// must restore it (or the operator must supply the credentials to).
//
// No-op when the store doesn't record raises (non-MySQL target) or none is
// recorded (the common fresh-run case).
func (m *Migrator) autoRevertDanglingQueryTimeoutRaise(ctx context.Context, rc resumeContext) error {
	if !rc.enabled {
		return nil
	}
	recorder, ok := rc.store.(ir.QueryTimeoutRaiseRecorder)
	if !ok {
		return nil
	}
	previous, dangling, err := recorder.ReadQueryTimeoutRaise(ctx, rc.migrationID)
	if err != nil {
		return fmt.Errorf("pipeline: read dangling query-timeout raise: %w", err)
	}
	if !dangling {
		return nil
	}
	if m.QueryTimeoutController == nil {
		return errors.New("pipeline: a previous run raised this PlanetScale keyspace's query timeout and left it raised, " +
			"but the credentials to revert it are not present on this run — re-run with --planetscale-org and the service token " +
			"(PLANETSCALE_SERVICE_TOKEN_ID / PLANETSCALE_SERVICE_TOKEN) so sluice can restore the keyspace, or revert it manually")
	}
	slog.InfoContext(ctx,
		"planetscale: a prior run left the keyspace query timeout raised; reverting it before starting (ADR-0182 crash recovery)",
		slog.String("migration_id", rc.migrationID))
	if err := m.QueryTimeoutController.Revert(ctx, previous); err != nil {
		return fmt.Errorf("pipeline: auto-revert dangling query-timeout raise: %w", err)
	}
	if err := recorder.ClearQueryTimeoutRaise(ctx, rc.migrationID); err != nil {
		return fmt.Errorf("pipeline: clear dangling query-timeout raise record after revert: %w", err)
	}
	return nil
}

// maybeRaiseQueryTimeout raises the keyspace query timeout (and waits out its
// rolling restart) BEFORE the copy phase when the feature is armed, a
// controller is present, the migrate-state store can record the raise, and
// the migration is past the size gate. It returns a revert closure the caller
// MUST defer — it runs the post-migration revert on every exit path — or nil
// when no raise happened (so the caller's defer is a no-op).
//
// The revert closure is idempotent-safe and runs on an uncancellable,
// timeout-bounded context so an aborting migration still restores the
// keyspace.
func (m *Migrator) maybeRaiseQueryTimeout(ctx context.Context, rc resumeContext, schema *ir.Schema, rr ir.RowReader) (revert func(), err error) {
	if !m.RaiseQueryTimeout {
		return nil, nil
	}
	// Armed but unable to act: refuse loudly rather than silently no-op — the
	// operator explicitly asked for the raise and should learn why it can't
	// happen. (The CLI already refuses a flag set without a planetscale
	// target / credentials; this covers a target engine whose store can't
	// record the raise, i.e. the crash-safety guarantee cannot be honored.)
	if m.QueryTimeoutController == nil {
		return nil, errors.New("pipeline: --planetscale-raise-query-timeout is set but no PlanetScale control-plane controller is available " +
			"(non-planetscale target, or missing --planetscale-org / service token)")
	}
	if !rc.enabled {
		return nil, errors.New("pipeline: --planetscale-raise-query-timeout requires a resumable migrate-state store on the target to record the raise crash-safely, which this target does not provide")
	}
	recorder, ok := rc.store.(ir.QueryTimeoutRaiseRecorder)
	if !ok {
		return nil, errors.New("pipeline: --planetscale-raise-query-timeout requires the target's migrate-state store to record the raise crash-safely, which this target does not support")
	}

	// Size gate: two rolling restarts for a small migration are worse than
	// the wall it would never hit.
	largest, known := largestEstimatedRows(ctx, rr, schema)
	if known && largest < queryTimeoutRaiseMinRows {
		slog.InfoContext(ctx,
			"planetscale: skipping the query-timeout raise — the largest table is below the size floor, so the migration is unlikely to hit the statement-time wall (ADR-0182 size gate); two rolling restarts would cost more than they save",
			slog.Int64("largest_table_estimated_rows", largest),
			slog.Int64("floor_rows", queryTimeoutRaiseMinRows))
		return nil, nil
	}

	raised := false
	raiseErr := m.QueryTimeoutController.Raise(ctx, func(previous string) error {
		// Persist BEFORE the raise takes effect so a crash mid-rollout is
		// recoverable. Only a successful record permits the raise to proceed.
		if err := recorder.RecordQueryTimeoutRaise(ctx, rc.migrationID, previous); err != nil {
			return err
		}
		raised = true
		return nil
	})
	if raiseErr != nil {
		// If the record landed but the raise (or its rollout) then failed, we
		// may have applied a partial change; revert now so we don't leave the
		// keyspace raised, and clear the record on success.
		if raised {
			m.revertQueryTimeoutRaise(ctx, rc, recorder)
		}
		return nil, fmt.Errorf("pipeline: raise PlanetScale query timeout: %w", raiseErr)
	}
	if !raised {
		// The controller decided there was nothing to do (already at the
		// maximum): no record was written, so there is nothing to revert.
		return nil, nil
	}

	return func() { m.revertQueryTimeoutRaise(ctx, rc, recorder) }, nil
}

// revertQueryTimeoutRaise restores the keyspace query timeout to the recorded
// previous value and clears the record. Best-effort and cancel-immune: it runs
// on an uncancellable, timeout-bounded context so the reason a migration is
// aborting is never the reason its keyspace stays raised. Failures WARN (the
// operator can still auto-revert on the next run, since the record is only
// cleared on success) rather than mask the migration's own outcome.
func (m *Migrator) revertQueryTimeoutRaise(ctx context.Context, rc resumeContext, recorder ir.QueryTimeoutRaiseRecorder) {
	revertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), revertQueryTimeoutTimeout)
	defer cancel()

	previous, dangling, err := recorder.ReadQueryTimeoutRaise(revertCtx, rc.migrationID)
	if err != nil {
		slog.WarnContext(revertCtx,
			"planetscale: could not read the query-timeout raise record to revert it; the keyspace may still be raised — re-run to auto-revert, or revert manually",
			slog.String("error", err.Error()))
		return
	}
	if !dangling {
		return // already reverted (e.g. by a concurrent path); nothing to do.
	}
	if err := m.QueryTimeoutController.Revert(revertCtx, previous); err != nil {
		slog.WarnContext(revertCtx,
			"planetscale: FAILED to revert the keyspace query timeout; it is still raised — re-run the migration (with the same credentials) to auto-revert it, or revert it manually",
			slog.String("error", err.Error()))
		return
	}
	if err := recorder.ClearQueryTimeoutRaise(revertCtx, rc.migrationID); err != nil {
		slog.WarnContext(revertCtx,
			"planetscale: reverted the keyspace query timeout but could not clear its crash-recovery record; a later run will harmlessly re-revert",
			slog.String("error", err.Error()))
	}
}

// largestEstimatedRows returns the largest per-table estimated row count over
// the schema via the source reader's optional [ir.RowCountEstimator], and
// whether any estimate was available. Estimate failures are non-fatal (the
// size gate then errs toward raising rather than skipping). Mirrors the
// pre-stream estimate the parallel-copy chunk decision already uses.
func largestEstimatedRows(ctx context.Context, rr ir.RowReader, schema *ir.Schema) (int64, bool) {
	estimator, ok := rr.(ir.RowCountEstimator)
	if !ok {
		return 0, false
	}
	var (
		largest int64
		known   bool
	)
	for _, t := range schema.Tables {
		n, err := estimator.EstimateRowCount(ctx, t)
		if err != nil {
			slog.DebugContext(ctx, "planetscale: query-timeout size gate: row estimate failed for a table; ignoring it",
				slog.String("table", t.Name), slog.String("error", err.Error()))
			continue
		}
		if n <= 0 {
			continue // no estimate for this table
		}
		known = true
		if n > largest {
			largest = n
		}
	}
	return largest, known
}
