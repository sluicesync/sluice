// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"fmt"
)

// Change-log retention for the trigger-CDC SOURCE (ADR-0137, Bug 165). The
// capture triggers never reap consumed rows, so `sluice_change_log` grows
// unbounded for the life of a continuous sync. [Prune] / [PruneD1] reap rows the
// TARGET has already durably applied — keyed off a `cut` id the CLI derives from
// the target's persisted CDC watermark, NEVER the source reader's read cursor
// (which runs ahead of the durable frontier; pruning there would delete
// not-yet-applied rows → silent loss on warm-resume). This package only executes
// the DELETE; computing a safe `cut` is the CLI's job (ADR-0137 §1).

// PruneOptions controls a change-log prune. Cut is the inclusive upper bound —
// rows with id <= Cut are deleted. The caller guarantees Cut is a safe bound (at
// or below the target's durably-applied frontier minus a margin); this package
// trusts it and does not re-derive it.
type PruneOptions struct {
	// Cut deletes change-log rows with id <= Cut. The CLI only calls Prune when
	// Cut > 0 (a non-positive cut is a no-op it handles before dispatching).
	Cut int64

	// Vacuum runs VACUUM after the DELETE to reclaim file space (off by default —
	// VACUUM rewrites the whole database). On D1 it executes over the /query API
	// (best-effort; surfaces loudly if the endpoint rejects it).
	Vacuum bool

	// DryRun reports the current change-log stats without deleting anything.
	DryRun bool
}

// PruneResult is the operator-facing outcome of a prune.
type PruneResult struct {
	Deleted      int64 // rows DELETEd (0 on a dry-run)
	Vacuumed     bool  // VACUUM was applied
	RemainingMin int64 // MIN(id) of the change-log after the prune (0 when empty)
	Remaining    int64 // COUNT(*) of the change-log after the prune
}

// Prune reaps durably-applied rows from a local SQLite file's change-log.
func Prune(ctx context.Context, dsn string, opts PruneOptions) (*PruneResult, error) {
	return prune(ctx, localBackend(dsn), opts)
}

// PruneD1 reaps durably-applied rows from a live Cloudflare D1 database's
// change-log over the /query HTTP API (ADR-0136 transport).
func PruneD1(ctx context.Context, dsn string, opts PruneOptions) (*PruneResult, error) {
	b, err := d1Backend(dsn)
	if err != nil {
		return nil, err
	}
	return prune(ctx, b, opts)
}

// PruneConsumedChangeLog implements [ir.ChangeLogPruner] (ADR-0137 Phase B): the
// streamer's in-stream auto-prune sidecar calls it on a cadence with the TARGET's
// durably-persisted position token. The decode stays engine-owned — it reuses
// [AppliedLastID] (which refuses a FOREIGN token loudly) to extract the applied
// frontier, then reaps `id <= appliedLastID - keep`. A non-positive cut is a safe
// no-op. The DELETE runs on a FRESH writable executor opened from the reader's
// backend (not the read-only poll executor), so it never contends with — or races
// the Close of — the polling connection. Both the local SQLite file and the D1
// transport are handled transparently by the shared [prune] path.
func (r *CDCReader) PruneConsumedChangeLog(ctx context.Context, durablePositionToken string, keep int64) (int64, error) {
	appliedLastID, err := AppliedLastID(durablePositionToken)
	if err != nil {
		return 0, err
	}
	cut := appliedLastID - keep
	if cut <= 0 {
		// Nothing safely below the durable frontier minus the margin yet.
		return 0, nil
	}
	res, err := prune(ctx, r.b, PruneOptions{Cut: cut})
	if err != nil {
		return 0, err
	}
	return res.Deleted, nil
}

// prune is the transport-neutral reaper used by both [Prune] (local file) and
// [PruneD1] (D1 over HTTP). It opens a writable executor, verifies the change-log
// exists (refusing loudly otherwise — a prune against a non-setup source is an
// operator error, not a silent no-op), DELETEs id <= Cut, optionally VACUUMs,
// and reports the post-prune stats. Idempotent: re-running with the same Cut
// deletes nothing new.
func prune(ctx context.Context, b backend, opts PruneOptions) (*PruneResult, error) {
	exec, err := b.openExec(ctx, false)
	if err != nil {
		return nil, fmt.Errorf("%s: prune: open: %w", b.driver, err)
	}
	defer func() { _ = exec.close() }()

	exists, err := exec.changeLogExists(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: prune: check change-log: %w", b.driver, err)
	}
	if !exists {
		return nil, fmt.Errorf(
			"%s: prune: change-log table %q not found on the source — run `sluice trigger setup` first",
			b.driver, ChangeLogTable,
		)
	}

	res := &PruneResult{}
	if opts.DryRun {
		if res.RemainingMin, res.Remaining, err = exec.changeLogStats(ctx); err != nil {
			return nil, fmt.Errorf("%s: prune: stats: %w", b.driver, err)
		}
		return res, nil
	}

	if res.Deleted, err = exec.pruneChangeLog(ctx, opts.Cut); err != nil {
		return nil, fmt.Errorf("%s: prune: delete: %w", b.driver, err)
	}
	if opts.Vacuum {
		if err := exec.vacuum(ctx); err != nil {
			return res, fmt.Errorf("%s: prune: vacuum: %w", b.driver, err)
		}
		res.Vacuumed = true
	}
	if res.RemainingMin, res.Remaining, err = exec.changeLogStats(ctx); err != nil {
		return res, fmt.Errorf("%s: prune: stats: %w", b.driver, err)
	}
	return res, nil
}
