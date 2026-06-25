// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # Post-copy DDL-phase reparent retry (ADR-0114)
//
// The DDL sibling of ADR-0108's cold-copy target-WRITE reparent-retry and
// ADR-0109's source-READ reconnect-retry. The cold-copy DATA phase rides a
// PlanetScale storage-grow reparent fine (the row writers are wrapped in
// the reparent/grow-gate machinery), but the POST-COPY DDL phases —
// CreateIndexes, CreateConstraints (foreign keys), CreateViews, and the
// identity-sequence sync — exec their DDL directly against the target and
// returned the raw driver error with NO retry. A grow/reparent that lands
// on the index build (or any DDL phase) therefore aborted the WHOLE
// migrate / restore, even though the data was fully and correctly copied.
//
// Live finding (Track-C cross-engine MySQL→PG restore, 2026-06-23): all 29
// tables' rows landed byte-perfect against the manifest riding the grow,
// then `create index idx_lc_k on live_churn` died with
// `FATAL: terminating connection due to administrator command (57P01)`
// because PG entered a second reparent exactly when the index phase began.
//
// This helper wraps an idempotent DDL phase in a bounded
// classify-and-retry loop that MIRRORS the cold-copy reparent-retry
// envelope: on a CLASSIFIED transient (the engine's own applier classifier,
// surfaced engine-neutrally via [ir.TransientClassifier]) it backs off and
// re-runs the phase; on a non-transient (a real DDL fault) it returns the
// error unchanged and the phase fails loudly, exactly as before; on
// wall-clock exhaustion it surfaces a loud terminal error. It NEVER hides a
// genuinely-broken DDL or an unreachable target.
//
// SAFETY — every wrapped phase must be IDEMPOTENT on re-run, which both
// engines already guarantee for these phases (this helper relies on it, it
// does not add it):
//
//   - CreateIndexes — PG `CREATE INDEX IF NOT EXISTS` (atomic, non-
//     CONCURRENTLY: a killed build leaves nothing, IF NOT EXISTS skips
//     ones already done); MySQL pre-checks existing indexes and skips them
//     (no 1061 "duplicate key name").
//   - CreateConstraints — both engines detect-then-skip an already-present
//     FK/constraint via the catalog (Bug 131 idempotent-resume).
//   - CreateViews — `CREATE OR REPLACE` for regular views; matviews are
//     detect-as-success; runViewsPhase's own dependency-pass retry tolerates
//     "already exists".
//   - SyncIdentitySequences — `setval` / AUTO_INCREMENT reset is naturally
//     idempotent (re-running writes the same high-water value).
//
// A SchemaWriter that does NOT implement [ir.TransientClassifier] (a future
// engine, a test stub) degrades to a single attempt — pre-ADR-0114
// behaviour, byte-for-byte.

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// DDL-phase retry bounds. Package vars (not config fields) so there is no
// EnableX-defaulting-true zero-value trap (the v0.99.51 lesson): every
// construction path — CLI, restore, migrate, broker, tests — shares the
// same envelope. They MIRROR the cold-copy reparent-retry envelope
// (ADR-0108/0109): exponential 100ms→…→30s cap, the REAL bound being a
// ~30-min wall clock so a single DDL phase rides a prolonged multi-step
// storage-grow / failover, and a high attempt count as a pure runaway
// backstop. Vars (not consts) ONLY so unit tests can shrink the envelope to
// keep the suite fast; production never mutates them.
var (
	ddlPhaseRetryMaxWall     = 30 * time.Minute
	ddlPhaseRetryAttempts    = 100000
	ddlPhaseRetryBackoffBase = 100 * time.Millisecond
	ddlPhaseRetryBackoffCap  = 30 * time.Second
)

// ddlPhaseBackoff returns the per-attempt backoff: exponential doubling
// from ddlPhaseRetryBackoffBase, capped at ddlPhaseRetryBackoffCap.
// attempt is 1-based (attempt 1 is the wait BEFORE the second try).
// Mirrors coldCopySourceReadBackoff's shape (ADR-0109).
func ddlPhaseBackoff(attempt int) time.Duration {
	b := ddlPhaseRetryBackoffBase
	for i := 1; i < attempt; i++ {
		b *= 2
		if b > ddlPhaseRetryBackoffCap {
			return ddlPhaseRetryBackoffCap
		}
	}
	return b
}

// runDDLPhaseWithReparentRetry runs one idempotent post-copy DDL phase
// (do) with the ADR-0114 bounded reparent/transient retry around it. It is
// the ONE place the DDL-phase retry policy lives so every DDL entry point
// (restore + migrate, all variants) shares one shape, one log, one bound —
// the DDL-phase analog of flushWithReparentRetry / copyTableWithSourceReadRetry.
//
//   - phase names the phase for the WARN/terminal logs ("indexes",
//     "constraints", "views", "identity-sequences").
//   - classifierSrc is the engine SchemaWriter; if it implements
//     [ir.TransientClassifier] its verdict gates the retry, otherwise the
//     phase runs exactly once (pre-ADR-0114).
//   - do runs the phase. It MUST be idempotent on re-run (see file header).
//
// The first error is classified; the loop retries ONLY a classified
// transient (PlanetScale reparent / storage-grow / connection-drop). Any
// non-transient (a real DDL fault, a type/constraint error, a ctx-cancel)
// returns unchanged — no retry — exactly as before. On budget exhaustion it
// returns a LOUD terminal error wrapping the most recent transient (never
// silent, never infinite).
func runDDLPhaseWithReparentRetry(
	ctx context.Context,
	phase string,
	classifierSrc any,
	do func(ctx context.Context) error,
) error {
	err := do(ctx)
	if err == nil {
		return nil
	}

	classifier, ok := classifierSrc.(ir.TransientClassifier)
	if !ok {
		// No engine classifier ⇒ single attempt, terminal on any error
		// (pre-ADR-0114 behaviour, byte-for-byte).
		return err
	}

	// WALL-CLOCK BOUND: retry until success or this deadline — not a fixed
	// attempt count — so a DDL phase rides a prolonged grow regardless of
	// retry cadence (mirrors coldCopyReparentMaxWallVar).
	deadline := time.Now().Add(ddlPhaseRetryMaxWall)

	for try := 1; ; try++ {
		// Classify the MOST RECENT error. Only a transient is retriable;
		// a real DDL fault returns unchanged (terminal).
		if !classifier.IsTransientError(err) {
			return err
		}
		if time.Now().After(deadline) || try >= ddlPhaseRetryAttempts {
			return fmt.Errorf(
				"pipeline: %s phase still failing after riding the storage-grow/reparent window "+
					"(%s wall-clock, %d attempts; the target keeps dropping the DDL connection — it may be wedged, "+
					"or a prolonged storage-grow keeps reparenting): %w",
				phase, ddlPhaseRetryMaxWall, try, err,
			)
		}

		backoff := ddlPhaseBackoff(try)
		slog.WarnContext(
			ctx, "pipeline: post-copy DDL phase hit a transient target error (likely a storage auto-grow / reparent); backing off and retrying the phase (ADR-0114)",
			slog.String("phase", phase),
			slog.Int("attempt", try),
			slog.Duration("elapsed", time.Since(deadline.Add(-ddlPhaseRetryMaxWall))),
			slog.Duration("max_wall", ddlPhaseRetryMaxWall),
			slog.Duration("backoff", backoff),
			slog.String("err", err.Error()),
		)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}

		err = do(ctx)
		if err == nil {
			return nil
		}
	}
}
