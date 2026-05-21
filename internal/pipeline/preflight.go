// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cold-start pre-flight: refuse a fresh migration when the dest
// already contains data.
//
// Bug 9 (open as of v0.3.0): if a previous cold-start was killed
// mid-bulk-copy and the operator drops the source replication slot
// but leaves the half-baked dest tables in place, restarting cold
// start hangs in confusing ways — schema apply silently no-ops
// (CREATE TABLE IF NOT EXISTS), bulk-copy hits PRIMARY KEY collisions
// with the leftover rows, the writer errors, the row-reader's
// goroutine wedges holding a snapshot transaction open, and PG
// surfaces "idle in transaction" sessions until the operator drops
// the dest tables manually.
//
// The fix has three parts. This file owns the first one: detect
// pre-existing data on the dest and refuse with a clear error
// pointing at the recovery commands. The other two parts:
//
//  1. progress.go - "bulk copy complete rows=N" mis-logs on writer
//     error; switched to a status-aware Stop so failures surface
//     as "bulk copy aborted" instead.
//  2. migrate.go::copyTable - the row-reader goroutine leaks when
//     WriteRows errors. copyTable now derives a child context and
//     cancels it on the writer's error path so the reader unwinds
//     cleanly.
//
// The pre-flight is scoped to cold-start. Resume mode (Migrator)
// already has TableProgress + truncate-and-redo and *expects* dest
// tables to have data — the check would refuse every legitimate
// resume. The Streamer's warm-resume path doesn't run bulk-copy at
// all; only its cold-start branch invokes the check.
//
// Operators who deliberately want to bulk-copy into a populated
// table (rare) can pass --force-cold-start to skip the check. The
// flag is documented as "use with caution — INSERT into a non-empty
// table will collide on PRIMARY KEY".

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/orware/sluice/internal/ir"
)

// errColdStartRefused is the sentinel cause for a pre-flight refusal.
// Wrapped with the per-table message and the hint suffix; tests use
// errors.Is to assert on it without coupling to the message text.
var errColdStartRefused = errors.New("pipeline: cold-start refused")

// preflightMode selects which recovery hint to emit on a cold-start
// refusal. The migrator and the streamer hit different failure modes
// upstream of the refusal:
//
//   - Migrator (one-shot) only fails mid-bulk-copy; recovery is via
//     `--resume` on `sluice migrate` (per-table truncate-and-redo,
//     ADR-0015 / ADR-0018) or by dropping the partial dest tables.
//   - Streamer (sync) can fail either mid-bulk-copy OR
//     post-bulk-copy-pre-first-CDC-apply (GitHub issue #15). v0.40.1
//     closed the latter wedge on the happy path by persisting the
//     cdc-state row at the cold-start → CDC-mode boundary, but stale
//     wedge-state from an earlier-version run is still recoverable —
//     the streamer's hint guides the operator at `--reset-target-data`.
type preflightMode int

const (
	preflightModeMigrate preflightMode = iota
	preflightModeSync
)

// preflightColdStart probes every table in the schema for pre-
// existing rows on the target. Returns nil if every table is empty
// (or the writer doesn't expose [ir.TableEmptyChecker], in which
// case we silently fall back to v0.3.0 behaviour). Returns a wrapped
// errColdStartRefused on the first table that has data, so the
// operator gets a deterministic error rather than a list that
// changes between runs.
//
// The probe runs against the row writer because that's where the
// connection pool already lives — opening another reader would
// double the resource cost for what's essentially a SELECT 1 LIMIT 1
// per table. Engines that don't implement TableEmptyChecker on
// their RowWriter cause the pre-flight to skip silently; the
// existing v0.3.0 behaviour (no check) remains for them.
//
// Cost: one round-trip per source table. On a 200-table migration
// that's ~200 cheap queries before bulk-copy starts; on the typical
// failure mode the first non-empty table errors out in milliseconds.
//
// `mode` selects the recovery hint — migrate-mode reaches here only
// via the bulk-copy phase failing mid-table; sync-mode can also reach
// here from a v0.40.0-or-earlier wedge where bulk-copy completed but
// the first CDC apply failed before a cdc-state row was written
// (GitHub issue #15).
func preflightColdStart(ctx context.Context, schema *ir.Schema, rw ir.RowWriter, force bool, mode preflightMode) error {
	if force {
		slog.InfoContext(ctx, "cold-start pre-flight skipped: --force-cold-start set")
		return nil
	}
	checker, ok := rw.(ir.TableEmptyChecker)
	if !ok {
		// Engine doesn't expose the surface — silently keep v0.3.0
		// behaviour. The pre-flight is opportunistic; engines that
		// can't be probed cheaply skip it without complaint.
		return nil
	}
	for _, table := range schema.Tables {
		empty, err := checker.IsTableEmpty(ctx, table)
		if err != nil {
			return fmt.Errorf("pipeline: pre-flight probe of target table %q: %w", table.Name, err)
		}
		if !empty {
			return wrapWithHint(PhaseSchemaApply, fmt.Errorf(
				"%w: target table %q already contains data — %s",
				errColdStartRefused, table.Name, recoveryHintFor(mode),
			))
		}
	}
	return nil
}

// recoveryHintFor returns the operator-facing recovery text for a
// cold-start refusal. The two modes deliberately differ: a migrate
// failure means "bulk-copy died mid-table; redrive via --resume or
// re-cold-start"; a sync failure adds the GitHub #15 wedge as a
// possibility, where bulk-copy completed cleanly but the first CDC
// apply failed before any cdc-state row was persisted.
func recoveryHintFor(mode preflightMode) string {
	switch mode {
	case preflightModeSync:
		// Sync-mode covers two upstream failure shapes:
		//   (1) bulk-copy died mid-table (same as migrate)
		//   (2) bulk-copy completed but first CDC apply failed
		//       before a cdc-state row was written (issue #15
		//       wedge, fixed in v0.40.1 for new runs; stale state
		//       from older versions still surfaces this error)
		// Both recover via --reset-target-data. The slot-drop step
		// applies to PG sources only (Vitess / MySQL don't expose
		// it as a separate concept).
		return "no cdc-state row exists for this stream, so warm-resume isn't possible either. " +
			"Likely state after a previous sync cold-start exited before persisting a CDC position — either " +
			"mid-bulk-copy or post-bulk-copy-pre-first-CDC-apply (GitHub #15, fixed in v0.40.1 for new runs). " +
			"To recover: (a) re-run with `--reset-target-data --yes` to wipe the target tables and any stale " +
			"cdc-state row [preferred]; (b) manually `DROP TABLE` the affected target tables (and on PG sources, " +
			"drop the source slot via `sluice slot drop`), then re-run; (c) pass `--force-cold-start` to bulk-copy " +
			"into the populated table anyway (will collide on PRIMARY KEY in most cases — use with extreme caution)"
	default: // preflightModeMigrate
		return "this usually means a previous cold-start was killed mid-bulk-copy. " +
			"To recover: (1) drop the partial dest tables (or use `--resume` on `sluice migrate`), " +
			"(2) drop the source-side replication slot via `sluice slot drop` (PG sources only), " +
			"(3) re-run the cold-start. Pass --force-cold-start to bulk-copy into the populated " +
			"table anyway (use with caution — INSERT into a non-empty table will collide on PRIMARY KEY)"
	}
}
