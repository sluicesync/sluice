// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The warm-resume `--where` drift contract (audit 2026-07-23 D0-2, widened to
// every engine by audit 2026-07-26 SL-4, and made REACHABLE here after the
// v0.103.0 regression cycle found the widening inert — Bug 209).
//
// # Why this is its own phase
//
// The check used to live inside phaseResolvePublicationScope. That function
// opens with `scoper, ok := s.Source.(ir.PublicationScoper); if !ok { return
// nil }`, and Postgres is its only implementer — so widening the check to
// "every source engine" INSIDE it changed nothing at all for MySQL, Vitess,
// SQLite or the trigger-CDC engines. The inner gate was removed; the enclosing
// one was never looked at. The unit matrix passed because it called the drift
// predicate directly, pinning the function rather than the path that reaches
// it, which is the same defect class the same release fixed twice elsewhere.
//
// Living in its own phase, called unconditionally, is what makes the contract
// engine-neutral in fact rather than in intent.
//
// # What it protects
//
// A warm resume re-snapshots NOTHING. After a predicate change the target
// still holds exactly what the ORIGINAL predicate copied while the CDC leg
// begins classifying under the new one:
//
//   - narrowed  — out-of-scope rows stay on the target forever, and no future
//     event will ever remove them, because the source no longer sends them;
//   - widened   — the rows the first cold start skipped are never backfilled,
//     so the target is permanently missing a subset that `sync status` calls
//     healthy;
//   - removed   — the same as widened, at full table width.
//
// All three are silent. On a publication-scoped source there is a second,
// worse layer: the predicate is ALSO pushed into the publication as durable
// catalog state a warm resume never re-ensures, so the SERVER keeps filtering
// on the stale predicate and the admitted rows are never delivered at all.

// phaseCheckRowFilterDrift refuses a warm resume whose `--where` set differs
// from the one the stream was established with, and records the current hash
// for the next run. Runs for EVERY source engine.
func (s *Streamer) phaseCheckRowFilterDrift(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	st, rowExists, err := readRecordedPublicationState(ctx, applier, streamID)
	if err != nil {
		return err
	}

	currentHash := rowFilterFullHash(s.RowFilters)

	// A stream established before the hash widened recorded only the pushed
	// subset. Accept that spelling too, so upgrading cannot manufacture a
	// drift refusal on a stream whose flags never changed; the next
	// position-write rewrites it to the full-set hash.
	acceptable := []string{currentHash}
	if _, ok := s.Source.(ir.PublicationRowFilterer); ok {
		acceptable = append(acceptable, rowFilterPushdownHash(s.publicationRowFilters))
	}

	if rowFilterHashDriftAny(rowExists, s.RestartFromScratch, s.ResetTargetData, st.RowFilterHash, acceptable) {
		filtered := make([]string, 0, len(s.RowFilters))
		for table := range s.RowFilters {
			filtered = append(filtered, table)
		}
		sort.Strings(filtered)

		serverNote := ""
		if _, pushes := s.Source.(ir.PublicationRowFilterer); pushes {
			serverNote = " On this source the predicate is ALSO pushed into the publication as durable catalog " +
				"state a warm resume never re-ensures, so the server would keep filtering on the stale predicate " +
				"and the rows the new flags admit would never be delivered at all."
		}
		return sluicecode.Wrap(
			sluicecode.CodeWherePushdownDrift,
			"re-run with the --where this stream was established with, or --restart-from-scratch to re-snapshot under the new predicate (required for a widened filter anyway; on a PG source the restart first refuses on the stream's existing replication slot — drop it as that refusal instructs, then re-run), or --reset-target-data for a destructive reset",
			fmt.Errorf(
				"pipeline: warm resume refused: the current --where flags don't match the ones stream %q was "+
					"established with (recorded row_filter_hash %s, current %s; currently-filtered tables: [%s]). "+
					"A warm resume does not re-snapshot, so the target still holds exactly what the ORIGINAL "+
					"predicate copied while the CDC leg would begin classifying under the new one — a narrowed "+
					"filter strands out-of-scope rows on the target forever, a widened one never backfills what "+
					"the first cold start skipped.%s",
				streamID, st.RowFilterHash, currentHash, strings.Join(filtered, ", "), serverNote,
			),
		)
	}

	if !s.DryRun {
		if setter, ok := applier.(rowFilterHashSetter); ok {
			setter.SetRowFilterHash(currentHash)
		}
	}
	return nil
}
