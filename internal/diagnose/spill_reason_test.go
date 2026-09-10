// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diagnose

import (
	"context"
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// A SLOT LOOKUP THAT FINDS NOTHING MUST SAY SO.
//
// # Bug 281, filed against this file's own fix
//
// `--slot-name` is a SUFFIX: `--slot-name shard_a` creates
// `sluice_shard_a`. Audit A0909-AQ-M-1 was that the diagnose bundle
// probed the raw flag, found no such slot, and wrote NOTHING — absent
// counters being indistinguishable from a slot that simply had not
// spilled.
//
// The fix resolved the name and added a reason. The reason went on the
// `slot == ""` branch, which NEITHER CLI path can reach:
// `pipeline.SlotNameForSource` never returns empty for a postgres
// source. The branch a wrong or absent `--slot-name` actually lands on
// is this one — the probe succeeded and reported no row — and it still
// wrote nothing at all. So the fix for "an absent row reads like a
// healthy slot" left exactly that case silent, and the v0.148.3
// regression cycle caught it against real PostgreSQL.
//
// The lesson is the one this repo keeps paying for: a branch added to
// make a failure visible is worth nothing if the failure does not arrive
// on that branch. These cases exercise all three outcomes so the
// question "which branch does the real case take" has an answer that
// fails when it changes.
func TestSpillProbe_ReportsAReasonOnEveryOutcome(t *testing.T) {
	for _, tc := range []struct {
		name       string
		spiller    *stubSpillReporter
		wantReason bool
		wantCounts bool
	}{
		{
			name:       "no row for the slot — the case a wrong --slot-name lands on",
			spiller:    &stubSpillReporter{ok: false},
			wantReason: true,
		},
		{
			name:       "the probe itself errored",
			spiller:    &stubSpillReporter{err: errors.New("permission denied for pg_stat_replication_slots")},
			wantReason: true,
		},
		{
			name:       "a real reading",
			spiller:    &stubSpillReporter{ok: true, txns: 1, bytes: 10346545},
			wantCounts: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := map[string]any{}
			collectSpillStats(context.Background(), out, tc.spiller, "sluice_shard_a")

			_, gotReason := out["spill_reason"]
			if gotReason != tc.wantReason {
				t.Errorf("spill_reason present = %v; want %v.\n\n"+
					"A probe that found no row must SAY so. Writing neither counters nor a reason is what "+
					"Bug 281 was: the absence reads exactly like a healthy slot that has not spilled, which "+
					"is the defect the surrounding fix exists to prevent.", gotReason, tc.wantReason)
			}
			_, gotTxns := out["spill_txns"]
			if gotTxns != tc.wantCounts {
				t.Errorf("spill_txns present = %v; want %v", gotTxns, tc.wantCounts)
			}

			// The slot actually probed is recorded on every outcome, so a
			// reader can compare it against slot_name in
			// state/cdc_state.json — the independent expected value that was
			// already in the same bundle, unused.
			if got := out["spill_slot_probed"]; got != "sluice_shard_a" {
				t.Errorf("spill_slot_probed = %v; want the slot that was looked up, on every outcome", got)
			}
		})
	}
}

type stubSpillReporter struct {
	ok    bool
	err   error
	txns  int64
	bytes int64
}

func (s *stubSpillReporter) SlotSpillStats(_ context.Context, _ string) (ir.SpillStats, bool, error) {
	if s.err != nil {
		return ir.SpillStats{}, false, s.err
	}
	return ir.SpillStats{SpillTxns: s.txns, SpillBytes: s.bytes}, s.ok, nil
}
