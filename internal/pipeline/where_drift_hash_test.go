// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The four-cell matrix for audit 2026-07-26 SL-4 — the warm-resume `--where`
// drift ratchet must cover every source engine, not just the one that can push
// filters into a publication.
//
// The original contract was written for the PG-pushed subset because that is
// durable source-side catalog state. But the drift hazard is not exclusive to
// it: a warm resume re-snapshots NOTHING, so after a predicate change the
// target still holds what the original predicate copied while the CDC leg
// classifies under the new one. Narrowing strands out-of-scope rows on the
// target forever; widening never backfills what the first cold start skipped.
// Both silent. The cells are {pushes filters, does not} × {predicate changed,
// unchanged}.
package pipeline

import "testing"

func TestRowFilterHashDrift_FourCells(t *testing.T) {
	established := map[string]string{"orders": "region = 'EU'"}
	widened := map[string]string{"orders": "region != 'US'"}
	removed := map[string]string{}

	recorded := rowFilterFullHash(established)

	cases := []struct {
		name      string
		current   map[string]string
		wantDrift bool
	}{
		{"unchanged predicate", established, false},
		{"widened predicate", widened, true},
		{"removed predicate", removed, true},
		{"added table", map[string]string{"orders": "region = 'EU'", "events": "kind = 'x'"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rowFilterHashDriftAny(true, false, false, recorded, []string{rowFilterFullHash(tc.current)})
			if got != tc.wantDrift {
				t.Errorf("drift = %v, want %v — this cell is engine-independent, so a source that cannot push "+
					"filters must be guarded exactly like one that can (audit SL-4)", got, tc.wantDrift)
			}
		})
	}
}

// TestRowFilterHashDrift_EscapesAndLegacyRows pins the arms that must NOT
// refuse: the explicit operator escapes, a brand-new stream, a legacy row with
// no recorded hash, and — the upgrade case — a stream that recorded only the
// old pushed-subset spelling while its flags never changed.
func TestRowFilterHashDrift_EscapesAndLegacyRows(t *testing.T) {
	full := rowFilterFullHash(map[string]string{"orders": "region = 'EU'", "events": "kind = 'x'"})
	pushedOnly := rowFilterPushdownHash(map[string]string{"orders": "region = 'EU'"})

	cases := []struct {
		name       string
		rowExists  bool
		restart    bool
		reset      bool
		recorded   string
		acceptable []string
		wantDrift  bool
	}{
		{"brand-new stream", false, false, false, "", []string{full}, false},
		{"legacy row, nothing recorded", true, false, false, "", []string{full}, false},
		{"--restart-from-scratch", true, true, false, "deadbeefdeadbeef", []string{full}, false},
		{"--reset-target-data", true, false, true, "deadbeefdeadbeef", []string{full}, false},
		{
			name: "upgraded stream recorded the pushed-subset spelling, flags unchanged",
			// The legacy spelling must be accepted or upgrading manufactures a
			// drift refusal on a stream nobody touched.
			rowExists: true, recorded: pushedOnly,
			acceptable: []string{full, pushedOnly}, wantDrift: false,
		},
		{
			name:      "genuine drift matches neither spelling",
			rowExists: true, recorded: "0000000000000000",
			acceptable: []string{full, pushedOnly}, wantDrift: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rowFilterHashDriftAny(tc.rowExists, tc.restart, tc.reset, tc.recorded, tc.acceptable)
			if got != tc.wantDrift {
				t.Errorf("drift = %v, want %v", got, tc.wantDrift)
			}
		})
	}
}
