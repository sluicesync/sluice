// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestSequencePrimeReport_HasRefusals pins the orchestrator's gating
// signal: a report's HasRefusals() reports true iff any action's
// Outcome is "refused". The CLI uses this to gate exit-code 0.
func TestSequencePrimeReport_HasRefusals(t *testing.T) {
	cases := []struct {
		name string
		r    SequencePrimeReport
		want bool
	}{
		{
			name: "empty report",
			r:    SequencePrimeReport{},
			want: false,
		},
		{
			name: "all primed",
			r: SequencePrimeReport{Actions: []SequencePrimeAction{
				{Table: "a", Outcome: "primed"},
				{Table: "b", Outcome: "primed"},
			}},
			want: false,
		},
		{
			name: "noop and skipped only",
			r: SequencePrimeReport{Actions: []SequencePrimeAction{
				{Table: "a", Outcome: "noop"},
				{Table: "b", Outcome: "skipped"},
			}},
			want: false,
		},
		{
			name: "one refused",
			r: SequencePrimeReport{Actions: []SequencePrimeAction{
				{Table: "a", Outcome: "primed"},
				{Table: "b", Outcome: "refused"},
			}},
			want: true,
		},
		{
			name: "all refused",
			r: SequencePrimeReport{Actions: []SequencePrimeAction{
				{Table: "a", Outcome: "refused"},
				{Table: "b", Outcome: "refused"},
			}},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.r.HasRefusals()
			if got != tc.want {
				t.Errorf("HasRefusals() = %v; want %v", got, tc.want)
			}
		})
	}
}

// TestCutoverSequenceMarginDefault pins the default value at the IR
// contract layer so downstream engines / CLI can rely on it as the
// "operator passed --cutover-sequence-margin=0" fallback.
func TestCutoverSequenceMarginDefault(t *testing.T) {
	if CutoverSequenceMarginDefault != 1000 {
		t.Errorf("CutoverSequenceMarginDefault = %d; want 1000",
			CutoverSequenceMarginDefault)
	}
}
