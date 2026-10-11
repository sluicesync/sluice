// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline"
)

// TestVerifyExitError pins the Bug 190 fix at the CLI exit boundary:
// a completed verify run with tables that could not be verified must
// exit non-zero (verify's documented exit-2 "the check could not
// (fully) run" class), a mismatch keeps its long-documented exit 1,
// and a clean run keeps exit 0. Pinned through verifyExitError — the
// exact mapping Run ships — so the exit contract can't silently
// regress at the layer the report/exit split lives in.
//
// It also pins Bug 303: the closing line names each mismatch KIND from
// the per-table verdicts, so a column- or sample-mismatch whose counts
// matched is never reported as a row-count mismatch.
func TestVerifyExitError(t *testing.T) {
	// Per-table rows shaped as Verifier.Run leaves them.
	var (
		countDrift  = pipeline.VerifyTableResult{Name: "c", SourceRowCount: 3, TargetRowCount: 2, CountMismatch: true}
		columnDrift = pipeline.VerifyTableResult{Name: "m", SourceRowCount: 3, TargetRowCount: 3, ColumnsMissingOnTarget: []string{"c"}}
		sampleDrift = pipeline.VerifyTableResult{Name: "s", SourceRowCount: 2, TargetRowCount: 2, SampleSize: 2, SampleMismatch: 1, SampleMismatchPKs: []string{"1"}}
		unverified  = pipeline.VerifyTableResult{Name: "u", Reason: "table not on target"}
		clean       = pipeline.VerifyTableResult{Name: "ok", SourceRowCount: 1, TargetRowCount: 1}
	)
	res := func(tables ...pipeline.VerifyTableResult) *pipeline.VerifyResult {
		r := &pipeline.VerifyResult{Tables: tables}
		for _, tr := range tables {
			switch {
			case tr.Reason != "":
				r.Summary.TablesUnverified++
			case tr.CountMismatch || len(tr.ColumnsMissingOnTarget) > 0 || tr.SampleMismatch > 0:
				r.Summary.TablesMismatch++
			default:
				r.Summary.TablesClean++
			}
		}
		r.Summary.TablesChecked = len(tables)
		return r
	}
	cases := []struct {
		name     string
		result   *pipeline.VerifyResult
		wantExit int
		wantMsg  []string
		denyMsg  []string // substrings the message must NOT carry
	}{
		{"nil result is success", nil, 0, nil, nil},
		{"clean run exits 0", res(clean, clean), 0, nil, nil},
		{
			"count mismatch keeps its documented 1",
			res(countDrift, clean), 1,
			[]string{"drift detected: 1 table(s) mismatched (1 count-mismatch)"},
			nil,
		},
		{
			"column mismatch is named, not called a count mismatch (Bug 303)",
			res(columnDrift, clean), 1,
			[]string{"drift detected: 1 table(s) mismatched (1 column-mismatch)"},
			[]string{"count"},
		},
		{
			"sample mismatch is named, not called a count mismatch (Bug 303)",
			res(sampleDrift, clean), 1,
			[]string{"drift detected: 1 table(s) mismatched (1 sample-mismatch)"},
			[]string{"count"},
		},
		{
			"every kind is named, in verdict order",
			res(sampleDrift, columnDrift, countDrift, sampleDrift), 1,
			[]string{"drift detected: 4 table(s) mismatched (1 count-mismatch, 1 column-mismatch, 2 sample-mismatch)"},
			nil,
		},
		{
			"unverified table exits 2 (Bug 190)",
			res(unverified, clean), 2,
			[]string{"verify incomplete", "1 table(s) could not be verified", "not a pass"},
			[]string{"mismatch"},
		},
		{
			"mismatch + unverified exits 1 naming both",
			res(sampleDrift, unverified), 1,
			[]string{"1 table(s) mismatched (1 sample-mismatch)", "1 table(s) could not be verified"},
			[]string{"count"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := verifyExitError(c.result)
			if got := exitCodeLikeKong(err); got != c.wantExit {
				t.Errorf("exit code = %d; want %d (err: %v)", got, c.wantExit, err)
			}
			for _, want := range c.wantMsg {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("error %v should contain %q", err, want)
				}
			}
			for _, deny := range c.denyMsg {
				if err != nil && strings.Contains(err.Error(), deny) {
					t.Errorf("error %v should not contain %q", err, deny)
				}
			}
		})
	}
}
