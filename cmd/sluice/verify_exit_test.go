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
func TestVerifyExitError(t *testing.T) {
	res := func(mismatch, unverified int) *pipeline.VerifyResult {
		return &pipeline.VerifyResult{Summary: pipeline.VerifySummary{
			TablesChecked:    2,
			TablesClean:      2 - mismatch - unverified,
			TablesMismatch:   mismatch,
			TablesUnverified: unverified,
		}}
	}
	cases := []struct {
		name     string
		result   *pipeline.VerifyResult
		wantExit int
		wantMsg  []string
	}{
		{"nil result is success", nil, 0, nil},
		{"clean run exits 0", res(0, 0), 0, nil},
		{"mismatch keeps its documented 1", res(1, 0), 1, []string{"1 table(s) with row-count mismatch"}},
		{
			"unverified table exits 2 (Bug 190)",
			res(0, 1), 2,
			[]string{"verify incomplete", "1 table(s) could not be verified", "not a pass"},
		},
		{
			"mismatch + unverified exits 1 naming both",
			res(1, 1), 1,
			[]string{"1 table(s) with row-count mismatch", "1 table(s) could not be verified"},
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
		})
	}
}
