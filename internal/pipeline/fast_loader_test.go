// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// integerPKTable is a tiny fixture for the integer-PK eligible path. A
// local copy lives in migcore's chunk_test.go too — a private test
// fixture does not cross a package boundary.
func integerPKTable() *ir.Table {
	return &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{
			Name:    "pk",
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
}

// TestUseFastLoader is the ADR-0043 four-gate truth table. The gate
// is "fast loader IFF NOT resuming AND zero prior progress AND NOT
// force-cold-start". Gate (4) (live-add) is structurally vacuous for
// copyChunk and intentionally not a parameter (see useFastLoader's
// doc-comment), so it does not appear here.
func TestUseFastLoader(t *testing.T) {
	fresh := ir.TableChunkProgress{State: ir.TableProgressInProgress}
	withCursor := ir.TableChunkProgress{
		State:      ir.TableProgressInProgress,
		LastPK:     []any{int64(500)},
		RowsCopied: 500,
	}
	rowsOnly := ir.TableChunkProgress{
		State:      ir.TableProgressInProgress,
		RowsCopied: 12,
	}
	pkOnly := ir.TableChunkProgress{
		State:  ir.TableProgressInProgress,
		LastPK: []any{int64(1)},
	}
	complete := ir.TableChunkProgress{State: ir.TableProgressComplete}

	cases := []struct {
		name           string
		resuming       bool
		forceColdStart bool
		chunk          ir.TableChunkProgress
		want           bool
	}{
		// The single true row: cold, fresh, no force, zero progress.
		{"cold_fresh_noforce", false, false, fresh, true},

		// Gate (1): resume always disables the fast loader, even on a
		// zero-progress chunk (the crash-replay safety property).
		{"resume_even_if_fresh", true, false, fresh, false},
		{"resume_with_cursor", true, false, withCursor, false},

		// Gate (2): any recorded prior progress disables it.
		{"prior_cursor_and_rows", false, false, withCursor, false},
		{"prior_rows_only", false, false, rowsOnly, false},
		{"prior_pk_only", false, false, pkOnly, false},
		{"prior_state_complete", false, false, complete, false},

		// Gate (3): --force-cold-start disables it (target may be
		// populated; non-upsert WriteRows would collide).
		{"force_cold_start_even_if_fresh", false, true, fresh, false},

		// All gates failing simultaneously.
		{"resume_and_force_and_progress", true, true, withCursor, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := useFastLoader(tc.resuming, tc.forceColdStart, tc.chunk)
			if got != tc.want {
				t.Errorf("useFastLoader(resuming=%v, force=%v, chunk=%+v) = %v; want %v",
					tc.resuming, tc.forceColdStart, tc.chunk, got, tc.want)
			}
		})
	}
}
