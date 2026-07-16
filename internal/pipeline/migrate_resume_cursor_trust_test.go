// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the migrate-side legacy-cursor quarantine (audit
// 2026-07-15 CRITICAL-2 / HIGH-1): suspectResumeEntry must sweep the
// single-chunk LastPK AND every per-chunk bound/cursor slice, and stay
// quiet for clean (post-envelope) shapes.

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func cursorTrustTestTable() *ir.Table {
	return &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
}

func TestSuspectResumeEntry(t *testing.T) {
	table := cursorTrustTestTable()
	cases := []struct {
		name    string
		entry   ir.TableProgress
		suspect string // substring of the reason; "" = trusted
	}{
		{
			name:  "clean single-chunk cursor",
			entry: ir.TableProgress{State: ir.TableProgressInProgress, LastPK: []any{int64(9007199254740995)}},
		},
		{
			name:    "float-drifted single-chunk cursor",
			entry:   ir.TableProgress{State: ir.TableProgressInProgress, LastPK: []any{float64(1.75e18)}},
			suspect: "float-typed",
		},
		{
			name: "clean chunk bounds",
			entry: ir.TableProgress{State: ir.TableProgressInProgress, Chunks: []ir.TableChunkProgress{
				{ChunkIndex: 0, UpperPK: []any{int64(100)}},
				{ChunkIndex: 1, LowerPK: []any{int64(100)}, LastPK: []any{int64(150)}},
			}},
		},
		{
			name: "float-drifted chunk boundary",
			entry: ir.TableProgress{State: ir.TableProgressInProgress, Chunks: []ir.TableChunkProgress{
				{ChunkIndex: 0, UpperPK: []any{int64(100)}},
				{ChunkIndex: 1, LowerPK: []any{float64(9.007199254740996e15)}},
			}},
			suspect: "chunk 1",
		},
		{
			name: "U+FFFD-mangled chunk cursor",
			entry: ir.TableProgress{State: ir.TableProgressInProgress, Chunks: []ir.TableChunkProgress{
				{ChunkIndex: 0, LastPK: []any{"�A"}},
			}},
			suspect: "U+FFFD",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := suspectResumeEntry(table, tc.entry)
			if tc.suspect == "" && got != "" {
				t.Errorf("suspectResumeEntry = %q; want trusted", got)
			}
			if tc.suspect != "" && !strings.Contains(got, tc.suspect) {
				t.Errorf("suspectResumeEntry = %q; want reason containing %q", got, tc.suspect)
			}
		})
	}
}
