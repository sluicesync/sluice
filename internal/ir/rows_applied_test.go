// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "testing"

// TestIsRowDMLChange_FullMatrix pins the count semantics across EVERY
// [Change] family (the predicate dispatches on the change type, so the pin
// must exercise every member, not one representative — the Bug-74 lesson):
// the three row-level DML kinds count, and the four non-DML kinds do not.
func TestIsRowDMLChange_FullMatrix(t *testing.T) {
	p := Position{Engine: "e", Token: "t"}
	cases := []struct {
		name string
		c    Change
		want bool
	}{
		{"Insert", Insert{Position: p, Schema: "s", Table: "t", Row: Row{"id": int64(1)}}, true},
		{"Update", Update{Position: p, Schema: "s", Table: "t", Before: Row{"id": int64(1)}, After: Row{"id": int64(2)}}, true},
		{"Delete", Delete{Position: p, Schema: "s", Table: "t", Before: Row{"id": int64(1)}}, true},
		{"Truncate", Truncate{Position: p, Schema: "s", Table: "t"}, false},
		{"SchemaSnapshot", SchemaSnapshot{Position: p, Schema: "s", Table: "t", IR: &Table{Name: "t"}}, false},
		{"TxBegin", TxBegin{Position: p}, false},
		{"TxCommit", TxCommit{Position: p}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsRowDMLChange(tc.c); got != tc.want {
				t.Errorf("IsRowDMLChange(%s) = %v; want %v", tc.name, got, tc.want)
			}
			wantDelta := int64(0)
			if tc.want {
				wantDelta = 1
			}
			if got := RowsAppliedDelta(tc.c); got != wantDelta {
				t.Errorf("RowsAppliedDelta(%s) = %d; want %d", tc.name, got, wantDelta)
			}
		})
	}
}
