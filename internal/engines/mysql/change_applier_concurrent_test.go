// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The router / frontier / lane-apply unit pins moved to internal/laneapply
// (the ADR-0105 STEP-1 extraction of the engine-neutral concurrent key-hash
// apply core) — see internal/laneapply/{router,frontier,lane_apply}_test.go.
// The two pins below stay here because they exercise the MySQL-side decode
// helpers (rowChangeSchemaTable / pkChangedUpdate) the [laneApplierAdapter]
// owns behind the seam.

func TestPkChangedUpdate(t *testing.T) {
	pk := []string{"id"}
	cases := []struct {
		name string
		u    ir.Update
		want bool
	}{
		{"same-pk", ir.Update{Before: ir.Row{"id": int64(1), "v": "a"}, After: ir.Row{"id": int64(1), "v": "b"}}, false},
		{"changed-pk", ir.Update{Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2)}}, true},
		{"nil-before", ir.Update{Before: nil, After: ir.Row{"id": int64(1)}}, false},
		{"bytes-pk-same", ir.Update{Before: ir.Row{"id": []byte("k")}, After: ir.Row{"id": []byte("k")}}, false},
		{"bytes-pk-diff", ir.Update{Before: ir.Row{"id": []byte("k")}, After: ir.Row{"id": []byte("j")}}, true},
	}
	for _, tc := range cases {
		if got := pkChangedUpdate(tc.u, pk); got != tc.want {
			t.Errorf("%s: pkChangedUpdate=%v want %v", tc.name, got, tc.want)
		}
	}
}

func TestRowChangeSchemaTable(t *testing.T) {
	cases := []struct {
		c             ir.Change
		schema, table string
	}{
		{ir.Insert{Schema: "ks", Table: "t"}, "ks", "t"},
		{ir.Update{Schema: "ks", Table: "u"}, "ks", "u"},
		{ir.Delete{Schema: "ks", Table: "d"}, "ks", "d"},
		{ir.TxBegin{}, "", ""},
	}
	for _, tc := range cases {
		s, tb := rowChangeSchemaTable(tc.c)
		if s != tc.schema || tb != tc.table {
			t.Errorf("rowChangeSchemaTable(%T) = (%q,%q), want (%q,%q)", tc.c, s, tb, tc.schema, tc.table)
		}
	}
}
