// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// PK-change / row-identity helper pins. These moved here from the MySQL
// engine package in the ADR-0105 STEP-2 single-sourcing (both the MySQL and
// Postgres lane adapters route their PK-change decision through these), so
// the pin lives with the now-shared helpers.

func TestPKChangedUpdate(t *testing.T) {
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
		if got := PKChangedUpdate(tc.u, pk); got != tc.want {
			t.Errorf("%s: PKChangedUpdate=%v want %v", tc.name, got, tc.want)
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
		s, tb := RowChangeSchemaTable(tc.c)
		if s != tc.schema || tb != tc.table {
			t.Errorf("RowChangeSchemaTable(%T) = (%q,%q), want (%q,%q)", tc.c, s, tb, tc.schema, tc.table)
		}
	}
}
