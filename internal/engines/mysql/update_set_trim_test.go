// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"math"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestDropUnchangedKeyColumns_EveryKeyFamily pins GC-41 (e)'s SET trim across
// every value family a MySQL primary key can carry, not one representative:
// the comparison is reflect.DeepEqual over whatever Go type the decoder
// produced, and a family whose decoded values compare differently (a []byte
// slice, a time.Time with a location) would behave differently. For each
// family: an equal key is dropped from SET, an unequal one is kept.
func TestDropUnchangedKeyColumns_EveryKeyFamily(t *testing.T) {
	utc := time.Date(2026, 10, 2, 10, 0, 0, 123456000, time.UTC)
	families := []struct {
		name          string
		same, changed any
	}{
		{"int64", int64(7), int64(8)},
		{"uint64", uint64(1 << 63), uint64(1<<63 + 1)},
		{"string", "abc", "abd"},
		{"string case-only change", "abc", "ABC"},
		{"string trailing-space change", "abc", "abc "},
		{"bytes", []byte{0x00, 0xff}, []byte{0x00, 0xfe}},
		{"decimal string", "12.50", "12.5"},
		{"uuid string", "6f1c2c3e-0000-4000-8000-000000000001", "6f1c2c3e-0000-4000-8000-000000000002"},
		{"time.Time", utc, utc.Add(time.Microsecond)},
		{"time.Time same instant other location", utc, utc.In(time.FixedZone("X", 7200))},
		{"float64", float64(1.5), float64(-1.5)},
		// Signed zero: == (and so reflect.DeepEqual) calls these equal, and a
		// DOUBLE/FLOAT key stores them distinctly (mysql:8.4). Both widths
		// and both directions.
		{"float64 +0 to -0", float64(0), math.Copysign(0, -1)},
		{"float64 -0 to +0", math.Copysign(0, -1), float64(0)},
		{"float32 +0 to -0", float32(0), float32(math.Copysign(0, -1))},
		{"[]any float element +0 to -0", []any{1.0, 0.0}, []any{1.0, math.Copysign(0, -1)}},
		{"bool", true, false},
	}
	for _, f := range families {
		t.Run(f.name+"/unchanged is dropped", func(t *testing.T) {
			before := ir.Row{"k": f.same, "v": "a"}
			after := ir.Row{"k": f.same, "v": "b"}
			got := dropUnchangedKeyColumns(before, after, []string{"k"}, nil)
			if want := (ir.Row{"v": "b"}); !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, want %#v", got, want)
			}
			if _, ok := after["k"]; !ok {
				t.Fatal("the caller's after-image was mutated")
			}
		})
		t.Run(f.name+"/changed is kept", func(t *testing.T) {
			before := ir.Row{"k": f.same, "v": "a"}
			after := ir.Row{"k": f.changed, "v": "b"}
			got := dropUnchangedKeyColumns(before, after, []string{"k"}, nil)
			if !reflect.DeepEqual(got, after) {
				t.Fatalf("a changed key left SET: got %#v, want %#v", got, after)
			}
		})
	}
}

// TestDropUnchangedKeyColumns_Shapes pins the shape variants: composite keys,
// a key absent from an image, an after-image with nothing left to assign, a
// generated-only remainder, the name fold, and a non-key column (never
// trimmed, however unchanged).
func TestDropUnchangedKeyColumns_Shapes(t *testing.T) {
	gen := map[string]*ir.Column{"g": {Name: "g", GeneratedExpr: "v + 1"}}
	cases := []struct {
		name          string
		before, after ir.Row
		pk            []string
		colTypes      map[string]*ir.Column
		want          ir.Row
	}{
		{
			name:   "composite key, one member changed: only the unchanged one leaves",
			before: ir.Row{"tenant": int64(1), "id": int64(1), "v": "a"},
			after:  ir.Row{"tenant": int64(1), "id": int64(2), "v": "a"},
			pk:     []string{"tenant", "id"},
			want:   ir.Row{"id": int64(2), "v": "a"},
		},
		{
			name:   "composite key, both unchanged",
			before: ir.Row{"tenant": int64(1), "id": int64(1), "v": "a"},
			after:  ir.Row{"tenant": int64(1), "id": int64(1), "v": "b"},
			pk:     []string{"tenant", "id"},
			want:   ir.Row{"v": "b"},
		},
		{
			name:   "key absent from the before-image stays",
			before: ir.Row{"v": "a"},
			after:  ir.Row{"id": int64(1), "v": "b"},
			pk:     []string{"id"},
			want:   ir.Row{"id": int64(1), "v": "b"},
		},
		{
			name:   "partial after-image without the key",
			before: ir.Row{"id": int64(1), "v": "a"},
			after:  ir.Row{"added": "x"},
			pk:     []string{"id"},
			want:   ir.Row{"added": "x"},
		},
		{
			name:   "nothing left to assign: returned whole",
			before: ir.Row{"a": int64(1), "b": int64(2)},
			after:  ir.Row{"a": int64(1), "b": int64(2)},
			pk:     []string{"a", "b"},
			want:   ir.Row{"a": int64(1), "b": int64(2)},
		},
		{
			name:     "only a generated column left: returned whole",
			before:   ir.Row{"id": int64(1), "g": int64(2)},
			after:    ir.Row{"id": int64(1), "g": int64(3)},
			pk:       []string{"id"},
			colTypes: gen,
			want:     ir.Row{"id": int64(1), "g": int64(3)},
		},
		{
			name:   "key name folds",
			before: ir.Row{"ID": int64(1), "v": "a"},
			after:  ir.Row{"ID": int64(1), "v": "b"},
			pk:     []string{"id"},
			want:   ir.Row{"v": "b"},
		},
		{
			name:   "unchanged non-key column stays",
			before: ir.Row{"id": int64(1), "cust": int64(4), "v": "a"},
			after:  ir.Row{"id": int64(1), "cust": int64(4), "v": "b"},
			pk:     []string{"id"},
			want:   ir.Row{"cust": int64(4), "v": "b"},
		},
		{
			name:   "no primary key: nothing trimmed",
			before: ir.Row{"id": int64(1), "v": "a"},
			after:  ir.Row{"id": int64(1), "v": "b"},
			want:   ir.Row{"id": int64(1), "v": "b"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := dropUnchangedKeyColumns(c.before, c.after, c.pk, c.colTypes)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %#v, want %#v", got, c.want)
			}
		})
	}
}

// TestBuildUpdateSQL_LeavesUnchangedKeyOutOfSET pins the rendered statement:
// the unchanged key is gone from SET and still in WHERE (which is what routes
// the statement on vtgate), and a key change still names the key in SET.
func TestBuildUpdateSQL_LeavesUnchangedKeyOutOfSET(t *testing.T) {
	before := ir.Row{"id": int64(7), "v": "a"}
	gotSQL, gotArgs, err := buildUpdateSQL(addressEveryMatch, "s", "t", before, ir.Row{"id": int64(7), "v": "b"}, []string{"id"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := "UPDATE `s`.`t` SET `v` = ? WHERE `id` = ? AND `v` = ?"; gotSQL != want {
		t.Fatalf("unchanged key:\n got %q\nwant %q", gotSQL, want)
	}
	if want := []any{"b", int64(7), "a"}; !reflect.DeepEqual(gotArgs, want) {
		t.Fatalf("args: got %#v, want %#v", gotArgs, want)
	}

	gotSQL, _, err = buildUpdateSQL(addressEveryMatch, "s", "t", before, ir.Row{"id": int64(8), "v": "a"}, []string{"id"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := "UPDATE `s`.`t` SET `id` = ?, `v` = ? WHERE `id` = ? AND `v` = ?"; gotSQL != want {
		t.Fatalf("key change:\n got %q\nwant %q", gotSQL, want)
	}
}
