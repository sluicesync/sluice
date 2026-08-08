// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

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

// keyFamily is one row of the PK-value family matrix
// TestValuesEqualForKey_FamilyMatrix and
// TestValuesEqualForKey_ImpliesSameLane share. `same` is a second,
// independently-constructed value that must compare EQUAL to `a` (never the
// same variable — an identity comparison would pass under a broken
// implementation), and `diff` a value of the same family that must not.
type keyFamily struct {
	name       string
	a          any
	same       any
	diff       any
	comparable bool // false = Go `==` on this family PANICS
}

// keyFamilies enumerates every Go value kind docs/value-types.md permits in
// an ir.Row that can also sit in a PRIMARY KEY column, at every SHAPE the
// kind has — the Bug-74 "pin the class, not the representative" matrix
// applied to row IDENTITY rather than to a value codec.
//
// The three uncomparable families are the point (roadmap item 154): each
// PANICS under Go's `==`, each is produced by a real reader for a column
// type Postgres or MySQL permits as a primary key, and none of them was
// covered before. The comparable families are carried alongside so a future
// change to the comparison cannot quietly alter them.
func keyFamilies() []keyFamily {
	return []keyFamily{
		// --- comparable scalars (the pre-item-154 coverage) ---
		{"bool", true, true, false, true},
		{"int64", int64(1), int64(1), int64(2), true},
		{"uint64", uint64(1), uint64(1), uint64(2), true},
		{"float64", 1.5, 1.5, 2.5, true},
		{"string", "k", "k" + "", "j", true},
		{"decimal-string", "1.10", "1.10" + "", "1.100", true},
		{"json.Number", json.Number("1.5"), json.Number("1.5"), json.Number("1.6"), true},
		{"time.Time", time.Unix(0, 0).UTC(), time.Unix(0, 0).UTC(), time.Unix(1, 0).UTC(), true},
		{"nil", nil, nil, int64(1), true},

		// --- uncomparable: []byte (Binary/Blob/Geometry/JSON keys) ---
		{"bytes", []byte("k"), []byte("k"), []byte("j"), false},
		{"bytes-empty", []byte{}, []byte{}, []byte("k"), false},

		// --- uncomparable: []any (ir.Array — PG btree array_ops PK) ---
		{"array-1d", []any{int64(1), int64(2)}, []any{int64(1), int64(2)}, []any{int64(1), int64(3)}, false},
		{"array-text-1d", []any{"a", "b"}, []any{"a", "b"}, []any{"a", "c"}, false},
		{"array-2d", []any{[]any{int64(1)}, []any{int64(2)}}, []any{[]any{int64(1)}, []any{int64(2)}}, []any{[]any{int64(1)}, []any{int64(3)}}, false},
		{"array-null-element", []any{int64(1), nil}, []any{int64(1), nil}, []any{int64(1), int64(0)}, false},
		{"array-empty", []any{}, []any{}, []any{int64(1)}, false},

		// --- uncomparable: []string (ir.Set — MySQL SET is indexable) ---
		{"set", []string{"a", "b"}, []string{"a", "b"}, []string{"a"}, false},

		// --- uncomparable: map[string]any (pgtrigger jsonb, btree jsonb_ops PK) ---
		{"jsonb-object", map[string]any{"a": int64(1)}, map[string]any{"a": int64(1)}, map[string]any{"a": int64(2)}, false},
		{"jsonb-nested", map[string]any{"a": []any{int64(1)}}, map[string]any{"a": []any{int64(1)}}, map[string]any{"a": []any{int64(2)}}, false},
	}
}

// eqPanics reports whether Go's `==` panics on the pair — the pre-fix
// behaviour of valuesEqualForKey. It is the matrix's ANTI-VACUITY floor:
// without it a later edit could replace the uncomparable corpus entries with
// comparable ones and the matrix would still pass while testing nothing that
// item 154 was about.
func eqPanics(a, b any) (panicked bool) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	_ = a == b
	return false
}

// TestValuesEqualForKey_FamilyMatrix drives every family × shape through the
// PUBLIC entry point the appliers call ([PKChangedUpdate]) — not the private
// helper — so it exercises the path that crashed: postgres and mysql
// change_applier_concurrent.go and mysql change_applier_multirow.go all reach
// the comparison through this function.
//
// Before roadmap item 154 the five uncomparable families below panicked here
// with "comparing uncomparable type []interface {}" rather than returning.
func TestValuesEqualForKey_FamilyMatrix(t *testing.T) {
	pk := []string{"id"}
	var sawUncomparable int
	for _, f := range keyFamilies() {
		if got := eqPanics(f.a, f.same); got != !f.comparable {
			t.Errorf("%s: `==` panics = %v, but the matrix declares comparable=%v; "+
				"the corpus no longer exercises what it claims", f.name, got, f.comparable)
		}
		if !f.comparable {
			sawUncomparable++
		}
		unchanged := ir.Update{Before: ir.Row{"id": f.a}, After: ir.Row{"id": f.same}}
		if PKChangedUpdate(unchanged, pk) {
			t.Errorf("%s: PKChangedUpdate on an unchanged key = true, want false", f.name)
		}
		changed := ir.Update{Before: ir.Row{"id": f.a}, After: ir.Row{"id": f.diff}}
		if !PKChangedUpdate(changed, pk) {
			t.Errorf("%s: PKChangedUpdate on a changed key = false, want true", f.name)
		}
	}
	if sawUncomparable < 5 {
		t.Errorf("matrix carries %d uncomparable families; want >= 5 "+
			"([]byte, []any 1-D, []any nested, []string, map[string]any)", sawUncomparable)
	}
}

// TestValuesEqualForKey_NilVersusEmptyByteSlice pins the ONE behavioural
// divergence item 154 introduced, in the direction the doc comment claims:
// bytes.Equal called nil and empty EQUAL, structural equality calls them
// different, and "different" routes to the barrier — the safe answer. Named
// here so the wart is a test rather than a sentence.
func TestValuesEqualForKey_NilVersusEmptyByteSlice(t *testing.T) {
	u := ir.Update{Before: ir.Row{"id": []byte(nil)}, After: ir.Row{"id": []byte{}}}
	if !PKChangedUpdate(u, []string{"id"}) {
		t.Error("nil vs empty []byte PK = unchanged; want changed (the conservative barrier direction)")
	}
}

// TestValuesEqualForKey_ImpliesSameLane binds valuesEqualForKey to
// [WriteCanonicalKeyValue], which is the property both callers actually
// depend on and which neither function can hold alone.
//
// A false result SKIPS the barrier, and the barrier exists because a key
// migration's old and new keys could hash to DIFFERENT lanes. So "equal"
// must imply "the router hashes them together": otherwise the old-key delete
// and the new-key insert commit on two lanes with no ordering between them.
// The converse is NOT required and deliberately not asserted — unequal
// values that happen to encode alike merely take a barrier they did not need.
//
// SCOPE, stated rather than implied: this reaches the value families in
// [keyFamilies] and the single-column key. It says nothing about a value
// kind no reader produces today; adding one to the corpus is what extends it.
func TestValuesEqualForKey_ImpliesSameLane(t *testing.T) {
	families := keyFamilies()
	corpus := make([]any, 0, len(families)*3)
	for _, f := range families {
		corpus = append(corpus, f.a, f.same, f.diff)
	}
	r := NewRouter(8)
	for _, a := range corpus {
		for _, b := range corpus {
			if !valuesEqualForKey(a, b) {
				continue
			}
			var ba, bb bytes.Buffer
			WriteCanonicalKeyValue(&ba, a)
			WriteCanonicalKeyValue(&bb, b)
			if !bytes.Equal(ba.Bytes(), bb.Bytes()) {
				t.Errorf("valuesEqualForKey(%#v, %#v) = true but the canonical key bytes differ (%q vs %q): "+
					"a key migration between them would skip the barrier AND hash to different lanes", a, b, ba.Bytes(), bb.Bytes())
			}
			if la, lb := r.LaneFor("s.t", []any{a}), r.LaneFor("s.t", []any{b}); la != lb {
				t.Errorf("valuesEqualForKey(%#v, %#v) = true but lanes differ (%d vs %d)", a, b, la, lb)
			}
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
