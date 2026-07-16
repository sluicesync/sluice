// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func shapeTable(name string, pk *ir.Index, cols ...*ir.Column) *ir.Table {
	return &ir.Table{Name: name, Columns: cols, PrimaryKey: pk}
}

func intPK(cols ...string) *ir.Index {
	idx := &ir.Index{Name: "PRIMARY"}
	for _, c := range cols {
		idx.Columns = append(idx.Columns, ir.IndexColumn{Column: c})
	}
	return idx
}

// TestTableColumnShape_EqualAcrossFamilies pins the equal verdict per
// type FAMILY, not one representative (the Bug 74 lesson): the gate
// dispatches on typeString over every IR type family, and a green
// compare on Integer proves nothing about Decimal or temporal shapes.
// Column order is deliberately shuffled on the actual side — the
// compare is order-insensitive by contract.
func TestTableColumnShape_EqualAcrossFamilies(t *testing.T) {
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
		{Name: "flag", Type: ir.Boolean{}, Nullable: true},
		{Name: "amount", Type: ir.Decimal{Precision: 10, Scale: 2}},
		{Name: "ratio", Type: ir.Float{Precision: ir.FloatDouble}, Nullable: true},
		{Name: "code", Type: ir.Char{Length: 36}},
		{Name: "name", Type: ir.Varchar{Length: 255}, Nullable: true},
		{Name: "body", Type: ir.Text{Size: ir.TextLong}, Nullable: true},
		{Name: "blob", Type: ir.Varbinary{Length: 128}, Nullable: true},
		{Name: "created", Type: ir.Timestamp{}, Nullable: true},
		{Name: "day", Type: ir.Date{}, Nullable: true},
		{Name: "doc", Type: ir.JSON{Binary: true}, Nullable: true},
		{Name: "state", Type: ir.Enum{Values: []string{"a", "b"}}, Nullable: true},
	}
	expected := shapeTable("t", intPK("id"), cols...)

	// Actual side: same columns, reversed order, independently built
	// values (no shared pointers).
	actCols := make([]*ir.Column, 0, len(cols))
	for i := len(cols) - 1; i >= 0; i-- {
		c := *cols[i]
		actCols = append(actCols, &c)
	}
	actual := shapeTable("t", intPK("id"), actCols...)

	if got := TableColumnShape(expected, actual); len(got) != 0 {
		t.Errorf("TableColumnShape = %+v; want empty (equal shapes, order-insensitive)", got)
	}
}

// TestTableColumnShape_AutoIncrementExcluded pins the Bug-81
// sibling-shard regression: an existing target table whose integer
// column lacks (or carries) AutoIncrement while the intended table
// differs ONLY in that flag is shape-EQUAL — PG cannot round-trip the
// flag (a bigserial/identity id reads back plain Int64), and the flag
// never affects whether the copy can land rows. Both directions and
// both PK/non-PK positions pinned, plus the guard that a REAL type
// difference on the same column still refuses.
func TestTableColumnShape_AutoIncrementExcluded(t *testing.T) {
	expected := shapeTable(
		"t", intPK("id"),
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
		&ir.Column{Name: "n", Type: ir.Integer{Width: 32, AutoIncrement: true}, Nullable: true},
	)
	actual := shapeTable(
		"t", intPK("id"),
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
		&ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true},
	)
	if got := TableColumnShape(expected, actual); len(got) != 0 {
		t.Errorf("AutoIncrement-only diff: got %+v; want empty (excluded from compare)", got)
	}
	// Reverse direction: target HAS the flag, intent doesn't.
	if got := TableColumnShape(actual, expected); len(got) != 0 {
		t.Errorf("AutoIncrement-only diff (reverse): got %+v; want empty", got)
	}
	// Guard: a genuine width difference on the same column still refuses.
	narrower := shapeTable(
		"t", intPK("id"),
		&ir.Column{Name: "id", Type: ir.Integer{Width: 32}},
		&ir.Column{Name: "n", Type: ir.Integer{Width: 32}, Nullable: true},
	)
	if got := TableColumnShape(expected, narrower); len(got) != 1 || got[0].Column != "id" {
		t.Errorf("width diff under excluded flag: got %+v; want exactly the id mismatch", got)
	}
}

// TestTableColumnShape_MismatchAxes pins every mismatch axis the gate
// refuses on: a column missing on the target, an extra column on the
// target, a type difference, and a nullability difference.
func TestTableColumnShape_MismatchAxes(t *testing.T) {
	base := func() *ir.Table {
		return shapeTable(
			"t", intPK("id"),
			&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
			&ir.Column{Name: "name", Type: ir.Varchar{Length: 255}, Nullable: true},
		)
	}
	cases := []struct {
		name    string
		mutate  func(*ir.Table)
		col     string
		wantIn  string // substring of the rendered mismatch sides
		wantLen int
	}{
		{
			name:    "column missing on target",
			mutate:  func(a *ir.Table) { a.Columns = a.Columns[:1] },
			col:     "name",
			wantIn:  "(absent)",
			wantLen: 1,
		},
		{
			name: "extra column on target",
			mutate: func(a *ir.Table) {
				a.Columns = append(a.Columns, &ir.Column{Name: "stray", Type: ir.Integer{Width: 32}})
			},
			col:     "stray",
			wantIn:  "(absent)",
			wantLen: 1,
		},
		{
			name:    "type differs",
			mutate:  func(a *ir.Table) { a.Columns[1].Type = ir.Varchar{Length: 64} },
			col:     "name",
			wantIn:  "Varchar(64)",
			wantLen: 1,
		},
		{
			name:    "nullability differs",
			mutate:  func(a *ir.Table) { a.Columns[1].Nullable = false },
			col:     "name",
			wantIn:  "NOT NULL",
			wantLen: 1,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual := base()
			c.mutate(actual)
			got := TableColumnShape(base(), actual)
			if len(got) != c.wantLen {
				t.Fatalf("mismatches = %+v; want %d", got, c.wantLen)
			}
			if got[0].Column != c.col {
				t.Errorf("mismatch column = %q; want %q", got[0].Column, c.col)
			}
			if !strings.Contains(got[0].Expected+got[0].Actual, c.wantIn) {
				t.Errorf("rendered mismatch %+v missing %q", got[0], c.wantIn)
			}
		})
	}
}

// TestTableColumnShape_PKNullabilityExcluded pins the named carve-out:
// engines force PK columns NOT NULL regardless of the declared flag
// and readers report the enforced state, so the redundant flag is
// excluded from the compare for PK-member columns — but their TYPE is
// still compared.
func TestTableColumnShape_PKNullabilityExcluded(t *testing.T) {
	expected := shapeTable(
		"t", intPK("id"),
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}, Nullable: true}, // declared nullable
	)
	actual := shapeTable(
		"t", intPK("id"),
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false}, // engine-enforced NOT NULL
	)
	if got := TableColumnShape(expected, actual); len(got) != 0 {
		t.Errorf("PK nullability compared: %+v; want excluded", got)
	}

	// The type compare still applies to PK columns.
	actual.Columns[0].Type = ir.Varchar{Length: 36}
	got := TableColumnShape(expected, actual)
	if len(got) != 1 || got[0].Column != "id" {
		t.Errorf("PK type mismatch not surfaced: %+v", got)
	}
}

// TestTableColumnShape_SortedDeterministic pins the deterministic
// ordering the refusal message relies on.
func TestTableColumnShape_SortedDeterministic(t *testing.T) {
	expected := shapeTable(
		"t", nil,
		&ir.Column{Name: "zeta", Type: ir.Integer{Width: 32}},
		&ir.Column{Name: "alpha", Type: ir.Integer{Width: 32}},
	)
	actual := shapeTable(
		"t", nil,
		&ir.Column{Name: "zeta", Type: ir.Varchar{Length: 10}},
		&ir.Column{Name: "alpha", Type: ir.Varchar{Length: 10}},
	)
	got := TableColumnShape(expected, actual)
	if len(got) != 2 || got[0].Column != "alpha" || got[1].Column != "zeta" {
		t.Errorf("mismatches not sorted by column: %+v", got)
	}
}

// TestTableColumnShape_PrimaryKeyPresence pins the M1.7 axis in BOTH
// directions: expected-has-PK vs actual-has-none is a mismatch (named
// via the PrimaryKeyMismatchColumn sentinel with the expected column
// list); actual-has-PK vs expected-has-none is NOT a mismatch — it is
// the tolerated direction surfaced separately by UnexpectedPrimaryKey.
func TestTableColumnShape_PrimaryKeyPresence(t *testing.T) {
	cols := func() []*ir.Column {
		return []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "ts", Type: ir.Timestamp{}},
			{Name: "name", Type: ir.Varchar{Length: 255}, Nullable: true},
		}
	}
	withPK := shapeTable("t", intPK("id", "ts"), cols()...)
	// The PK-less twin must declare its former PK members NOT NULL
	// explicitly: engines force PK members NOT NULL, and the compare
	// normalizes PK-member nullability only via EXPECTED's PK set.
	noPKCols := cols()
	noPK := shapeTable("t", nil, noPKCols...)

	t.Run("expected PK, actual none → named mismatch", func(t *testing.T) {
		got := TableColumnShape(withPK, noPK)
		if len(got) != 1 || got[0].Column != PrimaryKeyMismatchColumn {
			t.Fatalf("got %+v; want exactly the primary-key presence mismatch", got)
		}
		if got[0].Expected != "(id, ts)" || got[0].Actual != "none" {
			t.Errorf("mismatch sides = %q vs %q; want \"(id, ts)\" vs \"none\"", got[0].Expected, got[0].Actual)
		}
	})

	t.Run("actual has extra PK → tolerated, no mismatch", func(t *testing.T) {
		if got := TableColumnShape(noPK, withPK); len(got) != 0 {
			t.Fatalf("extra actual-side PK produced mismatches %+v; want none (tolerated with WARN)", got)
		}
		pk, extra := UnexpectedPrimaryKey(noPK, withPK)
		if !extra || pk != "(id, ts)" {
			t.Errorf("UnexpectedPrimaryKey = %q,%v; want \"(id, ts)\",true", pk, extra)
		}
	})

	t.Run("both have PKs → no presence mismatch and no WARN", func(t *testing.T) {
		if got := TableColumnShape(withPK, withPK); len(got) != 0 {
			t.Fatalf("got %+v; want none", got)
		}
		if _, extra := UnexpectedPrimaryKey(withPK, withPK); extra {
			t.Error("UnexpectedPrimaryKey fired with a PK on both sides")
		}
	})
}

// TestTableColumnShape_CharsetCompare pins the M1.6 opt-in charset/
// collation compare across EVERY string-leaf family that carries one —
// Char, Varchar, Text (the Bug 74 lesson: the compare dispatches per
// type, so a green Varchar pin proves nothing about Text) — times the
// three claim shapes: both sides resolved and conflicting (refuse-
// worthy mismatch), one side silent (no claim → tolerated), both equal
// (silent). Plus: the zero-value options keep charset OUT (cross-engine
// callers unchanged), and collation participates independently.
func TestTableColumnShape_CharsetCompare(t *testing.T) {
	opts := ShapeCompareOptions{CompareCharset: true}
	families := []struct {
		name string
		mk   func(charset, collation string) ir.Type
	}{
		{"char", func(cs, coll string) ir.Type { return ir.Char{Length: 8, Charset: cs, Collation: coll} }},
		{"varchar", func(cs, coll string) ir.Type { return ir.Varchar{Length: 255, Charset: cs, Collation: coll} }},
		{"text", func(cs, coll string) ir.Type { return ir.Text{Size: ir.TextLong, Charset: cs, Collation: coll} }},
	}
	table := func(typ ir.Type) *ir.Table {
		return shapeTable("t", nil, &ir.Column{Name: "c", Type: typ, Nullable: true})
	}

	for _, fam := range families {
		t.Run(fam.name, func(t *testing.T) {
			utf8 := table(fam.mk("utf8mb4", "utf8mb4_0900_ai_ci"))
			latin1 := table(fam.mk("latin1", "latin1_swedish_ci"))
			silent := table(fam.mk("", ""))

			t.Run("both resolved, conflicting → mismatch naming the charset", func(t *testing.T) {
				got := TableColumnShapeWithOptions(utf8, latin1, opts)
				if len(got) != 1 {
					t.Fatalf("got %+v; want exactly one charset mismatch", got)
				}
				if !strings.Contains(got[0].Expected, "CHARACTER SET utf8mb4") ||
					!strings.Contains(got[0].Actual, "CHARACTER SET latin1") {
					t.Errorf("mismatch must name both charsets: want=%q actual=%q", got[0].Expected, got[0].Actual)
				}
			})

			t.Run("one side silent → no claim, tolerated", func(t *testing.T) {
				if got := TableColumnShapeWithOptions(silent, latin1, opts); len(got) != 0 {
					t.Errorf("silent-expected vs resolved-actual mismatched: %+v", got)
				}
				if got := TableColumnShapeWithOptions(utf8, silent, opts); len(got) != 0 {
					t.Errorf("resolved-expected vs silent-actual mismatched: %+v", got)
				}
			})

			t.Run("both equal → silent", func(t *testing.T) {
				other := table(fam.mk("utf8mb4", "utf8mb4_0900_ai_ci"))
				if got := TableColumnShapeWithOptions(utf8, other, opts); len(got) != 0 {
					t.Errorf("equal charsets mismatched: %+v", got)
				}
			})

			t.Run("collation-only conflict under one charset → mismatch", func(t *testing.T) {
				a := table(fam.mk("utf8mb4", "utf8mb4_0900_ai_ci"))
				b := table(fam.mk("utf8mb4", "utf8mb4_general_ci"))
				got := TableColumnShapeWithOptions(a, b, opts)
				if len(got) != 1 || !strings.Contains(got[0].Actual, "utf8mb4_general_ci") {
					t.Errorf("collation conflict: got %+v; want one mismatch naming the collation", got)
				}
			})

			t.Run("zero-value options exclude charset entirely", func(t *testing.T) {
				if got := TableColumnShape(utf8, latin1); len(got) != 0 {
					t.Errorf("default compare included charset: %+v", got)
				}
			})
		})
	}
}
