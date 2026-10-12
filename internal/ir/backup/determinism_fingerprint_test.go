// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The Char/Varchar/Text.Determinism fingerprint-exclusion gate
// (KEY-COLLATION review F-C, guarding against a repeat of item 104).
//
// Determinism now RIDES THE WIRE so a decoded schema — chain restore's
// synthetic SchemaSnapshots, a history-primed applier — can arm the
// key-collation guard for a Postgres `text COLLATE "C"` key. The type
// envelope is reached by ComputeSchemaHash through ir.Column.MarshalJSON, so
// without an exclusion the key would land in the fingerprint of every chain
// carrying an explicitly-collated Postgres column, and a new epoch would
// make chains an operator already holds unrestorable.
//
// Written as an EQUALITY with and without the field plus a byte-level
// absence check against the key an older binary never wrote — not as a
// golden built from post-change values.

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func schemaWithDeterminism(d ir.CollationDeterminism) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "app", Name: "codes",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Default: ir.DefaultNone{}},
			{Name: "c", Type: ir.Char{Length: 4, Collation: "C", Determinism: d}, Default: ir.DefaultNone{}},
			{Name: "v", Type: ir.Varchar{Length: 20, Collation: "C", Determinism: d}, Default: ir.DefaultNone{}},
			{Name: "t", Type: ir.Text{Size: ir.TextLong, Collation: "C", Determinism: d}, Default: ir.DefaultNone{}},
			{Name: "ta", Type: ir.Array{Element: ir.Text{Size: ir.TextLong, Collation: "C", Determinism: d}}, Default: ir.DefaultNone{}},
			{Name: "td", Type: ir.Domain{Name: "code", BaseType: ir.Text{Size: ir.TextLong, Collation: "C", Determinism: d}}, Default: ir.DefaultNone{}},
		},
	}}}
}

func TestSchemaHash_ExcludesCollationDeterminism(t *testing.T) {
	without, err := ComputeSchemaHash(schemaWithDeterminism(ir.CollationDeterminismUnknown))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []ir.CollationDeterminism{ir.CollationDeterministic, ir.CollationNonDeterministic} {
		got, err := ComputeSchemaHash(schemaWithDeterminism(d))
		if err != nil {
			t.Fatal(err)
		}
		if got != without {
			t.Errorf("Determinism=%d moved the schema fingerprint (%s vs %s): a new chain epoch for every "+
				"Postgres schema with an explicitly collated column. Exclude it in fingerprintType.", d, got, without)
		}
		b, err := json.Marshal(canonicalSchemaForHash(schemaWithDeterminism(d)))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "collation_determinism") {
			t.Errorf("the hashed bytes carry a collation_determinism key an older binary never wrote:\n%s", b)
		}
	}
	// Anti-vacuity: the hash still sees the collation itself.
	changed := schemaWithDeterminism(ir.CollationDeterministic)
	changed.Tables[0].Columns[3].Type = ir.Text{Size: ir.TextLong, Collation: "POSIX", Determinism: ir.CollationDeterministic}
	if got, _ := ComputeSchemaHash(changed); got == without {
		t.Error("a collation change did not move the fingerprint: the gate above is vacuous")
	}
}

func TestCollationDeterminism_RidesTheWireAndHashingDoesNotStripIt(t *testing.T) {
	s := schemaWithDeterminism(ir.CollationDeterministic)
	if _, err := ComputeSchemaHash(s); err != nil {
		t.Fatal(err)
	}
	b, err := ir.MarshalTable(s.Tables[0])
	if err != nil {
		t.Fatal(err)
	}
	back, err := ir.UnmarshalTable(b)
	if err != nil {
		t.Fatal(err)
	}
	for i, col := range back.Columns[1:] {
		typ := col.Type
		switch v := typ.(type) {
		case ir.Array:
			typ = v.Element
		case ir.Domain:
			typ = v.BaseType
		}
		var got ir.CollationDeterminism
		switch v := typ.(type) {
		case ir.Char:
			got = v.Determinism
		case ir.Varchar:
			got = v.Determinism
		case ir.Text:
			got = v.Determinism
		}
		if got != ir.CollationDeterministic {
			t.Errorf("column %d (%s) decoded with determinism %d, want deterministic (wire: %s)", i+1, col.Name, got, b)
		}
	}
}
