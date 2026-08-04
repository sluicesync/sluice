// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ir.Macaddr.Width fingerprint-exclusion gate
// (roadmap item 117 / Bug 225, guarding against a repeat of item 104).
//
// ir.Macaddr gained a Width so a source `macaddr8` column stops being narrowed
// to MACADDR on emit. The width RIDES THE WIRE — it must, or a manifest
// records a macaddr8 column as a bare Macaddr and restore silently rebuilds it
// six bytes wide — and the type envelope is reached by ComputeSchemaHash
// through ir.Column.MarshalJSON, so without an exclusion it lands in the
// fingerprint of every chain carrying a MAC column.
//
// `omitempty` does NOT save this one, for exactly the reason it did not save
// ir.Index.ConstraintNamed: it drops only the ZERO value, and the Postgres
// reader sets the width to SIX for an ORDINARY `macaddr` column. So the key
// would be present on every real Postgres chain, the fingerprint would move,
// and a new epoch would be minted that makes chains an operator already holds
// unrestorable — surfaced to them as "your backup is corrupt".
//
// The gate is written as an EQUALITY between a schema WITH the field and the
// same schema WITHOUT it, never as a frozen golden built from post-change
// values. Item 104's own frozen golden could not catch item 104 because its
// fixture carried the new field at its new value — self-referential by
// construction, proving only that one binary agrees with itself. An equality
// between two shapes of the SAME binary cannot be satisfied that way; it can
// only be satisfied by the field genuinely not reaching the hash.

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// schemaWithMacaddrWidth builds the fixture at a given width, in the three
// shapes a MAC column actually takes on a Postgres source: the scalar, the
// array (`macaddr8[]` is real, and an exclusion that reached only the scalar
// would still have moved the fingerprint of every chain carrying one), and a
// DOMAIN over the scalar (the other type that nests another).
func schemaWithMacaddrWidth(w int) *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Schema: "app",
			Name:   "devices",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Default: ir.DefaultNone{}},
				{Name: "mac", Type: ir.Macaddr{Width: w}, Default: ir.DefaultNone{}},
				{Name: "macs", Type: ir.Array{Element: ir.Macaddr{Width: w}}, Default: ir.DefaultNone{}},
				{
					Name:    "mac_dom",
					Type:    ir.Domain{Name: "hwaddr", BaseType: ir.Macaddr{Width: w}},
					Default: ir.DefaultNone{},
				},
			},
		}},
	}
}

// TestSchemaHash_ExcludesMacaddrWidth is the no-new-epoch gate.
func TestSchemaHash_ExcludesMacaddrWidth(t *testing.T) {
	without, err := ComputeSchemaHash(schemaWithMacaddrWidth(0))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(unspecified): %v", err)
	}
	// BOTH widths, not just the interesting one. Six is the value the reader
	// sets for the ordinary `macaddr` that every existing chain already
	// carries, so it is the width whose inclusion would break the field; eight
	// is the new shape. A gate that pinned only one of them would certify half
	// the exclusion.
	for _, w := range []int{ir.MacaddrEUI48, ir.MacaddrEUI64} {
		got, err := ComputeSchemaHash(schemaWithMacaddrWidth(w))
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%d): %v", w, err)
		}
		if got != without {
			t.Errorf("ir.Macaddr.Width=%d moved the schema fingerprint:\n"+
				"  with the width:    %s\n"+
				"  without the width: %s\n\n"+
				"That mints a NEW backup-chain epoch for every Postgres schema carrying a macaddr "+
				"column — the reader sets the width to 6 on the ORDINARY one, so `omitempty` does not "+
				"hold it back. Chains written by this build would not restore on any earlier release, "+
				"reported to the operator as `your backup is corrupt`. This is roadmap item 104 "+
				"repeating. Exclude it in fingerprintType (chain.go).", w, got, without)
		}
	}
}

// TestSchemaHash_MacaddrWidthAbsentFromTheHashedBytes is the byte-level half,
// and it is what makes "the fingerprint did not move" a statement about
// EARLIER RELEASES rather than about this binary agreeing with itself. The
// equality above proves width 6 and width 8 hash like width 0; this proves
// width 0's canonical bytes carry no macaddr key at all — which is exactly
// what a pre-item-117 binary, whose envelope had no such field, emitted.
func TestSchemaHash_MacaddrWidthAbsentFromTheHashedBytes(t *testing.T) {
	for _, w := range []int{0, ir.MacaddrEUI48, ir.MacaddrEUI64} {
		b, err := json.Marshal(canonicalSchemaForHash(schemaWithMacaddrWidth(w)))
		if err != nil {
			t.Fatalf("marshal canonical (width %d): %v", w, err)
		}
		if strings.Contains(string(b), "macaddr_width") {
			t.Errorf("the bytes hashed for a width-%d schema carry a macaddr_width key, so they are NOT "+
				"the bytes an older binary produced for the same schema:\n%s", w, b)
		}
	}
}

// TestSchemaHash_MacaddrExclusionDoesNotMutateTheCaller: the exclusion must be
// a COPY. The manifest records the schema verbatim, so a fingerprint helper
// that stripped the width in place would make the restore rebuild `macaddr8`
// as `MACADDR` — reintroducing Bug 225 by way of the hash.
func TestSchemaHash_MacaddrExclusionDoesNotMutateTheCaller(t *testing.T) {
	s := schemaWithMacaddrWidth(ir.MacaddrEUI64)
	if _, err := ComputeSchemaHash(s); err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	cols := s.Tables[0].Columns
	if got := cols[1].Type.(ir.Macaddr).Width; got != ir.MacaddrEUI64 {
		t.Errorf("hashing CLEARED the scalar column's macaddr width (got %d)", got)
	}
	if got := cols[2].Type.(ir.Array).Element.(ir.Macaddr).Width; got != ir.MacaddrEUI64 {
		t.Errorf("hashing CLEARED the array element's macaddr width (got %d)", got)
	}
	if got := cols[3].Type.(ir.Domain).BaseType.(ir.Macaddr).Width; got != ir.MacaddrEUI64 {
		t.Errorf("hashing CLEARED the domain base type's macaddr width (got %d)", got)
	}
}

// TestMacaddrWidth_RidesTheWire is the other half of the trade, and the one
// that makes the exclusion safe rather than merely convenient: the width is
// kept OUT of the hash precisely because it is IN the recorded schema, so a
// restore rebuilds macaddr8 correctly. Delete the codec half and the exclusion
// becomes silent narrowing on the restore path.
func TestMacaddrWidth_RidesTheWire(t *testing.T) {
	s := schemaWithMacaddrWidth(ir.MacaddrEUI64)
	b, err := ir.MarshalTable(s.Tables[0])
	if err != nil {
		t.Fatalf("MarshalTable: %v", err)
	}
	if !strings.Contains(string(b), `"macaddr_width":8`) {
		t.Fatalf("the macaddr width did not reach the manifest wire; a restore would rebuild the "+
			"column as MACADDR and the 8-byte values would not fit (Bug 225). wire was: %s", b)
	}
	back, err := ir.UnmarshalTable(b)
	if err != nil {
		t.Fatalf("UnmarshalTable: %v", err)
	}
	for i, want := range []int{0, ir.MacaddrEUI64, ir.MacaddrEUI64, ir.MacaddrEUI64} {
		if i == 0 {
			continue // the id column
		}
		var got int
		switch v := back.Columns[i].Type.(type) {
		case ir.Macaddr:
			got = v.Width
		case ir.Array:
			got = v.Element.(ir.Macaddr).Width
		case ir.Domain:
			got = v.BaseType.(ir.Macaddr).Width
		}
		if got != want {
			t.Errorf("column %q decoded with macaddr width %d, want %d", back.Columns[i].Name, got, want)
		}
	}
}

// TestSchemaHash_StillSeesMacaddrColumnChanges is the anti-vacuity floor. A
// canonicaliser that zeroed too much — or that dropped MAC columns from the
// hash outright — would satisfy the exclusion trivially. A real change to one
// of these columns must still move the fingerprint, and a `macaddr` ⇄
// `macaddr8` retype must still be visible to the SCHEMA DIFF, which is where
// the exclusion moves that detection to.
func TestSchemaHash_StillSeesMacaddrColumnChanges(t *testing.T) {
	base, err := ComputeSchemaHash(schemaWithMacaddrWidth(ir.MacaddrEUI48))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(base): %v", err)
	}

	renamed := schemaWithMacaddrWidth(ir.MacaddrEUI48)
	renamed.Tables[0].Columns[1].Name = "hw"
	retyped := schemaWithMacaddrWidth(ir.MacaddrEUI48)
	retyped.Tables[0].Columns[1].Type = ir.Text{Size: ir.TextLong}

	for _, tc := range []struct {
		name string
		s    *ir.Schema
	}{
		{"a renamed mac column", renamed},
		{"a mac column retyped to text", retyped},
	} {
		got, err := ComputeSchemaHash(tc.s)
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%s): %v", tc.name, err)
		}
		if got == base {
			t.Errorf("%s did NOT move the fingerprint — the hash is not seeing these columns at all, "+
				"so the exclusion gate above is vacuous", tc.name)
		}
	}

	// And the detection the exclusion GIVES UP at the fingerprint is picked up
	// by String(), which is what both schema-diff paths compare. Asserting it
	// here keeps the trade honest: the width is not simply invisible.
	narrow := ir.Macaddr{Width: ir.MacaddrEUI48}.String()
	wide := ir.Macaddr{Width: ir.MacaddrEUI64}.String()
	if a := narrow; a == wide {
		t.Errorf("ir.Macaddr.String() renders both widths as %q, so the schema diff cannot see a "+
			"macaddr ⇄ macaddr8 retype either — and the fingerprint deliberately does not. Nothing "+
			"would then report it.", a)
	}
	// The unspecified width must keep rendering as the historical spelling, or
	// every legacy manifest reads as a type change against a fresh read.
	if got := (ir.Macaddr{}).String(); got != "Macaddr" {
		t.Errorf("ir.Macaddr{}.String() = %q, want %q — a legacy manifest would read as a retype", got, "Macaddr")
	}
}
