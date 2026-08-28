// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every array element family the PG READER can produce must be WRITABLE
// — roadmap item 141's gate, and the more valuable half of it.
//
// Item 141 itself is small: `bytea[]` could not reach a Postgres target
// at all, because convertArray had no ir.Blob arm. What made it survive
// is the general shape: **the read half worked perfectly.** Item 135's
// matrix pins `bytea[]` DECODING across the scan door, the chunked-scan
// door and the pgoutput door — 1-D / 2-D / NULL-element / empty / NULL,
// ground-truthed against array_dims + per-element encode() — and none of
// that says anything about whether the value can be WRITTEN. Nothing in
// the repo compared the two rosters, so a family could be fully
// decodable and completely unwritable and every test stayed green.
//
// This is that comparison, derived from the code on both sides.
//
// # What it reaches, stated so the name cannot be read as broader
//
//   - The ROSTER is derived from BOTH reader doors: the pgoutput CDC
//     door's array-OID→element-OID registry (pgArrayElementOID, resolved
//     through oidToType) AND the schema reader's text-keyed
//     builtinArrayElement (resolved through translateType). Deriving from
//     one would have been a gate narrower than its name, and provably so:
//     the first draft used the CDC door alone and MISSED timetz entirely,
//     because that door then resolved `timetz` to ir.Time with
//     WithTimeZone unset while the schema-read door set the flag (the
//     divergence the TIMETZ-PROJECTION fix later closed — both doors now
//     set it; the scalar-parity gate's zone-flag leg holds them there).
//     TestOIDToType_ArrayParity holds the two doors to the same FAMILY
//     set; it says nothing about the resolved ir.Type, which is what the
//     writer dispatches on.
//   - The CHECK is a DISPATCH probe: convertArray with an EMPTY array,
//     which reaches the type switch and nothing else. It proves the arm
//     EXISTS. It proves nothing about leaf fidelity, dimensions, or NULL
//     elements — that is the family × shape matrix's job
//     (TestMigrate_PGToPG_ByteaArrays, TestMigrate_PGToPG_JSONArrays,
//     TestMigrate_PGToPG_ArrayElementModifiers,
//     TestCDCApply_ArrayElementFamilies). Those two halves are
//     complementary and neither substitutes for the other.
//   - It reaches the POSTGRES writer only. The MySQL target's array
//     handling (convertArrayLikeToJSON) is a different mechanism.
package postgres

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// pgUnwritableArrayElement lists the element families the PG reader can
// produce and the PG writer deliberately refuses, each with the reason
// and the operator's remedy. An entry is a claim someone can check; a
// family that is neither writable nor listed here fails the test.
//
// This map is the finding's real output. Before item 141 it would have
// held THREE entries and only one of them on purpose; item 141 fixed
// bytea and item 145 fixed json/jsonb, so the one that remains is the
// one that was ever deliberate.
var pgUnwritableArrayElement = map[string]string{
	"ir.Time{WithTimeZone:true}": "timetz[] — DELIBERATE, and the refusal is loud (convertArray's own arm). " +
		"There is no faithful binary leaf: the per-conn scalar timetz codec is not registered for the " +
		"_timetz array OID, and pgx's built-in Time codec drops the zone, so the only alternatives to " +
		"refusing are flattening or corrupting. Remedy in the refusal text: migrate the column as a " +
		"scalar timetz, or exclude the table. Door history: this roster surfaced a resolved-type divergence " +
		"(the pgoutput door once resolved timetz with the flag UNSET, so a CDC timetz[] took the plain " +
		"time[] arm and refused further down at timeOfDayMicros — loud, but with the wrong message); the " +
		"TIMETZ-PROJECTION fix aligned the doors, so BOTH now produce this type and a CDC timetz[] hits " +
		"this arm's own refusal.",
}

// TestEveryDecodableArrayElementIsWritable is the divergence map.
func TestEveryDecodableArrayElementIsWritable(t *testing.T) {
	roster := decodableArrayElementRoster(t)

	// Anti-vacuity, on the inputs AND the output: the roster is derived, so
	// a derivation that broke would compare an empty set against an empty
	// set and pass. Both registries carry ~24 entries; they collapse to
	// FEWER distinct ir.Type families (int2/int4/int8 are all ir.Integer),
	// which is why the two floors differ.
	if len(pgArrayElementOID) < 20 || len(builtinArrayElement) < 20 {
		t.Fatalf("array element registries shrank (pgoutput %d, schema-read %d); the roster's inputs are "+
			"no longer the real ones", len(pgArrayElementOID), len(builtinArrayElement))
	}
	if len(roster) < 15 {
		t.Fatalf("derived only %d distinct element families from the two reader doors; the derivation is "+
			"broken and this roster would pass on a shrunken set", len(roster))
	}

	var unwritable []string
	for _, r := range roster {
		// A DISPATCH probe: an empty array reaches convertArray's type
		// switch and stops there, so a nil error means "this family has an
		// arm", not "this family round-trips".
		if _, err := convertArray([]any{}, r.elem); err != nil {
			unwritable = append(unwritable, r.name)
			if _, declared := pgUnwritableArrayElement[r.name]; !declared {
				t.Errorf("array element family %s (%s door) DECODES but cannot be WRITTEN: convertArray "+
					"refuses it with %q.\n\n"+
					"A family the reader accepts and the writer cannot emit means a table carrying that "+
					"column cannot be copied at all — the item-141 shape. Add the convertArray arm (with a "+
					"leaf type pgx plans for that element OID — Bug 74) plus a real-target dimension pin, "+
					"or record it in pgUnwritableArrayElement with the reason and the operator's remedy.",
					r.name, r.door, err)
			}
		}
	}

	// The other direction: an entry claiming a family is unwritable when
	// it now works overstates the debt and hides a fixed gap.
	for name := range pgUnwritableArrayElement {
		found := false
		for _, u := range unwritable {
			if u == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("pgUnwritableArrayElement lists %q, but convertArray now accepts it — remove the entry "+
				"(and, if it was an open gap, close the roadmap item that tracked it)", name)
		}
	}
	for name, why := range pgUnwritableArrayElement {
		if strings.TrimSpace(why) == "" {
			t.Errorf("pgUnwritableArrayElement records %q with no reason; an unexplained refusal is "+
				"indistinguishable from an oversight", name)
		}
	}

	// The bytea half of item 141, named explicitly so a regression that
	// removed the arm reports as itself rather than as a roster diff.
	for _, elem := range []ir.Type{ir.Binary{Length: 4}, ir.Varbinary{Length: 8}, ir.Blob{}} {
		if _, err := convertArray([]any{}, elem); err != nil {
			t.Errorf("convertArray refuses %T: %v — item 141's whole finding was that bytea[] could not "+
				"reach a PG target", elem, err)
		}
	}
	// And item 145, the family THIS gate found. Both spellings, because the
	// writer arm is keyed on ir.JSON while the two reader doors resolve
	// `json` and `jsonb` to the Binary=false / Binary=true variants of it.
	for _, elem := range []ir.Type{ir.JSON{}, ir.JSON{Binary: true}} {
		if _, err := convertArray([]any{}, elem); err != nil {
			t.Errorf("convertArray refuses %#v: %v — item 145's whole finding was that json[]/jsonb[] could "+
				"not reach a PG target", elem, err)
		}
	}
}

// arrayElementFamily is one row of the derived roster. door records which
// reader produced it, so a failure names the lane an operator would hit.
type arrayElementFamily struct {
	name string
	door string
	elem ir.Type
}

// decodableArrayElementRoster derives every element family EITHER reader
// door can hand the writer:
//
//   - pgoutput CDC: pgArrayElementOID → oidToType (the CDC reader's own
//     resolver).
//   - schema read: builtinArrayElement → translateType (the schema
//     reader's own resolver, fed the same columnMeta shape
//     scanColumn builds for an array element).
//
// Deriving both (rather than listing either) is what makes a newly
// supported family automatically subject to the writability check — and
// what keeps a per-door divergence in the resolved ir.Type visible.
func decodableArrayElementRoster(t *testing.T) []arrayElementFamily {
	t.Helper()
	seen := map[string]bool{}
	var out []arrayElementFamily
	add := func(door string, elem ir.Type) {
		name := arrayElementFamilyName(elem)
		if seen[name] {
			return
		}
		seen[name] = true
		out = append(out, arrayElementFamily{name: name, door: door, elem: elem})
	}

	for arrayOID, elemOID := range pgArrayElementOID {
		elem, err := oidToType(elemOID, -1)
		if err != nil {
			t.Fatalf("pgoutput door: array OID %d element OID %d does not resolve: %v", arrayOID, elemOID, err)
		}
		add("pgoutput CDC", elem)
	}
	for udt, dataType := range builtinArrayElement {
		elem, err := translateType(columnMeta{
			DataType:  dataType,
			UDTName:   strings.TrimPrefix(udt, "_"),
			AttTypmod: -1,
		})
		if err != nil {
			t.Fatalf("schema-read door: array udt %q (data_type %q) does not resolve: %v", udt, dataType, err)
		}
		add("schema read", elem)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// arrayElementFamilyName renders an ir.Type as the roster key. The two
// zone-carrying families get an explicit spelling because the writer
// dispatches on the flag, not on the Go type — a key of "ir.Time" alone
// would let a timetz exemption silently cover plain time[].
func arrayElementFamilyName(t ir.Type) string {
	switch v := t.(type) {
	case ir.Time:
		if v.WithTimeZone {
			return "ir.Time{WithTimeZone:true}"
		}
		return "ir.Time"
	case ir.Timestamp:
		if v.WithTimeZone {
			return "ir.Timestamp{WithTimeZone:true}"
		}
		return "ir.Timestamp"
	default:
		return fmt.Sprintf("%T", t)
	}
}
