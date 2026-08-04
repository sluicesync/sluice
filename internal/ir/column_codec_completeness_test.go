// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ir.Column ↔ columnWire codec-completeness gate
// (audit 2026-08-01, §4 gate proposal 3).
//
// [Column] has a hand-written MarshalJSON over an enumerated columnWire
// struct, because the sealed Type and Default interfaces need tagged-union
// envelopes. Hand-written means a field added to Column does NOT reach the
// wire until someone remembers to add it in three places — and nothing failed
// when they didn't.
//
// The cost is not theoretical. The audit predicted this class, and the very
// next field added to Column (OnUpdateCurrentTimestamp, audit S7) walked into
// it: the field was read from the source correctly and emitted to the target
// correctly, and would have been dropped in between by any path that
// round-trips the schema through JSON — which is every backup manifest, so a
// restore would have silently rebuilt the table without it. It also made a
// fingerprint-exclusion test pass for the wrong reason, since a field that
// never reaches the JSON cannot move a hash over that JSON.
//
// This gate is reflective rather than a curated list, because a curated list
// is the same failure mode one level up: it too needs remembering. Every
// exported field on Column must either appear in columnWire or carry a reason
// in wireExempt below.

package ir

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// wireExempt names the Column fields that deliberately do NOT ride the wire,
// each with the reason. A field here is a DECISION; a field merely missing
// from columnWire is a bug.
var wireExempt = map[string]string{
	"SourceColumnType": "the pre-translation source type, used for in-process " +
		"cross-engine decisions; a decoded schema is already translated, so " +
		"carrying it would record a value the reader of the manifest cannot act on",
	"SluiceInjected": "marks a column sluice itself synthesised (shard discriminator); " +
		"re-derived by the injecting pass rather than persisted",
	"StableID": "an in-process identity for diffing within one run; not stable " +
		"across processes, so persisting it would record a meaningless number",
}

// TestColumnCodecCoversEveryField is the gate.
func TestColumnCodecCoversEveryField(t *testing.T) {
	colT := reflect.TypeOf(Column{})
	wireT := reflect.TypeOf(columnWire{})

	onWire := make(map[string]bool, wireT.NumField())
	for i := range wireT.NumField() {
		onWire[wireT.Field(i).Name] = true
	}

	var missing []string
	for i := range colT.NumField() {
		f := colT.Field(i)
		if f.PkgPath != "" {
			continue // unexported
		}
		if onWire[f.Name] {
			continue
		}
		if reason, ok := wireExempt[f.Name]; ok {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("field %q is exempt from the wire codec with an EMPTY reason; "+
					"an exemption without a reason is indistinguishable from an oversight", f.Name)
			}
			continue
		}
		missing = append(missing, f.Name)
	}

	if len(missing) > 0 {
		t.Fatalf("ir.Column field(s) %v do not appear in columnWire and are not in wireExempt.\n\n"+
			"A field that is not on the wire is DROPPED by every path that round-trips a schema through "+
			"JSON — most importantly the backup manifest, so `restore` would rebuild the table without it, "+
			"silently. Either add the field to columnWire (struct field, MarshalJSON, UnmarshalJSON — all "+
			"three) or add it to wireExempt with the reason it is deliberately not persisted.",
			missing)
	}
}

// TestColumnCodecRoundTripsEveryWireField is the other half, and the one that
// catches the likelier mistake: a field added to the columnWire STRUCT but
// wired into only one of MarshalJSON / UnmarshalJSON. The struct-level gate
// above cannot see that — columnWire would list the field either way.
//
// It sets every wire-carried field on Column to a non-zero value, round-trips
// through the real codec, and requires equality. A field marshalled but never
// unmarshalled (or the reverse) comes back zero and fails here.
//
// # This gate previously could not do what that paragraph claims
//
// It set three of its eight wire fields — GeneratedExpr, GeneratedStored,
// GeneratedExprDialect — to the ZERO value, and then asserted only a
// hand-picked four. Both halves were vacuous in the same direction: a field
// that fails this way comes back ZERO, so a fixture that sends zero cannot
// tell a working round trip from a broken one, and a field nobody thought to
// assert is not checked at all. Its own anti-vacuity guard checked that each
// columnWire field EXISTS on Column — never that the fixture gives it a value.
//
// That is this project's most expensive recurring shape (a gate whose coverage
// is narrower than its name), and it is doubly bad here because this gate was
// itself built as a §4 audit remedy for exactly one field being forgotten.
//
// Both halves are now reflective over columnWire: every field must be non-zero
// in the fixture, and every field is compared after the round trip. Adding a
// field to columnWire fails this test until the fixture exercises it.
func TestColumnCodecRoundTripsEveryWireField(t *testing.T) {
	src := &Column{
		Name:                     "updated_at",
		Type:                     Timestamp{Precision: 3},
		Nullable:                 true,
		Default:                  DefaultExpression{Expr: "CURRENT_TIMESTAMP(3)"},
		Comment:                  "a comment",
		GeneratedExpr:            "concat(a, b)",
		GeneratedStored:          true,
		GeneratedExprDialect:     "mysql",
		OnUpdateCurrentTimestamp: true,
	}

	wireT := reflect.TypeOf(columnWire{})
	srcV := reflect.ValueOf(*src)

	// Anti-vacuity floor, and the half that was missing. Every columnWire
	// field must exist on Column AND be NON-ZERO in the fixture. Without the
	// second condition a field can be "covered" by a value that is
	// indistinguishable from the failure it is meant to detect.
	for i := range wireT.NumField() {
		name := wireT.Field(i).Name
		f := srcV.FieldByName(name)
		if !f.IsValid() {
			t.Fatalf("columnWire field %q has no counterpart on ir.Column", name)
		}
		if f.IsZero() {
			t.Fatalf("columnWire field %q is set to its ZERO value in this test's fixture.\n\n"+
				"A field dropped by MarshalJSON or UnmarshalJSON comes back ZERO, so a zero-valued "+
				"fixture cannot distinguish a working round trip from a broken one — the assertion below "+
				"would pass either way. Give %q a non-zero value.", name, name)
		}
	}

	b, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Column
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Compare every wire field, not a hand-picked subset — the subset is how a
	// newly added field goes unchecked while the test still reads as complete.
	gotV := reflect.ValueOf(got)
	for i := range wireT.NumField() {
		name := wireT.Field(i).Name
		want := srcV.FieldByName(name).Interface()
		have := gotV.FieldByName(name).Interface()
		if !reflect.DeepEqual(want, have) {
			t.Errorf("%s did not survive the round trip: got %#v, want %#v.\n"+
				"JSON was: %s\n"+
				"A field present in columnWire but wired into only one of MarshalJSON/UnmarshalJSON fails "+
				"exactly this way, and a schema restored from a backup would lose it silently.",
				name, have, want, b)
		}
	}
}
