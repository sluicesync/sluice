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
func TestColumnCodecRoundTripsEveryWireField(t *testing.T) {
	src := &Column{
		Name:                     "updated_at",
		Type:                     Timestamp{Precision: 3},
		Nullable:                 true,
		Default:                  DefaultExpression{Expr: "CURRENT_TIMESTAMP(3)"},
		Comment:                  "a comment",
		GeneratedExpr:            "",
		GeneratedStored:          false,
		GeneratedExprDialect:     "",
		OnUpdateCurrentTimestamp: true,
	}

	// Guard against this test silently stopping short of the wire surface:
	// every columnWire field must be represented above. Name-matched, so a
	// newly added wire field fails here until it is exercised.
	wireT := reflect.TypeOf(columnWire{})
	srcV := reflect.ValueOf(*src)
	for i := range wireT.NumField() {
		name := wireT.Field(i).Name
		f := srcV.FieldByName(name)
		if !f.IsValid() {
			t.Fatalf("columnWire field %q has no counterpart on ir.Column", name)
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

	if got.OnUpdateCurrentTimestamp != src.OnUpdateCurrentTimestamp {
		t.Errorf("OnUpdateCurrentTimestamp did not survive the round trip: got %v, want %v.\n"+
			"JSON was: %s\n"+
			"A field present in columnWire but wired into only one of MarshalJSON/UnmarshalJSON fails "+
			"exactly this way, and a schema restored from a backup would lose it silently.",
			got.OnUpdateCurrentTimestamp, src.OnUpdateCurrentTimestamp, b)
	}
	if got.Name != src.Name || got.Comment != src.Comment || got.Nullable != src.Nullable {
		t.Errorf("a baseline field did not round-trip; got %+v", got)
	}
	if got.Default == nil {
		t.Error("Default did not round-trip")
	}
}
