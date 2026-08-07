// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"encoding/json"
	"strings"
	"testing"
)

// SourceColumnType MUST NOT RIDE THE WIRE — and this is the test that
// says so, because until now nothing did.
//
// # The premise that holds up the restore lane
//
// [Column.SourceColumnType] is override CONTEXT, not a source-engine
// fact: it is set only when translate.ApplyMappings (or --infer-types)
// replaces a column's type, and MySQL's writer reads it to decide
// whether a `\x`+hex string on a binary column is PostgreSQL's bytea
// rendering or six ordinary bytes (columnIsNativelyBinary, audit B-2).
// A nil means "no override fired", and the writer treats a nil as
// natively binary — the pre-B-2 reading.
//
// [columnWire] does not carry the field, so EVERY column rebuilt from a
// backup manifest, a lineage envelope or any other persisted schema
// arrives with it nil and behaves exactly as it did before B-2. That is
// sound, and it was entirely unwritten — a property nobody had asserted,
// on a path (restore) that never sees an override anyway.
//
// # Why the absence needs a test and not just a comment
//
// Bug 47 wants this exact field on the wire for a different reason (the
// `{}` JSON-object-vs-empty-array disambiguation), and adding it there
// is a one-line change to a struct whose other fields all ride happily.
// The moment it does, restore stops taking the nil branch and starts
// deciding bytea provenance from whatever an old manifest recorded — the
// inverted behaviour, silently, on the one lane that has no operator
// watching. Per CLAUDE.md, an invariant is a hypothesis until you can
// name the test that fails when it breaks. This is that test.
//
// If the field is deliberately added to the wire later, this test is the
// place to record what was decided about restore's provenance reading —
// deleting it is not the fix.
func TestColumnWire_DoesNotCarrySourceColumnType(t *testing.T) {
	c := &Column{
		Name:             "body",
		Type:             Blob{Size: BlobLong},
		SourceColumnType: Text{Size: TextLong},
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(b)

	// (a) The envelope carries no key for it, under any plausible
	// spelling json.Marshal would pick for the field.
	for _, key := range []string{"source_column_type", "sourceColumnType", "SourceColumnType"} {
		if strings.Contains(got, key) {
			t.Fatalf("columnWire now emits %q (%s).\n\nRestore rebuilds columns from this envelope. "+
				"With the field present, a restored column stops taking the nil branch in MySQL's "+
				"columnIsNativelyBinary and starts deciding bytea provenance from whatever an OLD "+
				"manifest recorded — the pre-B-2 reading inverted, silently. If the addition is "+
				"deliberate (Bug 47 wants this field), decide what restore should do with it and "+
				"record the answer here.", key, got)
		}
	}

	// (b) The behavioural half: a decoded column takes the nil branch.
	// (a) alone would pass if the field rode the wire under a name none
	// of those spellings matched.
	var back Column
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.SourceColumnType != nil {
		t.Errorf("round-tripped column carries SourceColumnType = %T; every column rebuilt from a "+
			"persisted schema must arrive with it nil", back.SourceColumnType)
	}
	if back.Type == nil || back.Type.String() != (Blob{Size: BlobLong}).String() {
		t.Errorf("round-tripped Type = %v; want the declared type to survive", back.Type)
	}
}
