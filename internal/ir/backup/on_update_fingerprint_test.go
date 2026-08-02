// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The ON UPDATE CURRENT_TIMESTAMP fingerprint-exclusion gate
// (audit 2026-08-01 S7, guarding against a repeat of roadmap item 104).
//
// ir.Column gained OnUpdateCurrentTimestamp so the attribute stops being
// silently discarded. The schema fingerprint is a SHA-256 over the schema's
// struct JSON, so a new field that is TRUE on real data moves the hash of
// every chain carrying such a column — and `updated_at TIMESTAMP ... ON UPDATE
// CURRENT_TIMESTAMP` is close to universal in MySQL schemas. Including it
// would therefore have minted a FIFTH fingerprint epoch and broken restore of
// existing chains on the most ordinary shape there is.
//
// That is exactly how item 104 happened, and item 104's own frozen-golden gate
// could not catch it because the fixture carried the new field at its new
// value — self-referential by construction. So this gate is written the other
// way round: it compares a schema WITH the field against the same schema
// WITHOUT it and requires the hashes to be EQUAL. That comparison cannot be
// satisfied by a self-consistent binary; it can only be satisfied by the field
// genuinely not reaching the hash.

package backup

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func schemaWithOnUpdate(on bool) *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{{
			Schema: "app",
			Name:   "orders",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}, Default: ir.DefaultNone{}},
				{
					Name:                     "updated_at",
					Type:                     ir.Timestamp{Precision: 3},
					Default:                  ir.DefaultNone{},
					OnUpdateCurrentTimestamp: on,
				},
			},
		}},
	}
}

// TestSchemaHash_ExcludesOnUpdateCurrentTimestamp is the no-new-epoch gate.
func TestSchemaHash_ExcludesOnUpdateCurrentTimestamp(t *testing.T) {
	withField, err := ComputeSchemaHash(schemaWithOnUpdate(true))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(with): %v", err)
	}
	without, err := ComputeSchemaHash(schemaWithOnUpdate(false))
	if err != nil {
		t.Fatalf("ComputeSchemaHash(without): %v", err)
	}
	if withField != without {
		t.Fatalf("ON UPDATE CURRENT_TIMESTAMP moved the schema fingerprint:\n"+
			"  with the field:    %s\n"+
			"  without the field: %s\n"+
			"That mints a NEW backup-chain epoch for every MySQL schema carrying an `updated_at TIMESTAMP "+
			"... ON UPDATE CURRENT_TIMESTAMP` column — which is most of them — so chains written by this "+
			"build would not restore on any earlier release, reported as `your backup is corrupt`. This is "+
			"roadmap item 104 repeating. Exclude the field in canonicalColumnsForHash.",
			withField, without)
	}
}

// TestSchemaHash_StillSeesRealSchemaChanges is the anti-vacuity floor. A
// canonicaliser that zeroed too much — or a ComputeSchemaHash that had stopped
// hashing columns at all — would satisfy the test above trivially. Two schemas
// that genuinely differ must still hash differently.
func TestSchemaHash_StillSeesRealSchemaChanges(t *testing.T) {
	base := schemaWithOnUpdate(true)

	widened := schemaWithOnUpdate(true)
	widened.Tables[0].Columns[1].Type = ir.Timestamp{Precision: 6}

	renamed := schemaWithOnUpdate(true)
	renamed.Tables[0].Columns[1].Name = "modified_at"

	baseHash, err := ComputeSchemaHash(base)
	if err != nil {
		t.Fatalf("ComputeSchemaHash(base): %v", err)
	}
	for _, tc := range []struct {
		name string
		s    *ir.Schema
	}{
		{"a widened precision", widened},
		{"a renamed column", renamed},
	} {
		got, err := ComputeSchemaHash(tc.s)
		if err != nil {
			t.Fatalf("ComputeSchemaHash(%s): %v", tc.name, err)
		}
		if got == baseHash {
			t.Errorf("%s did NOT move the fingerprint — the hash is not seeing column changes at all, "+
				"so the exclusion gate above is vacuous", tc.name)
		}
	}
}
