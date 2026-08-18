// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// TestPin_DomainOverUUID_RedactPreflight pins the Bug 233 fix in
// preflightRedactTypes: a Postgres source can carry a `CREATE DOMAIN d AS uuid`
// column, whose STORAGE is uuid — the target's uuid column still refuses the
// non-hex 'X' bytes mask:uuid produces, so the preflight WARN must fire for it
// too, before the mid-bulk-copy refusal rather than after.
//
// Mutation check: revert the ir.UnwrapDomain in redact_preflight.go back to
// `col.Type.(ir.UUID)` and the domain case below stops refusing (the check
// `continue`s), so preflight returns nil and this pin reddens.
func TestPin_DomainOverUUID_RedactPreflight(t *testing.T) {
	schemaWith := func(col *ir.Column) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{Name: "users", Columns: []*ir.Column{col}}}}
	}

	// Baseline: a bare uuid column refuses (mutation-test anchor — true with or
	// without the fix).
	bare := redact.New()
	bare.Set("", "users", "id", redact.MaskUUID{})
	if err := preflightRedactTypes(bare, schemaWith(&ir.Column{Name: "id", Type: ir.UUID{}})); err == nil {
		t.Fatal("bare uuid column: expected mask:uuid refusal; got nil — scaffold wrong")
	}

	// The fix: a domain-over-uuid column refuses identically.
	r := redact.New()
	r.Set("", "users", "id", redact.MaskUUID{})
	err := preflightRedactTypes(r, schemaWith(&ir.Column{
		Name: "id",
		Type: ir.Domain{Name: "d_over_uuid", BaseType: ir.UUID{}},
	}))
	if err == nil {
		t.Fatal("DOMAIN-over-uuid column: expected mask:uuid refusal; got nil — the preflight WARN did not " +
			"fire, so the operator hits the non-hex refusal mid-bulk-copy instead of at startup (Bug 233)")
	}
	if !errors.Is(err, errRedactTypeMismatch) {
		t.Errorf("error should wrap errRedactTypeMismatch; got %v", err)
	}
	if !strings.Contains(err.Error(), "users.id") {
		t.Errorf("refusal should name the column users.id; got %v", err)
	}
}

// TestPin_DomainOverInt_RandomizeOverflowPreflight pins the sibling Bug 233 fix
// in preflightRedactTypes' randomize:int range check (a second col.Type site the
// gate's key-dedup had hidden behind the mask:uuid one): a randomize:int rule
// whose range overflows a DOMAIN-over-integer column's base width must still
// refuse at preflight, or the Bug-105 silent-clamp PII window re-opens for
// domain columns.
//
// Mutation check: revert the ir.UnwrapDomain in the randomize:int arm back to
// `col.Type.(ir.Integer)` and the domain case stops refusing (the check
// `continue`s), so preflight returns nil and this pin reddens.
func TestPin_DomainOverInt_RandomizeOverflowPreflight(t *testing.T) {
	// Max=INT32_MAX+1 overflows an int32 column; the column is a DOMAIN over
	// int32 and is its own PK (PG permits a domain-typed PK).
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "events",
		Columns: []*ir.Column{{
			Name: "id",
			Type: ir.Domain{Name: "d_over_int32", BaseType: ir.Integer{Width: 32, Unsigned: false}},
		}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	r := redact.New()
	r.Set("", "events", "id", redact.RandomizeInt{Min: 0, Max: 2_147_483_648})
	err := preflightRedactTypes(r, schema)
	if err == nil {
		t.Fatal("DOMAIN-over-int32 column: expected randomize:int overflow refusal; got nil — the base-width " +
			"overflow preflight was skipped, re-opening the Bug-105 silent-clamp PII window for domain columns " +
			"(Bug 233)")
	}
	if !errors.Is(err, errRedactRandomizeRangeOverflow) {
		t.Errorf("error should wrap errRedactRandomizeRangeOverflow; got %v", err)
	}
}
