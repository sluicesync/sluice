// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPin_DomainOverVerbatim_MarkerRecorded pins the Bug 233 fix in
// VerbatimExtensionColumnsIn: a Postgres source can carry a `CREATE DOMAIN d AS
// <extension-type>` column, whose STORAGE is the verbatim base — the restore
// target still needs the extension, so the verbatim-extension marker (and thus
// the restore-time refuseVerbatim* gate) must be recorded for it exactly as for
// a bare verbatim column.
//
// Mutation check: revert the ir.UnwrapDomain in catalog_helpers.go back to
// `col.Type.(ir.VerbatimType)` and the domain sub-case below drops out of the
// result set, reddening this pin.
func TestPin_DomainOverVerbatim_MarkerRecorded(t *testing.T) {
	verbatim := ir.VerbatimType{Definition: "public.geometry"}
	schema := &ir.Schema{
		Tables: []*ir.Table{
			{
				Name:   "t",
				Schema: "public",
				Columns: []*ir.Column{
					{Name: "bare", Type: verbatim},
					{Name: "wrapped", Type: ir.Domain{Name: "d_over_verbatim", BaseType: verbatim}},
					{Name: "plain", Type: ir.Text{}},
				},
			},
		},
	}

	got := VerbatimExtensionColumnsIn(schema)

	// The bare verbatim column is the mutation-test baseline: it stays recorded
	// whether or not the fix is present, so its presence proves the scaffold.
	if !contains(got, "public.t.bare") {
		t.Fatalf("bare verbatim column not recorded (%v) — test scaffold is wrong", got)
	}
	// The domain-over-verbatim column is the fix: only recorded once the
	// detector reads the storage type.
	if !contains(got, "public.t.wrapped") {
		t.Errorf("DOMAIN-over-verbatim column not recorded (%v) — the restore-time refuseVerbatim* gate would "+
			"not fire for it, so a verbatim-extension column silently restores to a target lacking the "+
			"extension (Bug 233)", got)
	}
	// A plain column must never be recorded.
	if contains(got, "public.t.plain") {
		t.Errorf("plain text column wrongly recorded as verbatim (%v)", got)
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
