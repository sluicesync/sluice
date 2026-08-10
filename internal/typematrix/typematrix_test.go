// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package typematrix

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The matrix's own anti-vacuity floor. Every consumer of [Cases] states its
// coverage in terms of "every IR type family"; this is the only thing that
// makes that statement true, and it is what fails when the ExtensionKind-style
// rot repeats — an IR type added to the universe and to no shape list.
func TestCases_CoverEveryIRTypeFamily(t *testing.T) {
	if missing := MissingFamilies(); len(missing) > 0 {
		t.Errorf("Cases() has no shape for %v.\n\nEvery engine's column-type gate states its coverage "+
			"as \"every ir.AllTypes family\", and this list is what makes that true. Add at least one "+
			"case per missing type — and prefer the shapes whose VERDICT can differ (a zero length, a "+
			"negative scale, a nil element), not just the zero value.", missing)
	}
	if got, want := len(ir.AllTypes()), len(Families()); got != want {
		t.Errorf("Cases() covers %d distinct families but ir.AllTypes() has %d — a case carries a type "+
			"that is not in the IR universe, or MissingFamilies has stopped seeing one", want, got)
	}
	// A truncated Cases() would satisfy the family check with one shape each
	// and lose every variant axis, which is the half that actually catches
	// verdict differences.
	if n := len(Cases()); n < 60 {
		t.Errorf("Cases() returned %d entries; the matrix is family × SHAPE and has been ~70 since it "+
			"was written — a drop this large means the shape axis was collapsed", n)
	}
}
