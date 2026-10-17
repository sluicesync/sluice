// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "sluicesync.dev/sluice/internal/ir"

// CollationLoosening implements [ir.CollationLooseningResolver] for
// Postgres: a nondeterministic collation (collisdeterministic = false, read
// into the type's Determinism) may fold case and accents — which of the two
// is not recorded, so both are reported — and a bpchar column ignores
// trailing spaces whatever its collation. A deterministic varchar/text
// column is byte equality. ok is false for a non-character type or one whose
// determinism the reader did not establish.
func (Engine) CollationLoosening(t ir.Type) (ir.CollationLoosening, bool) {
	var det ir.CollationDeterminism
	var l ir.CollationLoosening
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Char:
		det, l = v.Determinism, ir.LoosePad
	case ir.Varchar:
		det = v.Determinism
	case ir.Text:
		det = v.Determinism
	default:
		return 0, false
	}
	switch det {
	case ir.CollationNonDeterministic:
		return l | ir.LooseCase | ir.LooseAccent, true
	case ir.CollationDeterministic:
		return l, true
	}
	return 0, false
}
