// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// CollationLoosening implements [ir.CollationLooseningResolver] for
// Postgres: a nondeterministic collation (collisdeterministic = false, read
// into the type's Determinism) may fold case and accents — which of the two
// is not recorded, so both are reported — and a bpchar column ignores
// trailing spaces whatever its collation. A deterministic varchar/text
// column is byte equality. ok is false for a non-character type or one whose
// determinism the reader did not establish.
//
// citext — carried as an enabled [ir.ExtensionType] or, on the verbatim
// passthrough path, an [ir.VerbatimType] — compares through lower(), so it
// folds case (round-3 review: it read as unclassified, and a byte-exact
// source key onto a citext target key passed). An array compares element by
// element, so it takes its element's loosening.
func (e Engine) CollationLoosening(t ir.Type) (ir.CollationLoosening, bool) {
	var det ir.CollationDeterminism
	var l ir.CollationLoosening
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Char:
		det, l = v.Determinism, ir.LoosePad
	case ir.Varchar:
		det = v.Determinism
	case ir.Text:
		det = v.Determinism
	case ir.Array:
		return e.CollationLoosening(v.Element)
	case ir.ExtensionType:
		if isCitext(v.Name) {
			return ir.LooseCase, true
		}
		return 0, false
	case ir.VerbatimType:
		if isCitext(v.Definition) {
			return ir.LooseCase, true
		}
		return 0, false
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

// isCitext reports the citext type's name, schema-qualified or not.
func isCitext(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return n == "citext" || strings.HasSuffix(n, ".citext")
}
