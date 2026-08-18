// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/domaingate"
)

// TestDomainTransparency_DiffDispatchRoster is this package's instantiation of
// the Bug 233 gate (audit A-3): every column-type dispatch either reads the
// STORAGE type through ir.UnwrapDomain or carries a written, code-verified
// reason below.
//
// The two sites both live in actualForCompare and both concern the SAME field —
// Integer.AutoIncrement — which a DOMAIN can never carry (an identity/serial
// column cannot be DOMAIN-typed on PostgreSQL, the only DOMAIN producer). They
// are exempt, and unwrapping would additionally be actively wrong here (it
// would flatten the wrapper into the compared type string). See below.
//
// See the domaingate package doc for what a pass proves and what it does not.
func TestDomainTransparency_DiffDispatchRoster(t *testing.T) {
	domaingate.Assert(t, domaingate.Config{
		Dir:    ".",
		Engine: "ir/diff",
		// 2 dispatch sites; floor a touch under.
		MinSites: 1,
		Allowed:  diffDomainDispatchExemptions,
	})
}

var diffDomainDispatchExemptions = map[string]string{
	"column_shape.go:actualForCompare:normalized.Type": "IMPOSSIBLE-SHAPE + UNWRAP-WOULD-FLATTEN: the arm " +
		"normalizes Integer.AutoIncrement out of the shape compare, and a DOMAIN cannot wrap an AutoIncrement " +
		"integer (PostgreSQL — the only DOMAIN producer — forbids GENERATED … AS IDENTITY / SERIAL on a " +
		"DOMAIN-typed column, same as postgres cutover_sequence). So the arm is unreachable for a domain. " +
		"Unwrapping would also be actively wrong: the branch assigns the (unwrapped) Integer back into " +
		"normalized.Type, so unwrapping would STRIP the domain wrapper from the type the downstream " +
		"renderColumnShapeOpts compares, silently collapsing a domain-over-int vs bare-int difference.",
	"column_shape.go:actualForCompare:exp.Type": "IMPOSSIBLE-SHAPE: the paired read of the EXPECTED side of " +
		"the same AutoIncrement normalization; a DOMAIN cannot wrap an AutoIncrement integer, so this arm too " +
		"is unreachable for a domain, and the compare of a domain column's shape is handled by the type-string " +
		"render, not this AutoIncrement-only exclusion.",
}
