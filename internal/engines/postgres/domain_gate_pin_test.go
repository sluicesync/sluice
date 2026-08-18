// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// domainOverTable wraps one column of the given base type in an ir.Domain and
// returns a single-column table — the shape the PG SchemaReader produces for a
// `CREATE DOMAIN … AS <base>` column (Bug 113).
func domainOverTable(base ir.Type) *ir.Table {
	return &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "c", Type: ir.Domain{Name: "d_over_base", BaseType: base}},
		},
	}
}

// TestPin_DomainWrapped_PhysicalWritePath pins the six physical-write-path
// predicates that select the COPY-vs-INSERT path or a per-conn codec: a
// DOMAIN-wrapped column of the family MUST be routed exactly as the bare column
// is, because a domain stores and COPYs as its base type. Reverting any one of
// the six `ir.UnwrapDomain` fixes (mutation test) turns its case false and fails
// this pin.
func TestPin_DomainWrapped_PhysicalWritePath(t *testing.T) {
	cases := []struct {
		name string
		base ir.Type
		fn   func(*ir.Table) bool
	}{
		{"vector", ir.ExtensionType{Extension: "vector", Name: "vector"}, tableHasPGVectorColumn},
		{"hstore", ir.ExtensionType{Extension: "hstore", Name: "hstore"}, tableHasHstoreColumn},
		{"verbatim/money", ir.VerbatimType{Definition: "money"}, tableHasVerbatimColumn},
		{"interval", ir.Interval{}, tableHasIntervalColumn},
		{"timetz", ir.Time{WithTimeZone: true}, tableHasTimetzColumn},
		{"macaddr8[]", ir.Array{Element: ir.Macaddr{Width: ir.MacaddrEUI64}}, tableHasMacaddr8ArrayColumn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Bare column: the pre-existing behaviour, and the mutation-test
			// baseline — this stays true whether or not the fix is present.
			if !tc.fn(&ir.Table{Name: "t", Columns: []*ir.Column{{Name: "c", Type: tc.base}}}) {
				t.Fatalf("bare %s column not detected — test scaffold is wrong", tc.name)
			}
			// Domain-wrapped column: only true once the predicate unwraps.
			if !tc.fn(domainOverTable(tc.base)) {
				t.Errorf("DOMAIN over %s not detected by its physical-write-path predicate — a "+
					"domain-wrapped column would take the wrong (binary-COPY / no-codec) path (Bug 233)", tc.name)
			}
		})
	}
}

// TestPin_DomainOverEnum_RefusedAtEmit pins the LOUD REFUSAL that the enum-type
// lifecycle exemptions in the roster lean on: a domain-over-enum cannot be
// emitted, because emitColumnType has no standalone spelling for an enum base
// (it needs emitColumnDef's table+column context). This is the premise that
// makes schema_writer's enum-type-creation/preview/drop/ensure sites safe as
// raw dispatches — if this refusal ever stops firing (e.g. emitColumnType gains
// an enum arm), those exemptions must be revisited, and this pin forces that.
func TestPin_DomainOverEnum_RefusedAtEmit(t *testing.T) {
	_, err := emitCreateDomainType(
		ir.Domain{Name: "d_mood", BaseType: ir.Enum{Values: []string{"happy", "sad"}}},
		"public",
		emitOpts{},
	)
	if err == nil {
		t.Fatalf("emitCreateDomainType accepted a domain-over-enum; the enum-lifecycle exemptions in " +
			"postgresDomainDispatchExemptions assume it is refused loudly — revisit them")
	}
	if !strings.Contains(err.Error(), "d_mood") {
		t.Errorf("domain-over-enum refusal does not name the domain (%q); a loud refusal must name the row", err)
	}
}
