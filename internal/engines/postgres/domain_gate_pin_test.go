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

// TestPin_DomainOverEnum_EmitsNamedEnumType pins Bug 255's HALF 1: a
// domain-over-enum is EMITTED (no longer refused). emitCreateDomainType renders
// the CREATE DOMAIN referencing the enum by its schema-qualified NAME — the
// same NAMED type the writer's Phase 1a' creates first — because a PG enum is a
// named type and a CREATE DOMAIN carries no column context to synthesize one
// from. Reverting emitCreateDomainType to route the enum base through
// emitColumnType (whose ir.Enum arm refuses) reddens this pin.
func TestPin_DomainOverEnum_EmitsNamedEnumType(t *testing.T) {
	// PG source: the enum carries its original type name; the domain
	// references it verbatim.
	ddl, err := emitCreateDomainType(
		ir.Domain{Name: "d_mood", BaseType: ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}}},
		"public",
		emitOpts{},
	)
	if err != nil {
		t.Fatalf("emitCreateDomainType refused a domain-over-enum (Bug 255 HALF 1 regressed): %v", err)
	}
	if want := `CREATE DOMAIN "public"."d_mood" AS "public"."mood"`; !strings.Contains(ddl, want) {
		t.Errorf("CREATE DOMAIN does not reference the enum by its schema-qualified name.\n got: %s\nwant substring: %s", ddl, want)
	}

	// The paired CREATE TYPE the writer emits before the CREATE DOMAIN, named
	// from the same resolver, preserves enum value ORDER.
	create, err := emitCreateEnumTypeNamed(
		ir.Enum{TypeName: "mood", Values: []string{"happy", "sad"}},
		"public",
		resolveDomainEnumTypeName(ir.Enum{TypeName: "mood"}, "d_mood"),
		"domain d_mood",
	)
	if err != nil {
		t.Fatalf("emitCreateEnumTypeNamed for the domain enum base failed: %v", err)
	}
	if want := `CREATE TYPE "public"."mood" AS ENUM ('happy', 'sad')`; !strings.Contains(create, want) {
		t.Errorf("enum-type create for the domain base is wrong.\n got: %s\nwant substring: %s", create, want)
	}

	// Anonymous enum base (empty TypeName): the type name is synthesized
	// deterministically from the DOMAIN, and CREATE DOMAIN + CREATE TYPE agree.
	if got := resolveDomainEnumTypeName(ir.Enum{}, "d_mood"); got != "d_mood_enum" {
		t.Errorf("anonymous-enum-base domain type name = %q; want %q", got, "d_mood_enum")
	}
	ddl2, err := emitCreateDomainType(
		ir.Domain{Name: "d_mood", BaseType: ir.Enum{Values: []string{"happy", "sad"}}},
		"public",
		emitOpts{},
	)
	if err != nil {
		t.Fatalf("emitCreateDomainType refused an anonymous-enum-base domain: %v", err)
	}
	if want := `AS "public"."d_mood_enum"`; !strings.Contains(ddl2, want) {
		t.Errorf("anonymous-enum-base CREATE DOMAIN does not reference the synthesized name.\n got: %s\nwant substring: %s", ddl2, want)
	}
}
