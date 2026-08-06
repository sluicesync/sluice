// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// nsEngine is a minimal ir.Engine whose only real answers are its name and the
// namespace half of its capabilities — the two inputs
// [ValidateMultiNamespaceTarget] reads.
type nsEngine struct {
	ir.Engine // nil-panics on any real use

	name  string
	scope ir.SchemaScope
}

func (e nsEngine) Name() string { return e.name }

func (e nsEngine) Capabilities() ir.Capabilities {
	return ir.Capabilities{SchemaScope: e.scope}
}

// nsDeriverEngine adds [ir.DatabaseDSNDeriver] — the MySQL/Postgres shape,
// which routes each source namespace to its own target namespace.
type nsDeriverEngine struct{ nsEngine }

func (nsDeriverEngine) WithDatabase(dsn, _ string) (string, error) { return dsn, nil }

func (nsDeriverEngine) EnsureDatabase(context.Context, string, string) error { return nil }

// TestValidateMultiNamespaceTarget walks the predicate's whole truth table.
// The FLAT × many case is the defect (roadmap item 148, route 2); everything
// else must pass, because a fan-out refusal that fired on a namespaced target
// would break every multi-database migrate that works today.
func TestValidateMultiNamespaceTarget(t *testing.T) {
	flat := nsEngine{name: "sqlite", scope: ir.SchemaScopeFlat}
	namespaced := nsEngine{name: "postgres-like", scope: ir.SchemaScopeNamespaced}
	// A deriver whose declared scope is FLAT: MySQL's shape, and the case that
	// proves the two halves of the predicate are genuinely OR'd rather than
	// one being a proxy for the other.
	flatDeriver := nsDeriverEngine{nsEngine{name: "mysql-like", scope: ir.SchemaScopeFlat}}

	cases := []struct {
		name     string
		target   ir.Engine
		selected []string
		refuses  bool
	}{
		{
			// THE DEFECT.
			name:     "flat target, two source namespaces",
			target:   flat,
			selected: []string{"sales", "billing"},
			refuses:  true,
		},
		{
			name:     "flat target, three source namespaces",
			target:   flat,
			selected: []string{"a", "b", "c"},
			refuses:  true,
		},
		{
			// Allowed on purpose: one namespace needs no target namespacing,
			// and the result is byte-identical to a plain single-namespace run.
			name:     "flat target, ONE source namespace",
			target:   flat,
			selected: []string{"billing"},
			refuses:  false,
		},
		{
			name:     "flat target, empty selection",
			target:   flat,
			selected: nil,
			refuses:  false,
		},
		{
			// The capability half: a namespaced target honours TargetSchema, so
			// the `else` arm the defect lived in is safe for it.
			name:     "namespaced target, two source namespaces",
			target:   namespaced,
			selected: []string{"sales", "billing"},
			refuses:  false,
		},
		{
			// The deriver half, and the case that keeps the predicate honest:
			// MySQL declares a FLAT schema scope and is nonetheless safe,
			// because it routes per namespace via its own target DSN. A
			// predicate written on SchemaScope alone would refuse every
			// MySQL→MySQL multi-database migrate.
			name:     "flat-scope DERIVER target, two source namespaces",
			target:   flatDeriver,
			selected: []string{"sales", "billing"},
			refuses:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMultiNamespaceTarget(tc.target, "multi-namespace migrate", tc.selected)
			if !tc.refuses {
				if err != nil {
					t.Fatalf("must not refuse: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a multi-namespace fan-out into a flat target; every source namespace " +
					"would write bare names into ONE target namespace and same-named tables would merge")
			}
			ce, coded := sluicecode.FromError(err)
			if !coded || ce.Code != sluicecode.CodeConfigMultiNamespaceTargetFlat {
				t.Errorf("refusal must carry %s; got %+v (coded=%v)",
					sluicecode.CodeConfigMultiNamespaceTargetFlat, ce, coded)
			}
			// The operator's only route to a fix is knowing WHICH namespaces
			// and WHICH engine.
			for _, want := range append(append([]string{}, tc.selected...), tc.target.Name()) {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal must name %q; got %q", want, err.Error())
				}
			}
		})
	}
}

// TestValidateMultiNamespaceTarget_NilTarget pins the guard rather than a
// panic: this runs before the target writer is opened, so a nil engine is a
// programming error worth naming.
func TestValidateMultiNamespaceTarget_NilTarget(t *testing.T) {
	if err := ValidateMultiNamespaceTarget(nil, "multi-namespace migrate", []string{"a", "b"}); err == nil {
		t.Fatal("nil target must be refused, not dereferenced")
	}
}
