// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Multi-source aggregation `--target-schema` is PG-only in v0.25.0
// (ADR-0031). MySQL operators namespace per-source streams via
// distinct target databases on the same MySQL server (`--target=
// mysql://host:3306/customer_svc`). The pipeline-side
// validateTargetSchema helper is the gate; this test pins the MySQL
// engine's capability declaration so the gate keeps refusing if a
// future contributor accidentally flips the flag.

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMySQLEngine_DeclaresFlatSchemaScope is the load-bearing
// invariant that drives the orchestrator-side `--target-schema`
// refusal for MySQL targets. Adding a per-database namespacing
// concept to MySQL would require its own design pass — until then,
// the SchemaScopeFlat declaration keeps the orchestrator guard
// armed.
func TestMySQLEngine_DeclaresFlatSchemaScope(t *testing.T) {
	for _, flavor := range []Flavor{FlavorVanilla, FlavorPlanetScale} {
		flavor := flavor
		t.Run(flavor.String(), func(t *testing.T) {
			caps := flavor.capabilities()
			if caps.SchemaScope != ir.SchemaScopeFlat {
				t.Errorf("flavor %s SchemaScope = %v; want SchemaScopeFlat (multi-source --target-schema is PG-only)",
					flavor, caps.SchemaScope)
			}
		})
	}
}

// TestMySQLEngine_NoSchemaSetter pins the MySQL engine's deliberate
// non-implementation of [ir.SchemaSetter]. The orchestrator-side
// validateTargetSchema refuses --target-schema on flat-scope engines
// upstream of any Open* call, so MySQL's writers/readers don't need
// the setter; this test keeps a future contributor from accidentally
// adding one (which would be a footgun — implying support that the
// rest of the engine doesn't actually deliver).
func TestMySQLEngine_NoSchemaSetter(t *testing.T) {
	cases := []struct {
		name string
		v    any
	}{
		{"SchemaWriter", &SchemaWriter{}},
		{"SchemaReader", &SchemaReader{}},
		{"RowReader", &RowReader{}},
		{"RowWriter", &RowWriter{}},
		{"ChangeApplier", &ChangeApplier{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if _, ok := c.v.(ir.SchemaSetter); ok {
				t.Errorf("%s implements ir.SchemaSetter; multi-source --target-schema is PG-only — adding it here misleads the orchestrator", c.name)
			}
		})
	}
}
