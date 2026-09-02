// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// The selector-form × source-shape matrix for the redaction preflight
// (audit 2026-08-27 NEW-1). Selector forms are what the CLI/YAML
// parsers can produce — `table.column` (bare) and
// `schema.table.column` (qualified) — plus the table-less rule only a
// programmatic registry can build. Source shapes are what
// [ir.Table.Schema] carries: "" on a flat-scope source (MySQL single-
// database mode) and a namespace on a namespaced one (Postgres; MySQL
// multi-database). The load-bearing cell is qualified × flat: pre-fix
// it PASSED, and every bulk/backup row then missed the registry key.
//
// The bulk-copy / backup / CDC lanes themselves are pinned on real
// engines in internal/pipeline's
// TestRedactSelectorLaneMatrix_* integration tests; this file pins the
// preflight's verdict per cell.

func flatSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}, {Name: "email", Type: ir.Text{}}},
	}}}
}

func namespacedSchema(ns string) *ir.Schema {
	s := flatSchema()
	s.Tables[0].Schema = ns
	return s
}

func oneRule(schema, table, column string) *redact.Registry {
	r := redact.New()
	r.Set(schema, table, column, redact.Static{Value: "REDACTED"})
	return r
}

func TestPreflightRedactRules_SelectorNamespaceMatrix(t *testing.T) {
	cases := []struct {
		name          string
		reg           *redact.Registry
		schema        *ir.Schema
		passNamespace string
		wantRefuse    bool
		wantInMessage []string
	}{
		// ---- bare `table.column` ----
		{name: "bare × flat passes", reg: oneRule("", "users", "email"), schema: flatSchema()},
		{name: "bare × namespaced passes", reg: oneRule("", "users", "email"), schema: namespacedSchema("public")},
		{name: "bare × typo'd table refuses (Bug 99)", reg: oneRule("", "usres", "email"), schema: flatSchema(), wantRefuse: true, wantInMessage: []string{"usres.email", "typo"}},

		// ---- qualified `schema.table.column` ----
		{name: "qualified × namespaced, same namespace passes", reg: oneRule("public", "users", "email"), schema: namespacedSchema("public")},
		{
			// Rules() hands back the folded key ("public"); the source
			// spells it "Public". The runtime lookup folds both sides, so
			// the preflight must not refuse on case alone.
			name: "qualified × namespaced, case-folded namespace passes",
			reg:  oneRule("Public", "users", "email"), schema: namespacedSchema("Public"),
		},
		{
			// THE NEW-1 CELL. Pre-fix: passed, then every bulk-copy /
			// backup row (schema "") missed the `source_db.users.email`
			// key while CDC rows (schema "source_db") hit it.
			name: "qualified × flat REFUSES with the flat-scope recovery",
			reg:  oneRule("source_db", "users", "email"), schema: flatSchema(),
			wantRefuse:    true,
			wantInMessage: []string{"source_db.users.email", "flat (single-namespace) scope", "users.email", "--include-database=source_db"},
		},
		{
			name: "qualified × namespaced, foreign namespace refuses naming what the run reads",
			reg:  oneRule("other_svc", "users", "email"), schema: namespacedSchema("public"),
			wantRefuse:    true,
			wantInMessage: []string{"other_svc.users.email", `"other_svc"`, `"public"`},
		},
		{
			name: "qualified × namespaced, typo'd column in the right namespace refuses",
			reg:  oneRule("public", "users", "emial"), schema: namespacedSchema("public"),
			wantRefuse:    true,
			wantInMessage: []string{"public.users.emial", "typo"},
		},

		// ---- multi-namespace fan-out pass ----
		{
			// The pass for audit_svc sees a rule scoped to customer_svc:
			// that rule is customer_svc's pass's business (and
			// PreflightRedactNamespaces has proven it names a selected
			// namespace). Refusing here would fail a working multi-
			// database run on every sibling.
			name: "fan-out pass: qualified rule for a SIBLING namespace is handed off, not refused",
			reg:  oneRule("customer_svc", "users", "email"), schema: namespacedSchema("audit_svc"), passNamespace: "audit_svc",
		},
		{
			name: "fan-out pass: qualified rule for THIS namespace with a typo refuses",
			reg:  oneRule("audit_svc", "users", "emial"), schema: namespacedSchema("audit_svc"), passNamespace: "audit_svc",
			wantRefuse:    true,
			wantInMessage: []string{"audit_svc.users.emial"},
		},
		{
			name: "fan-out pass: bare rule for a table this namespace lacks refuses (per-pass, as before)",
			reg:  oneRule("", "orders", "email"), schema: namespacedSchema("audit_svc"), passNamespace: "audit_svc",
			wantRefuse: true,
		},

		// ---- table-less rule (programmatic registries only) ----
		{
			// Registry.Get is keyed by the full triple, so a rule with no
			// table can never match a row. Pre-fix the preflight SKIPPED
			// it as a future wildcard — a silent no-op.
			name: "table-less rule refuses instead of skipping",
			reg:  oneRule("", "", "email"), schema: flatSchema(),
			wantRefuse:    true,
			wantInMessage: []string{"rule has no table"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := PreflightRedactRules(tc.reg, tc.schema, tc.passNamespace)
			if !tc.wantRefuse {
				if err != nil {
					t.Fatalf("want pass; got refusal: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want loud refusal; got nil (a rule that applies to nothing is a silent PII leak)")
			}
			if !errors.Is(err, ErrRedactSelectorUnresolved) {
				t.Errorf("want ErrRedactSelectorUnresolved; got %v", err)
			}
			for _, want := range tc.wantInMessage {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("message should contain %q; got:\n%v", want, err)
				}
			}
		})
	}
}

// The per-strategy checks resolve the column through the same
// namespace-aware lookup: a qualified rule must find ITS namespace's
// column, so a mask:uuid rule for a foreign namespace is a selector
// refusal, not a silently-skipped type check.
func TestPreflightRedactRules_StrategyChecksUseNamespaceLookup(t *testing.T) {
	uuidSchema := &ir.Schema{Tables: []*ir.Table{{
		Schema:     "public",
		Name:       "users",
		Columns:    []*ir.Column{{Name: "id", Type: ir.UUID{}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	r := redact.New()
	r.Set("public", "users", "id", redact.MaskUUID{})
	err := PreflightRedactRules(r, uuidSchema, "")
	if !errors.Is(err, ErrRedactTypeMismatch) {
		t.Errorf("mask:uuid in the right namespace: want the Bug 60 type refusal; got %v", err)
	}

	r = redact.New()
	r.Set("other", "users", "id", redact.MaskUUID{})
	err = PreflightRedactRules(r, uuidSchema, "")
	if !errors.Is(err, ErrRedactSelectorUnresolved) || errors.Is(err, ErrRedactTypeMismatch) {
		t.Errorf("mask:uuid in a foreign namespace: want ONLY the selector refusal; got %v", err)
	}

	r = redact.New()
	r.Set("public", "users", "id", redact.RandomizeInt{Min: 0, Max: 10})
	if err := PreflightRedactRules(r, uuidSchema, ""); err != nil {
		t.Errorf("randomize on a PK-bearing table in the right namespace: want pass; got %v", err)
	}
}

func TestPreflightRedactNamespaces(t *testing.T) {
	selected := []string{"customer_svc", "Audit_Svc"}

	t.Run("nil / empty registry is a no-op", func(t *testing.T) {
		if err := PreflightRedactNamespaces(nil, selected); err != nil {
			t.Errorf("nil: %v", err)
		}
		if err := PreflightRedactNamespaces(redact.New(), selected); err != nil {
			t.Errorf("empty: %v", err)
		}
	})
	t.Run("bare rules are not this check's concern", func(t *testing.T) {
		if err := PreflightRedactNamespaces(oneRule("", "users", "email"), selected); err != nil {
			t.Errorf("bare: %v", err)
		}
	})
	t.Run("qualified rule naming a selected namespace passes, case-folded", func(t *testing.T) {
		if err := PreflightRedactNamespaces(oneRule("customer_svc", "users", "email"), selected); err != nil {
			t.Errorf("exact: %v", err)
		}
		if err := PreflightRedactNamespaces(oneRule("audit_svc", "users", "email"), selected); err != nil {
			t.Errorf("folded: %v", err)
		}
	})
	t.Run("qualified rule naming an unselected namespace refuses, listing the selection", func(t *testing.T) {
		err := PreflightRedactNamespaces(oneRule("billing_svc", "users", "email"), selected)
		if err == nil {
			t.Fatal("want refusal; got nil — every per-namespace pass would hand this rule off and none would validate it")
		}
		if !errors.Is(err, ErrRedactSelectorUnresolved) {
			t.Errorf("want ErrRedactSelectorUnresolved; got %v", err)
		}
		for _, want := range []string{"billing_svc.users.email", `"customer_svc"`, `"Audit_Svc"`, "--include-database"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("message should contain %q; got:\n%v", want, err)
			}
		}
	})
}
