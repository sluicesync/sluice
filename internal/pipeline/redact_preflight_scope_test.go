// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

// The pipeline-side glue for audit 2026-08-27 NEW-1: the multi-database
// scope hands its namespace to the preflight, and add-table narrows the
// stream's registry to the one table it copies. The preflight's own
// selector matrix lives in migcore.

func TestPreflightRedactTypesInScope_HandsSiblingNamespaceOff(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Schema:  "audit_svc",
		Name:    "users",
		Columns: []*ir.Column{{Name: "email", Type: ir.Text{}}},
	}}}
	r := redact.New()
	r.Set("customer_svc", "users", "email", redact.Static{Value: "x"})

	// Single-namespace run (nil scope): a rule for a namespace the run
	// does not read is refused.
	if err := preflightRedactTypesInScope(r, schema, nil); !errors.Is(err, errRedactSelectorUnresolved) {
		t.Errorf("nil scope: want selector refusal; got %v", err)
	}
	// The audit_svc pass of a fan-out that also selected customer_svc:
	// customer_svc's pass validates the rule; this one must not refuse.
	scope := &multiDBScope{database: "audit_svc", inScope: func(string) bool { return true }}
	if err := preflightRedactTypesInScope(r, schema, scope); err != nil {
		t.Errorf("sibling pass: want hand-off; got %v", err)
	}
	// The customer_svc pass itself sees the rule miss its schema → typo class.
	scope = &multiDBScope{database: "customer_svc", inScope: func(string) bool { return true }}
	if err := preflightRedactTypesInScope(r, schema, scope); !errors.Is(err, errRedactSelectorUnresolved) {
		t.Errorf("owning pass: want selector refusal; got %v", err)
	}
	if (*multiDBScope)(nil).namespace() != "" {
		t.Error("nil scope must read as the single-namespace run")
	}
}

func TestRedactRulesForTable_NarrowsToTheAddedTable(t *testing.T) {
	if got := redactRulesForTable(nil, "users"); got != nil {
		t.Errorf("nil registry: want nil passthrough; got %v", got)
	}
	r := redact.New()
	r.Set("", "users", "email", redact.Static{Value: "x"})
	r.Set("public", "Users", "phone", redact.Static{Value: "y"})
	r.Set("", "orders", "note", redact.Static{Value: "z"})

	got := redactRulesForTable(r, "users")
	if got.Get("", "users", "email") == nil || got.Get("public", "users", "phone") == nil {
		t.Errorf("rules for the added table must survive (case-folded); got %v", got.Rules())
	}
	if got.Get("", "orders", "note") != nil {
		t.Error("a rule for another table is not a typo and must not reach the single-table preflight")
	}

	// End to end: the orders rule alone would be an unresolved selector
	// against the single-table schema; narrowed, add-table's preflight
	// passes (the table is namespaced "public", so the qualified phone
	// rule resolves too), while a typo'd rule for the added table still
	// refuses.
	schema := &ir.Schema{Tables: []*ir.Table{{
		Schema:  "public",
		Name:    "users",
		Columns: []*ir.Column{{Name: "email", Type: ir.Text{}}, {Name: "phone", Type: ir.Text{}}},
	}}}
	if err := preflightRedactTypes(redactRulesForTable(r, "users"), schema); err != nil {
		t.Errorf("narrowed registry: want pass; got %v", err)
	}
	r.Set("", "users", "emial", redact.Static{Value: "x"})
	if err := preflightRedactTypes(redactRulesForTable(r, "users"), schema); !errors.Is(err, errRedactSelectorUnresolved) {
		t.Errorf("typo'd rule for the added table: want selector refusal; got %v", err)
	}
}
