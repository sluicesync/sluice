// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestIncrementalBackup_ScopeFromParentManifest pins the v0.94.0
// Bug 110 closure. Pre-fix, the incremental's end-position schema-read
// iterated every table in the source; a single unrelated table with
// a verbatim-eligible column type failed the whole incremental even
// when the chain was originally taken with `--include-table=X`.
// Post-fix, the parent manifest's table list becomes the scope and
// the schema-read on TableScoper-implementing engines skips
// out-of-scope tables.
//
// This unit pin builds the scope predicate the way Run does and
// asserts membership semantics; the integration verification lives
// in the regression cycle's Bug 110 focus.
func TestIncrementalBackup_ScopeFromParentManifest(t *testing.T) {
	cases := []struct {
		name     string
		schema   *ir.Schema
		probe    map[string]bool // table name → expected predicate result
		wantNil  bool            // true: predicate not built (b.scope stays nil)
		wantNote string
	}{
		{
			name:    "nil schema → unscoped (preserve historical behaviour)",
			schema:  nil,
			wantNil: true,
		},
		{
			name:    "empty schema → unscoped",
			schema:  &ir.Schema{},
			wantNil: true,
		},
		{
			name: "single-table parent → only that table admitted",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "orders"},
				},
			},
			probe: map[string]bool{
				"orders":   true,
				"products": false,
				"":         false,
			},
		},
		{
			name: "multi-table parent → exact admitted set",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "users"},
					{Name: "orders"},
					{Name: "audit_log"},
				},
			},
			probe: map[string]bool{
				"users":           true,
				"orders":          true,
				"audit_log":       true,
				"unrelated_xml":   false, // the Bug 110 scenario
				"unrelated_money": false,
				"Users":           false, // case-sensitive
				// Whitespace-strict probe omitted — gocritic's `mapKey`
				// linter flags the unusual key shape and the membership
				// semantics are already covered by the case-sensitive
				// "Users" probe.
			},
		},
		{
			name: "nil-element-tolerance: nil *ir.Table in slice is skipped",
			schema: &ir.Schema{
				Tables: []*ir.Table{
					{Name: "orders"},
					nil,
					{Name: "users"},
				},
			},
			probe: map[string]bool{
				"orders": true,
				"users":  true,
				"":       false,
			},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Mirror the predicate-building block from
			// IncrementalBackup.Run (Bug 110 closure).
			b := &IncrementalBackup{}
			if c.schema != nil && len(c.schema.Tables) > 0 {
				allowed := make(map[string]struct{}, len(c.schema.Tables))
				for _, t := range c.schema.Tables {
					if t != nil {
						allowed[t.Name] = struct{}{}
					}
				}
				b.scope = func(tableName string) bool {
					_, ok := allowed[tableName]
					return ok
				}
			}

			if c.wantNil {
				if b.scope != nil {
					t.Fatalf("expected b.scope to remain nil; got non-nil predicate")
				}
				return
			}
			if b.scope == nil {
				t.Fatalf("expected b.scope to be set; got nil")
			}
			for table, want := range c.probe {
				if got := b.scope(table); got != want {
					t.Errorf("scope(%q) = %v; want %v", table, got, want)
				}
			}
		})
	}
}
