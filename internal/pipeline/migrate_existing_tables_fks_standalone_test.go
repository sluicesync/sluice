// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the READ-TAKING form of the item-140 check — the one the
// multi-database sync fan-out uses because it has no shape compare to ride.
// The verdict itself is the covered path's and is pinned in
// migrate_existing_tables_fks_test.go; what is new here is everything AROUND
// the verdict: which inputs skip the target round trip entirely (the cost this
// form adds), and that an unreadable catalog stays a WARN rather than becoming
// a new failure mode on a path that previously made no such read at all.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fkStandaloneGate builds the gate the fan-out builds, with the target catalog
// scripted through the recordingEngine's SchemaReader.
func fkStandaloneGate(targetCatalog *ir.Schema) (*existingTablesGate, *recordingEngine) {
	tgt := newRecordingEngine("mysql")
	tgt.schema = targetCatalog
	return &existingTablesGate{
		Source:       newRecordingEngine("mysql"),
		Target:       tgt,
		TargetDSN:    "tgt",
		TargetSchema: "db7",
		Mode:         "sync cold-start",
	}, tgt
}

func TestPreExistingFKStandalone_RefusesAnInScopeParent(t *testing.T) {
	inScope := &ir.Schema{Tables: []*ir.Table{
		gateTable("orders", gateCols()...),
		gateTable("users", gateCols()...),
	}}
	g, _ := fkStandaloneGate(&ir.Schema{Tables: []*ir.Table{
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
		gateTable("users", gateCols()...),
	}})

	err := g.readAndCheckPreExistingForeignKeys(context.Background(), inScope)
	hint := assertPreexistingFKRefusal(t, err, "orders", "fk_orders_users", "users")
	if !strings.Contains(hint, "--skip-foreign-keys") {
		t.Errorf("hint does not name --skip-foreign-keys: %q", hint)
	}
	// The mode word is the one piece of caller identity in the prose, and the
	// fan-out is a sync cold start, not a migration.
	if !strings.Contains(err.Error(), "sync cold-start") {
		t.Errorf("refusal does not name the mode: %v", err)
	}
}

func TestPreExistingFKStandalone_WarnsOnAnOutOfScopeParent(t *testing.T) {
	logs := captureLogs(t)
	inScope := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}
	g, _ := fkStandaloneGate(&ir.Schema{Tables: []*ir.Table{
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
	}})

	if err := g.readAndCheckPreExistingForeignKeys(context.Background(), inScope); err != nil {
		t.Fatalf("an out-of-scope parent must not refuse: %v", err)
	}
	if out := logs.String(); !strings.Contains(out, "fk_orders_users") || !strings.Contains(out, "WARN") {
		t.Errorf("no WARN naming the tolerated constraint:\n%s", out)
	}
}

// TestPreExistingFKStandalone_SkipsTheReadEntirely pins the cost half. Each of
// these inputs must not reach the target at ALL — this form is per DATABASE on
// a fan-out that can carry hundreds, so "returns nil" is not the property that
// matters; "made no connection" is.
func TestPreExistingFKStandalone_SkipsTheReadEntirely(t *testing.T) {
	populated := &ir.Schema{Tables: []*ir.Table{
		fkGateTable("orders", []*ir.ForeignKey{fkTo("fk_orders_users", "users")}, gateCols()...),
		gateTable("users", gateCols()...),
	}}
	inScope := &ir.Schema{Tables: []*ir.Table{
		gateTable("orders", gateCols()...),
		gateTable("users", gateCols()...),
	}}

	cases := []struct {
		name   string
		schema *ir.Schema
		caps   ir.Capabilities
	}{
		{"nil schema", nil, ir.Capabilities{}},
		{"empty scope", &ir.Schema{}, ir.Capabilities{}},
		{"target bypasses FK enforcement", inScope, ir.Capabilities{BulkCopyBypassesForeignKeys: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, tgt := fkStandaloneGate(populated)
			tgt.caps = tc.caps
			// A read that CANNOT succeed: if the check reaches the target
			// anyway the WARN fallback fires and the log proves it.
			tgt.readSchemaErr = errors.New("boom")
			logs := captureLogs(t)

			if err := g.readAndCheckPreExistingForeignKeys(context.Background(), tc.schema); err != nil {
				t.Fatalf("want a silent skip; got %v", err)
			}
			if strings.Contains(logs.String(), "cannot read the target's existing tables") {
				t.Errorf("the target catalog was read for a case that must skip it:\n%s", logs.String())
			}
		})
	}
}

// TestPreExistingFKStandalone_UnreadableCatalogWarnsAndProceeds is the
// never-a-new-failure-mode property. This form introduces a target round trip
// on a path that previously made none, so a target the reader cannot reach
// must not turn a working fan-out into a refusal.
func TestPreExistingFKStandalone_UnreadableCatalogWarnsAndProceeds(t *testing.T) {
	logs := captureLogs(t)
	g, tgt := fkStandaloneGate(nil)
	tgt.readSchemaErr = errors.New("permission denied")
	inScope := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}

	if err := g.readAndCheckPreExistingForeignKeys(context.Background(), inScope); err != nil {
		t.Fatalf("an unreadable target catalog must WARN and proceed; got %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "cannot read the target's existing tables") {
		t.Errorf("no WARN naming the skipped check:\n%s", out)
	}
	// The WARN has to say what was skipped, not just that a read failed —
	// otherwise the operator reads it as a shape-gate notice.
	if !strings.Contains(out, "1452") {
		t.Errorf("the WARN does not tell the operator what it can no longer catch:\n%s", out)
	}
}
