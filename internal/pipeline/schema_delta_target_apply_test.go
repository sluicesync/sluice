// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The arming signal for the reader-side session-GUC cast refusal
// (audit 2026-08-31 SL-2). Two pins, because the defect has two halves and
// only one of them is logic.

// TestSchemaDeltaAppliesToTarget_Matrix pins the PREDICATE. The cell that
// matters most is the Shape A one: [singleStreamSchemaForwardActive] is
// FALSE under --inject-shard-column (its boundary router owns forwarding
// instead of the intercept), yet that router forwards ALTER COLUMN TYPE
// regardless of --schema-changes. A value-semantics refusal keyed on the
// narrower flag would therefore have shipped blind to Shape A — the exact
// narrower-than-its-name shape this class keeps leaking through.
func TestSchemaDeltaAppliesToTarget_Matrix(t *testing.T) {
	shardSpec := func() ShardColumnSpec {
		v := "shard-1"
		return ShardColumnSpec{Name: "shard_id", Value: &v}
	}
	for _, tc := range []struct {
		name          string
		build         func() *Streamer
		wantApplies   bool
		wantIntercept bool
	}{
		{
			name:          "default (forward), single stream",
			build:         func() *Streamer { return &Streamer{} },
			wantApplies:   true,
			wantIntercept: true,
		},
		{
			name:          "--schema-changes=refuse",
			build:         func() *Streamer { return &Streamer{SchemaChanges: "refuse"} },
			wantApplies:   false,
			wantIntercept: false,
		},
		{
			name:          "Shape A, forward mode: the router forwards, the intercept does not",
			build:         func() *Streamer { return &Streamer{InjectShardColumn: shardSpec()} },
			wantApplies:   true,
			wantIntercept: false,
		},
		{
			name:          "Shape A under --schema-changes=refuse: the router still forwards",
			build:         func() *Streamer { return &Streamer{SchemaChanges: "refuse", InjectShardColumn: shardSpec()} },
			wantApplies:   true,
			wantIntercept: false,
		},
		{
			name:          "Shape A with --no-coordinate-live-ddl: no router, no intercept, nothing applies",
			build:         func() *Streamer { return &Streamer{InjectShardColumn: shardSpec(), NoCoordinateLiveDDL: true} },
			wantApplies:   false,
			wantIntercept: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.build()
			if got := s.schemaDeltaAppliesToTarget(); got != tc.wantApplies {
				t.Errorf("schemaDeltaAppliesToTarget() = %v, want %v", got, tc.wantApplies)
			}
			if got := s.singleStreamSchemaForwardActive(); got != tc.wantIntercept {
				t.Errorf("singleStreamSchemaForwardActive() = %v, want %v", got, tc.wantIntercept)
			}
		})
	}
}

// TestSchemaDeltaTargetApplySetter_ArmedOnBothEntryPoints pins the WIRING.
// A reader-side refusal is inert unless it is armed, and there are exactly
// two places a stream reaches its reader: cold start and warm resume. The
// v0.99.51 zero-value trap and every "door that moved" finding in this
// project's history are the same shape — the logic was right and one
// caller never reached it — so both entry points are asserted by source,
// not by trusting that a change to one implies the other.
//
// Scope: it proves both entry points call the setter with the UNION
// predicate. It does not prove any engine implements the optional
// interface (an unimplemented interface silently no-ops the assertion) —
// that is docsync.TestSessionGUCCastRoster_EveryCDCLane's job, which reads
// the engine packages themselves.
//
// It reads the AST, not the source text (audit 2026-09-01 TCI-1: the first
// cut was strings.Contains over the file, which a commented-out call
// satisfied). A type assertion to schemaDeltaTargetApplySetter and a call
// SetSchemaDeltaAppliesToTarget(s.schemaDeltaAppliesToTarget()) must both
// exist as CODE in each entry-point file.
func TestSchemaDeltaTargetApplySetter_ArmedOnBothEntryPoints(t *testing.T) {
	for _, file := range []string{"streamer_coldstart.go", "streamer_warm_resume.go"} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		asserted, armedWithUnion := false, false
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.TypeAssertExpr:
				if id, ok := v.Type.(*ast.Ident); ok && id.Name == "schemaDeltaTargetApplySetter" {
					asserted = true
				}
			case *ast.CallExpr:
				sel, ok := v.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "SetSchemaDeltaAppliesToTarget" || len(v.Args) != 1 {
					return true
				}
				arg, ok := v.Args[0].(*ast.CallExpr)
				if !ok {
					return true
				}
				argSel, ok := arg.Fun.(*ast.SelectorExpr)
				if !ok || argSel.Sel.Name != "schemaDeltaAppliesToTarget" {
					return true
				}
				if recv, ok := argSel.X.(*ast.Ident); ok && recv.Name == "s" {
					armedWithUnion = true
				}
			}
			return true
		})
		if !asserted {
			t.Errorf("%s never type-asserts schemaDeltaTargetApplySetter — the session-GUC cast refusal is never armed on this entry point, so a MySQL TIMESTAMP⇄DATETIME MODIFY forwards silently for every stream that starts here", file)
			continue
		}
		if !armedWithUnion {
			t.Errorf("%s arms the setter with something other than s.schemaDeltaAppliesToTarget() — the arming condition must be the UNION of the intercept and Shape A, not the intercept alone", file)
		}
	}
}
