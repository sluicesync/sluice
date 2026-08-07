// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// arrayLeafSourceSchema is a source schema carrying one column per
// element family that matters here: the two byte-shaped leaves that
// cannot survive a target with no array type, and three controls that
// must NOT be refused.
func arrayLeafSourceSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "j", Type: ir.Array{Element: ir.JSON{}}},
			{Name: "b", Type: ir.Array{Element: ir.JSON{Binary: true}}},
			{Name: "y", Type: ir.Array{Element: ir.Blob{}}},
			{Name: "txt", Type: ir.Array{Element: ir.Text{}}},
			{Name: "n", Type: ir.Array{Element: ir.Integer{Width: 32}}},
			{Name: "scalar", Type: ir.JSON{}},
		},
	}}}
}

func TestPreflightArrayBytesLeafOnCDC(t *testing.T) {
	err := preflightArrayBytesLeafOnCDC(arrayLeafSourceSchema(), "mysql", "sync cold-start")
	if err == nil {
		t.Fatal("no refusal for json[]/jsonb[]/bytea[] against an array-less target")
	}
	if !errors.Is(err, errArrayBytesLeafOnCDC) {
		t.Errorf("refusal does not wrap the sentinel: %v", err)
	}
	// Every byte-shaped family is named, and no control is.
	for _, want := range []string{"t.j ", "t.b ", "t.y "} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q: %v", want, err)
		}
	}
	for _, unwanted := range []string{"t.txt", "t.n", "t.scalar"} {
		if strings.Contains(err.Error(), unwanted) {
			t.Errorf("refusal names %q, which carries fine on this lane: %v", unwanted, err)
		}
	}
	// The remedy has to be actionable.
	if !strings.Contains(err.Error(), "sluice migrate") {
		t.Errorf("refusal offers no remedy: %v", err)
	}
}

// TestPreflightArrayBytesLeafOnCDC_TargetEngineRoster is the family
// sweep on the OTHER axis. The MySQL flavors are separate registrations
// sharing one writer, and the last hand-kept list of them here EXCLUDED
// mariadb — a PG→mariadb run then skipped every PG-native refusal
// silently (the migcore.IsMySQLFamilyEngine comment records it). So each
// flavor is named, and the engines that must NOT be refused are named
// beside them rather than left to inference.
func TestPreflightArrayBytesLeafOnCDC_TargetEngineRoster(t *testing.T) {
	for _, tc := range []struct {
		engine     string
		wantRefuse bool
		why        string
	}{
		{"mysql", true, "no array type; ir.Array becomes a scalar JSON column"},
		{"planetscale", true, "MySQL flavor, same writer, same JSON collapse"},
		{"vitess", true, "MySQL flavor, same writer, same JSON collapse"},
		{"mariadb", true, "MySQL flavor — the one a hand-kept list dropped once already"},
		{"postgres", false, "keeps the column an array and has a per-family writer arm on both sides"},
		{"postgres-trigger", false, "delegates its writer to postgres; same arms"},
		{
			"sqlite", false,
			"refuses ir.Array outright at schema emit, which is louder and earlier than this — a " +
				"refusal here would be a second, vaguer message for a run that already cannot start",
		},
		{"", false, "no target engine resolved; nothing to decide on"},
	} {
		t.Run(tc.engine, func(t *testing.T) {
			err := preflightArrayBytesLeafOnCDC(arrayLeafSourceSchema(), tc.engine, "sync cold-start")
			if tc.wantRefuse && err == nil {
				t.Errorf("target %q was not refused (%s)", tc.engine, tc.why)
			}
			if !tc.wantRefuse && err != nil {
				t.Errorf("target %q was refused, but %s: %v", tc.engine, tc.why, err)
			}
		})
	}
}

// TestPreflightArrayBytesLeafOnCDC_InertInputs pins the shapes that must
// be no-ops rather than panics or false refusals.
func TestPreflightArrayBytesLeafOnCDC_InertInputs(t *testing.T) {
	if err := preflightArrayBytesLeafOnCDC(nil, "mysql", "sync cold-start"); err != nil {
		t.Errorf("nil schema: %v", err)
	}
	if err := preflightArrayBytesLeafOnCDC(&ir.Schema{}, "mysql", "sync cold-start"); err != nil {
		t.Errorf("empty schema: %v", err)
	}
	nilly := &ir.Schema{Tables: []*ir.Table{nil, {Name: "t", Columns: []*ir.Column{nil}}}}
	if err := preflightArrayBytesLeafOnCDC(nilly, "mysql", "sync cold-start"); err != nil {
		t.Errorf("nil table/column entries: %v", err)
	}
}

// TestPreflightArrayBytesLeafOnCDC_SourceColumnTypeWins covers the
// override lane: `--type-override=col=jsonb` rewrites Type to ir.JSON
// and parks the array in SourceColumnType, and it is the SOURCE type
// that decides whether the applier can carry the value.
func TestPreflightArrayBytesLeafOnCDC_SourceColumnTypeWins(t *testing.T) {
	overridden := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "j", Type: ir.JSON{Binary: true}, SourceColumnType: ir.Array{Element: ir.JSON{}}},
		},
	}}}
	if err := preflightArrayBytesLeafOnCDC(overridden, "mysql", "sync cold-start"); err == nil {
		t.Error("an overridden json[] column was not refused; SourceColumnType carries the array-ness " +
			"and the applier still cannot see it")
	}
}

// TestArrayBytesLeafPreflightCallSiteParity is the sibling enumeration.
// This refusal exists because a value rule that reached the migrate door
// and not the CDC door is exactly the recurring defect shape — so the
// two CDC entry points that ask the schema-keyed binary refusal must ask
// this one too. Derived from the AST rather than asserted, so a third
// entry point that adopts one and not the other fails the build.
func TestArrayBytesLeafPreflightCallSiteParity(t *testing.T) {
	binarySites := preflightCallerFunctions(t, "preflightBinaryTargetColumnsOnCDC")
	arraySites := preflightCallerFunctions(t, "preflightArrayBytesLeafOnCDC")

	// Anti-vacuity: a walk that stopped finding call sites would compare
	// two empty sets and pass.
	if len(binarySites) < 2 {
		t.Fatalf("found only %d callers of preflightBinaryTargetColumnsOnCDC (%v); the AST walk is broken",
			len(binarySites), binarySites)
	}
	for fn := range binarySites {
		if !arraySites[fn] {
			t.Errorf("%s asks preflightBinaryTargetColumnsOnCDC but not preflightArrayBytesLeafOnCDC. "+
				"Both refuse a source-side distinction the CDC applier's target-derived column types "+
				"cannot carry; an entry point that asks one and not the other is the sibling gap this "+
				"parity check exists to catch", fn)
		}
	}
	for fn := range arraySites {
		if !binarySites[fn] {
			t.Errorf("%s asks preflightArrayBytesLeafOnCDC but not preflightBinaryTargetColumnsOnCDC — "+
				"if that is deliberate, say so here rather than leaving the divergence implied", fn)
		}
	}
}

// preflightCallerFunctions returns the names of the non-test functions
// in this package that call the named preflight.
func preflightCallerFunctions(t *testing.T, callee string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		for _, decl := range file.Decls {
			fn, isFunc := decl.(*ast.FuncDecl)
			if !isFunc || fn.Body == nil || fn.Name.Name == callee {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				if ident, isIdent := call.Fun.(*ast.Ident); isIdent && ident.Name == callee {
					out[fn.Name.Name] = true
				}
				return true
			})
		}
	}
	return out
}
