// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestSourceEngineComparator_PostgresChainOrdersPositions pins what `backup
// compact` hands smart compaction for shape (B): the registry's position order
// for the chain's recorded SOURCE engine — Postgres today — and nothing for an
// engine that does not order positions or a chain that records no engine.
// Without it, smart compaction's gates cannot see the re-delivered boundary
// transaction collapse hides.
func TestSourceEngineComparator_PostgresChainOrdersPositions(t *testing.T) {
	for _, tc := range []struct {
		engine string
		want   bool
	}{
		{"postgres", true},
		{"mysql", false},
		{"", false},
		{"not-an-engine", false},
	} {
		t.Run(tc.engine, func(t *testing.T) {
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := lineage.WriteLineageCatalog(ctx, store, &lineage.Catalog{FormatVersion: 1, SourceEngine: tc.engine, Segments: []lineage.Segment{{SegmentID: "s0", FullManifestPath: lineage.ManifestFileName}}}); err != nil {
				t.Fatal(err)
			}
			if got := sourceEngineComparator(ctx, store) != nil; got != tc.want {
				t.Fatalf("source engine %q: comparator present = %v; want %v", tc.engine, got, tc.want)
			}
		})
	}
}

// TestBackupCompactPassesTheSourceComparator is the wiring half: every
// backup.CompactOpts literal in the CLI sets Comparator from
// sourceEngineComparator. Without it the helper above is dead code and smart
// compaction's (B) gates are blind, which is exactly how the class shipped.
// Floor: at least one CompactOpts literal must be found.
func TestBackupCompactPassesTheSourceComparator(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			sel, ok := lit.Type.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "CompactOpts" {
				return true
			}
			found++
			wired := false
			for _, el := range lit.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if k, ok := kv.Key.(*ast.Ident); !ok || k.Name != "Comparator" {
					continue
				}
				if call, ok := kv.Value.(*ast.CallExpr); ok {
					if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == "sourceEngineComparator" {
						wired = true
					}
				}
			}
			if !wired {
				t.Errorf("%s: a backup.CompactOpts literal does not set Comparator: sourceEngineComparator(...)", fset.Position(lit.Pos()))
			}
			return true
		})
	}
	if found == 0 {
		t.Fatal("anti-vacuity: no backup.CompactOpts literal found in cmd/sluice — re-anchor this gate")
	}
}
