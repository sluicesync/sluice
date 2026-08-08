// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// gradedInterfaceMethods are the calls this gate follows that are NOT made on
// the enclosing function's own receiver. Both are the interface calls the
// invariant is about: the one that mints single-schema parallel readers, and
// the one that opens a spanning stream. Keeping this an explicit allowlist —
// rather than following every selector — is what stops the graph from fusing
// unrelated types that happen to share a method name (an early cut of this
// gate reported a false path through `b.Run` on the broker).
var gradedInterfaceMethods = map[string]bool{
	"ImportSnapshot":                  true,
	"OpenMultiDatabaseSnapshotStream": true,
}

// TestSpanningSnapshotNeverReachesTheParallelColdStartLane holds the second
// premise ADR-0075's consistency argument rests on — the one the ADR does not
// state.
//
// # The claim, and why it needed a gate
//
// ADR-0075 says a multi-schema `sync` cold start is consistent because "the
// slot's exported snapshot spans the entire database (all schemas) by
// construction". That half is about PostgreSQL and is ground-truthed against a
// real server by postgres' TestExportedSnapshotSpansEverySchema. It is not
// sufficient on its own: a spanning snapshot only produces a spanning COPY if
// the readers that drain it qualify each table by that table's OWN schema.
//
// The readers minted by [ir.SnapshotImporter.ImportSnapshot] for the ADR-0079
// parallel cold start do NOT — postgres' snapshot importer builds them with
// qualifyBySchema=false and the DSN's bound schema. So a spanning stream
// routed into the fast lane would read the default schema N times over. In the
// canonical multi-schema shape — the same table name in every tenant schema —
// that is not an error the operator would see; it is N copies of one schema's
// rows fanned into N target schemas, with CDC then delivering the real
// per-schema changes on top of them.
//
// runColdStartParallel states the counter-argument in a comment: the spanning
// opener is reached ONLY from coldStartMultiDatabase, which copies serially,
// so the fast lane can never see a spanning stream. That was true, and nothing
// asserted it — a "holds by construction" invariant of exactly the shape the
// 2026-07-28 lesson is about. This test is the assertion.
//
// # What it grades, and what it cannot see
//
// A call graph over the NON-TEST sources of package pipeline, keyed on
// RECEIVER TYPE + function name so two methods sharing a name on different
// types stay distinct. Edges are (a) calls on the enclosing function's own
// receiver, (b) calls to package-level functions, and (c) the two interface
// methods in [gradedInterfaceMethods]. Three negative claims (the invariant)
// and four positive controls (proof the walker can find a path when one
// exists, so a negative result means "no path" rather than "broken walker").
//
// Scope, stated rather than implied: the walk is package-local — it does not
// follow a call into another package — and a route built by storing a method
// value in a struct field and invoking it under a different name would be
// invisible to it. That residual is why the postgres reader ALSO refuses at
// the point of harm (errSchemaEscape in row_reader.go): this gate catches the
// wiring change at review time, the refusal catches anything that reaches the
// reader anyway.
func TestSpanningSnapshotNeverReachesTheParallelColdStartLane(t *testing.T) {
	callees := pipelineCallGraph(t)

	// Anti-vacuity floor. A walker that stopped matching the package would
	// pass every negative claim below for free.
	const minFuncs = 200
	if len(callees) < minFuncs {
		t.Fatalf("call graph has %d functions (floor %d) — the walker is not seeing package pipeline, "+
			"and every negative claim below would pass vacuously", len(callees), minFuncs)
	}
	for _, seed := range []string{
		"Streamer.coldStart",
		"Streamer.coldStartMultiDatabase",
		"Streamer.runColdStartParallel",
		"Streamer.coldStartCopyOneDatabase",
	} {
		if _, ok := callees[seed]; !ok {
			t.Fatalf("seed %q is no longer a declared function in package pipeline — this gate is stale "+
				"and grades nothing; re-derive the lane names before trusting it", seed)
		}
	}

	// Positive controls FIRST: each of these paths exists today, so a walker
	// that found nothing would fail here rather than silently bless the
	// negatives below.
	controls := []struct {
		from, to, why string
	}{
		{"Streamer.coldStart", "Streamer.runColdStartParallel", "the single-schema cold start IS the fast lane's only caller"},
		{"Streamer.runColdStartParallel", "iface.ImportSnapshot", "the fast lane mints its parallel readers through the importer"},
		{"Streamer.coldStartMultiDatabase", "iface.OpenMultiDatabaseSnapshotStream", "the multi-schema cold start IS the spanning opener's only caller"},
		{"Streamer.coldStartMultiDatabase", "Streamer.coldStartCopyOneDatabase", "the multi-schema cold start copies serially, per schema"},
	}
	for _, c := range controls {
		if !reaches(callees, c.from, c.to) {
			t.Fatalf("control path %s -> %s is NOT reachable in the call graph (%s). The walker is broken or "+
				"the lane was renamed; the negative claims in this test are meaningless until this passes",
				c.from, c.to, c.why)
		}
	}

	// The invariant.
	forbidden := []struct {
		from, to, harm string
	}{
		{
			"Streamer.coldStartMultiDatabase", "Streamer.runColdStartParallel",
			"the multi-schema cold start would hand a SPANNING snapshot to the ADR-0079 fast lane, whose " +
				"importer-minted readers are single-schema (qualifyBySchema=false)",
		},
		{
			"Streamer.coldStartMultiDatabase", "iface.ImportSnapshot",
			"the multi-schema cold start would mint importer readers, which do not qualify by the table's own schema",
		},
		{
			"Streamer.coldStart", "iface.OpenMultiDatabaseSnapshotStream",
			"the single-schema cold start — the fast lane's caller — would be opening a SPANNING stream",
		},
	}
	for _, f := range forbidden {
		if reaches(callees, f.from, f.to) {
			t.Errorf("%s now reaches %s.\n"+
				"ADR-0075's consistency argument rests on these two lanes staying disjoint: %s.\n"+
				"Either re-gate the fast lane on the stream being non-spanning, or teach the snapshot importer "+
				"to mint qualifyBySchema readers — do not just widen this test.",
				f.from, f.to, f.harm)
		}
	}
}

// pipelineCallGraph parses every non-test .go file in this package and returns
// a node -> called-node set. Nodes are "RecvType.Func" for methods and ".Func"
// for package-level functions, plus the synthetic "iface.Method" nodes for
// [gradedInterfaceMethods]. Function literals contribute to their ENCLOSING
// function, which is what makes the fast lane's readerFactory closure visible.
func pipelineCallGraph(t *testing.T) map[string]map[string]bool {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	out := map[string]map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			recvType, recvName := receiverOf(fn)
			node := recvType + "." + fn.Name.Name
			calls := out[node]
			if calls == nil {
				calls = map[string]bool{}
				out[node] = calls
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch v := call.Fun.(type) {
				case *ast.Ident:
					calls["."+v.Name] = true
				case *ast.SelectorExpr:
					if gradedInterfaceMethods[v.Sel.Name] {
						calls["iface."+v.Sel.Name] = true
						return true
					}
					// Only follow a call on this function's OWN receiver;
					// any other selector could be any type in the tree.
					if id, ok := v.X.(*ast.Ident); ok && recvName != "" && id.Name == recvName {
						calls[recvType+"."+v.Sel.Name] = true
					}
				}
				return true
			})
		}
	}
	return out
}

// receiverOf returns the receiver's base type name (pointer stripped) and the
// receiver variable's name. Both are empty for a package-level function.
func receiverOf(fn *ast.FuncDecl) (typeName, varName string) {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return "", ""
	}
	field := fn.Recv.List[0]
	expr := field.Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		typeName = id.Name
	}
	if len(field.Names) > 0 {
		varName = field.Names[0].Name
	}
	return typeName, varName
}

// reaches reports whether to is transitively called from from.
func reaches(callees map[string]map[string]bool, from, to string) bool {
	seen := map[string]bool{from: true}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for callee := range callees[cur] {
			if callee == to {
				return true
			}
			if !seen[callee] {
				seen[callee] = true
				queue = append(queue, callee)
			}
		}
	}
	return false
}
