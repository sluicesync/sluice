// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// EVERY ORCHESTRATOR THAT CARRIES ITS OWN `Mappings` AND HANDS THE MAPPED
// SCHEMA TO A CDC LANE MUST ASK preflightBinaryTypeOverrideOnCDC.
//
// # Why this gate exists (and why "there are only two" is the reason, not
// the counter-argument)
//
// Audit B-2's CDC half is closed by refusal: a `--type-override` onto a
// binary target type cannot be applied faithfully on a CDC lane, because
// the change applier resolves column descriptors from the TARGET's
// information_schema where the override left no trace. The refusal
// shipped wired into `sync` alone. `schema add-table` carries the SAME
// field, applies it with the same [translate.ApplyMappings] call, creates
// the target column from the mapped type, and then hands the table to the
// LIVE stream (ADR-0030) — so the identical configuration reproduced the
// identical divergence, at exit 0, with matching row counts.
//
// Two implementors is exactly the size at which a roster gets skipped and
// then regretted (CLAUDE.md's sibling-sweep step: "a guard that reached
// dispatchRow and not dispatchCDCRow"). So the population is DERIVED, not
// listed: every struct declared in this package with a
// `Mappings []config.Mapping` field. A new orchestrator that grows the
// field fails this test until someone states which side of the line it is
// on.
//
// # Scope, stated so the name cannot be read as broader than the truth
//
//   - It proves the CALL EXISTS on a method of each enforcing type, for
//     BOTH halves of the refusal (see mappingsCDCLanePreflights). It does
//     not prove the call precedes the work — the config-keyed half is
//     pinned behaviourally by TestAddTableRefusesBinaryTypeOverride,
//     which asserts no writer was opened first.
//   - The SCHEMA-keyed half has its own stated reach in its doc comment,
//     and this roster does NOT widen it: the multi-database cold-start
//     fan-out and warm resume are Streamer paths that do not run it, and
//     a per-TYPE roster cannot see that. It grades "does this type ask
//     the question anywhere", not "on every path".
//   - The population is every struct in THIS package. A CDC-lane
//     orchestrator that lived in another package and carried mappings
//     would be invisible here; none does today (the field is declared in
//     five places, all of them in this file's package).
var mappingsCDCLaneRoster = map[string]string{
	// ---- Enforced: hands the mapped schema to a CDC lane. ----
	"Streamer": "", // `sync`: cold-start copy AND the continuous applier
	"AddTable": "", // `schema add-table`: copies, then joins the LIVE stream

	// ---- Exempt, with the reason. ----
	"Migrator": "`migrate` has no CDC lane at all: the override rewrites the type the READER " +
		"decodes with, so the value reaches the writer as []byte and provenance is decided end to " +
		"end. Deliberately NOT refused — refusing it would break ADR-0024's recommended binary_uuid " +
		"override for one-shot migrations",
	"Differ": "`schema diff` is read-only: it applies the mappings to compute the EXPECTED shape and " +
		"compares it against the target catalog. It writes no value anywhere",
	"Previewer": "`schema preview` is read-only: it renders the DDL the mappings would produce. It " +
		"opens no target connection",
}

// mappingsCDCLanePreflights is every refusal an enforced entry point
// must ask. They are separate functions on purpose — one reads the
// FLAGS of this invocation and needs no connection, the other reads the
// two SCHEMAS and needs the target catalog — and the second exists
// because the first cannot see a binary target column that an EARLIER
// run created. Rostering them together is what stops the next arrival
// from landing on one entry point's call sites and missing the other's;
// that is exactly how the add-table door stayed open.
var mappingsCDCLanePreflights = []string{
	"preflightBinaryTypeOverrideOnCDC",
	"preflightBinaryTargetColumnsOnCDC",
}

// mappingsCDCLaneEnforced is the subset that must call the preflights —
// derived from the roster so the two lists cannot drift apart.
func mappingsCDCLaneEnforced() map[string]bool {
	out := map[string]bool{}
	for name, reason := range mappingsCDCLaneRoster {
		if reason == "" {
			out[name] = true
		}
	}
	return out
}

const (
	// mappingsCDCLaneStructFloor is the anti-vacuity floor on the derived
	// population. A walker that stopped matching the field type (a rename
	// of config.Mapping, an import alias, a switch to a named slice type)
	// would otherwise report a clean roster over an EMPTY set forever —
	// the shape that makes a gate defend the defect.
	//
	// Both floors sit BELOW the current count on purpose (five structs,
	// two callers today). A floor set AT the count would fire first on an
	// ordinary deletion and hide the specific, actionable assertion
	// underneath it — the mutation run for the add-table door reported
	// "only 1 type calls the preflight" instead of naming AddTable, which
	// is a worse diagnostic for the failure that will actually happen.
	mappingsCDCLaneStructFloor = 3
	// mappingsCDCLaneEnforcedFloor is the same floor on the other side:
	// a rename of the preflight itself finds ZERO callers and must fail
	// loudly rather than bless everything by finding nothing.
	mappingsCDCLaneEnforcedFloor = 1
	// mappingsCDCLaneFileFloor guards the file walk itself.
	mappingsCDCLaneFileFloor = 20
)

func TestMappingsCDCLaneRoster(t *testing.T) {
	structs, callersByPreflight, parsed := scanMappingsCDCLane(t, ".")

	if parsed < mappingsCDCLaneFileFloor {
		t.Fatalf("parsed only %d non-test files — the walk is not seeing the package", parsed)
	}
	if len(structs) < mappingsCDCLaneStructFloor {
		t.Fatalf(
			"discovered only %d structs carrying `Mappings []config.Mapping` (floor %d) — the walker "+
				"is broken, not the tree. Found: %v",
			len(structs), mappingsCDCLaneStructFloor, sortedStrings(structs),
		)
	}

	enforced := mappingsCDCLaneEnforced()

	for _, preflight := range mappingsCDCLanePreflights {
		callers := callersByPreflight[preflight]
		if len(callers) < mappingsCDCLaneEnforcedFloor {
			t.Fatalf(
				"only %d type(s) call %s (floor %d). Either it was renamed (re-point this gate) or "+
					"the refusal has been dropped — in which case a binary column is silently accepted "+
					"on a CDC lane again, and the first CDC update to a `\\x`+hex value stores it SHORT. "+
					"Found: %v",
				len(callers), preflight, mappingsCDCLaneEnforcedFloor, sortedStrings(callers),
			)
		}

		for _, name := range sortedStrings(structs) {
			reason, listed := mappingsCDCLaneRoster[name]
			if !listed {
				continue // reported once, below
			}
			if enforced[name] && !callers[name] {
				t.Errorf(
					"%s is rostered as ENFORCED but no method of it calls %s.\n"+
						"That is audit B-2's CDC half, re-opened for this entry point: its own copy phase\n"+
						"stores the source's bytes and the CDC applier stores the hex reading of the same\n"+
						"value — `\\xdead` as 2 bytes where the source held 6 — with no error on either side.\n"+
						"callers found: %v", name, preflight, sortedStrings(callers),
				)
			}
			if !enforced[name] && callers[name] {
				t.Errorf(
					"%s is rostered as EXEMPT (%q) but DOES call %s. One of the two is wrong; the "+
						"roster is the place the answer is written down.", name, reason, preflight,
				)
			}
		}
	}

	for _, name := range sortedStrings(structs) {
		if _, listed := mappingsCDCLaneRoster[name]; listed {
			continue
		}
		t.Errorf(
			"%s carries `Mappings []config.Mapping` and is NOT on mappingsCDCLaneRoster.\n"+
				"Decide: does it hand the mapped schema to a CDC lane (a change applier, a live\n"+
				"stream)? If yes, call both preflights in mappingsCDCLanePreflights — a binary target\n"+
				"column cannot be applied faithfully there, because the applier reads column types\n"+
				"from the TARGET and cannot see what made the column binary. If no, add it with the\n"+
				"reason.", name,
		)
	}

	for name := range mappingsCDCLaneRoster {
		if !structs[name] {
			t.Errorf(
				"mappingsCDCLaneRoster lists %s but no such struct carries `Mappings []config.Mapping` "+
					"any more — remove the entry. A stale blessing is how a roster starts covering less "+
					"than its name implies.", name,
			)
		}
	}
}

// scanMappingsCDCLane parses every non-test .go file in dir and returns
// the set of struct type names carrying a `Mappings []config.Mapping`
// field, and — per preflight in [mappingsCDCLanePreflights] — the set of
// receiver type names with a method that calls it.
func scanMappingsCDCLane(t *testing.T, dir string) (structs map[string]bool, callers map[string]map[string]bool, parsed int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	structs = map[string]bool{}
	callers = map[string]map[string]bool{}
	for _, p := range mappingsCDCLanePreflights {
		callers[p] = map[string]bool{}
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		parsed++
		for _, d := range f.Decls {
			switch decl := d.(type) {
			case *ast.GenDecl:
				collectMappingsStructs(decl, structs)
			case *ast.FuncDecl:
				if decl.Body == nil || decl.Recv == nil || len(decl.Recv.List) == 0 {
					continue
				}
				// receiverTypeName is shared with the chunk-ceiling gate
				// next door — one unwrapper for `(s *Streamer)`.
				recv := receiverTypeName(decl.Recv.List[0].Type)
				if recv == "" {
					continue
				}
				for _, p := range mappingsCDCLanePreflights {
					if callsFunc(decl, p) {
						callers[p][recv] = true
					}
				}
			}
		}
	}
	return structs, callers, parsed
}

// collectMappingsStructs records every struct type in decl that declares
// a `Mappings []config.Mapping` field.
func collectMappingsStructs(decl *ast.GenDecl, out map[string]bool) {
	if decl.Tok != token.TYPE {
		return
	}
	for _, spec := range decl.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok || st.Fields == nil {
			continue
		}
		for _, field := range st.Fields.List {
			if !fieldIsNamed(field, "Mappings") || !isConfigMappingSlice(field.Type) {
				continue
			}
			out[ts.Name.Name] = true
		}
	}
}

func fieldIsNamed(f *ast.Field, want string) bool {
	for _, n := range f.Names {
		if n.Name == want {
			return true
		}
	}
	return false
}

// isConfigMappingSlice matches `[]config.Mapping`. A named slice type or
// an aliased import would not match — which the struct floor above turns
// into a loud failure rather than a silent pass.
func isConfigMappingSlice(expr ast.Expr) bool {
	arr, ok := expr.(*ast.ArrayType)
	if !ok || arr.Len != nil {
		return false
	}
	sel, ok := arr.Elt.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Mapping" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "config"
}

// callsFunc reports whether fn contains a call to the package-level
// function named name.
func callsFunc(fn *ast.FuncDecl, name string) bool {
	found := false
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == name {
			found = true
		}
		return true
	})
	return found
}

func sortedStrings(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
