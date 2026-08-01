// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The copy-phase WIRING gate: a capability the operator can ask for must
// reach the orchestrator from a field, never from a constant.
//
// # Why this exists
//
// [TestCopyPhaseFlagParityMigratorStreamer] proves a copy-phase flag EXISTS
// on both the Migrator and the Streamer. It cannot prove the value gets
// anywhere. Both halves of that gap shipped, and both were invisible:
//
//   - [Streamer.runColdStartParallel] — the ADR-0079 fast path, which is the
//     DEFAULT cold-start for any Postgres source that exports a shareable
//     snapshot — passed literal `false` for runBulkCopyPhases' upfrontIndexes
//     and analyzeAfter arguments, with a comment calling both "a migrate-path
//     flag". They were live `sync start` flags, and the SERIAL sibling had
//     threaded them since item 111. So `sync start --upfront-indexes` against
//     Postgres was silently ignored while the struct-field gate stayed green.
//   - The multi-database cold-copy's [bulkCopyOpts] literal omitted
//     CopyFanoutDegree, so --copy-fanout-degree was inert on that path.
//
// The class is "the capability is declared, and the call site substitutes a
// constant (or nothing) for it". This file gates the two crossings where the
// substitution is possible: runBulkCopyPhases' argument list, and the
// bulkCopyOpts literal. Both walks carry an anti-vacuity floor and both
// resolve their targets BY NAME, so a rename fails the gate loudly instead of
// silently checking the wrong argument.

// copyPhaseKnobParams are runBulkCopyPhases parameters that carry an
// operator-facing copy-phase capability. Each must be passed from a field at
// every call site. Parameters that are legitimately constant at a call site
// (`resuming` is `false` for a fresh cold start by construction) are
// deliberately absent — the policy is the list, the walk is the mechanism.
var copyPhaseKnobParams = []string{"upfrontIndexes", "analyzeAfter"}

// bulkCopyOptsKnobs are the [bulkCopyOpts] fields every PRODUCTION
// construction site must set, or record a reason for omitting.
var bulkCopyOptsKnobs = []string{
	"SkipSchemaApply",
	"CreateSchema",
	"Redactor",
	"Shard",
	"UpfrontIndexes",
	"AnalyzeAfter",
	"CopyFanoutDegree",
	"NoIntraTableStealing",
}

// bulkCopyOptsOmissionReason records, per "file.go:Field", why a production
// bulkCopyOpts literal leaves a knob unset. Every entry must name a reason the
// field is PROVABLY zero on that path — "not needed" is not a reason.
var bulkCopyOptsOmissionReason = map[string]string{
	// Multi-database sync refuses both of these at validateMultiDatabaseStream
	// (ADR-0074) before any copy runs, so both are provably zero here; passing
	// them would imply a support this path explicitly does not have.
	"streamer_multidb.go:Shard":           "validateMultiDatabaseStream REFUSES --inject-shard-column in multi-database mode (ADR-0074), so ShardColumnSpec is provably un-engaged on this path",
	"streamer_multidb.go:SkipSchemaApply": "validateMultiDatabaseStream REFUSES --schema-already-applied in multi-database mode (ADR-0074); the multi-database cold-start owns per-namespace creation",
	"streamer_multidb.go:CreateSchema":    "the ADR-0166 pre-create shape gate that produces a create SUBSET runs on the single-database cold-start only (coldStartGatePreflight); multi-database creates every in-scope table, which nil already means",
}

// parsePipelineFiles parses every non-test .go file in the package.
func parsePipelineFiles(t *testing.T) (fset *token.FileSet, files map[string]*ast.File) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset = token.NewFileSet()
	files = map[string]*ast.File{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		files[name] = f
	}
	if len(files) < 20 {
		t.Fatalf("parsed only %d non-test files in internal/pipeline — the walk is not seeing the package", len(files))
	}
	return fset, files
}

// paramIndexes resolves a function's parameter names to their positional
// index, expanding grouped declarations (`schema, createSchema *ir.Schema`).
func paramIndexes(fn *ast.FuncDecl) map[string]int {
	idx := map[string]int{}
	pos := 0
	for _, field := range fn.Type.Params.List {
		if len(field.Names) == 0 {
			pos++
			continue
		}
		for _, n := range field.Names {
			idx[n.Name] = pos
			pos++
		}
	}
	return idx
}

func TestCopyPhaseKnobsReachTheOrchestratorFromAField(t *testing.T) {
	fset, files := parsePipelineFiles(t)

	// Locate runBulkCopyPhases and resolve the knob parameters BY NAME. A
	// rename or a reordered signature fails here rather than silently
	// checking a different argument (the position-indexing trap).
	var decl *ast.FuncDecl
	for _, f := range files {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Name.Name == "runBulkCopyPhases" {
				decl = fn
			}
		}
	}
	if decl == nil {
		t.Fatal("runBulkCopyPhases not found — the gate has stopped matching the orchestrator entry point; re-point it")
	}
	idx := paramIndexes(decl)
	want := map[int]string{}
	for _, p := range copyPhaseKnobParams {
		i, ok := idx[p]
		if !ok {
			t.Fatalf("runBulkCopyPhases has no parameter named %q — it was renamed or removed; update copyPhaseKnobParams "+
				"(leaving it stale would make the gate check nothing)", p)
		}
		want[i] = p
	}

	callSites := 0
	var offenders []string
	for name, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok || fn.Name != "runBulkCopyPhases" {
				return true
			}
			callSites++
			for i, param := range want {
				if i >= len(call.Args) {
					offenders = append(offenders, fmt.Sprintf("%s: call passes only %d args; no %s argument", name, len(call.Args), param))
					continue
				}
				if isFieldSourced(call.Args[i]) {
					continue
				}
				pos := fset.Position(call.Args[i].Pos())
				offenders = append(offenders, fmt.Sprintf("%s:%d  %s is not field-sourced", filepath.Base(pos.Filename), pos.Line, param))
			}
			return true
		})
	}

	if callSites < 2 {
		t.Fatalf("found only %d runBulkCopyPhases call site(s) (floor 2: migrate + the ADR-0079 fast sync cold-start); "+
			"the walk is vacuous — re-point it", callSites)
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("%d runBulkCopyPhases argument(s) do not come from a field:\n  %s\n\n"+
			"An operator-facing copy-phase capability passed as a CONSTANT is silently inert — the flag parses, the "+
			"struct carries it, and the value never arrives (the ADR-0079 fast cold-start hardcoded `false` for both "+
			"--upfront-indexes and --analyze-after while the serial sibling honoured them). Pass s.X / m.X.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

// isFieldSourced reports whether an argument expression reads a struct field
// (s.UpfrontIndexes, opts.AnalyzeAfter) rather than being a literal constant.
func isFieldSourced(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	_, ok = sel.X.(*ast.Ident)
	return ok
}

func TestBulkCopyOptsProductionSitesSetEveryKnob(t *testing.T) {
	_, files := parsePipelineFiles(t)

	type site struct {
		file string
		set  map[string]bool
	}
	var sites []site
	for name, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			id, ok := lit.Type.(*ast.Ident)
			if !ok || id.Name != "bulkCopyOpts" {
				return true
			}
			s := site{file: name, set: map[string]bool{}}
			for _, elt := range lit.Elts {
				if kv, ok := elt.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok {
						s.set[k.Name] = true
					}
				}
			}
			sites = append(sites, s)
			return true
		})
	}

	if len(sites) < 2 {
		t.Fatalf("found only %d production bulkCopyOpts literal(s) (floor 2: the single-database and multi-database "+
			"sync cold-starts); the walk is vacuous — re-point it", len(sites))
	}

	for _, s := range sites {
		for _, knob := range bulkCopyOptsKnobs {
			if s.set[knob] {
				continue
			}
			key := s.file + ":" + knob
			if strings.TrimSpace(bulkCopyOptsOmissionReason[key]) == "" {
				t.Errorf("%s builds a bulkCopyOpts without setting %s — the knob silently takes its zero value on that "+
					"cold-start path (--copy-fanout-degree was inert on the multi-database copy this way). Set it, or "+
					"record %q in bulkCopyOptsOmissionReason with the reason it is PROVABLY zero there.", s.file, knob, key)
			}
		}
	}

	// Stale-entry hygiene: an omission reason for a knob that is now set, or
	// for a file that no longer builds a bulkCopyOpts, must be dropped.
	for key, reason := range bulkCopyOptsOmissionReason {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			t.Errorf("bulkCopyOptsOmissionReason key %q is malformed; want \"file.go:Field\"", key)
			continue
		}
		matched := false
		for _, s := range sites {
			if s.file != parts[0] {
				continue
			}
			matched = true
			if s.set[parts[1]] {
				t.Errorf("bulkCopyOptsOmissionReason[%q] records an omission, but the site now SETS %s — drop the "+
					"stale entry (reason was: %s)", key, parts[1], reason)
			}
		}
		if !matched {
			t.Errorf("bulkCopyOptsOmissionReason[%q] matches no production bulkCopyOpts literal — the file was renamed "+
				"or the literal removed; drop or update the stale entry", key)
		}
	}
}
