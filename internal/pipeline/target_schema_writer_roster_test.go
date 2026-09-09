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

// EVERY FUNCTION THAT OPENS A TARGET ROW WRITER EITHER APPLIES THE
// OPERATOR'S `--target-schema` OR IS EXEMPT HERE, BY NAME, WITH A REASON.
//
// # What went wrong without it
//
// `--target-schema` (ADR-0031) is threaded onto a freshly-opened writer
// through [migcore.ApplyTargetSchema], which is a capability-only setter:
// an engine that never hears it keeps its DSN default. The PostgreSQL
// writer qualifies every statement it builds with its own schema field
// (buildRawCopyFromStmt, buildBatchInsert), so a writer that was never
// told addresses `public`.
//
// Audit 2026-09-09 P1 measured the consequence. Seven of the package's
// eight OpenRowWriter sites applied it and [openOneChunkConn] — the
// opener behind all three parallel chunk cores — did not, so on a PG
// target every table above the parallel threshold had chunk 0 land in
// the named schema while every peer chunk addressed `public`. The
// threshold is 80,000 divided by the table count with a floor of 10,000,
// which on any real schema means most tables took the broken path.
//
// The harm set has two arms and only one of them is loud. LOUD when
// `public.<table>` does not exist or its keys collide: the observed runs
// died on `SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING` and on a duplicate
// key. SILENT when `public.<table>` exists and accepts the rows — an
// operator who once migrated without the flag and re-runs into a
// schema, or a keyless table: the named schema holds one chunk, `public`
// holds the rest, exit 0, and CDC afterwards applies against the
// qualified table.
//
// # Why a roster rather than one more test
//
// The existing coverage was TestMigrate_PG_TargetSchema, whose fixtures
// are a handful of INSERT rows and therefore never cross the parallel
// threshold. Its doc-comment asserts "public stays empty" — an invariant
// nobody checked on the chunk path. Adding rows to that test (done in
// the same change) closes the one site; only a roster closes the class,
// and this one derives its universe from the code rather than a list
// someone maintains.
//
// # What it reaches, stated so the name cannot be read as broader
//
// The universe is every function in `internal/pipeline` and
// `internal/pipeline/backup`, non-test files only, whose body contains a
// call to a method named OpenRowWriter. It matches by NAME, not by type,
// so a writer opened through a differently-named method or in another
// package is outside it. Application is checked through the
// package-local call graph, so a site that applies through a helper
// passes; one that applies through a function value reads as missing,
// which is the false-positive direction a reviewer resolves by reading.
// It says nothing about SCHEMA writers or change appliers — those have
// their own wiring and their own gates.
func TestOpenRowWriterSitesApplyTargetSchema(t *testing.T) {
	// The one site that deliberately does not apply it. Its premise —
	// that SyncFromBackup has no target-schema field to apply — is
	// machine-checked by TestBrokerHasNoTargetSchemaToApply below, so
	// this exemption cannot quietly outlive its reason.
	exempt := map[string]string{
		"pipeline:(*SyncFromBackup).dropExistingTargetTables": "the broker lane has no --target-schema: " +
			"SyncFromBackup carries no such field and the CLI never sets one, so there is nothing to " +
			"apply. It opens a row writer only to reach ir.TableDropper. If the broker ever grows the " +
			"flag, this drop loop addresses the WRONG namespace and must apply it — the premise is " +
			"pinned by TestBrokerHasNoTargetSchemaToApply",
	}

	dirs := []string{".", "backup"}
	opens := map[string]string{} // "dir:decl" -> file
	calls := map[string]map[string]bool{}
	appliers := map[string]bool{} // decls calling ApplyTargetSchema directly
	parsed := 0
	fset := token.NewFileSet()

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			f, perr := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if perr != nil {
				t.Fatalf("parse %s/%s: %v", dir, name, perr)
			}
			parsed++
			pkg := dir
			if dir == "." {
				pkg = "pipeline"
			}
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				self := pkg + ":" + declIdentity(fn)
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch callee := call.Fun.(type) {
					case *ast.Ident:
						if calls[self] == nil {
							calls[self] = map[string]bool{}
						}
						calls[self][callee.Name] = true
					case *ast.SelectorExpr:
						switch callee.Sel.Name {
						case "OpenRowWriter":
							opens[self] = pkg + "/" + name
						case "ApplyTargetSchema":
							appliers[self] = true
						}
						if x, ok := callee.X.(*ast.Ident); ok && x.Name == receiverName(fn) {
							if calls[self] == nil {
								calls[self] = map[string]bool{}
							}
							calls[self][receiverType(fn)+"."+callee.Sel.Name] = true
						}
					}
					return true
				})
			}
		}
	}

	if parsed < 20 {
		t.Fatalf("parsed only %d non-test files across %v — the walk is not seeing the packages", parsed, dirs)
	}
	// Anti-vacuity, both halves. EIGHT sites exist today: migrate's
	// primary writer, the parallel chunk opener, add-table, the two
	// single-stream cold-start writers, the multi-database one, the
	// broker's drop loop, and restore's. A walk finding fewer has stopped
	// matching the call shape — re-point it rather than lowering this.
	if len(opens) < 8 {
		got := make([]string, 0, len(opens))
		for d := range opens {
			got = append(got, d)
		}
		sort.Strings(got)
		t.Fatalf("found %d function(s) calling OpenRowWriter (%v); at least 8 exist. The call shape this "+
			"gate matches has changed — re-point it, do not lower the floor.", len(opens), got)
	}
	if len(appliers) == 0 {
		t.Fatal("nothing in these packages calls ApplyTargetSchema — either it was renamed (re-point this " +
			"gate) or the threading surface is gone, in which case every assertion below is vacuous.")
	}

	applied := 0
	for decl, file := range opens {
		if reachesCall(decl, appliers, calls) {
			applied++
			if why, isExempt := exempt[decl]; isExempt {
				t.Errorf("%s (%s) is listed as exempt from applying --target-schema (%q) but now DOES "+
					"apply it — drop the exemption so the roster keeps meaning what it says", decl, file, why)
			}
			continue
		}
		if _, isExempt := exempt[decl]; isExempt {
			continue
		}
		t.Errorf("%s (%s) opens a target row writer but never reaches migcore.ApplyTargetSchema.\n\n"+
			"A writer that was never told the operator's --target-schema keeps its DSN default, and the "+
			"PostgreSQL writer qualifies every COPY/INSERT it builds with that schema — so this lane "+
			"writes to `public` while the lanes that DO apply it write to the named schema. Loud when "+
			"`public.<table>` is missing or collides; SILENT when it exists and accepts the rows, which "+
			"splits one table across two schemas at exit 0 (audit 2026-09-09 P1, OBSERVED on PG for the "+
			"parallel chunk opener). Call migcore.ApplyTargetSchema on the writer, or add %q to this "+
			"file's exempt map with the reason this lane cannot address the wrong namespace.",
			decl, file, decl)
	}
	// The seven applying sites must all still apply. Without this floor an
	// exemption map that grew to cover everything would pass.
	if applied < 7 {
		t.Errorf("only %d OpenRowWriter site(s) reach ApplyTargetSchema; floor 7 (migrate primary, "+
			"parallel chunk opener, add-table, cold-start, cold-start float repair, multi-database, "+
			"restore)", applied)
	}
	for decl := range exempt {
		if _, stillOpens := opens[decl]; !stillOpens {
			t.Errorf("the exempt map lists %s, but nothing by that name opens a target row writer any "+
				"more — drop the entry rather than leaving a stale blanket", decl)
		}
	}
}

// TestBrokerHasNoTargetSchemaToApply pins the PREMISE the broker's
// exemption above rests on.
//
// An exemption is only as good as the fact it cites, and this project has
// paid for exemptions whose reason was true when written and quietly
// stopped being true. The broker's reason is a fact about the code — that
// SyncFromBackup has no target-schema field — so it can be asserted
// rather than trusted. If the broker grows the flag, this fails and
// points at the drop loop that would then be deleting relations in the
// wrong namespace.
func TestBrokerHasNoTargetSchemaToApply(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "broker.go", nil, 0)
	if err != nil {
		t.Fatalf("parse broker.go: %v", err)
	}
	found := false
	for _, d := range f.Decls {
		gen, ok := d.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, s := range gen.Specs {
			ts, ok := s.(*ast.TypeSpec)
			if !ok || ts.Name.Name != "SyncFromBackup" {
				continue
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				t.Fatal("SyncFromBackup is no longer a struct; re-point this gate")
			}
			found = true
			for _, fld := range st.Fields.List {
				for _, n := range fld.Names {
					if strings.Contains(strings.ToLower(n.Name), "targetschema") {
						t.Errorf("SyncFromBackup now has a %s field, so the broker lane CAN carry a "+
							"target-schema override — but (*SyncFromBackup).dropExistingTargetTables is "+
							"exempted from applying it in TestOpenRowWriterSitesApplyTargetSchema on the "+
							"grounds that no such field exists. That drop loop now deletes relations in "+
							"whatever schema the DSN defaults to, which is not the one the operator "+
							"named. Apply it there and remove the exemption.", n.Name)
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("no SyncFromBackup type in broker.go — the exemption's premise cannot be checked; " +
			"re-point this gate or re-derive the exemption")
	}
}
