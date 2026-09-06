// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vitess.io/vitess/go/vt/proto/binlogdata"
)

// The STATEMENT-DML dispatcher roster (audit 2026-09-06 H1).
//
// WHY THIS EXISTS. The 2026-09-01 SLM-3 sweep gave the VStream lane the
// statement-DML refusal and enumerated the dispatchers by hand: three. There
// were four. copyStream.dispatchCopyEvent — the concurrent cross-table COPY
// pump (ADR-0099), engaged whenever vstream_copy_table_parallelism >= 2 —
// kept dropping INSERT/REPLACE/UPDATE/DELETE VEvents at its default arm, so
// on exactly the configuration that opts into throughput, a statement-format
// write during COPY vanished at exit 0. Every guard the sweep left behind
// (TestVStreamStatementDML's dispatcher map, the mirror-parity gate's two
// tracked types, the capture-completeness matrix row) named the same three.
// A hand list of three is what let the fourth through.
//
// WHAT THIS REACHES — stated so the name cannot be read as broader than the
// truth:
//
//   - Universe: every FUNCTION OR METHOD in this package's non-test .go files
//     that contains a `switch` with a `case binlogdata.VEventType_ROW` arm.
//     That is the mechanical definition of "a VEvent dispatcher that carries
//     user data", and it is derived from the source on every run — a fifth
//     dispatcher joins the roster by existing, not by being remembered.
//   - Requirement: that same switch must carry a case arm naming ALL FOUR
//     statement-DML VEvent types, and the arm's body must reach
//     statementDMLRefusal. A present-but-empty arm does not satisfy it.
//   - Required type set: derived from the PRODUCTION map
//     vstreamStatementDMLVerbs, not hand-listed here. If the vendored
//     vstreamer grows a fifth statement category and the map records it,
//     every dispatcher is held to it automatically.
//   - NOT reached: the binlog lane (its own roster,
//     TestDispatchEventRoster_EveryWriteBearingType), other engines, and the
//     BODIES of the refusal arms (the behavioural cells in
//     TestVStreamStatementDML pin those).
//
// Exemptions are fail-by-default: a dispatcher without the arm fails unless
// it is listed below WITH a reason.
func TestVStreamStatementDMLRoster_EveryVEventDispatcher(t *testing.T) {
	// exempt maps "recvType.MethodName" (or a bare function name) to the
	// architectural reason a VEvent dispatcher may drop statement-format DML.
	// Empty today: every dispatcher that can see one refuses it.
	exempt := map[string]string{}

	// The required arm's callee. All four dispatchers reach the shared
	// vstreamStatementDMLError through a per-type method of this name.
	const refusalCallee = "statementDMLRefusal"

	// Required case identifiers, derived from the production verb table.
	// binlogdata's generated String() yields the bare enum name ("INSERT"),
	// and the Go constant is VEventType_ + that name.
	wantTypes := map[string]bool{}
	for typ := range vstreamStatementDMLVerbs {
		wantTypes["VEventType_"+typ.String()] = true
	}
	if len(wantTypes) != len(vstreamStatementDMLVerbs) || len(wantTypes) < 4 {
		t.Fatalf("derived %d required case identifiers from %d verb-table entries; the enum naming assumption "+
			"(VEventType_ + String()) no longer holds, so this gate would require nothing", len(wantTypes), len(vstreamStatementDMLVerbs))
	}
	if !wantTypes["VEventType_"+binlogdata.VEventType_INSERT.String()] {
		t.Fatalf("the derived set %v does not contain the INSERT arm; re-point this gate", wantTypes)
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()

	type dispatcher struct {
		file  string
		types map[string]bool
		// armReachesRefusal is true when the case clause naming the
		// statement-DML types has statementDMLRefusal in its body.
		armReachesRefusal bool
	}
	found := map[string]*dispatcher{}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sw, ok := n.(*ast.SwitchStmt)
				if !ok {
					return true
				}
				types := map[string]bool{}
				reaches := false
				for _, stmt := range sw.Body.List {
					cc, ok := stmt.(*ast.CaseClause)
					if !ok {
						continue
					}
					armTypes := map[string]bool{}
					for _, expr := range cc.List {
						if id := vEventTypeIdent(expr); id != "" {
							types[id] = true
							armTypes[id] = true
						}
					}
					if !armTypes["VEventType_"+binlogdata.VEventType_INSERT.String()] {
						continue
					}
					ast.Inspect(cc, func(inner ast.Node) bool {
						if sel, ok := inner.(*ast.SelectorExpr); ok && sel.Sel.Name == refusalCallee {
							reaches = true
						}
						return true
					})
				}
				if !types["VEventType_"+binlogdata.VEventType_ROW.String()] {
					return true
				}
				key := funcRosterKey(fn)
				found[key] = &dispatcher{file: name, types: types, armReachesRefusal: reaches}
				return true
			})
		}
	}

	// Anti-vacuity floor. Four dispatchers exist today (the standalone
	// reader's CDC dispatch, the snapshot stream's post-COPY CDC dispatch,
	// its sequential COPY dispatch, and the concurrent COPY pump's). A parse
	// that finds fewer is not finding the switches, and every assertion below
	// would pass trivially.
	if len(found) < 4 {
		t.Fatalf("found %d VEvent dispatchers (a switch with a %s arm) in this package; want at least the four "+
			"that exist — this gate is not parsing what it thinks it is. Found: %v",
			len(found), "VEventType_ROW", rosterKeys(found))
	}

	// The roster's reach, printed so a reviewer can read what the gate
	// actually covers rather than what its name suggests.
	t.Logf("VEvent dispatcher roster (%d): %v", len(found), rosterKeys(found))

	for key, d := range found {
		if reason, ok := exempt[key]; ok {
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is exempted with an empty reason; an exemption without a written reason is a hand list", key)
			}
			continue
		}
		var missing []string
		for want := range wantTypes {
			if !d.types[want] {
				missing = append(missing, want)
			}
		}
		if len(missing) > 0 {
			t.Errorf("%s (%s) dispatches VEvents carrying rows but has no arm for %v. Under a source session with "+
				"binlog_format=STATEMENT the vendored vstreamer forwards these as typed VEvents carrying the SQL in "+
				"Dml; a dispatcher without the arm drops them at its default and the write is lost at exit 0 "+
				"(audit 2026-09-01 SLM-3, 2026-09-06 H1). Add the arm calling %s, or add %q to exempt with a reason.",
				key, d.file, missing, refusalCallee, key)
			continue
		}
		if !d.armReachesRefusal {
			t.Errorf("%s (%s) has the statement-DML case arm but its body never reaches %s — a present-but-inert "+
				"arm is the silent drop wearing the shape of a fix", key, d.file, refusalCallee)
		}
	}
}

// vEventTypeIdent returns the VEventType_* identifier a case expression
// names, or "" when the expression is not one.
func vEventTypeIdent(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "binlogdata" {
		return ""
	}
	if !strings.HasPrefix(sel.Sel.Name, "VEventType_") {
		return ""
	}
	return sel.Sel.Name
}

// funcRosterKey renders "recvType.Method" for a method and the bare name for
// a plain function.
func funcRosterKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return fn.Name.Name
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	if id, ok := typ.(*ast.Ident); ok {
		return id.Name + "." + fn.Name.Name
	}
	return fn.Name.Name
}

func rosterKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
