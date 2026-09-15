// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// scopeExemptMarker is how a refusal that must fire BEFORE the scope gate
// declares itself. It sits at the site — in the comment block immediately
// above the return, or on the return's own lines — and carries a reason,
// the lineageExemptMarker shape this package has already proven. Today
// there is exactly one legitimate bearer: the binlog reader's
// "rows event for unknown table_id" floor, which fires before the table's
// identity is known and therefore before the question can be asked.
const scopeExemptMarker = "scope-exempt:"

// scopeGateAskers are the identifiers whose presence in an `if` condition
// makes that statement THE scope gate of a dispatcher. Named rather than
// pattern-matched, like the session-TZ roster's list, so a fourth spelling
// is a deliberate edit here.
var scopeGateAskers = map[string]bool{
	"tableInScope":        true,
	"vstreamTableInScope": true,
	"scopeAllowed":        true,
}

// EVERY STREAM-KILLING REFUSAL A ROW DISPATCHER CAN REACH SITS BEHIND
// THE TABLE-SCOPE GATE.
//
// # The class, and why a per-refusal roster could not hold it
//
// The sync's table filter is applied one stage downstream of this
// engine's CDC readers, so a reader sees every table in the bound
// database and a refusal that runs before the filter kills the stream
// over tables the operator excluded — a working configuration, refused,
// with a remedy that cannot run because excluding the table changes
// nothing the reader sees. Bug 246 closed it for the XA refusal; audit
// 2026-09-09 A0909-AQ-M-2 for the session-time_zone refusal, with a
// roster keyed on THAT refusal's name; audit 2026-09-15 A0915-ARCH-MEDIUM-3 then found
// three more siblings on the same code path — the TABLE_MAP shape guard,
// the TINYINT(1) range refusal (both CDC lanes), the partial-image belt
// — and the session-TZ roster said, honestly, that it could not see them.
//
// A roster keyed on refusal names can only ever grade the refusals it
// names. This one grades the EXITS instead: every `return` in each row
// dispatcher that does not return the literal nil is a way the stream can
// die, whatever produced the error — a coded refusal, a decode failure,
// a malformed-event floor, a future belt nobody has written yet. Each one
// must sit lexically AFTER the dispatcher's scope gate, or carry the
// exempt marker with a reason.
//
// # What makes "after" a proof rather than a proxy
//
// The gate is required to be a TOP-LEVEL statement of the function body
// — a direct child, not nested in a loop or branch — of the shape
// `if <asker...> { return nil }`. A top-level early return dominates
// everything below it, so a return positioned after the gate is reached
// only when the table is in scope. Nesting the gate, or turning its body
// into anything but `return nil`, fails the walk.
//
// # What this roster reaches, stated so the name cannot be read wider
//
// Every method named dispatchRows / dispatchRow / dispatchCDCRow in this
// package's non-test files — the three row dispatchers, one per CDC lane
// (binlog, VStream standalone, VStream cold-start snapshot) — and only the
// returns lexically inside those bodies. A refusal inside a CALLEE is
// graded through the call site's `return err`; a refusal reached from a
// different dispatcher (DDL, FIELD, schema-snapshot arms) is not graded
// here — the session-TZ roster holds the schema-refusal sites, and DDL
// events carry no row to mis-decode.
func TestStreamKillingRefusalsInDispatchAreScopeGated(t *testing.T) {
	t.Parallel()

	dispatchers := map[string]bool{"dispatchRows": true, "dispatchRow": true, "dispatchCDCRow": true}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()

	type graded struct {
		site    string
		gated   bool
		exempt  bool
		message string
	}
	var sites []graded
	found := 0

	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		lines := strings.Split(string(src), "\n")
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv == nil || !dispatchers[fn.Name.Name] {
				continue
			}
			found++
			where := path + ":" + fn.Name.Name

			gate := findScopeGate(fn.Body)
			if gate == nil {
				t.Errorf("%s has no scope gate: no top-level `if <tableInScope|vstreamTableInScope|scopeAllowed…> "+
					"{ return nil }` statement. Every refusal in a row dispatcher kills the stream, and the sync's "+
					"table filter runs one stage DOWNSTREAM, so without the gate each of them fires on tables the "+
					"operator excluded (Bug 246 → A0909-AQ-M-2 → audit 2026-09-15 A0915-ARCH-MEDIUM-3).", where)
				continue
			}
			gateEnd := gate.End()

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ret, ok := n.(*ast.ReturnStmt)
				if !ok || returnsLiteralNil(ret) {
					return true
				}
				line := fset.Position(ret.Pos()).Line
				g := graded{site: where + ":" + strconv.Itoa(line)}
				switch {
				case ret.Pos() > gateEnd:
					g.gated = true
				default:
					window := siteWindow(lines, line, fset.Position(ret.End()).Line)
					idx := strings.Index(window, scopeExemptMarker)
					if idx < 0 {
						g.message = "this return can kill the stream and sits BEFORE the dispatcher's scope gate, so it fires " +
							"on tables the sync excludes. Move it below the gate, or — only if the table's identity is " +
							"genuinely unknown at this point — write `" + scopeExemptMarker + " <why>` on the site."
					} else if reason := strings.TrimSpace(window[idx+len(scopeExemptMarker):]); len(reason) < 30 {
						g.message = "carries " + scopeExemptMarker + " with no real reason (" + reason + ")"
					} else {
						g.exempt = true
					}
				}
				sites = append(sites, g)
				return true
			})
		}
	}

	// Anti-vacuity, at the true count: three lanes, three dispatchers. A
	// walk that finds fewer has broken; re-point it rather than lower this.
	if found != 3 {
		t.Fatalf("found %d row dispatcher(s); this engine has THREE CDC lanes, each with one — the walk is "+
			"not seeing the package", found)
	}

	gatedCount, exemptCount := 0, 0
	var failures []string
	for _, g := range sites {
		switch {
		case g.gated:
			gatedCount++
		case g.exempt:
			exemptCount++
		default:
			failures = append(failures, g.site+": "+g.message)
		}
	}
	t.Logf("graded %d stream-killing returns across %d dispatchers: %d gated, %d exempt, %d ungated", len(sites), found, gatedCount, exemptCount, len(failures))
	sort.Strings(failures)
	for _, f := range failures {
		t.Error(f)
	}
	// The floor the audit asked for is four graded sites; the real number
	// is an order of magnitude higher (the binlog dispatcher alone has more
	// than ten). Pinned well above four so a walk that silently lost a
	// dispatcher's body still fails.
	if gatedCount < 12 {
		t.Errorf("only %d stream-killing returns are graded as gated across the three dispatchers; the binlog "+
			"lane alone has more than ten. The walk has lost a body.", gatedCount)
	}
	// Exactly one site may fire before the question can be asked — the
	// unknown-table_id floor. A second exemption is a second thing to
	// justify, out loud, here.
	if exemptCount != 1 {
		t.Errorf("%d scope-exempt sites; want exactly 1 (the binlog reader's unknown-table_id floor). A new "+
			"exemption needs its reason argued at the site AND this count raised deliberately.", exemptCount)
	}
}

// findScopeGate returns the dispatcher's scope gate — the first top-level
// `if` whose condition consults a scope asker and whose body is exactly
// `return nil` — or nil when the body has none. Top-level only: a nested
// gate does not dominate the returns that follow it.
func findScopeGate(body *ast.BlockStmt) *ast.IfStmt {
	for _, stmt := range body.List {
		ifs, ok := stmt.(*ast.IfStmt)
		if !ok || ifs.Init != nil || ifs.Else != nil {
			continue
		}
		asks := false
		ast.Inspect(ifs.Cond, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.Ident:
				if scopeGateAskers[node.Name] {
					asks = true
				}
			case *ast.SelectorExpr:
				if scopeGateAskers[node.Sel.Name] {
					asks = true
				}
			}
			return true
		})
		if !asks {
			continue
		}
		// The body may carry a debug log ahead of the return, nothing
		// else; the LAST statement must be the bare `return nil`.
		if len(ifs.Body.List) == 0 {
			continue
		}
		last, ok := ifs.Body.List[len(ifs.Body.List)-1].(*ast.ReturnStmt)
		if !ok || !returnsLiteralNil(last) {
			continue
		}
		return ifs
	}
	return nil
}

// returnsLiteralNil reports whether ret is `return nil` — the one exit
// shape that does not kill the stream. A bare `return` in a function that
// returns error cannot compile, so a result list of length one is the
// only shape to check.
func returnsLiteralNil(ret *ast.ReturnStmt) bool {
	if len(ret.Results) != 1 {
		return false
	}
	id, ok := ret.Results[0].(*ast.Ident)
	return ok && id.Name == "nil"
}
