// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// pumpJoinVerdict is the grade a rostered type carries.
type pumpJoinVerdict string

const (
	// joinsItsGoroutine — Close waits for the goroutine to exit before it
	// releases anything the goroutine touches.
	joinsItsGoroutine pumpJoinVerdict = "joins"
	// exemptWithReason — Close does not join, and the reason says why that is
	// safe HERE. "Cancellation is enough" is not a reason; cancellation is
	// asynchronous and that is the whole defect.
	exemptWithReason pumpJoinVerdict = "exempt"
	// openFinding — the shape is present and NOT resolved. This is deliberately
	// not a third flavour of exemption: it records that someone looked, found
	// something, and did not fix it, so the roster cannot be read as an
	// all-clear. Its reason must name where the finding is filed.
	openFinding pumpJoinVerdict = "open"
)

type pumpJoinEntry struct {
	verdict pumpJoinVerdict
	reason  string
}

// pumpJoinRoster is fail-by-default over every type in internal/engines/** that
// BOTH launches a goroutine from one of its methods AND has a Close/close
// method. Adding such a type without an entry fails this test.
//
// Keys are "<package dir>.<TypeName>".
var pumpJoinRoster = map[string]pumpJoinEntry{
	"postgres.CDCReader": {
		joinsItsGoroutine,
		"pumpDone, joined before r.db is closed. Pinned by TestCDCReaderCloseJoinsPumpBeforeReleasingCatalogPool.",
	},
	"mysql.CDCReader": {
		joinsItsGoroutine,
		"pumpDone, joined before the syncer and the pool are closed. Pinned by the mysql close-join test.",
	},
	"mysql.vstreamCDCReader": {
		joinsItsGoroutine,
		"pumpDone via startPump/joinPump, joined in Close AND in applyReshardState — the latter is the one " +
			"with the data consequence (a straggling VGTID overwriting the post-reshard position). Pinned by " +
			"TestVStreamClose_JoinsThePump and TestApplyReshardState_JoinsThePumpBeforeReplacingTheLayout.",
	},
	"pgtrigger.CDCReader": {
		joinsItsGoroutine,
		"pumpDone, joined before r.db is closed. Pinned by TestCDCReaderClose_JoinsThePumpBeforeClosingThePool.",
	},
	"sqlite-trigger.CDCReader": {
		joinsItsGoroutine,
		"pumpDone, joined before r.exec is closed. Pinned by " +
			"TestCDCReaderClose_JoinsThePumpBeforeClosingTheExecutor.",
	},

	// --- Not CDC readers. The walker reaches them because the SHAPE is the
	// same; the grade below is why the shape is not the defect there.

	"mysql.RowReader": {
		exemptWithReason,
		"Close releases r.closer and NILS NOTHING the stream goroutine dereferences — the goroutine holds " +
			"its own *sql.Rows and writes r.err under r.mu. Closing the pool under an open Rows is a " +
			"database/sql-defined error on the next Next(), which the goroutine reports through Err(); it is " +
			"not a nil deref and not an unguarded field write. The CDC readers differ precisely in that they " +
			"nil the pool/executor the pump is about to use.",
	},
	"postgres.RowReader": {
		exemptWithReason,
		"Same shape and same reason as mysql.RowReader: Close delegates to r.closer, nils no field, and the " +
			"stream goroutine's only shared write (r.err) is mutex-guarded.",
	},
	"sqlite.RowReader": {
		exemptWithReason,
		"Same shape and same reason as mysql.RowReader.",
	},
	"sqlite.D1RowReader": {
		exemptWithReason,
		"Same shape and same reason as mysql.RowReader; the D1 transport is request/response, so there is no " +
			"long-lived handle for Close to pull out from under the goroutine.",
	},
	"mydumper.RowReader": {
		exemptWithReason,
		"Same shape and same reason as mysql.RowReader; the goroutine reads a file it opened itself.",
	},
	"mysql.RowWriter": {
		exemptWithReason,
		"Its goroutines are launched INSIDE WriteRowsIdempotentParallel, which wg.Wait()s before returning " +
			"(that join is ADR-0007's durability guarantee, not a lifecycle nicety). No goroutine outlives " +
			"the call, so Close cannot race one.",
	},
	"mysql.concurrentBinlogRows": {
		exemptWithReason,
		"Close snapshots r.readers under connMu before closing them — the audit 2026-08-01 P2 sweep already " +
			"took this one, deliberately NOT resting on 'no caller does that today'. It nils nothing, and the " +
			"stream goroutines' shared writes go through errMu / the cursor map's own lock.",
	},
	"mysql.vstreamSnapshotStream": {
		openFinding,
		"CONFIRMED same shape, NOT fixed: close() cancels the gRPC context, broadcasts cond and closes " +
			"s.conn without joining the COPY pumps. It is not the one-line fix the CDC readers were — the " +
			"pumps coordinate through a sync.Cond plus a copyComplete flag, so a join has to be placed " +
			"inside that protocol and validated on a real Vitess cluster, which is a chunk of its own. " +
			"Filed in docs/dev/audit-backlog.md under the 2026-08-08 invariant sweep.",
	},
}

// TestEngineClosesJoinTheGoroutinesTheyStart is the sibling-sweep gate for the class
// "Close tears down state a goroutine it started still owns".
//
// # Why a roster and not three tests
//
// The class has now surfaced three times, one engine at a time, and each time
// the fix shipped describing itself as closing the class: the PG CDC reader's
// documented promise to join its pump that it did not keep (2026-07-28),
// sqlite-trigger's Close that nil'd the executor under a live pump
// (2026-08-07, found by accident), and — the sweep that produced this file —
// pgtrigger and the VStream reader, which had the identical shape and were
// named by nothing. A commit that says it fixed a class owes the enumeration;
// this file IS the enumeration, so the next one cannot be a promise.
//
// # What it grades, and what it does not
//
// A type qualifies when a non-test file under internal/engines/** declares a
// method on it containing a `go` statement, AND declares a Close or close
// method on it. Both halves are syntactic, and that is the point — the walker
// finds the shape, a human grades it.
//
// The join signal is deliberately broad: a channel receive, a call whose name
// contains "join", or a WaitGroup Wait, anywhere in the Close body or in a
// method Close calls on its own receiver. A broad signal cannot prove a join is
// CORRECT — only that one is present. Correctness is what the per-engine pins
// named in each roster reason are for, and a roster entry without one of those
// is doing less work than it looks like it is doing.
//
// Known scope, stated so the name cannot be read as broader than the truth:
// it reaches only types under internal/engines/** that have BOTH halves of the
// shape. A type that spawns a goroutine and exposes teardown under some other
// name (Stop, Shutdown, Release) is invisible to it, as is every pipeline-level
// pump. It also cannot see a goroutine started by a package-level function
// rather than a method. Those are gaps, not decisions.
func TestEngineClosesJoinTheGoroutinesTheyStart(t *testing.T) {
	found := findPumpTypesWithClose(t)

	const typeFloor = 12
	if len(found) < typeFloor {
		t.Fatalf("discovered only %d goroutine-launching types with a Close (floor %d) — the walker is "+
			"broken, not the tree; every roster claim below would pass vacuously", len(found), typeFloor)
	}
	for _, pkg := range []string{"mysql", "postgres", "pgtrigger", "sqlite-trigger"} {
		hit := false
		for key := range found {
			if strings.HasPrefix(key, pkg+".") {
				hit = true
				break
			}
		}
		if !hit {
			t.Errorf("no goroutine-launching type with a Close found in the %q engine; the walker is not "+
				"reaching it, and its readers would silently fall out of this gate's scope", pkg)
		}
	}

	keys := make([]string, 0, len(found))
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		site := found[key]
		entry, listed := pumpJoinRoster[key]
		if !listed {
			t.Errorf("%s (%s) starts a goroutine and has a Close, and is NOT on the roster.\n"+
				"Close must JOIN that goroutine before releasing anything the goroutine reads — cancelling "+
				"is asynchronous, so a Close landing in the window between the cancel and the goroutine's "+
				"next field access races it (and, for a pool or executor that Close nils, dereferences nil "+
				"inside a goroutine nothing recovers).\n"+
				"Add a pumpJoinRoster entry: %q if it joins (naming the pin), or %q with a reason that is "+
				"not \"cancellation is enough\".", key, site, joinsItsGoroutine, exemptWithReason)
			continue
		}
		switch entry.verdict {
		case joinsItsGoroutine, exemptWithReason, openFinding:
		default:
			t.Errorf("%s: roster verdict %q is not one of %q / %q / %q",
				key, entry.verdict, joinsItsGoroutine, exemptWithReason, openFinding)
			continue
		}
		if strings.TrimSpace(entry.reason) == "" {
			t.Errorf("%s: roster entry has no reason; the reason IS the gate", key)
			continue
		}
		if entry.verdict == openFinding {
			// Logged, not passed over in silence: the roster must not read as
			// an all-clear while one of its rows is a known live defect.
			t.Logf("OPEN FINDING — %s (%s): %s", key, site, entry.reason)
		}
		if entry.verdict == joinsItsGoroutine && !site.closeJoins {
			t.Errorf("%s: rostered as %q, but its Close shows no join — no channel receive, no join call, "+
				"no WaitGroup Wait, in Close or in anything Close calls on its own receiver (%s).\n"+
				"Either the join was removed or the roster is stale; both are the defect this gate exists for.",
				key, joinsItsGoroutine, site)
		}
	}

	for key := range pumpJoinRoster {
		if _, ok := found[key]; !ok {
			t.Errorf("roster lists %s but no such goroutine-launching type with a Close exists any more — "+
				"remove the entry. A stale blessing is how a roster starts covering less than its name implies.", key)
		}
	}
}

type pumpSite struct {
	goStmtAt   string
	closeAt    string
	closeJoins bool
}

func (s pumpSite) String() string { return fmt.Sprintf("go at %s, Close at %s", s.goStmtAt, s.closeAt) }

// findPumpTypesWithClose walks internal/engines/** and returns the types that
// both launch a goroutine and declare a Close/close, keyed "<pkgdir>.<Type>".
func findPumpTypesWithClose(t *testing.T) map[string]pumpSite {
	t.Helper()

	type typeInfo struct {
		goStmtAt  string
		closeAt   string
		closeBody *ast.BlockStmt
		// methods on this type, by name, for the one-hop join lookthrough.
		methods  map[string]*ast.FuncDecl
		recvName map[string]string
	}
	infos := map[string]*typeInfo{}

	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		pkgDir := filepath.Base(filepath.Dir(path))
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) == 0 {
				continue
			}
			typeName, recvName := pumpReceiverOf(fn)
			if typeName == "" {
				continue
			}
			key := pkgDir + "." + typeName
			info := infos[key]
			if info == nil {
				info = &typeInfo{methods: map[string]*ast.FuncDecl{}, recvName: map[string]string{}}
				infos[key] = info
			}
			info.methods[fn.Name.Name] = fn
			info.recvName[fn.Name.Name] = recvName
			if info.goStmtAt == "" && containsGoStmt(fn.Body) {
				info.goStmtAt = fmt.Sprintf("%s:%d", path, fset.Position(fn.Pos()).Line)
			}
			if fn.Name.Name == "Close" || fn.Name.Name == "close" {
				info.closeAt = fmt.Sprintf("%s:%d", path, fset.Position(fn.Pos()).Line)
				info.closeBody = fn.Body
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/engines: %v", err)
	}

	out := map[string]pumpSite{}
	for key, info := range infos {
		if info.goStmtAt == "" || info.closeBody == nil {
			continue
		}
		out[key] = pumpSite{
			goStmtAt:   info.goStmtAt,
			closeAt:    info.closeAt,
			closeJoins: bodyJoins(info.closeBody) || closeCallsAJoiningMethod(info.closeBody, info.methods, info.recvName["Close"], info.recvName["close"]),
		}
	}
	return out
}

func pumpReceiverOf(fn *ast.FuncDecl) (typeName, varName string) {
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

func containsGoStmt(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if _, ok := n.(*ast.GoStmt); ok {
			found = true
			return false
		}
		return true
	})
	return found
}

// bodyJoins reports whether body contains a wait-for-something: a channel
// receive, a call whose name contains "join", or a WaitGroup Wait.
func bodyJoins(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.UnaryExpr:
			if v.Op == token.ARROW {
				found = true
			}
		case *ast.CallExpr:
			if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
				name := sel.Sel.Name
				if name == "Wait" || strings.Contains(strings.ToLower(name), "join") {
					found = true
				}
			}
		}
		return !found
	})
	return found
}

// closeCallsAJoiningMethod follows ONE hop: a Close that delegates the wait to
// a helper on the same receiver still counts as joining.
func closeCallsAJoiningMethod(body *ast.BlockStmt, methods map[string]*ast.FuncDecl, recvNames ...string) bool {
	recv := map[string]bool{}
	for _, r := range recvNames {
		if r != "" {
			recv[r] = true
		}
	}
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || !recv[id.Name] {
			return true
		}
		if m, ok := methods[sel.Sel.Name]; ok && m.Body != nil && bodyJoins(m.Body) {
			found = true
			return false
		}
		return true
	})
	return found
}
