// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every MySQL *RowWriter connection ACQUIRE must ride the transient
// classifier — roadmap item 139.
//
// The defect this gate exists for was not a missing retry; it was a retry
// that reached the OPERATION and not the CONNECTION the operation needs.
// Item 122 taught the classifier vtgate's wording for a dropped
// connection and wired it into the flush, and the field report's run 2
// then died on `pin connection: … vtgate connection error … connection
// reset by peer` — the identical error one line earlier in the same
// function, on a path where 78 of them had just been absorbed.
//
// So the enumeration is mechanical rather than promised. The roadmap
// filing named five acquire sites from a stale read of the tree; the walk
// below finds EIGHT — the two fan-out lanes each acquire twice (the
// len(workers)==1 shortcut AND the per-worker goroutine), and
// checkLocalInfile's session probe is the eighth. That is the whole
// reason this is an AST roster and not a list in a commit message.
//
// # SCOPE — stated so the name cannot be read as broader than the truth
//
// This gate reaches METHODS ON *RowWriter IN PACKAGE mysql, and nothing
// else. It does NOT cover:
//
//   - *SchemaWriter (DDL/index/constraint acquires). Those are a
//     different lane with their own retry (schema_writer_index_reparent_
//     retry.go) and were not the field-report class; they are an
//     UNGATED sibling, named here rather than implied.
//   - *RowReader / the snapshot readers. A read-side acquire failure is
//     recovered by re-running the table, not by replay tolerance.
//   - The Postgres engine. Its cold-copy acquire already sits INSIDE
//     copyChunkWithRetry's attempt closure; postgres/raw_copy.go's
//     acquire is the one sibling of this class there, fixed in the same
//     change and pinned by TestImportRawCopy_AcquireTripsTheGrowGate.
//   - The CDC APPLY path on either engine. Its acquires
//     (postgres/change_applier_pipelined.go) sit inside the pipeline's
//     ADR-0038 apply retry, which re-drives the whole batch — a
//     different, already-closed loop, not an ungated one.
package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// rowWriterConnAcquireExempt lists the *RowWriter methods that may call
// w.db.Conn directly, each with the reason. An entry is a claim someone
// can check; an unexplained one fails below.
var rowWriterConnAcquireExempt = map[string]string{
	"acquireConnWithRetry": "IS the retry loop (item 139); its own acquires are the ones being retried",
	"flushWithReparentRetry": "the ADR-0108 flush loop's mid-retry re-acquire already rides that loop's " +
		"budget — a failure there becomes the next iteration's classified error, so wrapping it in a " +
		"SECOND budget would nest two wall-clock bounds",
}

// rowWriterAcquireHelper is the one sanctioned door.
const rowWriterAcquireHelper = "acquireConnWithRetry"

func TestEveryRowWriterConnAcquireRidesTheTransientRetry(t *testing.T) {
	direct, viaHelper, helperSites, files := discoverRowWriterConnAcquires(t)

	// Anti-vacuity, both halves. A walk that stops seeing the tree, or
	// stops recognising a raw acquire, would pass on nothing — which is
	// exactly how a gate ends up proving only that it still compiles.
	if files < 20 {
		t.Fatalf("walked only %d non-test files in package mysql; the parse is not reaching the tree", files)
	}
	// EIGHT call SITES across SIX methods: the five bulk-write acquires,
	// the two extra fan-out sites (each fan-out lane acquires twice — the
	// len==1 shortcut and the per-worker goroutine, the pair the roadmap
	// filing's hand-written roster missed), and checkLocalInfile's probe.
	if helperSites < 8 || len(viaHelper) < 6 {
		t.Fatalf("found only %d *RowWriter call site(s) of %s across %d method(s) (%v); all EIGHT "+
			"acquires are supposed to route through it, so this roster is passing on a shrunken set",
			helperSites, rowWriterAcquireHelper, len(viaHelper), sortedNames(viaHelper))
	}
	if len(direct) == 0 {
		t.Fatalf("the walk found NO direct w.db.Conn call anywhere on *RowWriter — not even the two " +
			"exempt ones. The detector has stopped recognising a raw acquire, so it can no longer fail")
	}

	var missing []string
	for name := range direct {
		if _, ok := rowWriterConnAcquireExempt[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("*RowWriter method(s) %v acquire a connection with a bare w.db.Conn.\n\n"+
			"A target drop DURING acquisition is the same transient the flush one line later absorbs; "+
			"returned raw it terminates the run (roadmap item 139, field report 2026-08-05 run 2). "+
			"Call w.%s(ctx, table) instead, or add the method to rowWriterConnAcquireExempt with the "+
			"reason it cannot.", missing, rowWriterAcquireHelper)
	}

	// The other direction: a stale exemption lies about the debt.
	for name, why := range rowWriterConnAcquireExempt {
		if strings.TrimSpace(why) == "" {
			t.Errorf("exemption %q carries no reason; an unexplained exemption is indistinguishable from "+
				"an oversight", name)
		}
		if !direct[name] {
			t.Errorf("rowWriterConnAcquireExempt lists %q, which no longer acquires a connection directly — "+
				"remove the exemption", name)
		}
	}
}

// discoverRowWriterConnAcquires walks package mysql's non-test files and
// reports, per *RowWriter method, whether it acquires a connection
// directly (a `<expr>.db.Conn(...)` call) and whether it goes through the
// sanctioned helper. helperSites counts CALL SITES rather than methods
// (the fan-out lanes acquire twice each) and files counts the files
// parsed, so the caller can refuse to pass on a shrunken walk.
func discoverRowWriterConnAcquires(t *testing.T) (direct, viaHelper map[string]bool, helperSites, files int) {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	fset := token.NewFileSet()
	direct = map[string]bool{}
	viaHelper = map[string]bool{}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, e.Name(), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", e.Name(), perr)
		}
		files++
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !isRowWriterMethod(fn) {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				ce, isCall := n.(*ast.CallExpr)
				if !isCall {
					return true
				}
				sel, isSel := ce.Fun.(*ast.SelectorExpr)
				if !isSel {
					return true
				}
				if sel.Sel.Name == rowWriterAcquireHelper {
					viaHelper[fn.Name.Name] = true
					helperSites++
					return true
				}
				// A raw acquire is `<anything>.db.Conn(...)` — matching on
				// the `db` field rather than the receiver name so a rename
				// of `w` cannot silently blind the walk.
				if sel.Sel.Name != "Conn" {
					return true
				}
				if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "db" {
					direct[fn.Name.Name] = true
				}
				return true
			})
		}
	}
	return direct, viaHelper, helperSites, files
}

// isRowWriterMethod reports whether fn is a method on *RowWriter (or
// RowWriter).
func isRowWriterMethod(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	t := fn.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	id, ok := t.(*ast.Ident)
	return ok && id.Name == "RowWriter"
}

func sortedNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
