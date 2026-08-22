// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// skipFlushVerdict grades one position-writing function.
type skipFlushVerdict string

const (
	// flushesTheLedger — the function's own body (closures included) calls
	// flushSkippedTables, so a skip accumulated before this position write
	// lands durably before (or atomically with) the position.
	flushesTheLedger skipFlushVerdict = "flushes"
	// flushExempt — the function writes a position without flushing, and the
	// reason says why no un-flushed skip can be pending there. "It probably
	// never has skips" is not a reason; the reason must name the callers or
	// the mechanism.
	flushExempt skipFlushVerdict = "exempt"
)

type skipFlushEntry struct {
	verdict skipFlushVerdict
	reason  string
}

// skipLedgerFlushRoster is fail-by-default over every function under
// internal/engines/{mysql,postgres} whose body calls writePositionTx or
// writePositionPipelined — i.e. every place a CDC resume position can become
// durable. Keys are "<pkgdir>.<RecvType>.<FuncName>" ("-" for a plain
// function's receiver).
//
// The invariant this holds (appliershared/skipped_tables.go, C-11/H-4): the
// coalesced skip ledger is flushed at EVERY position-write boundary, BEFORE
// the covering position becomes durable. A position write that skips the
// flush advances the resume position past unrecorded skips; a crash then
// loses the increments with no re-delivery, the ledger row may never exist,
// and the skip silently becomes log-only — the exact shape the header's
// "the skip is never log-only" rules out. The 2026-08-22 invariant sweep
// re-derived every site below; this roster is what keeps the enumeration
// from rotting when the next position-write path is added or a refusal
// moves (the moved-door caller-roster rule).
var skipLedgerFlushRoster = map[string]skipFlushEntry{
	// --- MySQL ---
	"mysql.ChangeApplier.applyOneImpl": {
		flushesTheLedger,
		"serial per-change path; flush precedes the in-tx position write, failure rolls the tx back.",
	},
	"mysql.ChangeApplier.persistSourceTxCommit": {
		flushesTheLedger,
		"CDCPOS-2 TxCommit boundary write; flush precedes BeginTx.",
	},
	"mysql.laneApplierAdapter.WriteCheckpoint": {
		flushesTheLedger,
		"ADR-0104 frontier checkpoint — the concurrent path's only position-write boundary; flush precedes it.",
	},
	"mysql.ChangeApplier.WritePosition": {
		flushExempt,
		"bare anchor write with rowsApplied=0. Its production callers — broker.go cold-start restore, " +
			"migrate.go's snapshot→CDC handoff anchor, streamer_coldstart.go and streamer_multidb.go anchors — " +
			"all run BEFORE any CDC apply on this applier, so the accumulator cannot hold entries. A future " +
			"caller that writes an anchor mid-apply re-opens the hazard; move it behind a flushing boundary.",
	},
	"mysql.mysqlBatchTx.writePosition": {
		flushExempt,
		"in-tx helper: flushes pending DATA, not the skip ledger. Its only production driver is the batch " +
			"loop's WritePosition closure (mysql.ChangeApplier.batchConfig), which calls flushSkippedTables " +
			"after this returns and before Commit — held mechanically by the companion check below.",
	},

	// --- Postgres ---
	"postgres.ChangeApplier.applyOneImpl": {
		flushesTheLedger,
		"serial per-change path; flush precedes the in-tx position write, failure rolls the tx back.",
	},
	"postgres.ChangeApplier.persistSourceTxCommit": {
		flushesTheLedger,
		"CDCPOS-2 TxCommit boundary write; flush precedes BeginTx.",
	},
	"postgres.ChangeApplier.batchConfig": {
		flushesTheLedger,
		"the batch-loop WritePosition closure (both the pipelined queue arm and the serial *sql.Tx arm) " +
			"flushes at the same boundary it writes the position.",
	},
	"postgres.laneApplierAdapter.WriteCheckpoint": {
		flushesTheLedger,
		"ADR-0104 frontier checkpoint; flush precedes it.",
	},
	"postgres.ChangeApplier.WritePosition": {
		flushExempt,
		"bare anchor write with rowsApplied=0 — same callers and same pre-apply argument as the MySQL twin.",
	},
}

// skipFlushCompanionMustFlush lists functions that are NOT position writers
// themselves but that an exemption above leans on for its flush — each must
// mechanically contain a flushSkippedTables call, so the exemption's reason
// cannot rot silently.
var skipFlushCompanionMustFlush = []string{
	"mysql.ChangeApplier.batchConfig",
}

// TestPositionWritersFlushTheSkipLedger is the caller roster for the C-11/H-4
// skip-ledger invariant: "flushSkippedTables runs at every position-write
// boundary, before the covering position becomes durable."
//
// # What it grades, and what it does not
//
// A function qualifies when a non-test file under internal/engines/mysql or
// internal/engines/postgres declares it and its body (closures included)
// calls writePositionTx or writePositionPipelined. The writePositionTx
// definitions themselves are excluded (they are the marker, not a caller).
// The grade is syntactic presence of a flushSkippedTables call in the same
// declaration — it proves a flush is WIRED at the boundary, not that its
// ordering is correct; ordering is what the per-path pins and comments at
// each site are for. A position write outside these two packages (the
// trigger-CDC engines have no applier; sqlite/D1 are not CDC targets) or one
// spelled through a new helper name is outside this gate's sight — extend
// the marker list when one appears.
func TestPositionWritersFlushTheSkipLedger(t *testing.T) {
	found := findPositionWritingFuncs(t)

	const funcFloor = 8
	if len(found) < funcFloor {
		t.Fatalf("discovered only %d position-writing functions (floor %d) — the walker is broken, not the "+
			"tree; every roster claim below would pass vacuously", len(found), funcFloor)
	}
	for _, pkg := range []string{"mysql", "postgres"} {
		n := 0
		for key := range found {
			if strings.HasPrefix(key, pkg+".") {
				n++
			}
		}
		if n < 3 {
			t.Errorf("only %d position-writing functions found in %q (floor 3); the walker is not reaching "+
				"the engine and its boundaries would fall out of this gate's scope", n, pkg)
		}
	}

	keys := make([]string, 0, len(found))
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	flushCount := 0
	for _, key := range keys {
		site := found[key]
		entry, listed := skipLedgerFlushRoster[key]
		if !listed {
			t.Errorf("%s (%s) writes a CDC resume position and is NOT on the roster.\n"+
				"Every position-write boundary must flush the coalesced skip ledger BEFORE the position "+
				"becomes durable (appliershared/skipped_tables.go H-4) — otherwise a crash after this write "+
				"loses accumulated skips with no re-delivery and the skip becomes silently log-only.\n"+
				"Add a skipLedgerFlushRoster entry: %q if it flushes, or %q with a reason that names why no "+
				"un-flushed skip can be pending at this boundary.", key, site, flushesTheLedger, flushExempt)
			continue
		}
		if strings.TrimSpace(entry.reason) == "" {
			t.Errorf("%s: roster entry has no reason; the reason IS the gate", key)
			continue
		}
		switch entry.verdict {
		case flushesTheLedger:
			flushCount++
			if !site.callsFlush {
				t.Errorf("%s: rostered as %q, but its body shows no flushSkippedTables call — either the "+
					"flush was removed or the roster is stale; both are the defect this gate exists for (%s).",
					key, flushesTheLedger, site)
			}
		case flushExempt:
			// The reason carries the argument; nothing mechanical to check here
			// beyond the companion list below.
		default:
			t.Errorf("%s: roster verdict %q is not one of %q / %q", key, entry.verdict, flushesTheLedger, flushExempt)
		}
	}
	if flushCount < 5 {
		t.Errorf("only %d rostered functions graded %q (floor 5) — the roster has drifted toward exemptions; "+
			"re-derive the boundaries", flushCount, flushesTheLedger)
	}

	for key := range skipLedgerFlushRoster {
		if _, ok := found[key]; !ok {
			t.Errorf("roster lists %s but no such position-writing function exists any more — remove the "+
				"entry. A stale blessing is how a roster starts covering less than its name implies.", key)
		}
	}

	// Companion checks: functions an exemption leans on must really flush.
	for _, key := range skipFlushCompanionMustFlush {
		site, ok := found[key]
		if !ok {
			// Not a position writer by the marker set — walk it directly.
			site, ok = findFuncByKey(t, key)
			if !ok {
				t.Errorf("companion check lists %s but the function no longer exists — an exemption above "+
					"leans on it; re-derive that exemption", key)
				continue
			}
		}
		if !site.callsFlush {
			t.Errorf("%s is the flush an exemption leans on, and it no longer calls flushSkippedTables — "+
				"the exempted boundary is now unflushed", key)
		}
	}
}

type posWriteSite struct {
	at         string
	callsFlush bool
}

func (s posWriteSite) String() string { return "declared at " + s.at }

// findPositionWritingFuncs walks internal/engines/{mysql,postgres} and returns
// every function whose body calls writePositionTx or writePositionPipelined,
// keyed "<pkgdir>.<RecvType>.<FuncName>". FuncDecls NAMED writePositionTx are
// excluded (definitions, not callers).
func findPositionWritingFuncs(t *testing.T) map[string]posWriteSite {
	t.Helper()
	out := map[string]posWriteSite{}
	walkEngineFuncDecls(t, func(key, at string, fn *ast.FuncDecl) {
		if fn.Name.Name == "writePositionTx" {
			return
		}
		if !bodyCallsAny(fn.Body, "writePositionTx", "writePositionPipelined") {
			return
		}
		out[key] = posWriteSite{at: at, callsFlush: bodyCallsAny(fn.Body, "flushSkippedTables")}
	})
	return out
}

// findFuncByKey resolves one "<pkg>.<Recv>.<Name>" key to its declaration,
// for the companion checks.
func findFuncByKey(t *testing.T, want string) (posWriteSite, bool) {
	t.Helper()
	var got posWriteSite
	found := false
	walkEngineFuncDecls(t, func(key, at string, fn *ast.FuncDecl) {
		if key == want {
			got = posWriteSite{at: at, callsFlush: bodyCallsAny(fn.Body, "flushSkippedTables")}
			found = true
		}
	})
	return got, found
}

// walkEngineFuncDecls parses every non-test .go file under the mysql and
// postgres engine dirs and yields each FuncDecl with its roster key.
func walkEngineFuncDecls(t *testing.T, visit func(key, at string, fn *ast.FuncDecl)) {
	t.Helper()
	fset := token.NewFileSet()
	for _, dir := range []string{"mysql", "postgres"} {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return perr
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				recv := "-"
				if fn.Recv != nil && len(fn.Recv.List) > 0 {
					recv = receiverTypeName(fn.Recv.List[0].Type)
				}
				key := dir + "." + recv + "." + fn.Name.Name
				visit(key, fset.Position(fn.Pos()).String(), fn)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
}

// receiverTypeName unwraps *T / generic receivers to the bare type name.
func receiverTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(e.X)
	case *ast.Ident:
		return e.Name
	case *ast.IndexExpr:
		return receiverTypeName(e.X)
	case *ast.IndexListExpr:
		return receiverTypeName(e.X)
	default:
		return "-"
	}
}

// bodyCallsAny reports whether any call expression in body (closures
// included) targets one of the named functions/methods.
func bodyCallsAny(body *ast.BlockStmt, names ...string) bool {
	if body == nil {
		return false
	}
	hit := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var callee string
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			callee = fun.Name
		case *ast.SelectorExpr:
			callee = fun.Sel.Name
		default:
			return true
		}
		for _, want := range names {
			if callee == want {
				hit = true
			}
		}
		return true
	})
	return hit
}
