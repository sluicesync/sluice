// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 228 call-site gate: every LONG-LIVED holder of a shared copy-gate token
// must declare itself as one.
//
// The gate's own unit tests pin what happens once a token is tagged as the
// base class. They cannot see whether the production call sites tag it — a
// one-word edit from AcquireBase back to Acquire in migrate_table_pool.go
// restores the deadlock in full and leaves every one of those tests green.
// That was verified by mutation, not assumed. This file is the missing half.
//
// The distinction being enforced (see migcore/copy_parallelism_gate.go):
//
//	Acquire/Release          — a CHUNK worker. Short-lived; finishes on its own.
//	AcquireBase/ReleaseBase  — a TABLE-POOL worker. Holds its token for the
//	                           whole table copy, INCLUDING while blocked in
//	                           errgroup.Wait on chunks that need tokens of
//	                           their own. It cannot finish by itself, so the
//	                           shrink floor must always leave a chunk slot
//	                           beside it.
//
// The roster below is the sibling enumeration CLAUDE.md asks for: both table
// pools and both chunk fan-outs, each stated fixed-or-exempt rather than
// promised. Its scope is exactly the four files named — it reaches every
// current caller of CopyParallelismGate, and a NEW caller elsewhere is not
// covered by it. That is why the anti-vacuity check below also fails if the
// package's set of gate callers grows beyond this roster.

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

// gateCallRoster maps a source file to the token-class methods it is allowed
// to call on a CopyParallelismGate.
var gateCallRoster = map[string]struct {
	allowed []string
	why     string
}{
	"migrate_table_pool.go": {
		allowed: []string{"AcquireBase", "ReleaseBase"},
		why: "migrate's cross-table pool holds this token for the WHOLE table copy while " +
			"waiting on runChunks' errgroup — the exact holder Bug 228 deadlocked on",
	},
	filepath.Join("backup", "backup_table_pool.go"): {
		allowed: []string{"AcquireBase", "ReleaseBase"},
		why: "backup's table pool has the identical topology (base token held across the " +
			"range fan-out). Nothing on the backup read path calls ShrinkAndBackoff today, so " +
			"the deadlock is currently UNREACHABLE there — declared anyway, because the class " +
			"is what is being fixed, not the reachable instance",
	},
	"migrate_parallel.go": {
		allowed: []string{"Acquire", "Release", "ShrinkAndBackoff", "NoteOpenSucceeded"},
		why:     "acquireChunkConn is the CHUNK class: it takes a token, opens, copies, releases",
	},
	filepath.Join("backup", "backup_parallel.go"): {
		allowed: []string{"Acquire", "Release"},
		why:     "the backup range fan-out is the CHUNK class, same as migrate's",
	},
}

// TestCopyGateTokenClass_LongLivedHoldersDeclareTheBaseClass walks the real
// source and fails if any gate call site uses a class the roster does not
// permit for it.
func TestCopyGateTokenClass_LongLivedHoldersDeclareTheBaseClass(t *testing.T) {
	// Anti-vacuity floor #1: every rostered file must actually be found and
	// must actually contain gate calls. A roster entry pointing at a moved or
	// renamed file would otherwise silently check nothing.
	totalCalls := 0

	for rel, want := range gateCallRoster {
		got := gateMethodCallsIn(t, rel)
		if len(got) == 0 {
			t.Errorf("%s: found NO CopyParallelismGate calls.\n\nEither the file moved (fix this "+
				"roster) or the gate wiring was removed. A roster entry that matches nothing is a "+
				"gate that checks nothing.", rel)
			continue
		}
		totalCalls += len(got)

		for _, m := range got {
			if !containsClass(want.allowed, m) {
				t.Errorf("%s calls gate.%s, which its token class does not permit (allowed: %s).\n\n"+
					"Why this file is rostered: %s.\n\n"+
					"If this is migrate/backup_table_pool.go reverting to Acquire/Release, that is "+
					"BUG 228 exactly: a shrink then retires every chunk token and leaves this "+
					"long-lived one as the sole survivor, held by a goroutine waiting on the chunks "+
					"that can no longer start. The copy stops with no error and no exit code.",
					rel, m, strings.Join(want.allowed, "/"), want.why)
			}
		}
	}

	if totalCalls < 8 {
		t.Errorf("the roster matched only %d gate calls across %d files; the shipped wiring has "+
			"more than that, so the scanner has stopped matching how the gate is called — fix the "+
			"scanner rather than lowering this floor", totalCalls, len(gateCallRoster))
	}

	// Anti-vacuity floor #2: no gate caller outside the roster. This is what
	// keeps the gate's name honest as the code grows — a new long-lived holder
	// in a new file fails here rather than deadlocking in the field.
	for _, rel := range gateCallerFiles(t) {
		if _, ok := gateCallRoster[rel]; !ok {
			t.Errorf("%s calls a CopyParallelismGate method but is not in gateCallRoster.\n\n"+
				"Add it, and state which token CLASS it belongs to: a holder that keeps its token "+
				"while waiting on OTHER gate users is the base class (AcquireBase/ReleaseBase); a "+
				"worker that finishes on its own is the chunk class.", rel)
		}
	}
}

// gateMethodCallsIn returns the CopyParallelismGate method names called in rel,
// identified syntactically: a selector call whose receiver expression mentions
// "gate". Comments and strings cannot match, since this walks the AST.
func gateMethodCallsIn(t *testing.T, rel string) []string {
	t.Helper()
	src, err := os.ReadFile(rel)
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", rel, err)
	}

	seen := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		recv := string(src[fset.Position(sel.X.Pos()).Offset:fset.Position(sel.X.End()).Offset])
		if !strings.Contains(strings.ToLower(recv), "gate") {
			return true
		}
		seen[sel.Sel.Name] = true
		return true
	})

	out := make([]string, 0, len(seen))
	for m := range seen {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// gateCallerFiles finds every non-test .go file under internal/pipeline that
// mentions a CopyParallelismGate token method, so a new caller cannot be added
// without joining the roster.
func gateCallerFiles(t *testing.T) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		s := string(src)
		for _, m := range []string{".AcquireBase(", ".ReleaseBase(", ".NoteOpenSucceeded("} {
			if strings.Contains(s, m) {
				out = append(out, path)
				return nil
			}
		}
		// Bare Acquire/Release are common names, so only count them when the
		// file also names the gate type or a gate-ish receiver.
		if (strings.Contains(s, "copyGate.") || strings.Contains(s, "gate.Acquire(") ||
			strings.Contains(s, "gate.Release(")) &&
			!strings.Contains(path, "migcore") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(out)
	return out
}

func containsClass(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}
