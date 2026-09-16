// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

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

// A test may not point the process-wide default logger at an unguarded
// buffer, and this is the gate that makes a tenth such site impossible to add
// quietly.
//
// # The class
//
// `slog.SetDefault` is PROCESS-WIDE. A test that installs a handler writing
// into a plain `bytes.Buffer` and then reads that buffer races every goroutine
// still logging anywhere in the test binary — including goroutines the test
// does not own and cannot join. CI's `-race` shard caught it on run
// 35043003665: a helper read `buf.String()` while a go-mysql `BinlogSyncer`
// from an EARLIER subtest was still writing INFO lines. A syncer keeps logging
// after `Close()` returns, so there was nothing to wait for (A0915-LOGCAPTURE-1).
//
// The remedy is [logcapture.Buffer], and the sweep that closed this class
// converted every site in the tree. This gate is the ratchet.
//
// # Why this gate exists at all, and not just the sweep
//
// The class was closed once before, informally: nine separate authors each
// noticed the race in their own package and each wrote their own mutex-guarded
// buffer — `lockedBuffer` (x3), `syncBuffer`, `syncLogBuffer` (x2), `syncBuf`
// and `safeBuffer` (x2). Nine correct local fixes, four names, drifted APIs,
// and NOTHING stopping the tenth author from writing a bare `bytes.Buffer`
// instead, because none of the nine was discoverable from outside its file.
// A sweep without a gate reproduces exactly that state.
//
// # Why the floor is on the MATCHER, not on the findings
//
// The obvious anti-vacuity floor — "expect at least N unguarded captures" —
// would be exactly wrong, because the whole point is to drive that number to
// zero, and it IS zero as of the sweep. A gate that fails when its class is
// finally closed teaches people to delete the gate.
//
// So the floor is on the walk's REACH: every slog handler construction in test
// code, guarded or not. If that collapses, the matcher broke and the gate is
// blind; the findings count is free to stay at zero, which is the success
// condition.
//
// # Scope, stated so the name is not read as broader than the truth
//
// This grades a handler whose destination argument RESOLVES, within the same
// file, to a variable declared as a bare `bytes.Buffer`. It does not see a
// buffer reached through a struct field, returned by a helper in a different
// file, or aliased through an `io.Writer` variable first. Those spellings were
// searched during the sweep and none existed; a broader matcher would add
// false-positive surface for no coverage. If a future sweep finds one, widen
// the matcher here rather than filing a second gate beside it.
//
// It also says nothing about whether a capture RESTORES the default logger —
// a real and separate hazard that [logcapture.Buffer] deliberately does not
// solve either. See the logcapture package doc.
// The floor is measured, not guessed: the walk found 110 handler
// constructions when this landed, and 80 leaves room for test files to come
// and go without turning the gate into a maintenance chore. A first draft of
// this constant said 180 — a number picked before running the walk, which the
// gate itself rejected on its first run.
const logCaptureReachFloor = 80

// logCaptureGuardRoster classifies every test site that pipes a bare
// bytes.Buffer into an slog handler. An entry is a REASON the site may keep
// doing so; a site absent from this map fails the build.
//
// It is EMPTY, and that is the success condition rather than an oversight: the
// A0915-LOGCAPTURE-1 sweep converted every site in the tree. The anti-vacuity
// floor above is what keeps an empty roster honest — if the matcher broke,
// this map would also be empty, so emptiness alone proves nothing and the
// reach count is what distinguishes the two.
var logCaptureGuardRoster = map[string]string{}

func TestLogCaptureGuardRoster_NoTestPipesABareBufferIntoSlog(t *testing.T) {
	reach, candidates := discoverUnguardedLogCaptures(t)

	if reach < logCaptureReachFloor {
		t.Fatalf("anti-vacuity: the walk found only %d slog handler constructions in test code (floor %d) — "+
			"the AST matcher is broken and this gate is blind. NOTE the floor is on the matcher's REACH, not on "+
			"unguarded-capture findings: those are expected to be ZERO, which is the goal.", reach, logCaptureReachFloor)
	}

	var unclassified []string
	for key, pos := range candidates {
		if _, ok := logCaptureGuardRoster[key]; !ok {
			unclassified = append(unclassified, key+"  ("+pos+")")
		}
	}
	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("slog handler(s) writing into an UNGUARDED bytes.Buffer at:\n  %s\n\n"+
			"slog.SetDefault is PROCESS-WIDE, so this buffer is written by every goroutine still logging "+
			"anywhere in the test binary — including ones this test did not start and cannot join (a CDC "+
			"pump, a prune sidecar, or a go-mysql BinlogSyncer, which keeps logging after Close() returns). "+
			"Reading it while one of those is live is a data race, and CI's -race shard is where it surfaces "+
			"(audit A0915-LOGCAPTURE-1).\n\n"+
			"Use the shared guarded sink instead:\n"+
			"  import \"sluicesync.dev/sluice/internal/logcapture\"\n"+
			"  var buf logcapture.Buffer      // or &logcapture.Buffer{}\n"+
			"  slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))\n\n"+
			"It is a drop-in for bytes.Buffer across String/Bytes/Len/Reset/WriteString, so assertions do not "+
			"change. Do NOT write another private mutex-guarded buffer: nine of those already existed under "+
			"four names before this package, and none of them stopped the next one.",
			strings.Join(unclassified, "\n  "))
	}

	// The reverse guard: an exemption whose site vanished is a dead
	// classification, and a roster nobody prunes becomes a permanent allow.
	for key := range logCaptureGuardRoster {
		if _, ok := candidates[key]; !ok {
			t.Errorf("stale logCaptureGuardRoster entry %q: no such unguarded capture remains. "+
				"If it was converted, DELETE the entry — that is the class closing.", key)
		}
	}
}

// discoverUnguardedLogCaptures walks every _test.go file under internal/ and
// cmd/ and returns (total slog handler constructions, unguarded ones keyed by
// "<rel>::<func>").
//
// "Unguarded" is decided by RESOLVING the handler's destination argument to a
// declaration in the same file, which is what separates a bare bytes.Buffer
// from a logcapture.Buffer or any other writer.
func discoverUnguardedLogCaptures(t *testing.T) (reach int, candidates map[string]string) {
	t.Helper()

	root := repoRootFromDocsync(t)
	candidates = map[string]string{}
	fset := token.NewFileSet()

	for _, sub := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, sub), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := d.Name()
			if d.IsDir() {
				if name == "testdata" || name == "workspace" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(name, "_test.go") {
				return nil
			}
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %q: %v", path, perr)
			}
			rel, rerr := filepath.Rel(root, path)
			if rerr != nil {
				rel = path
			}
			rel = filepath.ToSlash(rel)

			bare := bareBufferIdents(file)

			ast.Inspect(file, func(n ast.Node) bool {
				fd, ok := n.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					return true
				}
				fn := fd.Name.Name
				if fd.Recv != nil && len(fd.Recv.List) > 0 {
					fn = "(" + receiverTypeName(fd.Recv.List[0].Type) + ")." + fn
				}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					callee := exprText(fset, call.Fun)
					if callee != "slog.NewTextHandler" && callee != "slog.NewJSONHandler" {
						return true
					}
					reach++
					if len(call.Args) == 0 {
						return true
					}
					if !bare[destIdentFromExpr(call.Args[0])] {
						return true
					}
					key := rel + "::" + fn
					if _, dup := candidates[key]; !dup {
						candidates[key] = fset.Position(call.Pos()).String()
					}
					return true
				})
				return false
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %q: %v", sub, err)
		}
	}
	return reach, candidates
}

// destIdentFromExpr resolves a handler's destination argument to the
// identifier it names, so `&buf` and `buf` resolve alike.
//
// It works on the AST and deliberately does NOT go through the shared
// exprText helper. That helper renders a placeholder — the literal string
// "<*ast.UnaryExpr>" — for any node that is not an identifier or selector, so
// a text comparison silently never matches `&buf`, which is the spelling
// almost every capture in this tree uses. The first draft of this gate did
// exactly that: it passed, green, with a bare bytes.Buffer capture sitting in
// the tree. Only the mutation run caught it. Do not "simplify" this back to
// comparing rendered source.
func destIdentFromExpr(e ast.Expr) string {
	if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.AND {
		e = u.X
	}
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// bareBufferIdents collects every identifier in a file declared as a plain
// bytes.Buffer, in the three spellings the tree actually used:
//
//	var buf bytes.Buffer
//	buf := &bytes.Buffer{}
//	buf := bytes.Buffer{}
func bareBufferIdents(file *ast.File) map[string]bool {
	out := map[string]bool{}

	isBytesBuffer := func(e ast.Expr) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Buffer" {
			return false
		}
		pkg, ok := sel.X.(*ast.Ident)
		return ok && pkg.Name == "bytes"
	}
	// &bytes.Buffer{} / bytes.Buffer{}
	isBytesBufferLit := func(e ast.Expr) bool {
		if u, ok := e.(*ast.UnaryExpr); ok {
			e = u.X
		}
		lit, ok := e.(*ast.CompositeLit)
		return ok && isBytesBuffer(lit.Type)
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.ValueSpec:
			if node.Type != nil && isBytesBuffer(node.Type) {
				for _, nm := range node.Names {
					out[nm.Name] = true
				}
			}
		case *ast.AssignStmt:
			for i, lhs := range node.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || i >= len(node.Rhs) {
					continue
				}
				if isBytesBufferLit(node.Rhs[i]) {
					out[id.Name] = true
				}
			}
		}
		return true
	})
	return out
}
