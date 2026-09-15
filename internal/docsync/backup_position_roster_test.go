// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// backupPositionExempt is the fail-by-default exemption map for
// [TestEveryCDCEngineRecordsABackupPosition]. It is EMPTY on purpose
// (roadmap item 163): every engine that declares a CDC mechanism can root
// a chain, so every one of them must record a position on a full. An entry
// here needs a reason, and the reason would have to explain why a chain
// off that engine may silently start "from now" — which is the class this
// gate exists to keep closed.
var backupPositionExempt = map[string]string{}

// TestEveryCDCEngineRecordsABackupPosition is the registry-derived roster
// for roadmap item 163: a `backup full` on ANY engine whose Capabilities
// declare a CDC mechanism records an EndPosition a chain can resume from.
//
// Two halves per engine, both required:
//
//   - the engine value implements [irbackup.SnapshotOpener] — the gap-free
//     primary path, where the anchor is taken inside the sweep's own read
//     (PG: the exported snapshot's LSN; MySQL: the in-transaction GTID /
//     file-pos; the trigger engines: the change log's settled anchor).
//     Assertable straight off the registry.
//   - its package carries the compile-time pin for the SchemaReader-level
//     [irbackup.PositionCapturer], the orchestrator's post-sweep fallback.
//     A SchemaReader needs a live DSN to open, so this half is graded on
//     the pin (a build fact) in the package the registry's engine value
//     lives in, resolved by reflection — never a hand-kept name → dir map.
//
// Why both: before item 163 the pgtrigger engine returned the composed
// postgres reader, whose capturer recorded a WAL LSN under the "postgres"
// tag — a position the trigger poller refuses as foreign — and the
// SQLite/D1 engines returned readers with no capturer at all, so their
// chains started "from now" and silently omitted the window between the
// full's sweep and the incremental's anchor. resumeStartFromParent carried
// those three as its one stated exemption; this roster is what makes the
// exemption's removal permanent.
//
// SCOPE, stated so the name is not read as broader than it is: the second
// half grades that the pin EXISTS, not that the engine's OpenSchemaReader
// returns the pinned type (that needs a live DSN). The returned type is
// held per engine elsewhere: sqlite-trigger by
// TestSchemaReader_PromotesTheComposedSurfaces (unit); postgres-trigger by
// the integration pins TestCaptureBackupPosition_IsUnavailableOnTheFallbackDoor
// and TestBackupChain_PGTrigger_PostFullRowsReachTheRestore (a composed
// reader's WAL LSN would fail the latter's EndPosition tag check);
// d1-trigger by OpenD1SchemaReader's own type refusal only — no live D1.
//
// Anti-vacuity: the registry must hold at least the eight CDC-capable
// engines the shipped binary registers (mysql, mariadb, planetscale,
// vitess, postgres, postgres-trigger, sqlite-trigger, d1-trigger), and
// at least one CDC-less engine must exist so the CDC distinction is live.
func TestEveryCDCEngineRecordsABackupPosition(t *testing.T) {
	names := engines.Names()
	if len(names) < 8 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list has drifted from cmd/sluice and "+
			"this gate is checking a subset of the fleet", len(names), names)
	}

	var graded, exempted []string
	sawCDCLess := false
	for _, name := range names {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() reported %q but engines.Get did not return it", name)
		}
		if e.Capabilities().CDC == ir.CDCNone {
			sawCDCLess = true
			continue
		}
		if reason, isExempt := backupPositionExempt[name]; isExempt {
			if reason == "" {
				t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is indistinguishable from an oversight", name)
			}
			exempted = append(exempted, name)
			continue
		}
		graded = append(graded, name)

		if _, ok := e.(irbackup.SnapshotOpener); !ok {
			t.Errorf("engine %q declares a CDC mechanism but does not implement irbackup.SnapshotOpener.\n\n"+
				"Without it a `backup full` falls to the v0.17.x post-sweep fallback, and a chain rooted at that full "+
				"has the during-backup write-window gap. Implement OpenBackupSnapshot so the anchor is taken inside "+
				"the sweep's own read (roadmap item 163), or add the engine to backupPositionExempt with the reason "+
				"a chain off it may start \"from now\".", name)
		}
		dir := enginePackageDir(t, e)
		if !packageDeclaresPositionCapturerPin(t, dir) {
			t.Errorf("engine %q (package %s) carries no `_ irbackup.PositionCapturer = (*T)(nil)` pin in a non-test "+
				"file.\n\nThe SchemaReader the engine hands back must implement CaptureBackupPosition (the full-backup "+
				"orchestrator's post-sweep fallback), and the pin is what turns that into a build fact. If the reader "+
				"is a delegated type from another package, pin THAT type here (as d1-trigger pins the sqlite-trigger "+
				"wrapper).", name, dir)
		}
	}

	if len(graded) < 8 {
		t.Fatalf("graded only %d CDC-capable engines (%v); want at least the eight the shipped binary registers", len(graded), graded)
	}
	if !sawCDCLess {
		t.Fatal("every registered engine declares a CDC mechanism — the CDC-less class this gate skips is empty, so the registry is partial")
	}
	registered := map[string]bool{}
	for _, n := range names {
		registered[n] = true
	}
	for name := range backupPositionExempt {
		if !registered[name] {
			t.Errorf("backupPositionExempt names %q, which is not a registered engine — the exemption is stale; drop it", name)
		}
	}
	if len(exempted) > 0 {
		t.Errorf("backupPositionExempt is expected to be EMPTY (every CDC engine records a backup position since roadmap "+
			"item 163); it exempts %v", exempted)
	}
	sort.Strings(graded)
	t.Logf("backup-position roster: %s", strings.Join(graded, ", "))
}

// enginePackageDir resolves the on-disk directory of the package the
// registry's engine VALUE is declared in, via its PkgPath — so a flavor
// registered from another package (mariadb from internal/engines/mysql) is
// graded where its code actually lives.
func enginePackageDir(t *testing.T, e ir.Engine) string {
	t.Helper()
	rt := reflect.TypeOf(e)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	const module = "sluicesync.dev/sluice/"
	pkg := rt.PkgPath()
	if !strings.HasPrefix(pkg, module) {
		t.Fatalf("engine %q is declared in %q, outside the module", e.Name(), pkg)
	}
	return filepath.Join("..", "..", filepath.FromSlash(strings.TrimPrefix(pkg, module)))
}

// packageDeclaresPositionCapturerPin reports whether any non-test .go file
// in dir DECLARES the compile-time pin for the SchemaReader-level surface —
// `var _ irbackup.PositionCapturer = (*T)(nil)`, with T either the
// package's own reader type or the delegated one it hands back (d1-trigger
// pins the sqlite-trigger wrapper it returns). Matched on the AST, not the
// file bytes, so a comment or string that merely QUOTES the pin cannot
// satisfy the gate; the declaration is what makes the surface a build fact.
func packageDeclaresPositionCapturerPin(t *testing.T, dir string) bool {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package dir %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	for _, ent := range entries {
		name := ent.Name()
		if ent.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok && isPositionCapturerPin(vs) {
					return true
				}
			}
		}
	}
	return false
}

// isPositionCapturerPin reports whether vs is
// `_ irbackup.PositionCapturer = (*T)(nil)`.
func isPositionCapturerPin(vs *ast.ValueSpec) bool {
	if len(vs.Names) != 1 || vs.Names[0].Name != "_" || len(vs.Values) != 1 {
		return false
	}
	sel, ok := vs.Type.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "PositionCapturer" {
		return false
	}
	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "irbackup" {
		return false
	}
	call, ok := vs.Values[0].(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	paren, ok := call.Fun.(*ast.ParenExpr)
	if !ok {
		return false
	}
	if _, ok := paren.X.(*ast.StarExpr); !ok {
		return false
	}
	arg, ok := call.Args[0].(*ast.Ident)
	return ok && arg.Name == "nil"
}
