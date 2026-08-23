// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// A comment that points at a moved file is a comment that sends the next
// reader nowhere — and nothing was checking.
//
// # Why this gate exists
//
// The 2026-08-05 audit review noted, in passing, that four comments referenced
// a cross-engine-supportable file directly under internal/pipeline, which does
// not exist: it moved into the migcore package in `bdef41e7`. (That path is
// spelled without backticks here on purpose — this gate matches backtick-quoted
// pointers, and it caught its own doc comment on the first run, which is a
// pleasing demonstration that it works.) Sweeping for the shape
// found ten stale pointers across six files, spanning at least three separate
// refactors — the backup/restore split into `pipeline/backup/`, the chunk
// codecs into `pipeline/blobcodec/`, and the migcore move.
//
// None of them broke a build, none of them broke a test, and every one of them
// was written by someone being helpful. That is exactly the class the project's
// own rule names: a comment is a hypothesis until something fails when it stops
// being true. These pointers had no such thing.
//
// The check is trivial and mechanical, which is the point — it costs one file
// walk and it converts a whole category of quiet doc rot into a build failure.
package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// codePointerRe matches a backtick-quoted repo-relative Go path in a comment,
// e.g. `internal/pipeline/migcore/cross_engine_supportable.go`. Backticks are
// required deliberately: they are how this repo spells "I mean this file", and
// requiring them keeps ordinary prose that happens to name a file out of scope.
var codePointerRe = regexp.MustCompile("`(internal/[A-Za-z0-9_./-]+\\.go)`")

func TestCodePointers_AllReferencedFilesExist(t *testing.T) {
	repoRoot := filepath.Join("..", "..")

	type ref struct{ path, from string }
	var (
		seen   int
		broken []ref
	)

	err := filepath.Walk(filepath.Join(repoRoot, "internal"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, m := range codePointerRe.FindAllStringSubmatch(string(b), -1) {
			seen++
			target := filepath.Join(repoRoot, filepath.FromSlash(m[1]))
			if _, statErr := os.Stat(target); statErr != nil {
				rel, _ := filepath.Rel(repoRoot, path)
				broken = append(broken, ref{path: m[1], from: filepath.ToSlash(rel)})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/: %v", err)
	}

	// Anti-vacuity floor. There are a dozen-plus such pointers today; a walker
	// finding none has stopped matching the convention, and a silent zero is
	// how a derived gate rots into a green no-op.
	if seen < 8 {
		t.Fatalf("found only %d backtick-quoted internal/*.go pointers; expected at least 8.\n\n"+
			"The matcher no longer reflects how this repo cites files — fix the matcher rather "+
			"than lowering the floor, or this gate silently checks nothing.", seen)
	}

	for _, b := range broken {
		t.Errorf("%s cites `%s`, which does not exist", b.from, b.path)
	}
	if len(broken) > 0 {
		t.Logf("\n%d of %d code pointers are stale. A comment that points at a moved file sends "+
			"the next reader nowhere, and nothing else in the build notices — that is why this "+
			"check exists. Update the path, or drop the citation if the code it described is gone.",
			len(broken), seen)
	}
}

// docLinePointerRe matches a backtick-quoted `internal/…​.go:LINE` or
// `…​.go:LINE-LINE` span in a doc — the shape `docs/dev/perf-parity-matrix.md`
// cites code by. The line group(s) let the gate check RANGE, not just existence.
var docLinePointerRe = regexp.MustCompile("`(internal/[A-Za-z0-9_./-]+\\.go):(\\d+)(?:-(\\d+))?`")

// TestDocCodePointers_ExistAndLineInRange is the G-13 gate (2026-08-05 audit
// proposal, built 2026-08-18): nothing mechanically checked the `file:line`
// citations in docs/, which is why perf-parity-matrix.md drifts a dozen at a
// time — each pre-release pass re-derived 5–30 by hand. This grades every
// backtick-quoted `internal/….go:LINE(-LINE)` span in docs/dev/*.md, asserting
// the file EXISTS and the line (and any range end) is WITHIN the file. It
// deliberately does NOT assert the symbol at that line — that would
// re-implement a compiler and false-fail on every unrelated edit above the
// cite; existence + in-range catches the deletion and whole-file-moved cases,
// which is what actually happens.
func TestDocCodePointers_ExistAndLineInRange(t *testing.T) {
	repoRoot := filepath.Join("..", "..")
	docsDir := filepath.Join(repoRoot, "docs", "dev")

	type badRef struct {
		from, path string
		line, max  int
	}
	var (
		graded int
		docs   = map[string]bool{}
		bad    []badRef
	)

	lineCounts := map[string]int{} // repo-relative go path → line count (cached)
	countLines := func(goRel string) (int, bool) {
		if n, ok := lineCounts[goRel]; ok {
			return n, n >= 0
		}
		b, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(goRel)))
		if err != nil {
			lineCounts[goRel] = -1
			return 0, false
		}
		n := strings.Count(string(b), "\n") + 1
		lineCounts[goRel] = n
		return n, true
	}

	err := filepath.Walk(docsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".md") {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(repoRoot, path)
		for _, m := range docLinePointerRe.FindAllStringSubmatch(string(b), -1) {
			graded++
			docs[path] = true
			goRel := m[1]
			max, ok := countLines(goRel)
			if !ok {
				bad = append(bad, badRef{from: filepath.ToSlash(rel), path: goRel, line: 0, max: -1})
				continue
			}
			// Check both the start line and (if present) the range end.
			for _, s := range []string{m[2], m[3]} {
				if s == "" {
					continue
				}
				var ln int
				for _, c := range s {
					ln = ln*10 + int(c-'0')
				}
				if ln < 1 || ln > max {
					bad = append(bad, badRef{from: filepath.ToSlash(rel), path: goRel, line: ln, max: max})
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk docs/dev: %v", err)
	}

	// Anti-vacuity: perf-parity-matrix.md alone carries dozens of these.
	if len(docs) < 1 || graded < 20 {
		t.Fatalf("graded only %d `path:line` spans across %d docs (floor: >=20 spans, >=1 doc) — "+
			"the matcher no longer reflects how docs/dev cite code; fix the matcher, not the floor", graded, len(docs))
	}

	for _, b := range bad {
		if b.max < 0 {
			t.Errorf("%s cites `%s:%d`, but that file does not exist", b.from, b.path, b.line)
		} else {
			t.Errorf("%s cites `%s:%d`, but that file has only %d lines — the citation drifted", b.from, b.path, b.line, b.max)
		}
	}
}

// docSymbolRe matches a backtick-quoted `pkg.Symbol` span (optionally
// spelled with a call suffix: `pkg.Symbol()` / `pkg.Symbol(...)`) — the
// shape a doc cites a Go identifier by. The qualifier is required to be
// lower-case (a package name), the symbol to be an identifier.
var docSymbolRe = regexp.MustCompile("`([a-z][a-zA-Z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)(?:\\(\\)|\\(\\.\\.\\.\\))?`")

// docSymbolFileExtTokens are "symbol" parts that mean the span is a
// FILENAME (`stream.go`, `chain.json`), not an identifier. Skipped, not
// graded — file existence is the first gate's job.
var docSymbolFileExtTokens = map[string]bool{
	"go": true, "md": true, "json": true, "yaml": true, "yml": true, "sh": true,
	"ps1": true, "sql": true, "txt": true, "tmpl": true, "conf": true, "env": true,
	"exe": true, "mjs": true, "js": true, "html": true, "css": true, "toml": true,
	"lock": true, "sum": true, "mod": true, "ico": true, "svg": true, "png": true,
}

// docSymbolExempt lists graded spans that are deliberately allowed to
// resolve to nothing, each with the reason. An entry no doc uses any
// more fails the test — a stale blessing is how a roster starts
// covering less than its name implies.
var docSymbolExempt = map[string]string{
	"backup.SourceEngineCommitsAfterRows": "cited AS the phantom: this is the symbol that never existed, named in the G-13 " +
		"filing (and its closure note) precisely because a doc once cited it as real — the citation is the history of this " +
		"gate, not a claim about the code",
}

// TestDocSymbolPointers_ResolveAgainstTheirPackage is the G-13 sibling
// (filed alongside the `path:line` gate above; built 2026-08-22): a doc
// sentence citing `pkg.Symbol` is a claim that the symbol exists, and
// nothing checked it — which is how the phantom
// `backup.SourceEngineCommitsAfterRows` survived in three docs.
//
// # What it reaches, stated rather than implied
//
// It grades backtick-quoted `pkg.Symbol` spans in the CLAIM-surface
// docs: docs/dev/*.md EXCLUDING roadmap.md and item49-phase3-prep.md,
// and excluding the docs/dev/design/ and docs/dev/notes/ trees — those
// are planning surfaces whose house convention deliberately names
// PROPOSED symbols (a roadmap entry doubles as a self-contained prompt),
// so "does not exist yet" is their normal state, not drift. A span is
// graded only when its qualifier matches an internal package DIRECTORY
// basename; same-basename packages (ir/backup vs pipeline/backup) are
// unioned. NOT graded, deliberately: Type.Field and receiver refs
// (`Streamer.FKEnablementChecker`, `col.Type` — the qualifier is not a
// package dir), stdlib refs (`strings.ToLower`), import-alias refs
// (`irdiff.TableDiff` — aliases are per-file, not derivable from a doc),
// and snake_case all-lowercase symbols (`mysql.general_log` — a
// DATABASE object citation; Go top-level decls here are camel-case).
// Resolution accepts any top-level decl name in the package, exported
// or not, including test files — docs legitimately cite unexported
// helpers and gate tests.
//
// The symbol is resolved by NAME only — no type checking, for the same
// reason the line gate does not assert the symbol at the line: existence
// catches the deletion/rename case, which is what actually happens.
func TestDocSymbolPointers_ResolveAgainstTheirPackage(t *testing.T) {
	repoRoot := filepath.Join("..", "..")

	// Build the resolution universe: package dir basename → the set of
	// top-level decl names across every .go file in every directory with
	// that basename.
	decls := map[string]map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.Walk(filepath.Join(repoRoot, "internal"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		dir := filepath.Base(filepath.Dir(path))
		if dir == "internal" || dir == "testdata" {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			// A file the parser cannot read contributes nothing; the build
			// would be broken anyway, and skipping keeps this walk total.
			return nil //nolint:nilerr // deliberate: see comment above
		}
		set := decls[dir]
		if set == nil {
			set = map[string]bool{}
			decls[dir] = set
		}
		for _, d := range f.Decls {
			switch dd := d.(type) {
			case *ast.FuncDecl:
				set[dd.Name.Name] = true
			case *ast.GenDecl:
				for _, spec := range dd.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						set[s.Name.Name] = true
					case *ast.ValueSpec:
						for _, n := range s.Names {
							set[n.Name] = true
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/: %v", err)
	}
	if len(decls) < 30 {
		t.Fatalf("resolution universe holds only %d package basenames; expected at least 30 — the universe builder broke, "+
			"and every graded span would false-fail against it", len(decls))
	}

	type badRef struct{ from, span string }
	var (
		graded      int
		docs        = map[string]bool{}
		bad         []badRef
		exemptInUse = map[string]bool{}
	)

	err = filepath.Walk(filepath.Join(repoRoot, "docs", "dev"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "design" || info.Name() == "notes" {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(path)
		if !strings.HasSuffix(base, ".md") || base == "roadmap.md" || base == "item49-phase3-prep.md" {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(repoRoot, path)
		for _, m := range docSymbolRe.FindAllStringSubmatch(string(b), -1) {
			pkg, sym := m[1], m[2]
			if docSymbolFileExtTokens[sym] {
				continue // a filename, not an identifier
			}
			if strings.Contains(sym, "_") && strings.ToLower(sym) == sym {
				continue // a database object (mysql.general_log), not a Go symbol
			}
			set, known := decls[pkg]
			if !known {
				continue // qualifier is not an internal package dir — out of scope, see the doc comment
			}
			graded++
			docs[path] = true
			span := pkg + "." + sym
			if set[sym] {
				continue
			}
			if _, exempt := docSymbolExempt[span]; exempt {
				exemptInUse[span] = true
				continue
			}
			bad = append(bad, badRef{from: filepath.ToSlash(rel), span: span})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk docs/dev: %v", err)
	}

	// Anti-vacuity: audit-backlog.md and perf-parity-matrix.md alone carry
	// dozens of these today.
	if graded < 40 || len(docs) < 2 {
		t.Fatalf("graded only %d `pkg.Symbol` spans across %d docs (floor: >=40 spans, >=2 docs) — the matcher or the "+
			"universe no longer reflects how docs/dev cite identifiers; fix the walker, not the floor", graded, len(docs))
	}

	for _, b := range bad {
		t.Errorf("%s cites `%s`, which no internal package of that name declares — the symbol was renamed, moved out of "+
			"the package, or never existed (the backup.SourceEngineCommitsAfterRows shape). Update the citation, or add a "+
			"docSymbolExempt entry with the reason if the doc cites it deliberately.", b.from, b.span)
	}
	for span := range docSymbolExempt {
		if !exemptInUse[span] {
			t.Errorf("docSymbolExempt lists %q but no graded doc span needs it any more — remove the entry", span)
		}
	}
}
