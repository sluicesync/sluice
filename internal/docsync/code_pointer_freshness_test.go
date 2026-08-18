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
