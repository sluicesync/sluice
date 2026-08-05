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
