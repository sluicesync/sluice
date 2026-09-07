// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// perfMatrixCitationRE matches the `<path>.go:<line>` citations the perf
// parity matrix uses. The path is often abbreviated (`pg/cdc_reader.go`,
// `sqlite/d1_rows.go`), so only the SUFFIX is matched against the tree.
var perfMatrixCitationRE = regexp.MustCompile(`([A-Za-z0-9_./-]+\.go):(\d+)`)

// perfMatrixPathAliases maps the matrix's shorthand directory names onto
// the real ones. `pg/` predates the package being called `postgres`.
var perfMatrixPathAliases = map[string]string{
	"pg/": "postgres/",
}

// TestPerfMatrixCitationsResolve is the durable half of the audit
// 2026-09-06 PERF-MATRIX-ARREARS fix.
//
// THE ARREARS. docs/dev/perf-parity-matrix.md had ZERO commits across
// v0.137.4..HEAD (131 commits) and ~61 of its 139 live citations had
// drifted — in BOTH directions and by differing amounts, so no uniform
// offset could repair them. The staleness predated the file's own
// re-derived stamp: change_applier.go had no commits since that stamp
// and its cited lines had still moved.
//
// WHY THIS GATE CHECKS FILES AND NOT LINES. A line number is stale the
// moment anything above it changes, so a gate over line numbers would
// fail constantly on correct edits and teach people to re-derive
// mechanically — which is how a uniform offset gets applied and the
// error gets encoded. What a gate CAN hold is that every cited file
// still exists: a citation whose FILE moved or was deleted points at
// nothing at all, and that is unambiguous rot rather than drift.
//
// The line numbers are deliberately left as approximate locators, and
// the matrix says so at the top. New cells should cite a SYMBOL, which
// is greppable and does not rot.
//
// WHAT IT DOES NOT REACH, stated so the name is not read wider: it does
// not verify a citation points at the RIGHT code, only that the file is
// real. Nothing cheap can verify the former, and the expensive version
// (an expected-symbol-per-cell roster) is the thing to build if this
// class recurs.
func TestPerfMatrixCitationsResolve(t *testing.T) {
	root := repoRootFromDocsync(t)
	path := filepath.Join(root, "docs", "dev", "perf-parity-matrix.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	seen := map[string]bool{}
	for _, m := range perfMatrixCitationRE.FindAllStringSubmatch(string(raw), -1) {
		p := m[1]
		for from, to := range perfMatrixPathAliases {
			if strings.HasPrefix(p, from) {
				p = to + strings.TrimPrefix(p, from)
			}
		}
		seen[p] = true
	}

	// Anti-vacuity: the matrix carries many dozens of distinct cited
	// files. A regex that stops matching must FAIL rather than pass on an
	// empty set — the SidecarCheckpointCost lesson.
	if len(seen) < 40 {
		t.Fatalf("extracted only %d distinct cited .go files from the perf matrix; it carries far more, "+
			"so the citation regex has drifted and this gate would pass on nothing", len(seen))
	}

	var missing []string
	for p := range seen {
		if resolvesInTree(root, p) {
			continue
		}
		missing = append(missing, p)
	}
	if len(missing) > 0 {
		t.Errorf("%d file(s) cited by docs/dev/perf-parity-matrix.md do not exist in the tree:\n  %s\n\n"+
			"A citation whose FILE moved points at nothing — unlike a stale line number, which is at "+
			"least in the right place. Re-point the cell, or add the shorthand to perfMatrixPathAliases "+
			"if the directory is merely abbreviated.",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// resolvesInTree reports whether suffix matches some file under
// internal/ or cmd/. Suffix matching is what the matrix's abbreviated
// citations need.
func resolvesInTree(root, suffix string) bool {
	found := false
	for _, dir := range []string{"internal", "cmd"} {
		_ = filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || found || info == nil || info.IsDir() {
				return nil //nolint:nilerr // a walk error on one subtree must not fail the whole probe
			}
			if strings.HasSuffix(filepath.ToSlash(p), "/"+suffix) {
				found = true
			}
			return nil
		})
		if found {
			return true
		}
	}
	return false
}
