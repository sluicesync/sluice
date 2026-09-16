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

// perfMatrixH1 is the exact first line docs/dev/perf-parity-matrix.md must
// carry. Pinning the LITERAL is the point: the defect this gate exists for
// left the H1 intact but appended it to 3,976 bytes of three other rows'
// text, so a looser "line 1 starts with a heading" check would have passed.
const perfMatrixH1 = "# Performance-technique parity matrix"

// perfMatrixRowNumRE pulls the leading row number out of a table row
// (`| 34. Neki online-DDL index builds …` -> "34").
var perfMatrixRowNumRE = regexp.MustCompile(`^\|\s*(\d+)\.`)

// TestPerfMatrixIsStructurallySound is the Tier-1 ratchet for a defect
// class that cost a pre-tag repair on 2026-09-15.
//
// THE DEFECT. b9efeee1 (the v0.152.0 doc pass) left line 1 of
// docs/dev/perf-parity-matrix.md as a 3,976-byte blob: fragments of rows
// 11, 7 and 4 concatenated together with the H1 stuck on the END. It
// survived four subsequent commits and two releases. Its visible harm was
// that row 4 went on publishing "CANNOT retry — one-shot io.Reader" and
// "the ONLY unretryable one-shot write lane left in the tree" while the
// code had had bounded retry on both legs since v0.152.0 — a doc
// contradicting the code in the one file whose whole purpose is to stop
// exactly that. f6ee65fb repaired it.
//
// WHY THE EXISTING GATE COULD NOT SEE IT. TestPerfMatrixCitationsResolve
// grades cited FILE existence, and every file the blob named still
// existed. The rot was in the file's SHAPE, and nothing graded shape.
//
// WHAT THIS GRADES — three structural invariants, all cheap:
//
//   - line 1 is exactly the H1 and nothing else (the blob's own signature);
//   - every table row has exactly the column count the HEADER row
//     declares. Derived FROM the header rather than hardcoded, so widening
//     the table does not silently disarm the check;
//   - every row's leading number is unique (two rows were both numbered
//     34 — the third drift the same review found).
//
// WHAT IT DOES NOT REACH, stated plainly because a gate whose name reads
// wider than it reaches is the failure this whole exercise is about: it
// grades STRUCTURE, never TRUTH. It cannot tell whether a cell's claim
// about the code is correct, whether a ✅ sits in the column it belongs
// in, or whether a row that should exist is missing altogether. Row 4's
// false "ONLY unretryable lane" assertion — the actual harm above —
// passes this gate cleanly the moment the columns line up. Content truth
// is what the perf-parity-checker agent and the matrix's own maintenance
// protocol are for. This gate guarantees only that the file still parses
// as the table it claims to be, which is the half that was ungated.
func TestPerfMatrixIsStructurallySound(t *testing.T) {
	root := repoRootFromDocsync(t)
	gradePerfMatrixStructure(t, filepath.Join(root, "docs", "dev", "perf-parity-matrix.md"))
}

// gradePerfMatrixStructure holds the three invariants. It is split out of
// the test so a mutation run can point it at a fixture and confirm the
// anti-vacuity floors FIRE on a file that parses to nothing, rather than
// reporting the green that an empty parse would otherwise produce.
func gradePerfMatrixStructure(t *testing.T, path string) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")

	var first string
	if len(lines) > 0 {
		first = lines[0]
	}
	if first != perfMatrixH1 {
		t.Fatalf("line 1 of %s must be exactly %q (%d bytes); it is %d bytes beginning %q.\n\n"+
			"b9efeee1 left line 1 as a 3,976-byte blob of rows 11, 7 and 4 with the H1 appended to the END, "+
			"and it survived four commits and two releases. A table row's text on line 1 means a row lost "+
			"its newline and was joined onto the heading — find that row and put the newline back.",
			path, perfMatrixH1, len(perfMatrixH1), len(first), truncateForMsg(first))
	}

	var (
		wantCols int
		headerAt int
		numbered int
	)
	seenRowNum := map[string]int{}

	for i, line := range lines {
		// The separator is `|---|---|…` in this file, which the `| `
		// prefix already skips; isPerfMatrixSeparator additionally covers
		// the `| --- | --- |` spelling so a reformat cannot smuggle a
		// separator in as a data row.
		if !strings.HasPrefix(line, "| ") {
			continue
		}
		cells := perfMatrixCells(line)
		if isPerfMatrixSeparator(cells) {
			continue
		}
		if wantCols == 0 {
			wantCols, headerAt = len(cells), i+1
			continue
		}

		if len(cells) != wantCols {
			t.Errorf("line %d (row %q) has %d columns; the header at line %d declares %d.\n"+
				"Every cell after a dropped one renders one column LEFT of where it reads, so the row's "+
				"trailing note lands under the wrong mode. Insert the missing cell — an explicit n/a where "+
				"the technique does not apply to that mode — rather than deleting a cell to match.",
				i+1, truncateForMsg(cells[0]), len(cells), headerAt, wantCols)
		}

		m := perfMatrixRowNumRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		numbered++
		if prev, dup := seenRowNum[m[1]]; dup {
			t.Errorf("row number %s is used twice — line %d and line %d (%q).\n"+
				"Row numbers are how gaps, ADRs and commit messages cite a cell, so a duplicate silently "+
				"redirects every reference to whichever row the reader finds first.",
				m[1], prev, i+1, truncateForMsg(cells[0]))
		} else {
			seenRowNum[m[1]] = i + 1
		}
	}

	// Anti-vacuity floors. Both exist because a parse that quietly matches
	// nothing reports the same green as a healthy file — the
	// SidecarCheckpointCost lesson. The numbered-row floor guards the parse
	// the duplicate check RIDES on: if row numbering stops being extracted,
	// uniqueness becomes vacuously true, and this fails instead.
	if wantCols == 0 {
		t.Fatalf("found no header row in %s, so nothing was graded — the table markup has changed shape "+
			"and this gate would have passed on an empty parse", path)
	}
	if numbered < 30 {
		t.Fatalf("graded only %d numbered table rows in %s (header at line %d declares %d columns); the "+
			"matrix carries far more, so the row parse has drifted and the duplicate-number check would "+
			"have been vacuously true", numbered, path, headerAt, wantCols)
	}
}

// perfMatrixCells splits a markdown table row into its trimmed cells. It
// counts raw `|` delimiters, so a cell containing a literal pipe would be
// miscounted — that surfaces as a LOUD column-count failure naming the
// row, never a silent pass, which is the safe direction for the error.
func perfMatrixCells(line string) []string {
	s := strings.TrimSpace(line)
	s = strings.TrimPrefix(s, "|")
	s = strings.TrimSuffix(s, "|")
	parts := strings.Split(s, "|")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// isPerfMatrixSeparator reports whether the cells are a markdown alignment
// row (`---`, `:---:`) rather than content.
func isPerfMatrixSeparator(cells []string) bool {
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		if c == "" || strings.Trim(c, "-:") != "" {
			return false
		}
	}
	return true
}

// truncateForMsg shortens a cell for a failure message. It slices RUNES:
// the matrix is full of ✅/❌ and a byte slice would print a broken one.
func truncateForMsg(s string) string {
	const maxRunes = 120
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes]) + "…"
}
