// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A `CAST(col AS TEXT) AS col` alias must not be sorted or grouped on.
//
// WHY THIS EXISTS (Bug 266, CRITICAL silent data loss, live D1). The D1
// transport projects integers it keys on as exact TEXT, so a JSON number
// cannot round them (ADR-0132). The idiomatic way to write that is
// `CAST(id AS TEXT) AS id` — which puts a TEXT expression in scope under the
// integer column's own name.
//
// SQLite resolves an ORDER BY term against the output aliases BEFORE the
// source columns. So `ORDER BY id` on that projection sorts the TEXT:
// 1, 10, 11, 12, 2, 3, 9. The change-log poll did exactly this. It was inert
// until v0.141.0 added an adaptive page, and then a truncated page skipped
// every id below the page's numeric maximum that sorted later — captured,
// never delivered, never read again, with the stream alive and reporting
// nothing. Measured: 50 of 53 rows reached the target.
//
// WHAT THIS GRADES, and the distinction is the whole point: the shadowing
// alias is NOT itself a defect. `readConsumersSQL` shadows `applied_id` and
// is completely safe, because it has no ORDER BY, GROUP BY or HAVING for the
// alias to hijack. The defect is a shadowing alias PLUS a clause that
// resolves against aliases. Refusing the alias outright would refuse the
// ADR-0132 idiom the D1 transport is built on; this refuses the combination.
//
// The remedy is to qualify the sort term (`ORDER BY "tbl".id`), which binds
// it to the column rather than the projection.
//
// SCOPE: this package's non-test .go files, which is where the trigger
// engine's SQL is written. It is a string-level check over the assembled
// query text, not a parser — a query split across variables far enough apart
// that the alias and the ORDER BY never appear in one literal would evade
// it. Both of today's cases are single literals; if that stops being true,
// this needs to grow, and the floor below is what will say so.
func TestNoSortedCastAliasShadow(t *testing.T) {
	// `CAST(<name> ...) AS <same name>` — the alias that shadows its column.
	// The CAST argument may be written bare (`CAST(id AS TEXT)`) or qualified
	// (`CAST("tbl".id AS TEXT)`), and in this repo the qualifier is a Go
	// concatenation, so anything up to the closing paren is tolerated. What
	// matters is the ALIAS: the shadow exists whenever the alias equals the
	// column name, however the argument is spelled. Keying on the bare form
	// alone made this gate stop seeing the poll the moment its argument was
	// qualified — and the poll is the reason the gate exists.
	shadow := regexp.MustCompile(`(?i)CAST\([^)]*?\b([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+\w+\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)`)

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	shadows, sortedShadows := 0, 0
	var problems []string

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, rerr := os.ReadFile(name)
		if rerr != nil {
			t.Fatalf("read %s: %v", name, rerr)
		}
		// Comment lines are blanked, not dropped, so indices still line up.
		// Without this the gate reads the prose ABOUT the defect as the
		// defect: the fix's own comment quotes the bad `ORDER BY id` to
		// explain it, and the first scoped cut flagged that. A checker that
		// cannot tell code from a comment describing code will always find
		// its own documentation.
		lines := strings.Split(string(body), "\n")
		for i, l := range lines {
			if strings.HasPrefix(strings.TrimSpace(l), "//") {
				lines[i] = ""
			}
		}
		for li, line := range lines {
			for _, m := range shadow.FindAllStringSubmatch(line, -1) {
				col, alias := m[1], m[2]
				if !strings.EqualFold(col, alias) {
					continue // aliased to something else; no shadow
				}
				shadows++
				// Scoped to THIS query, not the file. The first cut looked at
				// the whole file and flagged the LOCAL executor query, which
				// selects a bare id and has no shadow at all -- a gate whose
				// false positives point at correct code gets suppressed,
				// which is the same outcome as not having it. A query is a
				// run of concatenated literals, so a short window after the
				// CAST is the statement it belongs to.
				end := li + 8
				if end > len(lines) {
					end = len(lines)
				}
				window := strings.Join(lines[li:end], "\n")
				// A qualified term ("tbl".id or tbl.id) binds to the column
				// and is safe.
				for _, clause := range []string{"ORDER BY", "GROUP BY", "HAVING"} {
					unqualified := regexp.MustCompile(
						`(?i)` + clause + `\s+` + regexp.QuoteMeta(alias) + `\b`,
					)
					if unqualified.MatchString(window) {
						sortedShadows++
						problems = append(problems, fmt.Sprintf(
							"%s: `CAST(%s AS …) AS %s` shadows the column, and this file has an unqualified `%s %s`",
							filepath.Base(name), col, alias, clause, alias,
						))
					}
				}
			}
		}
	}

	// Anti-vacuity. Two shadowing aliases exist today (the change-log poll's
	// `id` and readConsumersSQL's `applied_id`). A regex that stopped
	// matching would report zero problems and pass, which is the failure
	// mode that makes a gate worse than none.
	if shadows < 2 {
		t.Fatalf("found only %d shadowing CAST alias(es); the pattern stopped matching and this gate is vacuous", shadows)
	}

	if len(problems) > 0 {
		t.Fatalf("a CAST-to-TEXT alias shadows its column AND is sorted/grouped on:\n  %s\n\n"+
			"SQLite resolves ORDER BY against output aliases first, so this sorts the TEXT — 1, 10, 11, 12, 2, 3, 9 —\n"+
			"and any LIMIT that truncates then skips every lower id that sorted later. That is silent CDC loss:\n"+
			"the stream advances past rows it never delivered (Bug 266).\n"+
			"Qualify the sort term (ORDER BY \"table\".col) so it binds to the column, not the projection.",
			strings.Join(problems, "\n  "))
	}
}
