// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFanOutFilterCallersReportUnmatchedPatterns is the gate on Bug 273's
// fourth arm.
//
// THE ARM. `TABLE-FILTER-PATTERN-UNMATCHED` warns when an operator's table
// filter pattern matched nothing — which on the exclude path fails OPEN, so
// the table they meant to keep out is copied at exit 0. A multi-database
// fan-out calls the filter door once per database over that database's tables
// only, so a pattern naming a table in database B looks dead while database A
// is being processed: one false warning per pass. Reporting per pass cannot
// be made correct, because no single pass has the universe.
//
// So fan-out passes call [migcore.ApplyTableFilterQuiet] and the driver calls
// [migcore.ReportUnmatchedPatterns] once at the end.
//
// WHY THIS GATE, AND WHY IT IS THE HALF THAT MATTERS. The failure mode of that
// arrangement is not a false fire — it is SILENCE. A file that goes quiet and
// never reports has removed the Bug 272 protection entirely for exactly the
// multi-database operators who most need it, and nothing observable says so:
// the run exits 0 either way, which is the whole reason Bug 272 existed. This
// repo has shipped that shape before ("a declaration with no refusal path is
// worse than none, because it reads as coverage"), so the quiet form is gated
// at the file level from the day it is introduced rather than after.
//
// WHAT IT REACHES, stated so the name cannot be read as broader: every .go
// file under internal/pipeline (recursively, excluding _test.go) that calls
// ApplyTableFilterQuiet must also call ReportUnmatchedPatterns somewhere in
// the same file, OR appear in fanOutReportExempt with a reason. It does NOT
// prove the report is reachable on every path, that it runs after the loop, or
// that the census is complete — only that the pairing exists. The behavioural
// half is TestFanOutReportsOnceAgainstTheWholeUniverse in migcore.
func TestFanOutFilterCallersReportUnmatchedPatterns(t *testing.T) {
	// Files that legitimately go quiet without reporting, each with the
	// reason. Fail-by-default: an entry naming a file that no longer calls
	// the quiet form fails too, so this list cannot rot into a permanent
	// excuse.
	fanOutReportExempt := map[string]string{}

	const (
		quietCall  = "ApplyTableFilterQuiet("
		reportCall = "ReportUnmatchedPatterns("
	)

	var quietFiles, reportFiles []string
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		// Parse rather than grep so a mention inside a comment does not
		// count as a call — the same defect the NOT VALID roster shipped
		// with and the 2026-09-06 audit mutation-proved.
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, b, 0)
		if perr != nil {
			return nil //nolint:nilerr // an unparseable file breaks the build anyway
		}
		var quiet, report bool
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case strings.TrimSuffix(quietCall, "("):
				quiet = true
			case strings.TrimSuffix(reportCall, "("):
				report = true
			}
			return true
		})
		base := filepath.Base(path)
		if quiet {
			quietFiles = append(quietFiles, base)
		}
		if report {
			reportFiles = append(reportFiles, base)
		}
		if quiet && !report {
			if _, ok := fanOutReportExempt[base]; !ok {
				t.Errorf("%s calls ApplyTableFilterQuiet and never calls ReportUnmatchedPatterns.\n\n"+
					"A fan-out that goes quiet and never reports has SILENTLY removed the Bug 272 protection "+
					"for multi-database runs — an --exclude-table pattern that matches nothing fails OPEN and "+
					"copies the table at exit 0, with nothing to observe. Either report once when the fan-out "+
					"completes, or add a fanOutReportExempt entry saying why this file cannot.", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/pipeline: %v", err)
	}

	// Anti-vacuity, both directions. A walk that stopped finding the quiet
	// form would report a clean sheet; a repo with no reporter would mean
	// the pairing is theoretical.
	if len(quietFiles) < 2 {
		t.Fatalf("found %d file(s) calling ApplyTableFilterQuiet (%v); at least two fan-out files use it "+
			"(the migrate and streamer multi-database paths). The AST walk has drifted — fix the walk, not the floor.",
			len(quietFiles), quietFiles)
	}
	if len(reportFiles) < 2 {
		t.Fatalf("found %d file(s) calling ReportUnmatchedPatterns (%v); floor 2. Either a driver stopped "+
			"reporting or the walk drifted.", len(reportFiles), reportFiles)
	}

	// Every exemption must still be earned.
	for file, why := range fanOutReportExempt {
		var stillQuiet bool
		for _, q := range quietFiles {
			if q == file {
				stillQuiet = true
			}
		}
		if !stillQuiet {
			t.Errorf("fanOutReportExempt lists %q (%s) but it no longer calls ApplyTableFilterQuiet — "+
				"remove the entry rather than leaving a standing excuse", file, why)
		}
	}
}
