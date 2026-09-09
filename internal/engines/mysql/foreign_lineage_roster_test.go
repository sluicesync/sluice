// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// lineageExemptMarker is how a "different lineage" verdict declares that
// it deliberately keeps ir.ErrPositionInvalid. It sits ON the site, in a
// comment within the return statement's own lines, and carries the
// reason — because the FIRST cut of this roster exempted by FILE NAME,
// reader_errors.go held two such sites, and the single reason recorded
// there was true of one and false of the other. The false one was the
// VStream reactive classification, whose stated cover ("the reactive
// door asks the reader") did not apply because the VStream reader was
// not an ir.LineageVerifier at all — so the exemption was defending the
// defect this class exists to close (pre-tag value-fidelity review,
// 2026-09-09).
const lineageExemptMarker = "lineage-exempt:"

// siteWindow returns the text of the statement spanning [startLine,
// endLine] together with the contiguous comment block immediately above
// it — the place an exemption for that statement is allowed to live.
// lines is the file split on "\n" (1-indexed by line number).
func siteWindow(lines []string, startLine, endLine int) string {
	first := startLine
	for first > 1 {
		above := strings.TrimSpace(lines[first-2])
		if !strings.HasPrefix(above, "//") {
			break
		}
		first--
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}
	return strings.Join(lines[first-1:endLine], "\n")
}

// Every return site in this engine whose diagnosis is "different
// lineage" / "different server" must carry ir.ErrPositionForeignLineage
// — or carry the exempt marker WITH a reason, at the site.
//
// The bug this exists for: v0.146.0 made a replaced instance terminal on
// the file/pos arm and declared the GTID arm covered "by construction";
// it was caught by construction and then routed into the automatic
// re-copy, which drops the target's tables and refills them from the
// other database at exit 0.
//
// Scope, stated so the name cannot be read as broader than the truth:
// non-test .go files in THIS package, and only return statements whose
// own string literals carry one of the two phrases. A lane that words
// its verdict differently is invisible here — which is why the reader
// roster below grades the CAPABILITY rather than the wording.
func TestForeignLineageVerdictsCarryTheSentinel(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	graded, carrying, exempted := 0, 0, 0
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(src)
		if !strings.Contains(text, "different lineage") && !strings.Contains(text, "different server") {
			continue
		}
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		lines := strings.Split(text, "\n")
		ast.Inspect(f, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok {
				return true
			}
			var mentions, carries bool
			ast.Inspect(ret, func(m ast.Node) bool {
				switch x := m.(type) {
				case *ast.BasicLit:
					if x.Kind == token.STRING &&
						(strings.Contains(x.Value, "different lineage") || strings.Contains(x.Value, "different server")) {
						mentions = true
					}
				case *ast.SelectorExpr:
					if x.Sel.Name == "ErrPositionForeignLineage" {
						carries = true
					}
				}
				return true
			})
			if !mentions {
				return true
			}
			graded++
			if carries {
				carrying++
				return true
			}
			// The exemption must sit at the site: inside the return, or in
			// the comment block immediately above it. The block is read
			// whole rather than a fixed number of lines — the first cut
			// looked back six lines, and a reason long enough to be worth
			// writing pushed its own marker out of the window, which read
			// as "no exemption" on a site that had one.
			window := siteWindow(lines, fset.Position(ret.Pos()).Line, fset.Position(ret.End()).Line)
			idx := strings.Index(window, lineageExemptMarker)
			if idx < 0 {
				t.Errorf("%s:%d returns a \"different lineage\" verdict without ir.ErrPositionForeignLineage — it "+
					"wraps the sentinel that routes the automatic re-copy, which drops the target and refills it "+
					"from the other database (A0909-MYSQL-HIGH-1). Carry the sentinel, or write `%s <why>` in a "+
					"comment on this site.", path, fset.Position(ret.Pos()).Line, lineageExemptMarker)
				return true
			}
			if reason := strings.TrimSpace(window[idx+len(lineageExemptMarker):]); len(reason) < 30 {
				t.Errorf("%s:%d carries %s with no real reason (%q)", path, fset.Position(ret.Pos()).Line,
					lineageExemptMarker, reason)
			}
			exempted++
			return true
		})
	}
	// Anti-vacuity: the four lanes' known sites (GTID continuity, file/pos
	// identity, MariaDB anchor ×2 + domain, VStream foreign) plus the two
	// exempt reactive classifications.
	if graded < 6 {
		t.Fatalf("graded only %d \"different lineage\" return sites; the roster expects at least 6 — the phrase "+
			"was reworded or the scan broke", graded)
	}
	if carrying < 5 {
		t.Fatalf("only %d sites carry the sentinel; at least 5 must", carrying)
	}
	if exempted < 1 {
		t.Fatalf("no site is exempt; the reactive error-text classifications are expected to be — the marker or " +
			"the scan broke")
	}
}

// EVERY CDC reader this engine can hand the pipeline must be able to
// answer the reactive path's lineage question.
//
// The compile-time `var _ ir.LineageVerifier` pins in cdc_lineage_verify.go
// prove the two readers that exist TODAY implement it. They cannot fail
// for a reader added tomorrow, which is exactly how the first cut shipped:
// the binlog reader had the method, the VStream reader did not, and the
// pipeline's door silently took its "engine without the capability"
// branch and re-copied over the target.
//
// So the universe is derived from the code rather than listed: every type
// in this package that implements the ir.CDCReader contract (a
// StreamChanges method) must also have a VerifyLineage method.
func TestEveryCDCReaderAnswersTheLineageQuestion(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	streamers := map[string]string{} // receiver type → file:line of StreamChanges
	verifiers := map[string]bool{}   // receiver type → has VerifyLineage
	exempt := map[string]bool{}      // receiver type → StreamChanges carries the marker
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(src), "\n")
		f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			recv := ""
			switch e := fn.Recv.List[0].Type.(type) {
			case *ast.StarExpr:
				if id, ok := e.X.(*ast.Ident); ok {
					recv = id.Name
				}
			case *ast.Ident:
				recv = e.Name
			}
			if recv == "" {
				continue
			}
			switch fn.Name.Name {
			case "StreamChanges":
				streamers[recv] = fset.Position(fn.Pos()).String()
				if strings.Contains(
					siteWindow(lines, fset.Position(fn.Pos()).Line, fset.Position(fn.Pos()).Line),
					lineageExemptMarker,
				) {
					exempt[recv] = true
				}
			case "VerifyLineage":
				verifiers[recv] = true
			}
		}
	}
	if len(streamers) < 2 {
		t.Fatalf("found %d CDC reader type(s) in this package; expected at least 2 (the binlog reader and the "+
			"VStream reader) — the scan broke", len(streamers))
	}
	answered := 0
	for recv, where := range streamers {
		if verifiers[recv] {
			answered++
			continue
		}
		if exempt[recv] {
			t.Logf("%s (%s) is exempt at its StreamChanges with a stated reason", recv, where)
			continue
		}
		if !verifiers[recv] {
			t.Errorf("%s implements StreamChanges (%s) but has no VerifyLineage, so the pipeline's reactive "+
				"re-snapshot cannot ask it whether the source is still the lineage the position came from — it "+
				"takes the no-capability branch and DROPS the target's tables to re-copy from whatever now "+
				"answers the DSN (A0909-MYSQL-HIGH-1). Give it VerifyLineage, even if the answer is always nil, "+
				"and say why at the method.", recv, where)
		}
	}
	// Anti-vacuity on the exemption: at least the two readers
	// [Engine.OpenCDCReader] can return must actually ANSWER, so a pass
	// cannot be bought by exempting everything.
	if answered < 2 {
		t.Fatalf("only %d reader type(s) implement VerifyLineage; the binlog and VStream readers both must", answered)
	}
}
