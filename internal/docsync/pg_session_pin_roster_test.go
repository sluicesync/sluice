// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Every Postgres session that renders a value AS TEXT must pin the GUCs that
// decide that rendering — and the roster is derived, not remembered.
//
// # Why this gate exists (audit 2026-08-05 B-1 and its sibling sweep)
//
// Postgres renders float and bytea through the type's TEXT output function,
// which honours the *session's* `extra_float_digits` and `bytea_output`. Those
// default from the server/database/role, never from sluice. Bug 194 established
// the float half; B-1 established the bytea half on the walsender.
//
// B-1's first fix pinned `bytea_output` on the replication connection AND
// STOPPED THERE. A review found two more lanes carrying PG-rendered text with
// no pin:
//
//   - the ordinary pgx pool (`postgres/connect.go`) — scalar bytea is immune
//     because pgx decodes it to raw bytes, but a `bytea[]` arrives as PG ARRAY
//     TEXT whose elements are formatted per the GUC. Under `bytea_output =
//     escape`, EVERY element of EVERY bytea array is silently corrupted on
//     migrate, sync cold-start, and into the backup archive.
//   - the pgtrigger capture function (`pgtrigger/setup.go`) — `to_jsonb()`
//     renders bytea through the same text path, in the FIRING application's
//     session, which sluice can never pin except on the function itself.
//
// Both were one-line fixes that nobody was going to find by remembering. The
// pattern is the one CLAUDE.md names: a fix that closes "the class" on the lane
// that surfaced it, while the siblings stay open and the commit message says
// the class is closed.
//
// So this gate derives the roster from the SOURCE: any site that pins
// extra_float_digits is, by construction, a session that renders values as
// text — and must therefore pin bytea_output too. A new lane that copies the
// float pin (which is what a new lane WILL copy, since that is the documented
// precedent) fails this until it copies both.
package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// pgTextRenderingGUCs are the session settings that decide how a value is
// spelled when Postgres renders it as text. Both are silent-loss surfaces:
// a wrong extra_float_digits rounds floats, a wrong bytea_output changes what
// a bytea's bytes even are.
var pgTextRenderingGUCs = []string{"extra_float_digits", "bytea_output"}

func TestPGSessionPins_EveryTextRenderingLanePinsBothGUCs(t *testing.T) {
	roots := []string{
		filepath.Join("..", "engines", "postgres"),
		filepath.Join("..", "engines", "pgtrigger"),
	}

	var (
		floatSites []string
		missing    []string
	)

	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			// Scan STRING LITERALS only, never raw file text. The pin is
			// always issued from a literal; prose about the pin lives in
			// comments, and several files discuss it at length. A substring
			// scan over the whole file flags those doc comments as unpinned
			// lanes — a false positive, and a gate that cries wolf is a gate
			// somebody suppresses.
			lits, parseErr := stringLiteralsIn(path)
			if parseErr != nil {
				return parseErr
			}
			if !anyContains(lits, "SET extra_float_digits = 3") {
				return nil
			}
			floatSites = append(floatSites, path)
			if !anyContains(lits, "SET bytea_output = hex") {
				missing = append(missing, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	// Anti-vacuity floor: three lanes pin the float GUC today (the pgx pool,
	// the walsender, and the pgtrigger capture function). A walker that finds
	// fewer has stopped matching the code, and a silent zero is exactly how a
	// derived gate rots into a green no-op.
	if len(floatSites) < 3 {
		t.Fatalf("found %d Postgres text-rendering session-pin site(s) %v; expected at least 3 "+
			"(the pgx pool, the walsender, the pgtrigger capture function).\n\n"+
			"The walker no longer matches the code, so this gate is vacuous — fix the walker, "+
			"do not lower the floor.", len(floatSites), floatSites)
	}

	if len(missing) > 0 {
		t.Errorf("these Postgres sessions pin extra_float_digits but NOT bytea_output: %v\n\n"+
			"Both GUCs decide how Postgres spells a value in its TEXT output, and both default "+
			"from the server/database/role rather than from sluice. A session that needs one pin "+
			"needs the other: under `bytea_output = escape` a bytea[] element, or a to_jsonb'd "+
			"bytea, arrives as escape-format ASCII and is stored verbatim as the target's bytes.\n\n"+
			"Add `SET bytea_output = hex` beside the float pin. If a lane genuinely does not need "+
			"it, say why where the pin lives — do not leave it implied.\n\n"+
			"Pinned GUCs checked: %v", missing, pgTextRenderingGUCs)
	}
}

// stringLiteralsIn returns every string-literal value in a Go source file.
// Comments are deliberately excluded — see the call site.
func stringLiteralsIn(path string) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		if bl, ok := n.(*ast.BasicLit); ok && bl.Kind == token.STRING {
			if v, err := strconv.Unquote(bl.Value); err == nil {
				out = append(out, v)
			} else {
				// Raw string literals with embedded backticks can fail to
				// unquote; fall back to the raw form rather than skipping,
				// since missing a literal here means missing a pin site.
				out = append(out, bl.Value)
			}
		}
		return true
	})
	return out, nil
}

func anyContains(lits []string, needle string) bool {
	for _, l := range lits {
		if strings.Contains(l, needle) {
			return true
		}
	}
	return false
}
