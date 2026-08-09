// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package errclassgate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// SQLStateTextConfig describes one package's instantiation of
// [AssertNoErrorTextClassification].
type SQLStateTextConfig struct {
	// Dir is the package directory to walk (usually ".").
	Dir string
	// Allowed are deliberate exceptions, keyed "file.go:literal". The value is
	// the REASON the text match is correct there; an entry without a real
	// reason is a fix that should have been made.
	Allowed map[string]string
	// MinContainsCalls is the anti-vacuity floor. The walk must see at least
	// this many strings.Contains calls in the package, otherwise the matcher
	// has stopped seeing the code (a rename, a helper indirection, a moved
	// file) and would pass forever. Set it from the CURRENT count minus a
	// little slack — never from zero.
	MinContainsCalls int
}

// errorTextTokens are the literals that classify an error by its PROSE when a
// structural answer exists. Two shapes:
//
//   - A SQLSTATE, matched by [sqlStateLiteral] — if the code is in the message
//     the driver also put it in *pgconn.PgError.Code, so errors.As is available
//     and strictly better.
//   - "does not exist", which is PostgreSQL's house phrasing for a whole family
//     of unrelated conditions: a missing table is 42P01, a missing column
//     42703, a missing function 42883, a missing object 42704, a missing role
//     22023 from SET ROLE but 42704 from DROP ROLE or GRANT (the code follows
//     the STATEMENT, not the object — measured on a real PG 16), and a missing
//     DATABASE is 3D000 — which a pooled *sql.DB surfaces
//     from any query after a re-dial. Matching the phrase collapses all of them
//     into whichever one the reading code assumed.
var errorTextTokens = []string{"does not exist"}

// sqlStateLiteral matches a bare 5-character SQLSTATE (42P01, 3D000, 55006).
// The rendered form "SQLSTATE 42P01" is deliberately NOT matched: it can only
// come from a driver that already classified the error, so it is a legitimate
// last-resort fallback for a value flattened with %v instead of %w.
var sqlStateLiteral = regexp.MustCompile(`^\d[0-9A-Z]{4}$`)

// AssertNoErrorTextClassification is the Tier-1 gate for the audit-backlog C-1
// class: classifying an error by matching its MESSAGE when the engine already
// hands you a structural answer.
//
// # Why this exists
//
// The class is not "text matching is inelegant" — it is that the text is both
// unstable and ambiguous, and that the sites which classify errors are
// disproportionately the sites that SWALLOW them. Every instance found in the
// 2026-08-08 sweep resolved to the same failure direction: some condition the
// author never considered rendered the phrase they matched, the code read it as
// the benign case it was written for, and the operator was told an action
// succeeded that had not happened. A truncate skipped against a vanished
// database. A replication slot reported dropped while it kept pinning WAL. A
// populated target read as empty.
//
// The instability is not hypothetical either: PostgreSQL renders errors through
// `lc_messages`, so a server with a translation installed keeps every SQLSTATE
// and loses every English phrase this class depends on.
//
// # What it does NOT cover, deliberately
//
// Plenty of legitimate text matching remains in the tree and is out of scope:
// MySQL and vttablet surface conditions that genuinely have no distinct code
// ("no space left on device", the vttablet reparent shapes), so prose is the
// only signal available. The gate is narrow ON PURPOSE — it fires only where a
// structural answer demonstrably exists, so a hit is always actionable and the
// allowlist stays near-empty. A broad version would need dozens of exemptions
// and would be ignored, which is the failure mode of most lint-shaped gates.
func AssertNoErrorTextClassification(t *testing.T, cfg SQLStateTextConfig) {
	t.Helper()

	fset := token.NewFileSet()
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		t.Fatalf("errclassgate: read dir %s: %v", cfg.Dir, err)
	}

	containsCalls := 0
	var findings []string

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(cfg.Dir, name)
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("errclassgate: parse %s: %v", path, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			// A slice of candidate substrings iterated against an error message
			// is the same classification wearing a loop — check the elements
			// too, or the gate is one refactor away from blind.
			//
			// PROSE tokens only, deliberately. A bare SQLSTATE in a slice is
			// far more often the CORRECT shape — `for _, code := range codes {
			// if pgErr.Code == code }` is a structural comparison, and the
			// first draft of this gate flagged exactly that in
			// grow_evidence.go's serving-transition list. Telling the two apart
			// needs dataflow the walker does not have, and a gate that cries
			// wolf on correct code gets suppressed, which is worse than the
			// narrower rule. NAMED GAP: a SQLSTATE parked in a slice and then
			// fed to strings.Contains is not caught here; the direct-call check
			// below is what covers the common spelling.
			if comp, ok := n.(*ast.CompositeLit); ok {
				for _, elt := range comp.Elts {
					lit, ok := elt.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					val, err := strconv.Unquote(lit.Value)
					if err != nil || !isErrorProseToken(val) {
						continue
					}
					key := name + ":" + val
					if _, allowed := cfg.Allowed[key]; allowed {
						continue
					}
					findings = append(findings, key+" at "+fset.Position(lit.Pos()).String())
				}
				return true
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Contains" {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "strings" {
				return true
			}
			containsCalls++
			if len(call.Args) != 2 {
				return true
			}
			lit, ok := call.Args[1].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			val, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			if !classifiesByErrorText(val) {
				return true
			}
			key := name + ":" + val
			if _, allowed := cfg.Allowed[key]; allowed {
				return true
			}
			findings = append(findings, key+" at "+fset.Position(call.Pos()).String())
			return true
		})
	}

	if containsCalls < cfg.MinContainsCalls {
		t.Fatalf(
			"errclassgate: only %d strings.Contains calls found in %s (floor %d); the walker has probably "+
				"stopped seeing the code it is meant to check, and a gate that sees nothing passes forever",
			containsCalls, cfg.Dir, cfg.MinContainsCalls,
		)
	}

	for _, f := range findings {
		t.Errorf(
			"errclassgate: %s classifies an error by its MESSAGE where a structural answer exists.\n"+
				"Compare the SQLSTATE/errno via errors.As on the driver error type instead. If the text really is "+
				"the only signal, add it to Allowed with the reason (audit backlog C-1).",
			f,
		)
	}
}

// classifiesByErrorText reports whether a string literal, used as the needle of
// a strings.Contains against an error message, is error-message classification.
func classifiesByErrorText(val string) bool {
	return sqlStateLiteral.MatchString(val) || isErrorProseToken(val)
}

// isErrorProseToken reports whether val is one of the ambiguous PROSE tokens.
// Unlike a SQLSTATE, a prose token has no correct structural use, so it is a
// finding wherever it appears — including inside a slice of candidates.
func isErrorProseToken(val string) bool {
	for _, tok := range errorTextTokens {
		if val == tok {
			return true
		}
	}
	return false
}
