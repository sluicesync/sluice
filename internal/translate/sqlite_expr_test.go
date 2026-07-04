// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import "testing"

// TestSQLiteExprToPG_Portable pins every allowlisted construct to its exact
// Postgres form. Output is fully parenthesised (SQLite precedence preserved).
func TestSQLiteExprToPG_Portable(t *testing.T) {
	cases := []struct{ in, want string }{
		// literals / idents
		{"a", "a"},
		{"42", "42"},
		{"3.14", "3.14"},
		{"'hi'", "'hi'"},
		{"NULL", "NULL"},
		// operators (PG keeps / — integer division matches SQLite)
		{"a + b", "(a + b)"},
		{"a - b * c", "(a - (b * c))"},
		{"a / b", "(a / b)"},
		{"a = 1", "(a = 1)"},
		{"a == 1", "(a = 1)"},
		{"a != 1", "(a <> 1)"},
		{"a <> 1", "(a <> 1)"},
		{"a >= 1 AND b < 2", "((a >= 1) AND (b < 2))"},
		{"a = 1 OR b = 2", "((a = 1) OR (b = 2))"},
		{"NOT a", "(NOT a)"},
		{"a IS NULL", "(a IS NULL)"},
		{"a IS NOT NULL", "(a IS NOT NULL)"},
		{"-a", "(-a)"},
		// concat (PG keeps ||)
		{"a || '-' || b", "(a || '-' || b)"},
		// functions
		{"abs(x)", "ABS(x)"},
		{"coalesce(a, b, c)", "COALESCE(a, b, c)"},
		{"ifnull(a, 0)", "COALESCE(a, 0)"},
		{"nullif(a, b)", "NULLIF(a, b)"},
		{"length(x)", "LENGTH(x)"},
		{"trim(x)", "TRIM(x)"},
		{"ltrim(x)", "LTRIM(x)"},
		{"rtrim(x)", "RTRIM(x)"},
		// substr: only literal start >= 1 (and literal len >= 0)
		{"substr(x, 1, 3)", "SUBSTRING(x, 1, 3)"},
		{"substr(x, 2)", "SUBSTRING(x, 2)"},
		// cast: text + real + numeric(PG)
		{"cast(x AS text)", "CAST(x AS TEXT)"},
		{"cast(x AS real)", "CAST(x AS DOUBLE PRECISION)"},
		{"cast(x AS numeric)", "CAST(x AS NUMERIC)"},
		// current-instant keywords
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"current_date", "CURRENT_DATE"},
		{"CURRENT_TIME", "CURRENT_TIME"},
		// a realistic combined gencol body
		{"coalesce(a, 'x') || '-' || cast(n AS text)", "(COALESCE(a, 'x') || '-' || CAST(n AS TEXT))"},
	}
	for _, c := range cases {
		got, ok := SQLiteExprToPG(c.in)
		if !ok {
			t.Errorf("SQLiteExprToPG(%q) ok=false; want %q", c.in, c.want)
			continue
		}
		if got != c.want {
			t.Errorf("SQLiteExprToPG(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestSQLiteExprToMySQL_Portable pins the MySQL forms: || → CONCAT, length →
// CHAR_LENGTH, min/max → LEAST/GREATEST, cast text/real → CHAR/DOUBLE.
func TestSQLiteExprToMySQL_Portable(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a + b", "(a + b)"},
		{"a - b * c", "(a - (b * c))"},
		{"a = 1", "(a = 1)"},
		{"a != 1", "(a <> 1)"},
		{"a >= 1 AND b < 2", "((a >= 1) AND (b < 2))"},
		{"NOT a", "(NOT a)"},
		{"a IS NULL", "(a IS NULL)"},
		{"a IS NOT NULL", "(a IS NOT NULL)"},
		// concat → CONCAT
		{"a || '-' || b", "CONCAT(a, '-', b)"},
		// functions
		{"abs(x)", "ABS(x)"},
		{"ifnull(a, 0)", "COALESCE(a, 0)"},
		{"nullif(a, b)", "NULLIF(a, b)"},
		{"length(x)", "CHAR_LENGTH(x)"}, // MySQL LENGTH is bytes; CHAR_LENGTH is chars
		{"trim(x)", "TRIM(x)"},
		{"substr(x, 1, 3)", "SUBSTRING(x, 1, 3)"},
		// min/max → LEAST/GREATEST (MySQL only — LEAST/GREATEST propagate NULL)
		{"min(a, b)", "LEAST(a, b)"},
		{"max(a, b, c)", "GREATEST(a, b, c)"},
		// cast: text + real only
		{"cast(x AS text)", "CAST(x AS CHAR)"},
		{"cast(x AS real)", "CAST(x AS DOUBLE)"},
		// current-instant keywords
		{"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"coalesce(a, 'x') || cast(n AS text)", "CONCAT(COALESCE(a, 'x'), CAST(n AS CHAR))"},
	}
	for _, c := range cases {
		got, ok := SQLiteExprToMySQL(c.in)
		if !ok {
			t.Errorf("SQLiteExprToMySQL(%q) ok=false; want %q", c.in, c.want)
			continue
		}
		if got != c.want {
			t.Errorf("SQLiteExprToMySQL(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestSQLiteExpr_TargetSpecific pins the constructs whose portability DIFFERS
// by target — the value-fidelity crux (each was a silent-corruption vector a
// per-representative test would have missed):
//
//   - `/`: PG matches SQLite integer division; MySQL `/` is always decimal.
//   - min/max: MySQL LEAST/GREATEST propagate NULL (match SQLite); PG skips NULLs.
//   - cast AS numeric: PG NUMERIC faithful; MySQL bare DECIMAL rounds to 0 dp.
func TestSQLiteExpr_TargetSpecific(t *testing.T) {
	pgOnly := []struct{ in, want string }{
		{"a / b", "(a / b)"},
		{"cast(x AS numeric)", "CAST(x AS NUMERIC)"},
		// 0x hex literal: an INTEGER on SQLite and on PG (16+; older
		// servers reject the spelling LOUDLY at DDL time), but on MySQL a
		// hexadecimal literal is a context-dependent BINARY STRING in
		// string context (bytes \x1a where SQLite stores '26') — refuse
		// there (value-fidelity review of the DEFAULT classification fix).
		{"0x1A", "0x1A"},
		{"a + 0x1A", "(a + 0x1A)"},
	}
	for _, c := range pgOnly {
		if got, ok := SQLiteExprToPG(c.in); !ok || got != c.want {
			t.Errorf("SQLiteExprToPG(%q) = (%q,%v); want (%q,true)", c.in, got, ok, c.want)
		}
		if _, ok := SQLiteExprToMySQL(c.in); ok {
			t.Errorf("SQLiteExprToMySQL(%q) ok=true; want false (not portable to MySQL)", c.in)
		}
	}

	mysqlOnly := []struct{ in, want string }{
		{"min(a, b)", "LEAST(a, b)"},
		{"max(a, b)", "GREATEST(a, b)"},
	}
	for _, c := range mysqlOnly {
		if got, ok := SQLiteExprToMySQL(c.in); !ok || got != c.want {
			t.Errorf("SQLiteExprToMySQL(%q) = (%q,%v); want (%q,true)", c.in, got, ok, c.want)
		}
		if _, ok := SQLiteExprToPG(c.in); ok {
			t.Errorf("SQLiteExprToPG(%q) ok=true; want false (LEAST/GREATEST skip NULLs, SQLite min/max propagate)", c.in)
		}
	}
}

// TestSQLiteExpr_BackslashStringLiterals pins the SEC-1 boundary: a '…'
// string literal containing a backslash REFUSES on MySQL (default sql_mode
// treats \ as an escape introducer; a literal ending in \ swallows its
// closing quote and shifts expression text into string position) and stays
// PORTABLE to PG byte-identically (standard_conforming_strings=on — pinned
// on every sluice PG session — makes \ an ordinary character, matching
// SQLite). The shape matrix: plain interior backslash, the trailing-\
// quote-swallow shape, a doubled backslash, and each expression position a
// literal can occupy (bare, concat, function arg, comparison) — plus
// backslash-free controls that must keep translating.
func TestSQLiteExpr_BackslashStringLiterals(t *testing.T) {
	backslashBodies := []string{
		`'C:\temp'`,               // plain interior backslash, bare literal
		`a || 'C:\temp'`,          // concat position
		`'trailing\'`,             // literal ending in \ — the quote-swallow shape
		`'\\'`,                    // doubled backslash (two literal chars in SQLite)
		`coalesce(a, 'x\y')`,      // function-argument position
		`substr(a, 1, 2) = 'a\b'`, // comparison position
	}
	for _, in := range backslashBodies {
		if got, ok := SQLiteExprToMySQL(in); ok {
			t.Errorf("SQLiteExprToMySQL(%q) = (%q, true); want ok=false (backslash literal has no provably-faithful MySQL spelling)", in, got)
		}
		if _, ok := SQLiteExprToPG(in); !ok {
			t.Errorf("SQLiteExprToPG(%q) ok=false; want true (PG treats \\ literally under standard_conforming_strings)", in)
		}
		if !SQLiteExprHasBackslashStringLiteral(in) {
			t.Errorf("SQLiteExprHasBackslashStringLiteral(%q) = false; want true", in)
		}
	}

	// PG carries the literal byte-identically (fully-parenthesised emit).
	if got, ok := SQLiteExprToPG(`a || 'C:\temp'`); !ok || got != `(a || 'C:\temp')` {
		t.Errorf(`SQLiteExprToPG(a || 'C:\temp') = (%q, %v); want ((a || 'C:\temp'), true)`, got, ok)
	}

	// Backslash-free literals — including the doubled-single-quote escape —
	// must keep translating on BOTH targets, and the classifier must not fire.
	controls := []string{`'hi'`, `'it''s'`, `a || '-' || b`, `coalesce(a, 'x')`}
	for _, in := range controls {
		if _, ok := SQLiteExprToMySQL(in); !ok {
			t.Errorf("SQLiteExprToMySQL(%q) ok=false; want true (backslash-free literal must stay portable)", in)
		}
		if _, ok := SQLiteExprToPG(in); !ok {
			t.Errorf("SQLiteExprToPG(%q) ok=false; want true", in)
		}
		if SQLiteExprHasBackslashStringLiteral(in) {
			t.Errorf("SQLiteExprHasBackslashStringLiteral(%q) = true; want false", in)
		}
	}

	// The classifier keys on STRING-LITERAL tokens only: a backslash outside
	// a literal (already refused by the grammar on both targets) and one
	// inside a comment (dropped by the tokenizer) must not classify.
	for _, in := range []string{`a \ b`, "a -- c:\\temp\n"} {
		if SQLiteExprHasBackslashStringLiteral(in) {
			t.Errorf("SQLiteExprHasBackslashStringLiteral(%q) = true; want false (no string literal carries the backslash)", in)
		}
	}
}

// TestSQLiteExpr_DoubleQuotedTokens pins the SEC-1 gap-1 boundary: any "…"
// double-quoted token — an identifier under SQLite's rules, or a string via
// the double-quoted-string misfeature — REFUSES on MySQL regardless of
// content (under default sql_mode, no ANSI_QUOTES, MySQL lexes it as a
// string literal WITH escape semantics: an intended identifier silently
// becomes a string, and a trailing backslash swallows the closing quote).
// PG keeps "…" verbatim: a well-formed PG identifier matching SQLite's
// primary meaning; an unknown column fails loudly (42703). Backtick idents
// stay portable to MySQL (its own identifier quoting).
func TestSQLiteExpr_DoubleQuotedTokens(t *testing.T) {
	dqBodies := []string{
		`"name"`,                 // bare-looking ident, no backslash — still refused
		`a || "C:\temp"`,         // misfeature string with interior backslash
		`a <> "x\"`,              // trailing backslash — the quote-swallow shape
		`coalesce(a, "z\q")`,     // function-argument position
		`"my col" IS NOT NULL`,   // non-bare ident (space) — the surviving-residue shape
		`"status" <> 'inactive'`, // predicate that would silently vacate on MySQL
	}
	for _, in := range dqBodies {
		if got, ok := SQLiteExprToMySQL(in); ok {
			t.Errorf("SQLiteExprToMySQL(%q) = (%q, true); want ok=false (double-quoted token is a string literal on MySQL)", in, got)
		}
		if _, ok := SQLiteExprToPG(in); !ok {
			t.Errorf("SQLiteExprToPG(%q) ok=false; want true (PG reads \"…\" as an identifier, matching SQLite)", in)
		}
		if !SQLiteExprHasDoubleQuotedToken(in) {
			t.Errorf("SQLiteExprHasDoubleQuotedToken(%q) = false; want true", in)
		}
	}

	// PG carries the token byte-identically.
	if got, ok := SQLiteExprToPG(`"my col" IS NOT NULL`); !ok || got != `("my col" IS NOT NULL)` {
		t.Errorf(`SQLiteExprToPG("my col" IS NOT NULL) = (%q, %v); want (("my col" IS NOT NULL), true)`, got, ok)
	}

	// Backtick identifiers are MySQL's own quoting — portable there (and the
	// classifier must not fire); PG rejects them loudly at DDL time, which is
	// outside the translator's jurisdiction.
	if got, ok := SQLiteExprToMySQL("`my col` IS NULL"); !ok || got != "(`my col` IS NULL)" {
		t.Errorf("SQLiteExprToMySQL(backtick ident) = (%q, %v); want ((`my col` IS NULL), true)", got, ok)
	}
	for _, in := range []string{"`my col` IS NULL", `'hi'`, "a + b"} {
		if SQLiteExprHasDoubleQuotedToken(in) {
			t.Errorf("SQLiteExprHasDoubleQuotedToken(%q) = true; want false", in)
		}
	}
}

// TestSQLiteExpr_NonPortableBoth pins the loud-fail boundary: every construct
// here returns ok=false on BOTH targets. This is the value-fidelity contract —
// a construct we cannot prove equivalent (for EVERY operand shape) must never
// be guessed.
func TestSQLiteExpr_NonPortableBoth(t *testing.T) {
	nonPortable := []string{
		// `%` diverges on non-integer operands on BOTH targets
		"a % b",
		"(a + b) % c",
		// ASCII-vs-Unicode case folding
		"upper(x)",
		"lower(x)",
		// half-away-from-zero vs half-to-even
		"round(x)",
		"round(x, 2)",
		// truncate-vs-round / murky bytes
		"cast(x AS integer)",
		"cast(x AS blob)",
		// substr negative / non-literal / < 1 start
		"substr(x, -3, 2)",
		"substr(x, 0, 2)",
		"substr(x, col)",
		"substr(x, 1, -1)",
		// temporal (format-string / epoch-base translation is out of scope)
		"strftime('%Y', d)",
		"julianday(d)",
		"unixepoch(d)",
		"date(d, '+1 day')",
		"time(d)",
		"datetime('now', '-1 hour')",
		// explicitly excluded functions
		"glob('a*', x)",
		"typeof(x)",
		"hex(x)",
		"unicode(x)",
		"char(65)",
		"randomblob(16)",
		"likelihood(x, 0.5)",
		"printf('%d', x)",
		"format('%d', x)",
		// unknown function / unknown token
		"foo(x)",
		"x -> 'k'",
		"x GLOB 'a*'",
		"x LIKE 'a%'",
		"x IN (1, 2)",
		"x BETWEEN 1 AND 2",
		// arity / shape violations
		"trim(x, 'ab')",      // 2-arg trim is not portable
		"min(x)",             // 1-arg min is the aggregate
		"max(x)",             // 1-arg max is the aggregate
		"cast(x AS varchar)", // non-affinity cast type
		"count(*)",
		// structural garbage
		"a b",
		"a +",
		"(a",
		"",
	}
	for _, in := range nonPortable {
		if got, ok := SQLiteExprToPG(in); ok {
			t.Errorf("SQLiteExprToPG(%q) = (%q, true); want ok=false", in, got)
		}
		if got, ok := SQLiteExprToMySQL(in); ok {
			t.Errorf("SQLiteExprToMySQL(%q) = (%q, true); want ok=false", in, got)
		}
	}
}

// TestSQLiteExprHasHexLiteral pins the hex-literal detector the PG writer's
// DEFAULT arm uses to keep 0x defaults on its warn-drop path (the spelling
// is PG 16+ only).
func TestSQLiteExprHasHexLiteral(t *testing.T) {
	for in, want := range map[string]bool{
		`0x1A`:       true,
		`0X00FF`:     true,
		`a + 0x1A`:   true,
		`42`:         false,
		`'0x1A'`:     false, // inside a string literal — not a hex token
		`x'00ff'`:    false, // blob literal, not a 0x integer
		`col0x`:      false, // identifier containing the letters, not a number
		`'a' || 'b'`: false,
		``:           false,
	} {
		if got := SQLiteExprHasHexLiteral(in); got != want {
			t.Errorf("SQLiteExprHasHexLiteral(%q) = %v; want %v", in, got, want)
		}
	}
}

// TestSQLiteExprMySQLDefaultVerbatimSafe pins the verbatim-residue
// allowlist behind the MySQL writer's DEFAULT-position drop boundary: only
// bodies proven to read identically (or fail loudly) on MySQL may be
// carried verbatim after a translator refusal — everything else MySQL may
// PARSE with different semantics (`||` as logical OR, `/` decimal
// division, `%` float modulo) and must be warn-dropped by the caller.
func TestSQLiteExprMySQLDefaultVerbatimSafe(t *testing.T) {
	for in, want := range map[string]bool{
		// Safe residues.
		`"draft"`:     true, // bare misfeature token — same string value on MySQL
		`x'00ff'`:     true, // blob literal — MySQL hex-string, same bytes
		`X'00FF'`:     true, // upper-case blob spelling
		`SOMEKEYWORD`: true, // lone word — MySQL rejects loudly in DEFAULT position
		// Value-divergence hazards → not safe.
		`upper('a') || 'b'`: false, // parses as UPPER('a') OR 'b' → 0
		`7/2`:               false, // decimal 3.5 vs SQLite's 3
		`7.5 % 2`:           false, // 1.5 vs SQLite's 1
		`myfunc(a) || 'x'`:  false,
		`'a' || 'b'`:        false, // (portable — translator handles it, never reaches the residue check)
		`0x1A`:              false, // context-dependent binary string on MySQL
		`"a\b"`:             false, // backslash-bearing dq token (also refused upstream)
		`'a\b'`:             false, // backslash literal (also refused upstream)
		`"dq" || 'x'`:       false, // dq token alongside other tokens
		``:                  false,
	} {
		if got := SQLiteExprMySQLDefaultVerbatimSafe(in); got != want {
			t.Errorf("SQLiteExprMySQLDefaultVerbatimSafe(%q) = %v; want %v", in, got, want)
		}
	}
}
