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
