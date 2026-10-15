// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package exprident

import (
	"strings"
	"testing"
)

// mysqlLikeCfg / pgLikeCfg are deliberately small configs that mirror
// the two real engine shapes (backtick vs double-quote, no-ws vs
// ws-before-paren) without importing the engine packages (which would
// be an import cycle). The full real reserved/grammar sets are
// exercised end-to-end by the per-engine wrapper tests
// (engines/mysql/reserved_idents_test.go,
// engines/postgres/reserved_idents_test.go), which call the thin
// wrappers that delegate here. This table pins the *mechanism*:
// literal-aware walk, already-quoted passthrough, call-position
// exclusion, grammar exclusion, numeric non-identification, and the
// SkipWSBeforeParen divergence that is the only behavioural difference
// between the two engines' historical helpers.
var (
	mysqlLikeCfg = Config{
		QuoteByte:         '`',
		Reserved:          map[string]struct{}{"ORDER": {}, "KEY": {}, "LEFT": {}, "AND": {}, "NULL": {}},
		GrammarExclusions: map[string]struct{}{"AND": {}, "NULL": {}},
		SkipWSBeforeParen: false,
	}
	pgLikeCfg = Config{
		QuoteByte:         '"',
		Reserved:          map[string]struct{}{"ORDER": {}, "USER": {}, "COALESCE": {}, "AND": {}, "NULL": {}},
		GrammarExclusions: map[string]struct{}{"AND": {}, "NULL": {}},
		SkipWSBeforeParen: true,
	}
)

func TestRequoteIdentifiers(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		in   string
		want string
	}{
		// --- MySQL-shaped config (backtick, no ws before paren) ---
		{"mysql reserved bare ident", mysqlLikeCfg, "if((x is null),order,NULL)", "if((x is null),`order`,NULL)"},
		{"mysql non-reserved passthrough", mysqlLikeCfg, "qty * price - discount", "qty * price - discount"},
		{"mysql reserved in call position untouched", mysqlLikeCfg, "left(name, 3)", "left(name, 3)"},
		{"mysql grammar keyword not requoted", mysqlLikeCfg, "a and b", "a and b"},
		{"mysql NULL literal not requoted", mysqlLikeCfg, "coalesce(x, NULL)", "coalesce(x, NULL)"},
		{"mysql already-backticked untouched", mysqlLikeCfg, "`order` + 1", "`order` + 1"},
		{"mysql string literal verbatim", mysqlLikeCfg, "x = 'order'", "x = 'order'"},
		{"mysql doubled-quote string literal verbatim", mysqlLikeCfg, "x = 'a''order''b'", "x = 'a''order''b'"},
		{"mysql numeric literal not an identifier", mysqlLikeCfg, "order > 100 + 2", "`order` > 100 + 2"},
		{"mysql bare numeric untouched", mysqlLikeCfg, "42 + 7", "42 + 7"},
		{"mysql multiple reserved refs", mysqlLikeCfg, "order + key", "`order` + `key`"},
		{"mysql empty input", mysqlLikeCfg, "", ""},
		{
			"mysql no ws-skip: ws before paren still treated as ident (requoted)",
			mysqlLikeCfg, "order (1)", "`order` (1)",
		},
		{"mysql case-insensitive reserved match", mysqlLikeCfg, "OrDeR + 1", "`OrDeR` + 1"},

		// --- PG-shaped config (double-quote, ws before paren) ---
		{"pg reserved bare ident", pgLikeCfg, "lower(user)", `lower("user")`},
		{"pg reserved ident order", pgLikeCfg, "x = order", `x = "order"`},
		{"pg non-reserved passthrough", pgLikeCfg, "a + b - c", "a + b - c"},
		{"pg reserved in call position untouched", pgLikeCfg, "coalesce(x, 0)", "coalesce(x, 0)"},
		{
			"pg ws-skip: reserved-name with space before paren is call position",
			pgLikeCfg, "coalesce (x, 0)", "coalesce (x, 0)",
		},
		{"pg grammar keyword not requoted", pgLikeCfg, "a and b", "a and b"},
		{"pg already-double-quoted untouched", pgLikeCfg, `"order" + 1`, `"order" + 1`},
		{"pg doubled-double-quote ident verbatim", pgLikeCfg, `"a""order""b" + 1`, `"a""order""b" + 1`},
		{"pg string literal verbatim", pgLikeCfg, "x = 'user'", "x = 'user'"},
		{"pg numeric literal not an identifier", pgLikeCfg, "user >= 2", `"user" >= 2`},
		{"pg bare numeric untouched", pgLikeCfg, "3.14 * r", "3.14 * r"},
		{"pg multiple reserved refs", pgLikeCfg, "order + user", `"order" + "user"`},
		{"pg empty input", pgLikeCfg, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := RequoteIdentifiers(c.in, c.cfg)
			if got != c.want {
				t.Errorf("RequoteIdentifiers(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

func TestScanPrimitives(t *testing.T) {
	if got := ScanStringLiteral("'a''b' rest", 0); got != 6 {
		t.Errorf("ScanStringLiteral doubled-quote: got %d want 6", got)
	}
	if got := ScanStringLiteral("'unterminated", 0); got != len("'unterminated") {
		t.Errorf("ScanStringLiteral unterminated: got %d want %d", got, len("'unterminated"))
	}
	if end, ok := ScanParenGroup("(a, (b), c) tail", 0); !ok || end != 10 {
		t.Errorf("ScanParenGroup nested: end=%d ok=%v want 10,true", end, ok)
	}
	if _, ok := ScanParenGroup("no paren", 0); ok {
		t.Error("ScanParenGroup on non-paren should be false")
	}
	args := SplitTopLevelArgs("a, f(b, c), 'x,y', d")
	want := []string{"a", " f(b, c)", " 'x,y'", " d"}
	if len(args) != len(want) {
		t.Fatalf("SplitTopLevelArgs len = %d want %d (%q)", len(args), len(want), args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("SplitTopLevelArgs[%d] = %q want %q", i, args[i], want[i])
		}
	}
	if SplitTopLevelArgs("   ") != nil {
		t.Error("SplitTopLevelArgs whitespace-only should be nil")
	}
	if !IsIdentifierByte('a') || !IsIdentifierByte('Z') || !IsIdentifierByte('0') || !IsIdentifierByte('_') {
		t.Error("IsIdentifierByte rejected a valid identifier byte")
	}
	if IsIdentifierByte('-') || IsIdentifierByte(' ') {
		t.Error("IsIdentifierByte accepted a non-identifier byte")
	}
	if !IsIdentStartByte('a') || !IsIdentStartByte('_') {
		t.Error("IsIdentStartByte rejected a valid start byte")
	}
	if IsIdentStartByte('0') {
		t.Error("IsIdentStartByte accepted a digit (numeric literal must not start an ident)")
	}
}

// TestRequoteIdentifiers_ResolvesColumnReferences pins the GitHub #356
// pass: with a table's columns bound, a bare column reference is emitted
// as the column's real spelling, quoted when the target's unquoted fold
// would change it, and every position where SQL grammar wants a non-column
// name is left alone even when a column shares that name.
func TestRequoteIdentifiers_ResolvesColumnReferences(t *testing.T) {
	cfg := pgLikeCfg
	cfg.GrammarExclusions = map[string]struct{}{"AND": {}, "NULL": {}, "AS": {}, "INTERVAL": {}}
	cfg.UnquotedFold = strings.ToLower
	cfg = cfg.WithColumns([]string{"STATUS", "Price", "Qty", "plain", "Day", "Year", "Text", "Order", "utf8mb4_bin"})

	cases := []struct{ name, in, want string }{
		{"case-mismatched ref resolves to the real spelling", "lower(status)", `lower("STATUS")`},
		{"exact upper-case ref is quoted", "lower(STATUS)", `lower("STATUS")`},
		{"mixed-case refs in arithmetic", "Price * Qty", `"Price" * "Qty"`},
		{"lower-case column stays bare", "plain + 1", "plain + 1"},
		{"lower-case column spelled upper is emitted as its real name", "PLAIN + 1", "plain + 1"},
		{"reserved mixed-case column", "Order > 0", `"Order" > 0`},
		{"function name sharing a column name stays a call", "year(ts) + Year", `year(ts) + "Year"`},
		{"unrelated identifier untouched", "other + 1", "other + 1"},
		{"string literal untouched", "x = 'STATUS'", "x = 'STATUS'"},
		{"already-quoted ident untouched", `"status" + 1`, `"status" + 1`},
		{"interval unit after a number is grammar", "ts + interval 1 day", `ts + interval 1 day`},
		{"interval unit after a string literal is grammar", "ts + interval '1' DAY", `ts + interval '1' DAY`},
		{"extract field is grammar", "extract(year from ts)", "extract(year from ts)"},
		{"column named like a unit is still a column elsewhere", "Day + 1", `"Day" + 1`},
		{"extract source naming a unit-named column", "extract(year from Day)", `extract(year from "Day")`},
		{"timestampdiff unit is grammar", "timestampdiff(DAY, a, b)", "timestampdiff(DAY, a, b)"},
		{"cast target type is grammar", "cast(x as text)", "cast(x as text)"},
		{"pg cast target type is grammar", "x::text", "x::text"},
		{"collation name is grammar", "x collate utf8mb4_bin", "x collate utf8mb4_bin"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RequoteIdentifiers(c.in, cfg); got != c.want {
				t.Errorf("RequoteIdentifiers(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}

	// Without columns bound the pass is exactly the reserved-word pass.
	if got := RequoteIdentifiers("lower(status) + Price", pgLikeCfg); got != "lower(status) + Price" {
		t.Errorf("unbound config changed a non-reserved token: %q", got)
	}
	if got := pgLikeCfg.WithColumns(nil); got.Columns != nil {
		t.Errorf("WithColumns(nil) bound a column map")
	}
}
