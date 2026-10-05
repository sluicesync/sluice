// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"
)

// TestInjectSessionSQLMode_AlwaysCarriesNoAutoValueOnZero pins the
// injection half of the NO_AUTO_VALUE_ON_ZERO fix over every tier of the
// sql_mode override policy. The CLI tier matters most: kong passes the
// flag's default LITERAL, so the engine's sqlMode is never nil through the
// CLI — a fix that only edited defaultStrictSQLMode would have reached unit
// tests and no operator. The tiers where sluice injects nothing (a DSN
// sql_mode, the "" escape hatch) are covered on the live session by
// ensureNoAutoValueOnZero, pinned in TestSessionInvariants_SQLModeTierMatrix.
func TestInjectSessionSQLMode_AlwaysCarriesNoAutoValueOnZero(t *testing.T) {
	strp := func(s string) *string { return &s }
	cases := []struct {
		name string
		mode *string
	}{
		{"nil override (bare Engine, tests, broker paths)", nil},
		{"kong default literal (every CLI invocation)", strp("STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO")},
		{"explicit list without the flag", strp("ANSI_QUOTES")},
		{"explicit list already carrying it, lower-case", strp("strict_trans_tables,no_auto_value_on_zero")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseDSN("user:pw@tcp(host:3306)/mydb")
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			injectSessionSQLMode(cfg, tc.mode)
			got := cfg.Params["sql_mode"]
			if !sqlModeHas(got, noAutoValueOnZero) {
				t.Fatalf("injected sql_mode %q lacks %s", got, noAutoValueOnZero)
			}
			if n := countFlag(got, noAutoValueOnZero); n != 1 {
				t.Errorf("injected sql_mode %q carries %s %d times, want once", got, noAutoValueOnZero, n)
			}
			// The operator's strictness choice is kept, not replaced.
			want := resolveSessionSQLMode(tc.mode)
			if got[:len(want)+1] != "'"+want {
				t.Errorf("injected sql_mode %q does not start with the resolved mode %q", got, want)
			}
		})
	}
}

func countFlag(mode, flag string) int {
	n := 0
	for _, part := range strings.Split(strings.Trim(mode, "'"), ",") {
		if strings.EqualFold(strings.TrimSpace(part), flag) {
			n++
		}
	}
	return n
}

// TestSQLModeHas pins the whole-element match: a flag must not be found
// inside a longer one, and quoting / case / spacing as a DSN or the server
// spells it must not hide it.
func TestSQLModeHas(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"NO_AUTO_VALUE_ON_ZERO", true},
		{"'STRICT_TRANS_TABLES,NO_AUTO_VALUE_ON_ZERO'", true},
		{"strict_trans_tables, no_auto_value_on_zero", true},
		{"", false},
		{"STRICT_TRANS_TABLES", false},
		{"XNO_AUTO_VALUE_ON_ZERO", false},
		{"NO_AUTO_VALUE_ON_ZEROX", false},
	}
	for _, tc := range cases {
		if got := sqlModeHas(tc.mode, noAutoValueOnZero); got != tc.want {
			t.Errorf("sqlModeHas(%q) = %v, want %v", tc.mode, got, tc.want)
		}
	}
}
