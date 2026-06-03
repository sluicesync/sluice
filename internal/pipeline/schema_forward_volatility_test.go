// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyDefaultVolatility_Class pins the Bug 74 "pin the class,
// not the representative" discipline: cover every volatility family
// (time / sequence / random / session-state) on both PG and MySQL
// syntax, plus the safe cases (literal / NULL / numeric / quoted
// string) that must continue to pass.
//
// Bug 90 (v0.79.1) — the v0.79.0 intercept refused only
// [ir.DefaultExpression] but the CDC reader's projection drops the
// DEFAULT field, leaving every computed default to forward silently.
// This pin matrix exercises the text-based scan that the fix wires
// through the source SchemaReader probe.
func TestClassifyDefaultVolatility_Class(t *testing.T) {
	cases := []struct {
		name       string
		expr       string
		wantSafe   bool
		wantReason string // substring expected when !wantSafe
	}{
		// ----- Safe: literal / no-default / quoted / numeric -----
		{"empty", "", true, ""},
		{"null", "NULL", true, ""},
		{"true", "TRUE", true, ""},
		{"false", "false", true, ""},
		{"quoted-string", "'active'", true, ""},
		{"quoted-with-special", "'pending review'", true, ""},
		{"integer-literal", "0", true, ""},
		{"negative-integer", "-1", true, ""},
		{"decimal-literal", "3.14", true, ""},
		{"scientific", "1.5e3", true, ""},
		{"quoted-with-cast-pg", "'active'::text", true, ""},
		{"quoted-with-cast-numeric", "0::numeric", true, ""},

		// ----- Time-volatile (PG syntax) -----
		{"now-pg", "now()", false, "now"},
		{"current-timestamp-pg-bare", "CURRENT_TIMESTAMP", false, "current_timestamp"},
		{"current-timestamp-pg-parens", "CURRENT_TIMESTAMP(6)", false, "current_timestamp"},
		{"current-date-pg", "CURRENT_DATE", false, "current_date"},
		{"current-time-pg", "CURRENT_TIME", false, "current_time"},
		{"localtime-pg", "LOCALTIME", false, "localtime"},
		{"localtimestamp-pg", "LOCALTIMESTAMP", false, "localtimestamp"},
		{"transaction-timestamp-pg", "transaction_timestamp()", false, "transaction_timestamp"},
		{"statement-timestamp-pg", "statement_timestamp()", false, "statement_timestamp"},
		{"clock-timestamp-pg", "clock_timestamp()", false, "clock_timestamp"},

		// ----- Time-volatile (MySQL syntax) -----
		{"now-mysql", "NOW()", false, "now"},
		{"now-mysql-precision", "NOW(6)", false, "now"},
		{"utc-timestamp-mysql", "UTC_TIMESTAMP()", false, "utc_timestamp"},
		{"utc-timestamp-mysql-bare", "UTC_TIMESTAMP", false, "utc_timestamp"},
		{"unix-timestamp-mysql", "UNIX_TIMESTAMP()", false, "unix_timestamp"},
		{"sysdate-mysql", "SYSDATE()", false, "sysdate"},
		{"curdate-mysql", "CURDATE()", false, "curdate"},

		// ----- Sequence-stateful (PG + MySQL) -----
		{"nextval-pg", "nextval('my_seq')", false, "nextval"},
		{"nextval-pg-qualified", "nextval('public.my_seq'::regclass)", false, "nextval"},
		// Bug 91 (v0.79.1) class pins: PG's information_schema returns
		// nextval/setval defaults with the ::regclass cast inside the
		// argument. The raw-text probe surfaces them verbatim; pin the
		// surface-syntax variants here so a regression in the classifier
		// (or in the cast-stripping helper) trips immediately.
		{"nextval-pg-qualified-uppercase", "NEXTVAL('public.bar_seq'::regclass)", false, "nextval"},
		{"nextval-pg-bare-regclass", "nextval('my_seq'::regclass)", false, "nextval"},
		{"setval-pg-regclass", "setval('s'::regclass, 1, true)", false, "setval"},
		{"setval-pg-qualified-uppercase", "SETVAL('public.bar_seq'::regclass, 100)", false, "setval"},
		{"currval-pg", "currval('my_seq')", false, "currval"},
		{"currval-pg-regclass", "currval('my_seq'::regclass)", false, "currval"},
		{"setval-pg", "setval('my_seq', 100)", false, "setval"},
		{"last-insert-id-mysql", "LAST_INSERT_ID()", false, "last_insert_id"},

		// ----- Random / non-deterministic -----
		{"random-pg", "random()", false, "random"},
		{"gen-random-uuid-pg", "gen_random_uuid()", false, "gen_random_uuid"},
		{"uuid-generate-v4-pg", "uuid_generate_v4()", false, "uuid_generate_v4"},
		{"rand-mysql", "RAND()", false, "rand"},
		{"uuid-mysql", "UUID()", false, "uuid"},
		{"uuid-short-mysql", "UUID_SHORT()", false, "uuid_short"},

		// ----- Session-state (PG) -----
		{"current-user-pg", "CURRENT_USER", false, "current_user"},
		{"session-user-pg", "SESSION_USER", false, "session_user"},
		{"current-schema-pg", "current_schema()", false, "current_schema"},
		{"current-database-pg", "current_database()", false, "current_database"},
		{"inet-client-addr-pg", "inet_client_addr()", false, "inet_client_addr"},

		// ----- Refuse-on-uncertainty: unknown function -----
		{"unknown-fn", "my_custom_default()", false, "unknown function"},
		{"unknown-fn-with-arg", "tenant_default('app')", false, "unknown function"},
		{"unknown-fn-pg-qualified", "myschema.my_default()", false, "unknown function"},

		// ----- Allowlisted deterministic functions -----
		{"abs-fn", "ABS(-5)", true, ""},
		{"coalesce-fn", "COALESCE(NULL, 0)", true, ""},
		{"cast-fn", "CAST(0 AS NUMERIC)", true, ""},

		// ----- Volatile nested under deterministic call (refuse) -----
		{"coalesce-of-now", "COALESCE(NULL, NOW())", false, "now"},
		{"upper-of-current-user", "UPPER(CURRENT_USER)", false, "current_user"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			safe, reason := classifyDefaultVolatility(tc.expr)
			if safe != tc.wantSafe {
				t.Errorf("classifyDefaultVolatility(%q) safe = %v; want %v (reason=%q)",
					tc.expr, safe, tc.wantSafe, reason)
			}
			if !tc.wantSafe && !strings.Contains(strings.ToLower(reason), tc.wantReason) {
				t.Errorf("reason %q does not mention %q (expr=%q)",
					reason, tc.wantReason, tc.expr)
			}
			if tc.wantSafe && reason != "" {
				t.Errorf("safe expr returned non-empty reason %q (expr=%q)",
					reason, tc.expr)
			}
		})
	}
}

// TestClassifyDefaultValueVolatility_IRTypes pins the wrapper that
// dispatches across [ir.DefaultValue] variants. DefaultNone /
// DefaultLiteral / nil all pass; DefaultExpression delegates to the
// text scanner; unrecognized types refuse.
func TestClassifyDefaultValueVolatility_IRTypes(t *testing.T) {
	cases := []struct {
		name     string
		val      ir.DefaultValue
		wantSafe bool
	}{
		{"nil", nil, true},
		{"none", ir.DefaultNone{}, true},
		{"literal-numeric", ir.DefaultLiteral{Value: "0"}, true},
		{"literal-string", ir.DefaultLiteral{Value: "active"}, true},
		{"expr-now", ir.DefaultExpression{Expr: "NOW()"}, false},
		{"expr-nextval", ir.DefaultExpression{Expr: "nextval('seq')"}, false},
		{"expr-literal", ir.DefaultExpression{Expr: "'pending'"}, true},
		{"expr-numeric-cast", ir.DefaultExpression{Expr: "0::integer"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			safe, reason := classifyDefaultValueVolatility(tc.val)
			if safe != tc.wantSafe {
				t.Errorf("classifyDefaultValueVolatility(%v) safe = %v; want %v (reason=%q)",
					tc.val, safe, tc.wantSafe, reason)
			}
		})
	}
}
