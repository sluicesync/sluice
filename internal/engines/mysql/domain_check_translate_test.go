// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTranslateDomainCheckToMySQL covers the documented translatable
// shapes (regex, range) and pins fallback behavior for everything
// else. Anything that doesn't translate exactly MUST return ok=false
// — broadening to "best-effort" translation would silently re-
// introduce the Bug 113 silent-loss class (a wrong CHECK on dst is
// more dangerous than no CHECK; operators see a CHECK in
// SHOW CREATE TABLE and assume parity).
func TestTranslateDomainCheckToMySQL(t *testing.T) {
	cases := []struct {
		name      string
		col       string
		body      string
		want      string // empty when wantOK == false
		wantOK    bool
		wantParts []string // optional substrings the clause must contain
	}{
		// --- Regex DOMAINs (the email_address exemplar) ---
		{
			// v0.97.1 fix: backslashes in the pattern double in the SQL
			// literal so MySQL's string-literal parser produces `\.` for
			// the regex engine, not `.` (which matches any char). Without
			// this, the email regex's `\.` collapses to `.` and the
			// constraint accepts `aliceXexample.com` (functionally
			// harmless on this regex because `@` carries the rejection,
			// but a strict-fidelity gap nonetheless).
			name:      "regex with ::text cast (canonical PG output, v0.97.1 backslash-doubling)",
			col:       "email",
			body:      `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text)`,
			wantOK:    true,
			wantParts: []string{"REGEXP_LIKE(`email`,", `'^[^@]+@[^@]+\\.[^@]+$'`},
		},
		{
			name:      "regex without cast (v0.97.1 backslash-doubling)",
			col:       "email",
			body:      `(VALUE ~ '^[^@]+@[^@]+\.[^@]+$')`,
			wantOK:    true,
			wantParts: []string{"REGEXP_LIKE(`email`,", `'^[^@]+@[^@]+\\.[^@]+$'`},
		},
		{
			// No backslash in the pattern → no doubling needed; the
			// translator emits the pattern verbatim.
			name:      "regex without outer parens (no backslashes — verbatim emit)",
			col:       "code",
			body:      `VALUE ~ '^[A-Z]{3}$'`,
			wantOK:    true,
			wantParts: []string{"REGEXP_LIKE(`code`,", `'^[A-Z]{3}$'`},
		},
		{
			// v0.97.1 pin for the strict-fidelity assertion: a PG-source
			// backslash MUST appear as TWO backslashes in the emitted SQL
			// literal so MySQL's string parser produces ONE backslash for
			// the regex engine. Tests both common shorthands (`\d`, `\s`)
			// and the literal-dot case at once.
			name:      "regex with multiple backslash escapes (\\d, \\s, \\.) all doubled",
			col:       "x",
			body:      `VALUE ~ '^\d+\s\.[a-z]+$'`,
			wantOK:    true,
			wantParts: []string{`'^\\d+\\s\\.[a-z]+$'`},
		},

		// --- Range DOMAINs (the percentage exemplar) ---
		{
			name:      "range with ::numeric casts (canonical PG output)",
			col:       "pct",
			body:      `((VALUE >= (0)::numeric) AND (VALUE <= (100)::numeric))`,
			wantOK:    true,
			wantParts: []string{"`pct` >= 0", "`pct` <= 100", "AND"},
		},
		{
			name:      "range without casts",
			col:       "pct",
			body:      `((VALUE >= 0) AND (VALUE <= 100))`,
			wantOK:    true,
			wantParts: []string{"`pct` >= 0", "`pct` <= 100"},
		},
		{
			name:      "range no inner parens",
			col:       "score",
			body:      `VALUE >= 0 AND VALUE <= 100`,
			wantOK:    true,
			wantParts: []string{"`score` >= 0", "`score` <= 100"},
		},
		{
			name:      "range with negative lower bound",
			col:       "temp",
			body:      `((VALUE >= (-273)::numeric) AND (VALUE <= (1000)::numeric))`,
			wantOK:    true,
			wantParts: []string{"`temp` >= -273", "`temp` <= 1000"},
		},
		{
			name:      "range with decimal bounds",
			col:       "ratio",
			body:      `((VALUE >= (0.5)::numeric) AND (VALUE <= (1.5)::numeric))`,
			wantOK:    true,
			wantParts: []string{"`ratio` >= 0.5", "`ratio` <= 1.5"},
		},

		// --- Fallback shapes (translator MUST decline) ---
		{
			name:   "function call shape (LENGTH(VALUE)) — declines",
			col:    "code",
			body:   `(LENGTH(VALUE) > 5)`,
			wantOK: false,
		},
		{
			name:   "IN list shape — declines",
			col:    "status",
			body:   `(VALUE IN ('a','b','c'))`,
			wantOK: false,
		},
		{
			name:   "regex with negation (!~) — declines (negated regex not in scope for v0.97.0)",
			col:    "email",
			body:   `(VALUE !~ 'spam')`,
			wantOK: false,
		},
		{
			name:   "single-sided range (only upper bound) — declines",
			col:    "pct",
			body:   `(VALUE <= (100)::numeric)`,
			wantOK: false,
		},
		{
			name:   "non-numeric range literal — declines",
			col:    "label",
			body:   `(VALUE >= 'a' AND VALUE <= 'z')`,
			wantOK: false,
		},
		{
			name:   "empty body — declines",
			col:    "x",
			body:   ``,
			wantOK: false,
		},
		{
			name:   "whitespace-only body — declines",
			col:    "x",
			body:   `   `,
			wantOK: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			clause, ok := translateDomainCheckToMySQL(c.col, ir.DomainCheck{Body: c.body})
			if ok != c.wantOK {
				t.Fatalf("ok = %v; want %v (clause=%q)", ok, c.wantOK, clause)
			}
			if !c.wantOK {
				if clause != "" {
					t.Errorf("clause should be empty when ok=false; got %q", clause)
				}
				return
			}
			if !strings.HasPrefix(clause, "CHECK (") {
				t.Errorf("clause should start with `CHECK (`; got %q", clause)
			}
			for _, part := range c.wantParts {
				if !strings.Contains(clause, part) {
					t.Errorf("clause = %q; want substring %q", clause, part)
				}
			}
		})
	}
}
