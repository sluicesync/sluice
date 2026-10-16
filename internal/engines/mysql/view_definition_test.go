// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "testing"

func TestStripOwnSchemaQualifier(t *testing.T) {
	cases := []struct {
		name, def, schema, want string
		ambiguous               bool
	}{
		{
			name:   "column and table references",
			def:    "select `app`.`t`.`a` AS `a` from `app`.`t`",
			schema: "app",
			want:   "select `t`.`a` AS `a` from `t`",
		},
		{
			name:   "join across two own tables",
			def:    "select `app`.`t`.`a` AS `a` from (`app`.`t` join `app`.`u` on((`app`.`t`.`id` = `app`.`u`.`id`)))",
			schema: "app",
			want:   "select `t`.`a` AS `a` from (`t` join `u` on((`t`.`id` = `u`.`id`)))",
		},
		{
			name:   "another database stays qualified",
			def:    "select `other`.`t`.`a` AS `a` from `other`.`t`",
			schema: "app",
			want:   "select `other`.`t`.`a` AS `a` from `other`.`t`",
		},
		{
			name:   "a database whose name extends the source's stays qualified",
			def:    "select `app2`.`t`.`a` AS `a` from `app2`.`t`",
			schema: "app",
			want:   "select `app2`.`t`.`a` AS `a` from `app2`.`t`",
		},
		{
			name:   "a single-quoted literal spelling the qualifier is untouched",
			def:    "select '`app`.`t`' AS `s` from `app`.`t`",
			schema: "app",
			want:   "select '`app`.`t`' AS `s` from `t`",
		},
		{
			name:   "escaped quotes inside a literal do not end it",
			def:    "select 'it''s \\' `app`.`t`' AS `s`, \"x\"\"`app`.`t`\" AS `d` from `app`.`t`",
			schema: "app",
			want:   "select 'it''s \\' `app`.`t`' AS `s`, \"x\"\"`app`.`t`\" AS `d` from `t`",
		},
		{
			name:   "a column alias spelling the database is not a qualifier",
			def:    "select `app`.`t`.`a` AS `app` from `app`.`t`",
			schema: "app",
			want:   "select `t`.`a` AS `app` from `t`",
		},
		{
			name:   "a backquote inside the database name",
			def:    "select `a``b`.`t`.`x` AS `x` from `a``b`.`t`",
			schema: "a`b",
			want:   "select `t`.`x` AS `x` from `t`",
		},
		{
			name:   "an identifier containing the qualifier text is not split",
			def:    "select `x``app``.``t` AS `c` from `app`.`t`",
			schema: "app",
			want:   "select `x``app``.``t` AS `c` from `t`",
		},
		{
			name:   "case differs: left alone",
			def:    "select `App`.`t`.`a` AS `a` from `App`.`t`",
			schema: "app",
			want:   "select `App`.`t`.`a` AS `a` from `App`.`t`",
		},
		{
			name:   "a table named like its database loses one qualifier, not two",
			def:    "select `app`.`app`.`x` AS `x` from `app`.`app`",
			schema: "app",
			want:   "select `app`.`x` AS `x` from `app`",
		},
		{
			name: "a table alias spelling the database is ambiguous: kept verbatim",
			// Stripping would turn the correlated `app`.`id` into `id`,
			// which binds to the inner u.id (measured on MySQL 8.4).
			def:       "select `app`.`id` AS `id`,(select count(0) from `app`.`u` where (`app`.`u`.`id` = `app`.`id`)) AS `c` from `app`.`t` `app`",
			schema:    "app",
			want:      "select `app`.`id` AS `id`,(select count(0) from `app`.`u` where (`app`.`u`.`id` = `app`.`id`)) AS `c` from `app`.`t` `app`",
			ambiguous: true,
		},
		{
			name:      "a derived table named like the database is ambiguous",
			def:       "select `app`.`a` AS `a` from (select `app`.`t`.`a` AS `a` from `app`.`t`) `app`",
			schema:    "app",
			want:      "select `app`.`a` AS `a` from (select `app`.`t`.`a` AS `a` from `app`.`t`) `app`",
			ambiguous: true,
		},
		{
			name:      "a CTE named like the database is ambiguous",
			def:       "with `app` as (select `app`.`t`.`a` AS `a` from `app`.`t`) select `app`.`a` AS `a` from `app`",
			schema:    "app",
			want:      "with `app` as (select `app`.`t`.`a` AS `a` from `app`.`t`) select `app`.`a` AS `a` from `app`",
			ambiguous: true,
		},
		{
			name:   "a column alias after AS is not ambiguous",
			def:    "select `app`.`t`.`a` AS `app` from `app`.`t`",
			schema: "app",
			want:   "select `t`.`a` AS `app` from `t`",
		},
		{
			name:   "a column named like the database, after a dot, is not a qualifier",
			def:    "select `app`.`t`.`app` AS `c` from `app`.`t`",
			schema: "app",
			want:   "select `t`.`app` AS `c` from `t`",
		},
		{
			name:   "empty schema",
			def:    "select `app`.`t`.`a` AS `a` from `app`.`t`",
			schema: "",
			want:   "select `app`.`t`.`a` AS `a` from `app`.`t`",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ambiguous := stripOwnSchemaQualifier(c.def, c.schema)
			if ambiguous != c.ambiguous {
				t.Fatalf("ambiguous = %t, want %t", ambiguous, c.ambiguous)
			}
			if got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}
