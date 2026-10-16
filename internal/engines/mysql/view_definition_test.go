package mysql

import "testing"

func TestStripOwnSchemaQualifier(t *testing.T) {
	cases := []struct {
		name, def, schema, want string
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
			name:   "empty schema",
			def:    "select `app`.`t`.`a` AS `a` from `app`.`t`",
			schema: "",
			want:   "select `app`.`t`.`a` AS `a` from `app`.`t`",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := stripOwnSchemaQualifier(c.def, c.schema); got != c.want {
				t.Errorf("got  %s\nwant %s", got, c.want)
			}
		})
	}
}
