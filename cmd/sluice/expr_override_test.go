package main

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

// TestParseExprOverride covers the CLI flag's split rules: first
// `.` separates table from column, first `=` after that separates
// column from expression. The expression can contain arbitrary
// characters (including more `=`, `(`, `)`, single quotes) without
// further escaping.
func TestParseExprOverride(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		want      config.ExpressionMapping
		wantErr   bool
		wantInErr string
	}{
		{
			name: "simple",
			raw:  "impact_items.is_cleared=coalesce(x, 0)",
			want: config.ExpressionMapping{
				Table:      "impact_items",
				Column:     "is_cleared",
				Expression: "coalesce(x, 0)",
			},
		},
		{
			name: "expression contains additional equals signs",
			raw:  "t.c=case when a = b then 1 else 0 end",
			want: config.ExpressionMapping{
				Table:      "t",
				Column:     "c",
				Expression: "case when a = b then 1 else 0 end",
			},
		},
		{
			name: "expression contains punctuation and quotes",
			raw:  "t.c=coalesce((qty IS NULL)::int, 0)",
			want: config.ExpressionMapping{
				Table:      "t",
				Column:     "c",
				Expression: "coalesce((qty IS NULL)::int, 0)",
			},
		},
		{
			name: "leading and trailing whitespace stripped",
			raw:  "  t.c = lower(email)  ",
			want: config.ExpressionMapping{
				Table:      "t",
				Column:     "c",
				Expression: "lower(email)",
			},
		},
		{
			name:      "empty value rejected",
			raw:       "",
			wantErr:   true,
			wantInErr: "empty value",
		},
		{
			name:      "missing equals rejected",
			raw:       "t.c-no-equals",
			wantErr:   true,
			wantInErr: "missing '='",
		},
		{
			name:      "missing dot rejected",
			raw:       "no_dot=foo",
			wantErr:   true,
			wantInErr: "missing '.'",
		},
		{
			name:      "empty table rejected",
			raw:       ".c=foo",
			wantErr:   true,
			wantInErr: "empty table name",
		},
		{
			name:      "empty column rejected",
			raw:       "t.=foo",
			wantErr:   true,
			wantInErr: "empty column name",
		},
		{
			name:      "empty expression rejected",
			raw:       "t.c=",
			wantErr:   true,
			wantInErr: "empty expression",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := parseExprOverride(c.raw)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error; got nil (got=%+v)", got)
				}
				if !strings.Contains(err.Error(), c.wantInErr) {
					t.Errorf("err = %v; want a %q message", err, c.wantInErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("got %+v; want %+v", got, c.want)
			}
		})
	}
}

// TestResolveExpressionMappings_CLIWinsOverYAML pins the wholesale-
// override precedence: when CLI flags are supplied they replace the
// YAML config entirely, matching the same shape resolveMappings uses
// for type overrides.
func TestResolveExpressionMappings_CLIWinsOverYAML(t *testing.T) {
	cfg := &config.Config{
		ExpressionMappings: []config.ExpressionMapping{
			{Table: "yaml_only", Column: "c", Expression: "yaml_expr"},
		},
	}

	t.Run("no CLI overrides → YAML preserved", func(t *testing.T) {
		got, err := resolveExpressionMappings(nil, cfg)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 1 || got[0].Table != "yaml_only" {
			t.Errorf("got %+v; want YAML preserved", got)
		}
	})

	t.Run("CLI overrides replace YAML wholesale", func(t *testing.T) {
		got, err := resolveExpressionMappings([]string{"t.c=cli_expr"}, cfg)
		if err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(got) != 1 || got[0].Table != "t" || got[0].Expression != "cli_expr" {
			t.Errorf("got %+v; want CLI override only", got)
		}
	})
}

// TestResolveExpressionMappings_CLIErrorIndex confirms the index in
// the error message points at the right CLI flag occurrence — a
// usability nicety on a `--expr-override` repeated five times where
// one of them is malformed.
func TestResolveExpressionMappings_CLIErrorIndex(t *testing.T) {
	_, err := resolveExpressionMappings([]string{"good.col=ok", "bad-no-dot=ok"}, &config.Config{})
	if err == nil {
		t.Fatal("expected error on second flag")
	}
	if !strings.Contains(err.Error(), "--expr-override[1]") {
		t.Errorf("err = %v; want index '[1]' to flag the second occurrence", err)
	}
}
