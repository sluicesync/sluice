package main

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

func TestParseTypeOverride(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want config.Mapping
	}{
		{
			"basic",
			"products.attrs=text",
			config.Mapping{Table: "products", Column: "attrs", TargetType: "text"},
		},
		{
			"whitespace tolerated around segments",
			"  products . attrs = longtext  ",
			config.Mapping{Table: "products", Column: "attrs", TargetType: "longtext"},
		},
		{
			"varchar with length",
			"users.bio=varchar(2048)",
			config.Mapping{Table: "users", Column: "bio", TargetType: "varchar", TargetTypeOptions: map[string]any{"length": 2048}},
		},
		{
			"decimal precision+scale",
			"t.amount=decimal(20,0)",
			config.Mapping{Table: "t", Column: "amount", TargetType: "decimal", TargetTypeOptions: map[string]any{"precision": 20, "scale": 0}},
		},
		{
			"numeric alias precision+scale",
			"t.amount=numeric(38,10)",
			config.Mapping{Table: "t", Column: "amount", TargetType: "numeric", TargetTypeOptions: map[string]any{"precision": 38, "scale": 10}},
		},
		{
			"decimal precision only",
			"t.amount=decimal(12)",
			config.Mapping{Table: "t", Column: "amount", TargetType: "decimal", TargetTypeOptions: map[string]any{"precision": 12}},
		},
		{
			"paren args tolerate whitespace",
			"t.amount=decimal( 20 , 4 )",
			config.Mapping{Table: "t", Column: "amount", TargetType: "decimal", TargetTypeOptions: map[string]any{"precision": 20, "scale": 4}},
		},
		{
			// Bug 171: the Bug-170 refusal message suggests an UPPERCASE
			// VARCHAR(n); SQL type names are case-insensitive, so it must parse
			// to the canonical lower-case `varchar` (else the suggested remedy
			// fails to copy-paste).
			"uppercase VARCHAR(n) canonicalised (Bug 171)",
			"tpk_text.code=VARCHAR(255)",
			config.Mapping{Table: "tpk_text", Column: "code", TargetType: "varchar", TargetTypeOptions: map[string]any{"length": 255}},
		},
		{
			"uppercase bare type name canonicalised",
			"t.n=BIGINT",
			config.Mapping{Table: "t", Column: "n", TargetType: "bigint"},
		},
		{
			"mixed-case decimal canonicalised",
			"t.amount=Decimal(20,2)",
			config.Mapping{Table: "t", Column: "amount", TargetType: "decimal", TargetTypeOptions: map[string]any{"precision": 20, "scale": 2}},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := parseTypeOverride(c.in)
			if err != nil {
				t.Fatalf("parseTypeOverride: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %#v; want %#v", got, c.want)
			}
		})
	}
}

func TestParseTypeOverride_Errors(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantSub string
	}{
		{"empty string", "", "empty value"},
		{"missing equals", "products.attrs", "missing '='"},
		{"missing dot", "attrs=text", "missing '.'"},
		{"empty table", ".attrs=text", "empty table"},
		{"empty column", "products.=text", "empty column"},
		{"empty target type", "products.attrs=", "empty target_type"},
		{"unbalanced paren", "t.c=decimal(20,0", "unbalanced"},
		{"empty parens", "t.c=decimal()", "empty parentheses"},
		{"non-integer arg", "t.c=decimal(x)", "is not an integer"},
		{"too many decimal args", "t.c=decimal(1,2,3)", "takes (precision)"},
		{"varchar two args", "t.c=varchar(1,2)", "single (length)"},
		{"parens on non-parametric type", "t.c=text(5)", "does not take parenthesised"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := parseTypeOverride(c.in)
			if err == nil {
				t.Fatalf("expected error containing %q; got nil", c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("error %q missing substring %q", err.Error(), c.wantSub)
			}
		})
	}
}

// TestResolveMappings_PrecedencePolicy locks in the wholesale-
// override behavior: when --type-override is set on the CLI, the
// YAML config's mappings: list is fully ignored. Same shape as the
// table-filter resolution.
func TestResolveMappings_PrecedencePolicy(t *testing.T) {
	yamlMappings := []config.Mapping{
		{Table: "users", Column: "bio", TargetType: "text"},
	}
	cfg := &config.Config{Mappings: yamlMappings}

	t.Run("no CLI flags falls back to YAML", func(t *testing.T) {
		got, err := resolveMappings(nil, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, yamlMappings) {
			t.Errorf("got %#v; want %#v", got, yamlMappings)
		}
	})

	t.Run("CLI flags override YAML wholesale", func(t *testing.T) {
		got, err := resolveMappings([]string{"products.attrs=longtext"}, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []config.Mapping{
			{Table: "products", Column: "attrs", TargetType: "longtext"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v; want %#v — CLI should replace YAML, not merge", got, want)
		}
	})

	t.Run("malformed CLI flag surfaces clear error before any DSN dial", func(t *testing.T) {
		_, err := resolveMappings([]string{"badformat"}, cfg)
		if err == nil {
			t.Fatal("expected error for malformed flag")
		}
		if !strings.Contains(err.Error(), "--type-override[0]") {
			t.Errorf("error should include flag index; got %q", err.Error())
		}
	})
}
