package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/config"
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
			"target_type with parentheses",
			"users.bio=varchar(2048)",
			config.Mapping{Table: "users", Column: "bio", TargetType: "varchar(2048)"},
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
