package main

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

// TestResolveNamespaceScopeArgs pins the --*-schema / --*-database
// synonym merge (ADR-0075): the two spellings populate the SAME internal
// filter, and supplying BOTH forms in one invocation is a loud error.
func TestResolveNamespaceScopeArgs(t *testing.T) {
	cases := []struct {
		name string

		includeDatabase []string
		excludeDatabase []string
		allDatabases    bool

		includeSchema []string
		excludeSchema []string
		allSchemas    bool

		wantInclude []string
		wantExclude []string
		wantAll     bool
		wantErr     string
	}{
		{
			name:    "neither form — empty",
			wantAll: false,
		},
		{
			name:            "database include only",
			includeDatabase: []string{"app_*"},
			wantInclude:     []string{"app_*"},
		},
		{
			name:          "schema include maps to same filter",
			includeSchema: []string{"sales", "billing"},
			wantInclude:   []string{"sales", "billing"},
		},
		{
			name:          "schema exclude maps to same filter",
			excludeSchema: []string{"scratch"},
			wantExclude:   []string{"scratch"},
		},
		{
			name:       "all-schemas maps to all",
			allSchemas: true,
			wantAll:    true,
		},
		{
			name:         "all-databases maps to all",
			allDatabases: true,
			wantAll:      true,
		},
		{
			name:            "BOTH forms — loud error",
			includeDatabase: []string{"app"},
			includeSchema:   []string{"sales"},
			wantErr:         "synonyms",
		},
		{
			name:            "both forms across different sub-flags still errors",
			excludeDatabase: []string{"scratch"},
			allSchemas:      true,
			wantErr:         "synonyms",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			inc, exc, all, err := resolveNamespaceScopeArgs(
				c.includeDatabase, c.excludeDatabase, c.allDatabases,
				c.includeSchema, c.excludeSchema, c.allSchemas,
			)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v; want substring %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(inc, c.wantInclude) {
				t.Errorf("include = %v; want %v", inc, c.wantInclude)
			}
			if !reflect.DeepEqual(exc, c.wantExclude) {
				t.Errorf("exclude = %v; want %v", exc, c.wantExclude)
			}
			if all != c.wantAll {
				t.Errorf("all = %v; want %v", all, c.wantAll)
			}
		})
	}
}

// TestResolveNamespaceMapArgs pins the ADR-0142 --map-database / --map-schema
// synonym merge + YAML fallback: the two spellings render to the SAME OLD=NEW
// pair list, supplying BOTH in one invocation is a loud error, CLI flags
// override the YAML namespace_map block wholesale, and the YAML map renders to
// sorted pairs for deterministic construction.
func TestResolveNamespaceMapArgs(t *testing.T) {
	cases := []struct {
		name        string
		mapDatabase []string
		mapSchema   []string
		cfg         *config.Config
		want        []string
		wantErr     string
	}{
		{
			name: "neither — identity (nil)",
			cfg:  &config.Config{},
			want: nil,
		},
		{
			name:        "map-database only",
			mapDatabase: []string{"app=app_prod"},
			cfg:         &config.Config{},
			want:        []string{"app=app_prod"},
		},
		{
			name:      "map-schema only (synonym)",
			mapSchema: []string{"sales=sales_prod"},
			cfg:       &config.Config{},
			want:      []string{"sales=sales_prod"},
		},
		{
			name:        "BOTH forms — loud error",
			mapDatabase: []string{"app=app_prod"},
			mapSchema:   []string{"sales=sales_prod"},
			cfg:         &config.Config{},
			wantErr:     "synonyms",
		},
		{
			name: "YAML fallback, sorted",
			cfg:  &config.Config{NamespaceMap: map[string]string{"zeta": "z", "alpha": "a"}},
			want: []string{"alpha=a", "zeta=z"},
		},
		{
			name:        "CLI overrides YAML wholesale",
			mapDatabase: []string{"app=app_prod"},
			cfg:         &config.Config{NamespaceMap: map[string]string{"ignored": "x"}},
			want:        []string{"app=app_prod"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := resolveNamespaceMapArgs(c.mapDatabase, c.mapSchema, c.cfg)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v; want substring %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("pairs = %v; want %v", got, c.want)
			}
		})
	}
}
