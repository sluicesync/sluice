package main

import (
	"reflect"
	"strings"
	"testing"
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
