package main

import (
	"reflect"
	"testing"

	"github.com/orware/sluice/internal/config"
)

// TestResolveTableFilterArgs covers the precedence rule between
// CLI flags and the YAML config: any CLI-side --include-table or
// --exclude-table fully overrides whatever the YAML carried.
// Only when both CLI lists are empty does the config win.
func TestResolveTableFilterArgs(t *testing.T) {
	cases := []struct {
		name        string
		cliInclude  []string
		cliExclude  []string
		cfg         *config.Config
		wantInclude []string
		wantExclude []string
	}{
		{
			"no cli, no cfg — empty",
			nil, nil,
			&config.Config{},
			nil, nil,
		},
		{
			"cli include only",
			[]string{"users"},
			nil,
			&config.Config{},
			[]string{"users"},
			nil,
		},
		{
			"cli exclude only",
			nil,
			[]string{"audit_log"},
			&config.Config{},
			nil,
			[]string{"audit_log"},
		},
		{
			"cfg include only",
			nil, nil,
			&config.Config{IncludeTables: []string{"users", "orders"}},
			[]string{"users", "orders"},
			nil,
		},
		{
			"cfg exclude only",
			nil, nil,
			&config.Config{ExcludeTables: []string{"audit_*"}},
			nil,
			[]string{"audit_*"},
		},
		{
			"cli include overrides cfg include",
			[]string{"users"},
			nil,
			&config.Config{IncludeTables: []string{"orders"}},
			[]string{"users"},
			nil,
		},
		{
			"cli exclude overrides cfg include",
			nil,
			[]string{"audit_log"},
			&config.Config{IncludeTables: []string{"orders"}},
			nil,
			[]string{"audit_log"},
		},
		{
			"cli include overrides cfg exclude",
			[]string{"users"},
			nil,
			&config.Config{ExcludeTables: []string{"audit_log"}},
			[]string{"users"},
			nil,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotInc, gotExc := resolveTableFilterArgs(c.cliInclude, c.cliExclude, c.cfg)
			if !reflect.DeepEqual(gotInc, c.wantInclude) {
				t.Errorf("include = %v; want %v", gotInc, c.wantInclude)
			}
			if !reflect.DeepEqual(gotExc, c.wantExclude) {
				t.Errorf("exclude = %v; want %v", gotExc, c.wantExclude)
			}
		})
	}
}
