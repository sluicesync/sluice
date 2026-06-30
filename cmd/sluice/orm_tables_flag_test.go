// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/pipeline"
)

// TestIncludeORMTablesFlagDefault confirms the CLI policy (ADR-0143):
// --include-orm-tables defaults to false on both `migrate` and `sync start`,
// so SkipORMTables = !IncludeORMTables defaults to TRUE (loud-skip-by-default
// on the CLI); passing the flag flips it to false (keep the tables).
func TestIncludeORMTablesFlagDefault(t *testing.T) {
	cases := []struct {
		name        string
		args        []string
		wantInclude bool
		get         func(*CLI) bool
	}{
		{
			"migrate default skips (include=false)",
			[]string{"migrate", "--source-driver", "mysql", "--source", "dsn", "--target-driver", "postgres", "--target", "dsn"},
			false,
			func(c *CLI) bool { return c.Migrate.IncludeORMTables },
		},
		{
			"migrate --include-orm-tables keeps (include=true)",
			[]string{"migrate", "--source-driver", "mysql", "--source", "dsn", "--target-driver", "postgres", "--target", "dsn", "--include-orm-tables"},
			true,
			func(c *CLI) bool { return c.Migrate.IncludeORMTables },
		},
		{
			"sync start default skips (include=false)",
			[]string{"sync", "start", "--source-driver", "mysql", "--source", "dsn", "--target-driver", "postgres", "--target", "dsn"},
			false,
			func(c *CLI) bool { return c.Sync.Start.IncludeORMTables },
		},
		{
			"sync start --include-orm-tables keeps (include=true)",
			[]string{"sync", "start", "--source-driver", "mysql", "--source", "dsn", "--target-driver", "postgres", "--target", "dsn", "--include-orm-tables"},
			true,
			func(c *CLI) bool { return c.Sync.Start.IncludeORMTables },
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Clear DSN env so kong doesn't pick up a stray SLUICE_SOURCE/TARGET.
			t.Setenv("SLUICE_SOURCE", "")
			t.Setenv("SLUICE_TARGET", "")
			cli := &CLI{}
			parser, err := kong.New(cli, kong.Vars{"version": "test"}, kong.Exit(func(int) {}))
			if err != nil {
				t.Fatalf("kong.New: %v", err)
			}
			if _, err := parser.Parse(c.args); err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if got := c.get(cli); got != c.wantInclude {
				t.Errorf("IncludeORMTables = %v; want %v", got, c.wantInclude)
			}
			// The load-bearing mapping the Run methods apply.
			if skip := !c.get(cli); skip == c.wantInclude {
				t.Errorf("derived SkipORMTables = %v; want %v", skip, !c.wantInclude)
			}
		})
	}
}

// TestPipelineSkipORMTablesZeroValue is the zero-value-safe contract
// (the v0.99.51 trap): a programmatically-constructed Migrator/Streamer
// gets SkipORMTables=false — it must NOT silently start dropping tables.
func TestPipelineSkipORMTablesZeroValue(t *testing.T) {
	if (pipeline.Migrator{}).SkipORMTables {
		t.Error("Migrator{}.SkipORMTables = true; want false (zero value must not skip)")
	}
	if (pipeline.Streamer{}).SkipORMTables {
		t.Error("Streamer{}.SkipORMTables = true; want false (zero value must not skip)")
	}
}
