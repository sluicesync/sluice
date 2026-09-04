// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"sluicesync.dev/sluice/internal/config"
)

// The D1 staging mangle refusal is graded against this predicate, and until
// 2026-09-04 no test reached the line that builds it.
//
// WHY THAT MATTERED. `--stage-local` copies the WHOLE D1 database before any
// table filter is consulted, so the refusal has to be scoped to what the run
// will actually read (Bug 265). Scope it wrong in the OTHER direction — swap
// the two argument lists, or negate the result — and every real table reads
// as out-of-scope: a mangled table the operator DID select gets only a
// warning, and its U+FFFD-substituted values are staged and copied to the
// target at exit 0. One character, silent loss, whole suite green.
//
// That is the "pin a value-gated fix THROUGH the CLI layer" shape from Bug
// 180: the branch fires only for particular flag values, so a direct-call
// unit test on the filter proves nothing about what the CLI hands it.
//
// BOTH SOURCES ARE GRADED. A fix that read only `m.IncludeTable` would have
// left a YAML-configured filter unscoped, which is the sibling this project
// keeps leaking.
func TestStageScopeFor_MatchesTheRunsFilterFromBothSources(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cmd     MigrateCmd
		cfg     config.Config
		allowed []string
		denied  []string
		wantNil bool
	}{
		{
			name:    "no filter at all means everything is in scope",
			wantNil: true,
		},
		{
			name:    "CLI --include-table",
			cmd:     MigrateCmd{IncludeTable: []string{"users", "orders"}},
			allowed: []string{"users", "orders"},
			denied:  []string{"audit_trail", "sessions"},
		},
		{
			name:    "CLI --exclude-table",
			cmd:     MigrateCmd{ExcludeTable: []string{"audit_trail"}},
			allowed: []string{"users", "orders"},
			denied:  []string{"audit_trail"},
		},
		{
			name:    "CLI --exclude-table with a glob",
			cmd:     MigrateCmd{ExcludeTable: []string{"audit_*"}},
			allowed: []string{"users"},
			denied:  []string{"audit_trail", "audit_2026"},
		},
		{
			// The sibling a flags-only fix would have missed.
			name:    "YAML include_tables",
			cfg:     config.Config{IncludeTables: []string{"users"}},
			allowed: []string{"users"},
			denied:  []string{"orders", "audit_trail"},
		},
		{
			name:    "YAML exclude_tables",
			cfg:     config.Config{ExcludeTables: []string{"audit_trail"}},
			allowed: []string{"users", "orders"},
			denied:  []string{"audit_trail"},
		},
		{
			// The CLI wins over the config; asserted here so the staging
			// predicate cannot disagree with the run about which one applies.
			name:    "a CLI flag overrides the config",
			cmd:     MigrateCmd{IncludeTable: []string{"users"}},
			cfg:     config.Config{IncludeTables: []string{"orders"}},
			allowed: []string{"users"},
			denied:  []string{"orders"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := tc.cmd
			cfg := tc.cfg
			got := stageScopeFor(&cmd, &cfg)

			if tc.wantNil {
				if got != nil {
					t.Fatal("an unfiltered run must yield a nil predicate, which staging reads as " +
						"everything-in-scope; a non-nil one here risks scoping the refusal off entirely")
				}
				return
			}
			if got == nil {
				t.Fatal("a filtered run yielded a nil predicate — the refusal would then be graded against " +
					"every table, which is the over-refusing direction Bug 265 was about")
			}
			for _, name := range tc.allowed {
				if !got(name) {
					t.Errorf("%q reads as OUT of scope but the run will read it — a mangled value in this "+
						"table would be warned about instead of refused, and copied to the target", name)
				}
			}
			for _, name := range tc.denied {
				if got(name) {
					t.Errorf("%q reads as IN scope but the run excludes it — a mangled value here would "+
						"fail the whole run, which is Bug 265 itself", name)
				}
			}
		})
	}
}
