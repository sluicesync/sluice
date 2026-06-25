// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strconv"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0118 (CLI flag-naming consistency) test suite. These pin the
// additive-alias / deprecation-WARN / new-flag contracts so a future tag
// rename or a dropped `aliases:` tag is caught loudly. They pin that we WIRED
// the tags (and the precedence/WARN logic), not kong's matcher itself.

// parseInto runs kong over the real CLI tree with the given argv (no leading
// program name) and returns the bound *CLI. A parse error fails the test.
func parseInto(t *testing.T, args ...string) *CLI {
	t.Helper()
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse(args); err != nil {
		t.Fatalf("parse %v: %v", args, err)
	}
	return cli
}

// ---- Finding 2: retry-flag aliases (both spellings bind the same field) ----

func TestADR0118_BackupStream_RetryAliases(t *testing.T) {
	const baseArgs = "backup stream run --source-driver=mysql --source=dsn --output-dir=/tmp/x"

	cases := []struct {
		name    string
		flag    string // the retry flag spelling under test
		val     string
		canon   func(*CLI) string // canonical-field accessor as string
		wantStr string
	}{
		{"primary attempts", "--retry-attempts", "11", func(c *CLI) string { return itoa(c.Backup.Stream.Run.RetryAttempts) }, "11"},
		{"alias attempts", "--apply-retry-attempts", "11", func(c *CLI) string { return itoa(c.Backup.Stream.Run.RetryAttempts) }, "11"},
		{"primary base", "--retry-backoff-base", "250ms", func(c *CLI) string { return c.Backup.Stream.Run.RetryBackoffBase.String() }, "250ms"},
		{"alias base", "--apply-retry-backoff-base", "250ms", func(c *CLI) string { return c.Backup.Stream.Run.RetryBackoffBase.String() }, "250ms"},
		{"primary cap", "--retry-backoff-cap", "45s", func(c *CLI) string { return c.Backup.Stream.Run.RetryBackoffCap.String() }, "45s"},
		{"alias cap", "--apply-retry-backoff-cap", "45s", func(c *CLI) string { return c.Backup.Stream.Run.RetryBackoffCap.String() }, "45s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append(strings.Fields(baseArgs), tc.flag+"="+tc.val)
			cli := parseInto(t, args...)
			if got := tc.canon(cli); got != tc.wantStr {
				t.Errorf("%s=%s bound %q; want %q", tc.flag, tc.val, got, tc.wantStr)
			}
		})
	}
}

func TestADR0118_SyncStart_RetryAliases(t *testing.T) {
	const baseArgs = "sync start --source-driver=mysql --source=src --target-driver=postgres --target=tgt"

	cases := []struct {
		name    string
		flag    string
		val     string
		canon   func(*CLI) string
		wantStr string
	}{
		{"primary attempts", "--apply-retry-attempts", "12", func(c *CLI) string { return itoa(c.Sync.Start.ApplyRetryAttempts) }, "12"},
		{"alias attempts", "--retry-attempts", "12", func(c *CLI) string { return itoa(c.Sync.Start.ApplyRetryAttempts) }, "12"},
		{"primary base", "--apply-retry-backoff-base", "300ms", func(c *CLI) string { return c.Sync.Start.ApplyRetryBackoffBase.String() }, "300ms"},
		{"alias base", "--retry-backoff-base", "300ms", func(c *CLI) string { return c.Sync.Start.ApplyRetryBackoffBase.String() }, "300ms"},
		{"primary cap", "--apply-retry-backoff-cap", "50s", func(c *CLI) string { return c.Sync.Start.ApplyRetryBackoffCap.String() }, "50s"},
		{"alias cap", "--retry-backoff-cap", "50s", func(c *CLI) string { return c.Sync.Start.ApplyRetryBackoffCap.String() }, "50s"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append(strings.Fields(baseArgs), tc.flag+"="+tc.val)
			cli := parseInto(t, args...)
			if got := tc.canon(cli); got != tc.wantStr {
				t.Errorf("%s=%s bound %q; want %q", tc.flag, tc.val, got, tc.wantStr)
			}
		})
	}
}

// ---- Finding 3: cutover --sequence-margin canonical + deprecated alias ----

func TestADR0118_Cutover_SequenceMarginAliases(t *testing.T) {
	const baseArgs = "cutover --source-driver=mysql --source=src --target-driver=postgres --target=tgt"

	for _, spell := range []string{"--sequence-margin", "--cutover-sequence-margin"} {
		t.Run(spell, func(t *testing.T) {
			args := append(strings.Fields(baseArgs), spell+"=7777")
			cli := parseInto(t, args...)
			if cli.Cutover.SequenceMargin != 7777 {
				t.Errorf("%s=7777 bound SequenceMargin=%d; want 7777", spell, cli.Cutover.SequenceMargin)
			}
		})
	}
}

// ---- Finding 4: new read-axis flags parse onto their fields ----

func TestADR0118_SyncStart_ReadAxisFlags(t *testing.T) {
	const baseArgs = "sync start --source-driver=mysql --source=src --target-driver=postgres --target=tgt"

	t.Run("defaults are 0 (unset)", func(t *testing.T) {
		cli := parseInto(t, strings.Fields(baseArgs)...)
		if cli.Sync.Start.VStreamCopyTableParallelism != 0 || cli.Sync.Start.CopyTableParallelism != 0 {
			t.Errorf("unset defaults = (%d,%d); want (0,0) — 0 must mean 'fall back to DSN'",
				cli.Sync.Start.VStreamCopyTableParallelism, cli.Sync.Start.CopyTableParallelism)
		}
	})
	t.Run("vstream flag binds", func(t *testing.T) {
		args := append(strings.Fields(baseArgs), "--vstream-copy-table-parallelism=6")
		cli := parseInto(t, args...)
		if cli.Sync.Start.VStreamCopyTableParallelism != 6 {
			t.Errorf("VStreamCopyTableParallelism=%d; want 6", cli.Sync.Start.VStreamCopyTableParallelism)
		}
	})
	t.Run("native flag binds", func(t *testing.T) {
		args := append(strings.Fields(baseArgs), "--copy-table-parallelism=3")
		cli := parseInto(t, args...)
		if cli.Sync.Start.CopyTableParallelism != 3 {
			t.Errorf("CopyTableParallelism=%d; want 3", cli.Sync.Start.CopyTableParallelism)
		}
	})
}

// ---- Finding 3 WARN: fires ONLY on the deprecated --cutover-sequence-margin
//      spelling, never on the canonical --sequence-margin (incl. its default).

func TestADR0118_SequenceMarginDeprecationWarnGate(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"deprecated alias passed → warn", []string{"cutover", "--cutover-sequence-margin=5"}, true},
		{"deprecated alias bare → warn", []string{"cutover", "--cutover-sequence-margin", "5"}, true},
		{"canonical passed → no warn", []string{"cutover", "--sequence-margin=5"}, false},
		{"neither passed (default) → no warn", []string{"cutover"}, false},
		// The zero-value-safety property: even passing the alias AT ITS DEFAULT
		// value (1000) still warns — detection is by spelling, not value.
		{"deprecated alias at default value → warn", []string{"cutover", "--cutover-sequence-margin=1000"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sequenceMarginDeprecatedAliasUsed(tc.args); got != tc.want {
				t.Errorf("sequenceMarginDeprecatedAliasUsed(%v) = %v; want %v", tc.args, got, tc.want)
			}
		})
	}
}

// ---- Finding 1(b) WARN: fires ONLY when an inert parallelism flag is
//      EXPLICITLY set on a MySQL/VStream source — not on a PG source, and not
//      when unset.

func TestADR0118_InertParallelismWarnGate(t *testing.T) {
	mysqlEng := mustEngine(t, "mysql")
	vstreamEng := mustEngine(t, "planetscale")
	pgEng := mustEngine(t, "postgres")

	cases := []struct {
		name     string
		args     []string
		source   ir.Engine
		wantFlag string
		wantOK   bool
	}{
		{"bulk-parallelism on mysql → warn", []string{"sync", "start", "--bulk-parallelism=8"}, mysqlEng, "bulk-parallelism", true},
		{"table-parallelism on vstream → warn", []string{"sync", "start", "--table-parallelism=4"}, vstreamEng, "table-parallelism", true},
		{"bulk-parallel-min-rows on mysql → warn", []string{"sync", "start", "--bulk-parallel-min-rows=100"}, mysqlEng, "bulk-parallel-min-rows", true},
		{"bulk-parallelism on PG → NO warn (it's honored there)", []string{"sync", "start", "--bulk-parallelism=8"}, pgEng, "", false},
		{"unset on mysql → NO warn", []string{"sync", "start"}, mysqlEng, "", false},
		{"nil source → NO warn", []string{"sync", "start", "--bulk-parallelism=8"}, nil, "", false},
		{"unrelated flag on mysql → NO warn", []string{"sync", "start", "--apply-concurrency=4"}, mysqlEng, "", false},
		// Default-value-but-explicit still warns (spelling, not value).
		{"bulk-parallelism at default value 0 on mysql → warn", []string{"sync", "start", "--bulk-parallelism=0"}, mysqlEng, "bulk-parallelism", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotFlag, gotOK := inertParallelismFlagUsed(tc.args, tc.source)
			if gotOK != tc.wantOK || gotFlag != tc.wantFlag {
				t.Errorf("inertParallelismFlagUsed = (%q,%v); want (%q,%v)", gotFlag, gotOK, tc.wantFlag, tc.wantOK)
			}
		})
	}
}

// mustEngine resolves a registered engine by name or fails the test.
func mustEngine(t *testing.T, name string) ir.Engine {
	t.Helper()
	e, err := resolveEngine(name)
	if err != nil {
		t.Fatalf("resolveEngine(%q): %v", name, err)
	}
	return e
}

// itoa is a tiny test-local int→string so the table cases stay one-liners.
func itoa(n int) string { return strconv.Itoa(n) }
