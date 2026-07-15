// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/kong"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// These tests pin the flat-file flag surface THROUGH the real kong parser
// (the Bug-180 lesson: behavior that hangs on an omitted vs empty CLI value
// must be pinned through the parser, not by direct construction — kong
// could collapse the distinction and a direct-call unit test would never
// notice). The observable is real behavior: the source engine
// resolveEngines returns, opening a real fixture file.

// parseMigrate parses a migrate command line through kong.
func parseMigrate(t *testing.T, args ...string) *CLI {
	t.Helper()
	cli := &CLI{}
	parser, err := kong.New(cli, kong.Exit(func(int) {}))
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}
	if _, err := parser.Parse(append([]string{"migrate"}, args...)); err != nil {
		t.Fatalf("kong parse: %v", err)
	}
	return cli
}

// writeFixture writes a small flat file into a temp dir.
func writeFixture(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// resolveSource runs the PRODUCTION resolveEngines path on a parsed CLI.
func resolveSource(t *testing.T, cli *CLI) (ir.Engine, error) {
	t.Helper()
	source, _, cleanup, err := cli.Migrate.resolveEngines(context.Background(), &cli.Globals)
	t.Cleanup(cleanup)
	return source, err
}

// migrateArgs builds a csv-source migrate arg list against a sqlite target
// (nothing dials; resolveEngines only resolves).
func migrateArgs(src string, extra ...string) []string {
	return append([]string{
		"--source-driver=csv", "--source=" + src,
		"--target-driver=sqlite", "--target=ignored.db",
	}, extra...)
}

// TestCSVNullFlagOmittedVsEmpty pins the load-bearing kong distinction:
// omitted --csv-null (nil) refuses an unquoted empty field as ambiguous,
// while an explicit --csv-null=” maps it to NULL — through the full
// parse → resolveEngines → engine-open path.
func TestCSVNullFlagOmittedVsEmpty(t *testing.T) {
	fixture := "a,b\n1,\n" // record with an unquoted empty field
	ctx := context.Background()

	t.Run("omitted refuses CSV-NULL-AMBIGUOUS", func(t *testing.T) {
		src := writeFixture(t, "x.csv", fixture)
		cli := parseMigrate(t, migrateArgs(src, "--csv-header")...)
		if cli.CSVNull != nil {
			t.Fatalf("omitted --csv-null parsed non-nil %q — the pointer flag lost the omitted/empty distinction", *cli.CSVNull)
		}
		source, err := resolveSource(t, cli)
		if err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		_, err = source.OpenSchemaReader(ctx, src)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCSVNullAmbiguous {
			t.Fatalf("open err = %v; want SLUICE-E-CSV-NULL-AMBIGUOUS", err)
		}
	})

	t.Run("explicit empty maps empty fields to NULL", func(t *testing.T) {
		src := writeFixture(t, "x.csv", fixture)
		cli := parseMigrate(t, migrateArgs(src, "--csv-header", "--csv-null=")...)
		if cli.CSVNull == nil || *cli.CSVNull != "" {
			t.Fatalf("--csv-null='' parsed as %v; want a non-nil empty string", cli.CSVNull)
		}
		source, err := resolveSource(t, cli)
		if err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		sr, err := source.OpenSchemaReader(ctx, src)
		if err != nil {
			t.Fatalf("open with --csv-null='': %v", err)
		}
		migcore.CloseIf(sr)
	})

	t.Run("literal repr reaches the engine", func(t *testing.T) {
		src := writeFixture(t, "x.csv", "a,b\n1,\\N\n2,\n")
		cli := parseMigrate(t, migrateArgs(src, "--csv-header", `--csv-null=\N`)...)
		source, err := resolveSource(t, cli)
		if err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		// With \N declared, the empty field on record 2 is an empty string —
		// the open must succeed (no ambiguity refusal).
		sr, err := source.OpenSchemaReader(ctx, src)
		if err != nil {
			t.Fatalf("open with --csv-null='\\N': %v", err)
		}
		migcore.CloseIf(sr)
	})
}

// TestCSVHeaderFlags pins the header-declaration surface: mutual exclusion
// at resolve time, the undeclared refusal at open, and --csv-no-header
// flowing through.
func TestCSVHeaderFlags(t *testing.T) {
	ctx := context.Background()

	t.Run("both flags refuse at resolve", func(t *testing.T) {
		src := writeFixture(t, "x.csv", "a\n1\n")
		cli := parseMigrate(t, migrateArgs(src, "--csv-header", "--csv-no-header")...)
		_, err := resolveSource(t, cli)
		if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("want the mutual-exclusion refusal; got %v", err)
		}
	})

	t.Run("neither flag refuses at open with CSV-HEADER-UNDECLARED", func(t *testing.T) {
		src := writeFixture(t, "x.csv", "a\n1\n")
		cli := parseMigrate(t, migrateArgs(src)...)
		source, err := resolveSource(t, cli)
		if err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		_, err = source.OpenSchemaReader(ctx, src)
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCSVHeaderUndeclared {
			t.Fatalf("open err = %v; want SLUICE-E-CSV-HEADER-UNDECLARED", err)
		}
	})

	t.Run("--csv-no-header flows through", func(t *testing.T) {
		src := writeFixture(t, "x.csv", "1,2\n")
		cli := parseMigrate(t, migrateArgs(src, "--csv-no-header")...)
		source, err := resolveSource(t, cli)
		if err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		sr, err := source.OpenSchemaReader(ctx, src)
		if err != nil {
			t.Fatalf("open with --csv-no-header: %v", err)
		}
		defer migcore.CloseIf(sr)
		schema, err := sr.ReadSchema(ctx)
		if err != nil {
			t.Fatalf("ReadSchema: %v", err)
		}
		if got := schema.Tables[0].Columns[0].Name; got != "col1" {
			t.Errorf("first column = %q; want col1", got)
		}
	})
}

// TestFlatFileAutoEngagesInferTypes pins the ADR-0163 auto-engage: a
// csv/tsv/ndjson source flips Migrate.InferTypes on through resolveEngines,
// and --infer-types stays refused for richly-typed sources.
func TestFlatFileAutoEngagesInferTypes(t *testing.T) {
	t.Run("csv auto-engages", func(t *testing.T) {
		src := writeFixture(t, "x.csv", "a\n1\n")
		cli := parseMigrate(t, migrateArgs(src, "--csv-header")...)
		if cli.Migrate.InferTypes {
			t.Fatal("precondition: --infer-types not passed")
		}
		if _, err := resolveSource(t, cli); err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		if !cli.Migrate.InferTypes {
			t.Error("InferTypes was not auto-engaged for a csv source")
		}
	})
	t.Run("ndjson accepts explicit --infer-types", func(t *testing.T) {
		src := writeFixture(t, "x.ndjson", `{"a":1}`+"\n")
		cli := parseMigrate(t, []string{
			"--source-driver=ndjson", "--source=" + src,
			"--target-driver=sqlite", "--target=ignored.db",
			"--infer-types",
		}...)
		if _, err := resolveSource(t, cli); err != nil {
			t.Fatalf("resolveEngines: %v", err)
		}
		if !cli.Migrate.InferTypes {
			t.Error("explicit --infer-types lost")
		}
	})
	t.Run("mysql source still refuses --infer-types", func(t *testing.T) {
		cli := parseMigrate(t,
			"--source-driver=mysql", "--source=u:p@tcp(h:3306)/db",
			"--target-driver=sqlite", "--target=ignored.db",
			"--infer-types")
		_, err := resolveSource(t, cli)
		if err == nil || !strings.Contains(err.Error(), "only supported for SQLite/D1 and csv/tsv/ndjson") {
			t.Fatalf("want the infer-types source refusal; got %v", err)
		}
	})
}

// TestCSVFlagsInertOnOtherEngines pins the Bug 189 contract: the
// --csv-* flags on a non-flat-file SOURCE refuse loudly (they were
// silently ignored, contradicting the documented refuse-loudly
// contract) — and the tsv delimiter contradiction is caught at resolve.
func TestCSVFlagsInertOnOtherEngines(t *testing.T) {
	t.Run("csv flags refused on mysql source (Bug 189)", func(t *testing.T) {
		cli := parseMigrate(t,
			"--source-driver=mysql", "--source=u:p@tcp(h:3306)/db",
			"--target-driver=sqlite", "--target=ignored.db",
			"--csv-header", "--csv-null=NULL")
		_, err := resolveSource(t, cli)
		if err == nil || !strings.Contains(err.Error(), "--source-driver csv|tsv|ndjson") {
			t.Fatalf("csv flags on a mysql source must refuse naming the flat-file drivers (Bug 189); got %v", err)
		}
	})
	t.Run("tsv driver refuses a contradicting delimiter at resolve", func(t *testing.T) {
		src := writeFixture(t, "x.tsv", "a\tb\n")
		cli := parseMigrate(t,
			"--source-driver=tsv", "--source="+src,
			"--target-driver=sqlite", "--target=ignored.db",
			"--csv-header", "--csv-delimiter=;")
		_, err := resolveSource(t, cli)
		if err == nil || !strings.Contains(err.Error(), "fixed to a TAB") {
			t.Fatalf("want the tsv fixed-tab refusal; got %v", err)
		}
	})
}
