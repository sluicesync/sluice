// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// These tests pin --stage-dir / SLUICE_STAGE_DIR THROUGH the real kong
// parser (the Bug-180 discipline: a staging path that hangs on a CLI
// value must be pinned via parse -> resolveEngines -> real engine open,
// not by direct Options construction). The observable is real staging
// behavior: the flat-file staged SQLite copy lands under the named
// directory while the reader is open.

// stagedDBsUnder lists sluice-flatfile-*.db files under dir.
func stagedDBsUnder(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "sluice-flatfile-*.db"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return matches
}

// assertStagesUnder opens the parsed CLI's csv source and asserts the
// staged copy is created under stageDir and removed on Close.
func assertStagesUnder(t *testing.T, cli *CLI, src, stageDir string) {
	t.Helper()
	source, err := resolveSource(t, cli)
	if err != nil {
		t.Fatalf("resolveEngines: %v", err)
	}
	sr, err := source.OpenSchemaReader(context.Background(), src)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	if got := stagedDBsUnder(t, stageDir); len(got) != 1 {
		migcore.CloseIf(sr)
		t.Fatalf("staged copies under --stage-dir while open = %d (%v); want 1", len(got), got)
	}
	migcore.CloseIf(sr)
	if got := stagedDBsUnder(t, stageDir); len(got) != 0 {
		t.Fatalf("staged copies under --stage-dir after Close = %d (%v); want 0", len(got), got)
	}
}

// TestStageDirFlag pins the --stage-dir flag end to end: flatfile staging
// creates (and cleans up) its temp SQLite copy under the named directory.
func TestStageDirFlag(t *testing.T) {
	stageDir := t.TempDir()
	src := writeFixture(t, "x.csv", "a,b\n1,\"x\"\n")
	cli := parseMigrate(t, migrateArgs(src, "--csv-header", "--stage-dir="+stageDir)...)
	if cli.StageDir != stageDir {
		t.Fatalf("parsed StageDir = %q; want %q", cli.StageDir, stageDir)
	}
	assertStagesUnder(t, cli, src, stageDir)
}

// TestStageDirEnvVar pins the SLUICE_STAGE_DIR env binding through kong:
// same behavioral observable, no flag on the command line.
func TestStageDirEnvVar(t *testing.T) {
	stageDir := t.TempDir()
	t.Setenv("SLUICE_STAGE_DIR", stageDir)
	src := writeFixture(t, "x.csv", "a,b\n1,\"x\"\n")
	cli := parseMigrate(t, migrateArgs(src, "--csv-header")...)
	if cli.StageDir != stageDir {
		t.Fatalf("StageDir from SLUICE_STAGE_DIR = %q; want %q", cli.StageDir, stageDir)
	}
	assertStagesUnder(t, cli, src, stageDir)
}

// TestStageDirFlag_SQLiteDumpMaterialize pins --stage-dir through the CLI
// for the sqlite engine's `.sql`-dump materialize (roadmap item 72
// leftover): the same parse -> resolveEngines -> real-open discipline as
// the flatfile pin above, observed via the sluice-sqlite-*.db temp file
// landing under the named directory while the reader is open and being
// removed on Close.
func TestStageDirFlag_SQLiteDumpMaterialize(t *testing.T) {
	stageDir := t.TempDir()
	src := writeFixture(t, "dump.sql", "CREATE TABLE t (id INTEGER PRIMARY KEY);\nINSERT INTO t VALUES (1);\n")
	cli := parseMigrate(
		t,
		"--source-driver=sqlite", "--source="+src,
		"--target-driver=postgres", "--target=ignored",
		"--stage-dir="+stageDir,
	)
	source, err := resolveSource(t, cli)
	if err != nil {
		t.Fatalf("resolveEngines: %v", err)
	}
	sr, err := source.OpenSchemaReader(context.Background(), src)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	staged, err := filepath.Glob(filepath.Join(stageDir, "sluice-sqlite-*.db"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(staged) != 1 {
		migcore.CloseIf(sr)
		t.Fatalf("materialized dumps under --stage-dir while open = %d (%v); want 1", len(staged), staged)
	}
	migcore.CloseIf(sr)
	if _, err := os.Stat(staged[0]); !os.IsNotExist(err) {
		t.Fatalf("materialized dump %q should be removed after Close; stat err = %v", staged[0], err)
	}
}

// TestStageDirMissingRefusesLoudly pins the loud-failure posture through
// the CLI layer: a --stage-dir that does not exist refuses at open,
// naming the flag — never a silent fallback to the system temp dir.
func TestStageDirMissingRefusesLoudly(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	src := writeFixture(t, "x.csv", "a\n1\n")
	cli := parseMigrate(t, migrateArgs(src, "--csv-header", "--stage-dir="+missing)...)
	source, err := resolveSource(t, cli)
	if err != nil {
		t.Fatalf("resolveEngines: %v", err)
	}
	_, err = source.OpenSchemaReader(context.Background(), src)
	if err == nil || !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), "--stage-dir") {
		t.Fatalf("open with a missing --stage-dir = %v; want a loud not-exist refusal naming the flag", err)
	}
}
