//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 190 integration pin — the exact v0.99.257-cycle repro. The
// mydumper fragment refusal (audit 2026-07-15 CRITICAL-1) correctly
// errors ExactRowCount on a dump with a severed INSERT fragment, but
// `verify --depth count` used to swallow that error into a SKIPPED
// row and exit 0 — an rc-gated verify passed while the table was
// never verified, contradicting the v0.99.257 "loud on both the
// migrate and verify count paths" claim. The fix flags the run
// unverified (the CLI maps that to a non-zero exit); a clean dump
// still verifies clean.

package pipeline

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// bug190Metadata is the minimal traditional-format mydumper metadata
// file (same shape as the mydumper package's unit fixtures).
const bug190Metadata = `Started dump at: 2026-07-15 10:00:00
SHOW MASTER STATUS:
	Log: mysql-bin.000002
	Pos: 12345
	GTID:00000000-0000-0000-0000-000000000001:1-42

Finished dump at: 2026-07-15 10:00:05
`

// writeBug190Dump lays down a two-table mydumper dump dir: table t
// (3 rows, chunk optionally severed mid-file) and table u (2 rows,
// always clean).
func writeBug190Dump(t *testing.T, severed bool) string {
	t.Helper()
	dir := t.TempDir()
	tChunk := "INSERT INTO `t` VALUES (1),(2);\nINSERT INTO `t` VALUES (3);\n"
	if severed {
		// The Bug-190 repro splice: a keyword-less INSERT tail
		// fragment between two statements.
		tChunk = "INSERT INTO `t` VALUES (1),(2);\n(4),(5);\nINSERT INTO `t` VALUES (3);\n"
	}
	files := map[string]string{
		"metadata":          bug190Metadata,
		"shop.t-schema.sql": "CREATE TABLE `t` (`id` bigint NOT NULL);",
		"shop.t.00000.sql":  tChunk,
		"shop.u-schema.sql": "CREATE TABLE `u` (`id` bigint NOT NULL);",
		"shop.u.00000.sql":  "INSERT INTO `u` VALUES (1),(2);\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

// TestVerify_MydumperSeveredFragment_Bug190 runs `verify --depth
// count` from a mydumper dump into a real MySQL target whose counts
// match the dump's intact reading.
func TestVerify_MydumperSeveredFragment_Bug190(t *testing.T) {
	_, tgtDSN, cleanup := startMySQL(t)
	defer cleanup()

	applyMySQLDDL(t, tgtDSN, `
		CREATE TABLE t (id BIGINT NOT NULL) ENGINE=InnoDB;
		INSERT INTO t VALUES (1),(2),(3);
		CREATE TABLE u (id BIGINT NOT NULL) ENGINE=InnoDB;
		INSERT INTO u VALUES (1),(2);
	`)

	md, ok := engines.Get("mydumper")
	if !ok {
		t.Fatal("mydumper engine not registered")
	}
	my, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("severed dump fails as unverified", func(t *testing.T) {
		dir := writeBug190Dump(t, true)
		var buf bytes.Buffer
		v := &Verifier{Source: md, Target: my, SourceDSN: dir, TargetDSN: tgtDSN, Depth: VerifyDepthCount, Out: &buf}
		r, err := v.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if !r.HasUnverified() {
			t.Errorf("severed-fragment count error must flag the run unverified (Bug 190); got %+v", r.Summary)
		}
		if r.Summary.TablesUnverified != 1 || r.Summary.TablesClean != 1 {
			t.Errorf("expected 1 unverified (t) / 1 clean (u); got %+v", r.Summary)
		}
		if r.HasMismatch() {
			t.Errorf("the refusal is unverified, not a count mismatch; got %+v", r.Summary)
		}
		out := buf.String()
		// The SKIPPED row must name the table, the chunk file, and the
		// fragment refusal, and the report must announce the non-zero
		// exit.
		for _, want := range []string{
			"\nt ", "shop.t.00000.sql", "does not begin with a SQL keyword",
			"1 could not be verified",
			"an unverified table is not a pass; non-zero exit code follows",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("expected %q in text output; got:\n%s", want, out)
			}
		}
	})

	t.Run("clean dump verifies clean", func(t *testing.T) {
		dir := writeBug190Dump(t, false)
		var buf bytes.Buffer
		v := &Verifier{Source: md, Target: my, SourceDSN: dir, TargetDSN: tgtDSN, Depth: VerifyDepthCount, Out: &buf}
		r, err := v.Run(ctx)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if r.HasUnverified() || r.HasMismatch() {
			t.Errorf("clean dump must verify clean; got %+v\nreport:\n%s", r.Summary, buf.String())
		}
		if r.Summary.TablesChecked != 2 || r.Summary.TablesClean != 2 {
			t.Errorf("expected 2 checked / 2 clean; got %+v", r.Summary)
		}
	})
}
