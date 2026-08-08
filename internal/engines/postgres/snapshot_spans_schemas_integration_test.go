//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0075's exported-snapshot-spans-all-schemas premise, ground-truthed
// against a real server (2026-08-08 invariant sweep). See the test doc.

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// spanTable is the per-schema table this suite reads. The table NAME is
// identical in every schema on purpose: that is the canonical multi-schema
// shape (tenant-per-schema), and it is the shape in which reading the wrong
// schema RESOLVES and returns rows instead of erroring.
func spanTable(schema string) *ir.Table {
	return &ir.Table{
		Schema: schema,
		Name:   "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "v", Type: ir.Text{}},
		},
	}
}

func spanValues(t *testing.T, rows []ir.Row) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s, ok := r["v"].(string)
		if !ok {
			t.Fatalf("row %v: column v is %T, want string", r, r["v"])
		}
		out = append(out, s)
	}
	return out
}

func spanSeedDDL(schemas []string) string {
	out := ""
	for _, s := range schemas {
		out += fmt.Sprintf(`
			CREATE SCHEMA %s;
			CREATE TABLE %s.t (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
			INSERT INTO %s.t (id, v) VALUES (1, '%s-before');
		`, quoteIdent(s), quoteIdent(s), quoteIdent(s), s)
	}
	return out
}

// TestExportedSnapshotSpansEverySchema ground-truths the premise ADR-0075's
// whole consistency model rests on:
//
//	"creating the logical replication slot returns an exported snapshot at a
//	 single consistent LSN, and that snapshot spans the ENTIRE database (all
//	 schemas) by construction"
//
// That is an environmental fact about PostgreSQL. Until 2026-08-08 it was
// asserted in the ADR and in three code comments and checked nowhere — the
// premise-naming step's exact shape. It is checkable in about thirty lines, so
// it is checked.
//
// # The provocation, and the independent expected value
//
// Three schemas, each with an identically-named table seeded with one row.
// The spanning snapshot is opened, and only THEN is a second row inserted into
// every schema's table on a separate connection. The independent expected
// value is the test's own two sets of writes — the "-before" rows must all be
// visible through the one pinned snapshot transaction and the "-after" rows
// must all be invisible, in EVERY schema.
//
// The multi-schema half is what makes this more than a re-run of
// TestSnapshotStream_NoGapNoOverlap: a snapshot that covered only the DSN's
// bound schema would still hide that schema's post-export write, and would
// expose the other two schemas'. Grading every schema is the only shape in
// which the spanning claim can fail.
//
// # It also binds the premise to sluice's USE of it
//
// Two facts can each be pinned and still leave the argument unpinned. So the
// reads go through the reader the orchestrator actually copies with —
// stream.Rows from OpenMultiDatabaseSnapshotStream — rather than through a
// hand-rolled SET TRANSACTION SNAPSHOT. A spanning snapshot the reader could
// not address per schema would satisfy the premise and still copy the wrong
// rows; asserting per-schema CONTENT ('<schema>-before', not another
// schema's) is what closes that.
//
// # What a mutation run can and cannot show here
//
// Clearing qualifyBySchema on the spanning reader fails this test, which is
// the sluice-side half. Nothing in this repository can mutate the OTHER half —
// a PostgreSQL whose exported snapshot stopped spanning namespaces — so this
// test's irreplaceable coverage is of a change in the SERVER, which no
// mutation of sluice can simulate. Same shape as the real-mydumper leg of
// TestMydumperIntegration_RealDumpEndToEnd, and it is why the premise needed a
// real server rather than a fixture.
func TestExportedSnapshotSpansEverySchema(t *testing.T) {
	dsn, cleanup := startPostgresForSnapshotCDC(t)
	defer cleanup()

	schemas := []string{"tenant_a", "tenant_b", "tenant_c"}
	applyPGSnap(t, dsn, spanSeedDDL(schemas))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng := Engine{}
	stream, err := eng.OpenMultiDatabaseSnapshotStream(ctx, dsn, schemas)
	if err != nil {
		t.Fatalf("OpenMultiDatabaseSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Post-snapshot writes, on a separate connection so they commit strictly
	// after the slot's consistent_point. One per schema — the whole point.
	after := ""
	for _, s := range schemas {
		after += fmt.Sprintf("INSERT INTO %s.t (id, v) VALUES (2, '%s-after');\n", quoteIdent(s), s)
	}
	applyPGSnap(t, dsn, after)

	for _, s := range schemas {
		got := spanValues(t, drainAllRows(t, ctx, stream.Rows, spanTable(s)))
		want := []string{s + "-before"}
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("schema %q read through the spanning snapshot = %v; want %v.\n"+
				"A '%s-after' row means the snapshot does NOT cover this schema — ADR-0075's "+
				"one-cut-across-all-schemas guarantee is false and every multi-schema cold start "+
				"has a gap. A different schema's value means the reader is not addressing this "+
				"schema at all.", s, got, want, s)
		}
	}

	// One position, one cut. The handoff LSN is a single value for every
	// schema; a per-schema position would be a different consistency model.
	if stream.Position.Token == "" {
		t.Fatal("spanning stream carries no position token; the CDC handoff has nothing to resume from")
	}
}

// TestNonSpanningReaderRefusesAForeignSchema is the harm proof for the second
// premise, on a real server.
//
// The premise ADR-0075 does not state is that whatever reader drains a
// spanning stream qualifies by the table's own schema. The snapshot-importer
// readers minted for the ADR-0079 parallel cold start do not. This test builds
// exactly that reader shape — a NON-spanning reader over the same database —
// and hands it a table from another schema, which is what a future rewiring of
// the fast lane would do.
//
// The control matters as much as the refusal: the same reader reading its OWN
// schema's identically-named table must succeed and return that schema's row.
// Without it, "it refused" would be indistinguishable from "it is broken".
func TestNonSpanningReaderRefusesAForeignSchema(t *testing.T) {
	dsn, cleanup := startPostgresForSnapshotCDC(t)
	defer cleanup()

	applyPGSnap(t, dsn, `
		CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO t (id, v) VALUES (1, 'public-row');
	`+spanSeedDDL([]string{"tenant_b"}))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng := Engine{}
	stream, err := eng.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// The refusal FIRST, so it is the pinned connection's first statement:
	// under a mutation that removes the guard, the query actually runs and the
	// values it returns are the finding. (Ordering it after the control would
	// make the mutant's failure a driver panic on the pinned conn instead of
	// the assertion below — a fail either way, but one that reads as flake.)
	rows, err := stream.Rows.ReadRows(ctx, spanTable("tenant_b"))
	if err == nil {
		got := []string{}
		for row := range rows {
			s, _ := row["v"].(string)
			got = append(got, s)
		}
		t.Fatalf("a single-schema reader accepted tenant_b.t and returned %v; want a loud refusal. "+
			"It read public.t under tenant_b's name — silent cross-schema divergence", got)
	}
	if !errors.Is(err, errSchemaEscape) {
		t.Fatalf("refusal = %v; want errSchemaEscape so callers can match it structurally", err)
	}

	// Control: its own schema still reads, and reads the right row. Without
	// this, "it refused" is indistinguishable from "it is broken".
	got := spanValues(t, drainAllRows(t, ctx, stream.Rows, spanTable("public")))
	if len(got) != 1 || got[0] != "public-row" {
		t.Fatalf("non-spanning reader on its own schema = %v; want [public-row] — the refusal above "+
			"is meaningless if the reader cannot read at all", got)
	}
}
