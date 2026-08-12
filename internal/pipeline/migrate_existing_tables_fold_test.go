// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for the fold-aware pre-create shape gate (audit 2026-08-11, PRF-2).
//
// Before this, the gate looked pre-existing target tables up with an exact,
// case-sensitive key, so on a folding target (SQLite always; MySQL under
// `lower_case_table_names != 0`) a source table `Orders` missed a stored
// `orders` entirely: `CREATE TABLE IF NOT EXISTS` silently no-opped and the
// bulk copy landed the source rows in the unrelated pre-existing table, exit 0,
// zero warnings — the top-ranked silent-alteration class, reproduced live on
// the HEAD binary against two SQLite files.
//
// The gate now folds the catalog lookup through the target's own identifier
// rule ([ir.TargetTableNameFolder]). A differing-shape fold-hit becomes the
// existing coded refusal instead of a silent merge; a matching-shape fold-hit
// proceeds (a folding MySQL stores sluice's own `Orders` as `orders`, so this
// is the resume/bootstrap case) but WARNs when the spellings differ, so the
// merge is never silent. A case-sensitive target (nil fold) is byte-exact and
// unchanged.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// asciiLower stands in for a folding target's identifier rule (MySQL lct!=0,
// SQLite): a source `Orders` and a stored `orders` fold to one key.
func asciiLower(s string) string { return strings.ToLower(s) }

// foldGateMigrator is gateMigrator with the target declaring a fold, so the
// pre-create gate exercises the [ir.TargetTableNameFolder] path.
func foldGateMigrator(srcName, tgtName string, targetSchema *ir.Schema, fold func(string) string) (*Migrator, *recordingEngine) {
	m, tgt := gateMigrator(srcName, tgtName, targetSchema)
	tgt.tableNameFold = fold
	return m, tgt
}

func TestMigrateShapeGate_FoldingTarget_PRF2(t *testing.T) {
	// The source table the operator intends to migrate, spelled `Orders`.
	intended := &ir.Schema{Tables: []*ir.Table{gateTable("Orders", gateCols()...)}}

	t.Run("differing shape under a fold-hit refuses instead of merging", func(t *testing.T) {
		// A pre-existing, unrelated `orders` with an incompatible shape.
		// Pre-fix: exact lookup missed it, IF NOT EXISTS no-opped, the copy
		// landed `Orders` rows in `orders` (rows into wrong columns), exit 0.
		existing := &ir.Schema{Tables: []*ir.Table{gateTable(
			"orders",
			&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
			&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
		)}}
		m, _ := foldGateMigrator("sqlite", "sqlite", existing, asciiLower)
		_, err := m.phasePlanExistingTables(context.Background(), intended)
		if err == nil {
			t.Fatal("fold-hit with a differing shape must refuse (PRF-2); got nil — the silent merge")
		}
		coded, ok := sluicecode.FromError(err)
		if !ok || coded.Code != sluicecode.CodeTargetTableShapeMismatch {
			t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetTableShapeMismatch)
		}
		if !strings.Contains(err.Error(), `"Orders"`) {
			t.Errorf("refusal %q must name the source table", err.Error())
		}
	})

	t.Run("matching shape under a fold-hit proceeds but WARNs, never silent", func(t *testing.T) {
		logs := captureLogs(t)
		existing := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}
		m, _ := foldGateMigrator("sqlite", "sqlite", existing, asciiLower)
		got, err := m.phasePlanExistingTables(context.Background(), intended)
		if err != nil {
			t.Fatalf("matching-shape fold-hit is the resume/bootstrap case and must proceed: %v", err)
		}
		if len(got.Tables) != 0 {
			t.Errorf("fold-matched table not skipped from the CREATE set; create set = %d", len(got.Tables))
		}
		out := logs.String()
		if !strings.Contains(out, "DIFFERENTLY-SPELLED") ||
			!strings.Contains(out, "Orders") || !strings.Contains(out, "orders") {
			t.Errorf("fold-merge WARN missing or not naming both spellings:\n%s", out)
		}
	})

	t.Run("case-sensitive target (identity fold) stays byte-exact", func(t *testing.T) {
		// nil fold: `Orders` does NOT match a stored `orders`, so the source
		// table is treated as absent and CREATEd — the pre-PRF-2 behaviour a
		// case-sensitive engine (Postgres, MySQL lct=0) must keep.
		existing := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}
		m, _ := foldGateMigrator("postgres", "postgres", existing, nil)
		got, err := m.phasePlanExistingTables(context.Background(), intended)
		if err != nil {
			t.Fatalf("case-sensitive target: %v", err)
		}
		if len(got.Tables) != 1 || got.Tables[0].Name != "Orders" {
			t.Fatalf("case-sensitive target must treat `Orders` as absent and create it; got %+v", got.Tables)
		}
	})

	t.Run("fold-read failure warns and falls back to exact match", func(t *testing.T) {
		logs := captureLogs(t)
		existing := &ir.Schema{Tables: []*ir.Table{gateTable("orders", gateCols()...)}}
		m, tgt := foldGateMigrator("mysql", "mysql", existing, nil)
		tgt.tableNameFoldErr = errors.New("lct probe refused")
		got, err := m.phasePlanExistingTables(context.Background(), intended)
		if err != nil {
			t.Fatalf("a fold-read failure must never fail the run: %v", err)
		}
		// Falls back to exact: `Orders` != stored `orders`, so it is created.
		if len(got.Tables) != 1 {
			t.Errorf("fold-read failure must fall back to exact (create everything); got %d", len(got.Tables))
		}
		if !strings.Contains(logs.String(), "identifier-fold setting") {
			t.Errorf("fold-read-failure WARN missing:\n%s", logs.String())
		}
	})
}

// TestMigrate_FoldingTargetMergeRefusedEndToEnd drives the full Run: a folding
// target carrying an incompatibly-shaped `orders` must make the whole migrate
// refuse BEFORE any table is created or any row copied — the end-to-end proof
// that the observed `Orders`→`orders` silent merge cannot happen.
func TestMigrate_FoldingTargetMergeRefusedEndToEnd(t *testing.T) {
	src := newRecordingEngine("sqlite")
	src.schema = &ir.Schema{Tables: []*ir.Table{gateTable("Orders", gateCols()...)}}
	tgt := newRecordingEngine("sqlite")
	tgt.schema = &ir.Schema{Tables: []*ir.Table{gateTable(
		"orders",
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
		&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
	)}}
	tgt.tableNameFold = asciiLower

	m := &Migrator{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt"}
	err := m.Run(context.Background())
	if err == nil {
		t.Fatal("folding target with an incompatible fold-twin must refuse; got nil — the silent merge")
	}
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeTargetTableShapeMismatch {
		t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetTableShapeMismatch)
	}
	for _, entry := range tgt.phaseLog {
		if entry == "CreateTablesWithoutConstraints" || strings.HasPrefix(entry, "WriteRows:") {
			t.Errorf("refusal must fire before any DDL/copy; phaseLog = %v", tgt.phaseLog)
			break
		}
	}
}
