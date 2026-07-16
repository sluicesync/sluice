// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0166 pre-create gate (roadmap item 71b): the
// per-verdict matrix (absent→create, present-equal→skip+INFO,
// present-differ→coded refusal, compare-uncomputable→WARN+proceed),
// the cross-engine retarget branch (the PlanetScale bootstrap story at
// fake level), the resume carve-out, and the end-to-end Run pin that
// the CREATE phase receives only the pruned table set while the copy
// still covers everything.

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// captureLogs routes slog.Default() into a buffer for the test's
// duration — the package-documented pattern for asserting log output.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func gateTable(name string, cols ...*ir.Column) *ir.Table {
	return &ir.Table{
		Name:       name,
		Columns:    cols,
		PrimaryKey: &ir.Index{Name: "PRIMARY", Columns: []ir.IndexColumn{{Column: cols[0].Name}}},
	}
}

func gateCols() []*ir.Column {
	return []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "name", Type: ir.Varchar{Length: 255}, Nullable: true},
	}
}

// gateMigrator wires a Migrator whose TARGET catalog read is scripted
// through the recordingEngine's SchemaReader (the same surface the
// real gate uses). srcName/tgtName pick the retarget rule, so the same
// helper covers same-engine and cross-engine pairs.
func gateMigrator(srcName, tgtName string, targetSchema *ir.Schema) (*Migrator, *recordingEngine) {
	tgt := newRecordingEngine(tgtName)
	tgt.schema = targetSchema
	return &Migrator{
		Source:    newRecordingEngine(srcName),
		Target:    tgt,
		SourceDSN: "src", TargetDSN: "tgt",
	}, tgt
}

// TestMigrateShapeGate_VerdictMatrix pins the gate's four verdicts per
// engine name (the "unit per engine" matrix): the gate itself is
// engine-neutral, but each engine family's name is what selects the
// retarget rule, so the matrix runs under every registered target
// family name.
func TestMigrateShapeGate_VerdictMatrix(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres", "sqlite", "planetscale"} {
		t.Run(engine, func(t *testing.T) {
			intended := &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}

			t.Run("absent creates", func(t *testing.T) {
				m, _ := gateMigrator(engine, engine, &ir.Schema{})
				got, err := m.phasePlanExistingTables(context.Background(), intended)
				if err != nil {
					t.Fatalf("gate: %v", err)
				}
				if got != intended {
					t.Errorf("absent table must pass the schema through unchanged; got %+v", got)
				}
			})

			t.Run("present equal skips with INFO", func(t *testing.T) {
				logs := captureLogs(t)
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}})
				got, err := m.phasePlanExistingTables(context.Background(), intended)
				if err != nil {
					t.Fatalf("gate: %v", err)
				}
				if len(got.Tables) != 0 {
					t.Errorf("equal pre-existing table not skipped; create set = %d tables", len(got.Tables))
				}
				if !strings.Contains(logs.String(), "matching column shape") || !strings.Contains(logs.String(), "items") {
					t.Errorf("skip INFO missing or not naming the table:\n%s", logs.String())
				}
			})

			t.Run("present differing refuses coded", func(t *testing.T) {
				differing := gateTable(
					"items",
					&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
					&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
				)
				m, _ := gateMigrator(engine, engine, &ir.Schema{Tables: []*ir.Table{differing}})
				_, err := m.phasePlanExistingTables(context.Background(), intended)
				if err == nil {
					t.Fatal("want the coded shape-mismatch refusal; got nil")
				}
				coded, ok := sluicecode.FromError(err)
				if !ok || coded.Code != sluicecode.CodeTargetTableShapeMismatch {
					t.Fatalf("err = %v; want %s", err, sluicecode.CodeTargetTableShapeMismatch)
				}
				msg := err.Error()
				for _, want := range []string{`table "items"`, `"name"`, `"only_col"`, "(absent)"} {
					if !strings.Contains(msg, want) {
						t.Errorf("refusal %q missing %q", msg, want)
					}
				}
				if !strings.Contains(coded.Hint, "--exclude-table") || !strings.Contains(coded.Hint, "--reset-target-data") {
					t.Errorf("hint %q missing the remedies", coded.Hint)
				}
			})

			t.Run("compare uncomputable warns and proceeds", func(t *testing.T) {
				logs := captureLogs(t)
				m, tgt := gateMigrator(engine, engine, nil)
				tgt.readSchemaErr = errors.New("catalog probe refused")
				got, err := m.phasePlanExistingTables(context.Background(), intended)
				if err != nil {
					t.Fatalf("uncomputable compare must never fail the run: %v", err)
				}
				if got != intended {
					t.Errorf("fallback must pass the schema through unchanged")
				}
				if !strings.Contains(logs.String(), "skipping the pre-create shape compare") ||
					!strings.Contains(logs.String(), "catalog probe refused") {
					t.Errorf("fallback WARN missing:\n%s", logs.String())
				}
			})
		})
	}
}

// TestMigrateShapeGate_PairMatrix pins the gate's ENGAGEMENT per
// engine pair — the audit-2026-07-16 HIGH-1 class fix, pinned by
// family per the Bug-74 discipline. Three postures exist:
//
//   - mapped via retarget rule (postgres→{mysql,planetscale,vitess}):
//     compare active — equal-after-retarget skips, different refuses;
//   - same storage family, no rule (mysql→planetscale): identity
//     compare active — equal skips, different refuses;
//   - NO mapping (mysql→postgres — the observed false-refusal
//     direction — plus sqlite→postgres): compare disengaged — the run
//     proceeds WHATEVER the pre-existing shape, with a WARN naming the
//     tolerated tables, never a refusal built on a comparison the
//     translation layer can't normalize.
//
// The no-mapping "equal" fixture mirrors the live repro's lossy PG
// read-back per family: MySQL TEXT reads back as Text[long], VARBINARY
// as Blob[long], INT UNSIGNED as BIGINT — a re-run over sluice's own
// completed output.
func TestMigrateShapeGate_PairMatrix(t *testing.T) {
	mysqlNative := gateTable(
		"repro",
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
		&ir.Column{Name: "body", Type: ir.Text{Size: ir.TextRegular}, Nullable: true},
		&ir.Column{Name: "payload", Type: ir.Varbinary{Length: 64}, Nullable: true},
		&ir.Column{Name: "qty", Type: ir.Integer{Width: 32, Unsigned: true}},
	)
	pgReadBack := gateTable(
		"repro",
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
		&ir.Column{Name: "body", Type: ir.Text{Size: ir.TextLong}, Nullable: true},
		&ir.Column{Name: "payload", Type: ir.Blob{Size: ir.BlobLong}, Nullable: true},
		&ir.Column{Name: "qty", Type: ir.Integer{Width: 64}},
	)

	t.Run("no mapping proceeds with WARN", func(t *testing.T) {
		for _, pair := range []struct{ src, tgt string }{
			{"mysql", "postgres"},
			{"planetscale", "postgres"},
			{"sqlite", "postgres"},
		} {
			t.Run(pair.src+"→"+pair.tgt, func(t *testing.T) {
				intended := &ir.Schema{Tables: []*ir.Table{mysqlNative}}
				for name, existing := range map[string]*ir.Table{
					// The re-run shape (lossy read-back of sluice's own
					// output) AND a genuinely-different table both proceed:
					// with no mapping the gate cannot tell them apart, so
					// it must never refuse either.
					"re-run over own output": pgReadBack,
					"genuinely different": gateTable("repro",
						&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true}),
				} {
					t.Run(name, func(t *testing.T) {
						logs := captureLogs(t)
						m, _ := gateMigrator(pair.src, pair.tgt, &ir.Schema{Tables: []*ir.Table{existing}})
						got, err := m.phasePlanExistingTables(context.Background(), intended)
						if err != nil {
							t.Fatalf("no-mapping pair must never refuse (audit 2026-07-16 HIGH-1): %v", err)
						}
						if got != intended {
							t.Errorf("no-mapping pair must pass the schema through unchanged")
						}
						for _, want := range []string{"tolerated WITHOUT a shape compare", "repro"} {
							if !strings.Contains(logs.String(), want) {
								t.Errorf("tolerated-tables WARN missing %q:\n%s", want, logs.String())
							}
						}
					})
				}
			})
		}
	})

	t.Run("no mapping with no pre-existing table stays silent", func(t *testing.T) {
		logs := captureLogs(t)
		m, _ := gateMigrator("mysql", "postgres", &ir.Schema{})
		got, err := m.phasePlanExistingTables(context.Background(), &ir.Schema{Tables: []*ir.Table{mysqlNative}})
		if err != nil || len(got.Tables) != 1 {
			t.Fatalf("fresh-target run altered: got=%+v err=%v", got, err)
		}
		if strings.Contains(logs.String(), "tolerated") {
			t.Errorf("WARN fired with nothing tolerated:\n%s", logs.String())
		}
	})

	t.Run("retarget-mapped pairs stay active", func(t *testing.T) {
		intended := &ir.Schema{Tables: []*ir.Table{gateTable(
			"items",
			&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
			&ir.Column{Name: "guid", Type: ir.UUID{}, Nullable: true},
		)}}
		bootstrapped := gateTable(
			"items",
			&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
			&ir.Column{Name: "guid", Type: ir.Char{Length: 36}, Nullable: true},
		)
		for _, tgt := range []string{"mysql", "planetscale", "vitess"} {
			t.Run("postgres→"+tgt, func(t *testing.T) {
				m, _ := gateMigrator("postgres", tgt, &ir.Schema{Tables: []*ir.Table{bootstrapped}})
				got, err := m.phasePlanExistingTables(context.Background(), intended)
				if err != nil {
					t.Fatalf("retarget-equal table refused: %v", err)
				}
				if len(got.Tables) != 0 {
					t.Errorf("retarget-equal table not skipped")
				}

				differing := gateTable("items",
					&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true})
				m, _ = gateMigrator("postgres", tgt, &ir.Schema{Tables: []*ir.Table{differing}})
				if _, err := m.phasePlanExistingTables(context.Background(), intended); err == nil {
					t.Error("genuinely-different table must still refuse where a rule exists")
				}
			})
		}
	})

	t.Run("same family cross-name identity stays active", func(t *testing.T) {
		intended := &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}
		m, _ := gateMigrator("mysql", "planetscale", &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}})
		got, err := m.phasePlanExistingTables(context.Background(), intended)
		if err != nil {
			t.Fatalf("same-family equal table refused: %v", err)
		}
		if len(got.Tables) != 0 {
			t.Errorf("same-family equal table not skipped")
		}

		differing := gateTable("items",
			&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true})
		m, _ = gateMigrator("mysql", "planetscale", &ir.Schema{Tables: []*ir.Table{differing}})
		if _, err := m.phasePlanExistingTables(context.Background(), intended); err == nil {
			t.Error("same-family different table must still refuse")
		}
	})
}

// TestMigrateShapeGate_HintNeverLeadsWithReset pins the refusal hint's
// remedy ordering (audit 2026-07-16 HIGH-1, second half): the
// non-destructive remedies (--exclude-table, inspect) come FIRST;
// --reset-target-data appears last WITH its blast radius spelled out —
// an operator following the hint on a sibling-shard load must not be
// steered into dropping the already-loaded shard's data.
func TestMigrateShapeGate_HintNeverLeadsWithReset(t *testing.T) {
	intended := &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}
	differing := gateTable("items",
		&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true})
	m, _ := gateMigrator("mysql", "mysql", &ir.Schema{Tables: []*ir.Table{differing}})
	_, err := m.phasePlanExistingTables(context.Background(), intended)
	coded, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("err = %v; want the coded refusal", err)
	}
	exclude := strings.Index(coded.Hint, "--exclude-table")
	reset := strings.Index(coded.Hint, "--reset-target-data")
	if exclude == -1 || reset == -1 || exclude > reset {
		t.Errorf("hint must name --exclude-table before --reset-target-data; hint = %q", coded.Hint)
	}
	for _, want := range []string{"every in-scope target table", "not just the conflicting"} {
		if !strings.Contains(coded.Hint, want) {
			t.Errorf("hint %q missing the reset blast-radius warning %q", coded.Hint, want)
		}
	}
}

// TestMigrateShapeGate_CrossEngineRetargetEqual pins the retarget
// branch and the PlanetScale bootstrap story at unit level: a PG
// source's uuid column lands on a MySQL-family target as CHAR(36), so
// a deploy-ddl-bootstrapped table that reads back as Char(36) must
// compare EQUAL against the intended ir.UUID — the skip is what lets a
// pre-created schema feed a fresh migrate without the item-71c
// refusal.
func TestMigrateShapeGate_CrossEngineRetargetEqual(t *testing.T) {
	intended := &ir.Schema{Tables: []*ir.Table{gateTable(
		"items",
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
		&ir.Column{Name: "guid", Type: ir.UUID{}, Nullable: true},
	)}}
	bootstrapped := gateTable(
		"items",
		&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
		&ir.Column{Name: "guid", Type: ir.Char{Length: 36, Charset: "utf8mb4"}, Nullable: true},
	)
	m, _ := gateMigrator("postgres", "planetscale", &ir.Schema{Tables: []*ir.Table{bootstrapped}})
	got, err := m.phasePlanExistingTables(context.Background(), intended)
	if err != nil {
		t.Fatalf("gate refused a retarget-equal table: %v", err)
	}
	if len(got.Tables) != 0 {
		t.Errorf("retarget-equal table not skipped; create set = %d tables", len(got.Tables))
	}
}

// TestMigrateShapeGate_PartialSkipKeepsSchemaObjects pins the pruning
// shape: only the equal table leaves the CREATE set; the differing-
// nothing sibling stays, and schema-level objects (sequences, views)
// are carried through untouched on the shallow clone.
func TestMigrateShapeGate_PartialSkipKeepsSchemaObjects(t *testing.T) {
	intended := &ir.Schema{
		Tables: []*ir.Table{
			gateTable("existing", gateCols()...),
			gateTable("fresh", gateCols()...),
		},
		Sequences: []*ir.Sequence{{Name: "seq1"}},
		Views:     []*ir.View{{Name: "v1", Definition: "SELECT 1"}},
	}
	m, _ := gateMigrator("postgres", "postgres", &ir.Schema{Tables: []*ir.Table{gateTable("existing", gateCols()...)}})
	got, err := m.phasePlanExistingTables(context.Background(), intended)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}
	if len(got.Tables) != 1 || got.Tables[0].Name != "fresh" {
		t.Fatalf("create set = %+v; want just \"fresh\"", got.Tables)
	}
	if len(got.Sequences) != 1 || len(got.Views) != 1 {
		t.Errorf("schema-level objects dropped by the clone: sequences=%d views=%d", len(got.Sequences), len(got.Views))
	}
	if len(intended.Tables) != 2 {
		t.Errorf("input schema mutated: %d tables", len(intended.Tables))
	}
}

// TestMigrate_ShapeGateEndToEnd drives the full Run: the target
// catalog carries an equal-shaped "existing" table, so the CREATE
// phase must receive ONLY "fresh" while the copy phase still covers
// both tables (a pre-created table still gets its data).
func TestMigrate_ShapeGateEndToEnd(t *testing.T) {
	src := newRecordingEngine("mysql")
	src.schema = &ir.Schema{Tables: []*ir.Table{
		gateTable("existing", gateCols()...),
		gateTable("fresh", gateCols()...),
	}}
	tgt := newRecordingEngine("mysql")
	tgt.schema = &ir.Schema{Tables: []*ir.Table{gateTable("existing", gateCols()...)}}

	m := &Migrator{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt"}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(tgt.createdTables) != 1 || tgt.createdTables[0] != "fresh" {
		t.Errorf("CreateTablesWithoutConstraints received %v; want just [fresh]", tgt.createdTables)
	}
	copied := map[string]bool{}
	for _, entry := range tgt.phaseLog {
		if name, ok := strings.CutPrefix(entry, "WriteRows:"); ok {
			copied[name] = true
		}
	}
	if !copied["existing"] || !copied["fresh"] {
		t.Errorf("copy phase must cover BOTH tables; phaseLog = %v", tgt.phaseLog)
	}
}

// TestMigrate_ShapeGateDifferRefusesBeforeCopy drives the full Run
// against a conflicting pre-existing table: the run refuses coded at
// the gate, and NOTHING was created or copied — the upfront refusal
// that replaces the v0.99.256-observed mid-copy Error-1054 retry wall.
func TestMigrate_ShapeGateDifferRefusesBeforeCopy(t *testing.T) {
	src := newRecordingEngine("mysql")
	src.schema = &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}
	tgt := newRecordingEngine("mysql")
	tgt.schema = &ir.Schema{Tables: []*ir.Table{gateTable(
		"items",
		&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
	)}}

	m := &Migrator{Source: src, Target: tgt, SourceDSN: "src", TargetDSN: "tgt"}
	err := m.Run(context.Background())
	if err == nil {
		t.Fatal("want the coded shape-mismatch refusal; got nil")
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

// TestMigrateShapeGate_SkippedOnResume pins the resume carve-out: the
// gate must not run under --resume (the prior attempt's own tables
// re-create idempotently; re-comparing would add a round-trip-fidelity
// failure mode to a path that has none). Pinned behaviorally: a resume
// run whose target catalog carries a DIFFERING same-name table must
// complete rather than hit the shape refusal.
func TestMigrateShapeGate_SkippedOnResume(t *testing.T) {
	src := newRecordingEngine("mysql")
	src.schema = &ir.Schema{Tables: []*ir.Table{gateTable("items", gateCols()...)}}
	tgt := newRecordingEngineWithStore("mysql")
	tgt.schema = &ir.Schema{Tables: []*ir.Table{gateTable(
		"items",
		&ir.Column{Name: "only_col", Type: ir.Varchar{Length: 10}, Nullable: true},
	)}}
	tgt.store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy}

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		Resume: true, MigrationID: "m1",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("resume run must skip the shape gate; got %v", err)
	}
	if len(tgt.createdTables) != 1 || tgt.createdTables[0] != "items" {
		t.Errorf("resume must re-create idempotently (full set); got %v", tgt.createdTables)
	}
}
