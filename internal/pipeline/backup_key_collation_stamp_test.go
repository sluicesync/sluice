// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestStampKeyCollationDeltaOnAttach_ShapeMatrix pins Bug 304's stamp over
// the delta shapes the writers actually record (each built by the real
// [migcore.DiffSchemas]): a collation change on a primary-key column and on a
// UNIQUE-key column, in both collation dialects (a MySQL charset-paired name
// and a bare Postgres one), and a charset-only change on a key, is stamped
// [irbackup.FormatVersionKeyCollationDelta]; so is a key collation change that
// rides beside another aspect. The controls keep their version: the same
// change on a non-key column (which no reader treats as a delta), a key
// column's type change with its collation untouched, a window with no change,
// and a FULL manifest carrying a key delta. The independent expected value is
// the rule stated here, graded per cell.
func TestStampKeyCollationDeltaOnAttach_ShapeMatrix(t *testing.T) {
	my := func(coll string) ir.Type { return ir.Varchar{Length: 80, Charset: "utf8mb4", Collation: coll} }
	pg := func(coll string) ir.Type { return ir.Text{Collation: coll} }
	table := func(k, other ir.Type, keyed string) *ir.Table {
		t := &ir.Table{Name: "kd", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: k},
			{Name: "v", Type: other},
		}}
		switch keyed {
		case "pk":
			t.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}
		case "unique":
			t.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
			t.Indexes = []*ir.Index{{Name: "uk", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}}
		}
		return t
	}
	cases := []struct {
		name          string
		kind          string
		before, after *ir.Table
		stamped       bool
	}{
		{
			"mysql/pk/ci-to-bin", irbackup.BackupKindIncremental,
			table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_0900_ai_ci"), "pk"), table(my("utf8mb4_bin"), my("utf8mb4_0900_ai_ci"), "pk"), true,
		},
		{
			"mysql/unique/ci-to-bin", irbackup.BackupKindIncremental,
			table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_0900_ai_ci"), "unique"), table(my("utf8mb4_bin"), my("utf8mb4_0900_ai_ci"), "unique"), true,
		},
		{
			"mysql/pk/charset-only", irbackup.BackupKindIncremental,
			table(ir.Varchar{Length: 80, Charset: "latin1", Collation: "latin1_swedish_ci"}, my("utf8mb4_bin"), "pk"),
			table(ir.Varchar{Length: 80, Charset: "utf8mb4", Collation: "latin1_swedish_ci"}, my("utf8mb4_bin"), "pk"), true,
		},
		{
			"postgres/pk/nondeterministic-to-C", irbackup.BackupKindIncremental,
			table(pg("ci"), pg(""), "pk"), table(pg("C"), pg(""), "pk"), true,
		},
		{
			"postgres/unique/nondeterministic-to-C", irbackup.BackupKindIncremental,
			table(pg("ci"), pg(""), "unique"), table(pg("C"), pg(""), "unique"), true,
		},
		{
			"mysql/pk/collation-and-width", irbackup.BackupKindIncremental,
			table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_bin"), "pk"),
			table(ir.Varchar{Length: 120, Charset: "utf8mb4", Collation: "utf8mb4_bin"}, my("utf8mb4_bin"), "pk"), true,
		},

		{
			"control/non-key-collation", irbackup.BackupKindIncremental,
			table(my("utf8mb4_bin"), my("utf8mb4_0900_ai_ci"), "pk"), table(my("utf8mb4_bin"), my("utf8mb4_bin"), "pk"), false,
		},
		{
			"control/key-width-only", irbackup.BackupKindIncremental,
			table(my("utf8mb4_bin"), my("utf8mb4_bin"), "pk"),
			table(ir.Varchar{Length: 120, Charset: "utf8mb4", Collation: "utf8mb4_bin"}, my("utf8mb4_bin"), "pk"), false,
		},
		{
			"control/no-change", irbackup.BackupKindIncremental,
			table(my("utf8mb4_bin"), my("utf8mb4_bin"), "pk"), table(my("utf8mb4_bin"), my("utf8mb4_bin"), "pk"), false,
		},
		{
			"control/full-manifest", irbackup.BackupKindFull,
			table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_bin"), "pk"), table(my("utf8mb4_bin"), my("utf8mb4_bin"), "pk"), false,
		},
	}
	stamped := 0
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			const start = irbackup.FormatVersionInjectiveChunkAAD
			m := &irbackup.Manifest{Kind: c.kind, FormatVersion: start}
			m.SchemaDelta = migcore.DiffSchemas(&ir.Schema{Tables: []*ir.Table{c.before}}, &ir.Schema{Tables: []*ir.Table{c.after}})
			stampKeyCollationDeltaOnAttach(m)
			want := start
			if c.stamped {
				want = irbackup.FormatVersionKeyCollationDelta
				stamped++
			}
			if m.FormatVersion != want {
				t.Errorf("FormatVersion = %d; want %d (schema_delta entries: %d)", m.FormatVersion, want, len(m.SchemaDelta))
			}
		})
	}
	if stamped == 0 {
		t.Fatal("no cell expected a stamp — the matrix is vacuous")
	}
}

// TestSchemaDeltaWritersStampKeyCollation is the roster for Bug 304's stamp:
// every function under internal/pipeline (this package and its
// sub-packages, non-test files) that WRITES a manifest's SchemaDelta — an
// assignment or a composite-literal field — must also call the stamp
// (stampKeyCollationDeltaOnAttach, or irbackup.StampKeyCollationDelta from a
// sub-package), so a third delta writer cannot record a key collation change
// an older binary reads as no change. The universe is derived from the AST,
// not listed; the floor (two writers today: `backup incremental` and the
// `backup stream` rollover) keeps a walk that matches nothing from passing.
//
// WHAT IT DOES NOT REACH (review 2026-10-12 finding 4): a path that commits
// a segment WITHOUT writing a SchemaDelta is invisible to it, because the
// roster keys on the write. The `backup stream` ctx-cancel drain-commit is
// exactly that path — it skips the refresh, writes no delta, and its window
// can still hold a key-collation DDL — and this gate stayed green while it
// committed such a window unstamped (measured). That path is pinned
// separately: stampKeyCollationHistory, by
// TestStampKeyCollationHistory (unit), TestCDCSegmentCommitsStampFromHistory (the
// roster below) and
// TestBackupStream_DrainCommit_StampsAKeyCollationWindow (integration).
func TestSchemaDeltaWritersStampKeyCollation(t *testing.T) {
	fset := token.NewFileSet()
	writers := 0
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			writes, stamps := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.AssignStmt:
					for _, lhs := range v.Lhs {
						if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "SchemaDelta" {
							writes = true
						}
					}
				case *ast.KeyValueExpr:
					if id, ok := v.Key.(*ast.Ident); ok && id.Name == "SchemaDelta" {
						writes = true
					}
				case *ast.CallExpr:
					switch fun := v.Fun.(type) {
					case *ast.Ident:
						stamps = stamps || fun.Name == "stampKeyCollationDeltaOnAttach"
					case *ast.SelectorExpr:
						stamps = stamps || fun.Sel.Name == "StampKeyCollationDelta"
					}
				}
				return true
			})
			if !writes {
				continue
			}
			writers++
			if !stamps {
				t.Errorf("%s: %s writes a manifest's SchemaDelta without stamping it — a key collation change it "+
					"records restores silently merged on a pre-v0.159.1 binary (Bug 304)",
					fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if writers < 2 {
		t.Fatalf("found %d SchemaDelta writer(s); want at least 2 (attachWindowEndSchema, refreshSchemaAndAttachDelta) — the walk is blind", writers)
	}
}

// TestStampKeyCollationHistory pins the history-based stamp every CDC commit
// runs (review 2026-10-12 finding 4, and #2 finding 2) over the window's
// SchemaHistory as the change stream records it: a key column whose
// collation the stream saw change stamps the segment, including a change
// the window reverted by its end (A → B → A: two history entries, no delta);
// a character key column the stream recorded with NO collation (VStream)
// stamps conservatively when the parent's shape records one; a non-key
// column's change, an unchanged key, a column neither side records a
// collation for, and a FULL manifest do not. A Postgres database-default key
// (no name, stamped deterministic) counts as a recorded collation. The
// independent expected value is the rule stated here, graded per cell.
func TestStampKeyCollationHistory(t *testing.T) {
	my := func(coll string) ir.Type { return ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: coll} }
	table := func(k, note ir.Type) *ir.Table {
		return &ir.Table{
			Schema: "s", Name: "kd", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: k}, {Name: "note", Type: note},
			}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{{Name: "uk", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}},
		}
	}
	before := &ir.Schema{Tables: []*ir.Table{table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_0900_ai_ci"))}}
	pgBefore := &ir.Schema{Tables: []*ir.Table{table(ir.Text{Collation: "ci", Determinism: ir.CollationNonDeterministic}, ir.Text{})}}
	bare := &ir.Schema{Tables: []*ir.Table{table(ir.Varchar{Length: 20}, ir.Varchar{Length: 20})}}
	cases := []struct {
		name    string
		kind    string
		before  *ir.Schema
		history []*ir.Table
		stamped bool
	}{
		{"unique key ci -> bin", irbackup.BackupKindIncremental, before, []*ir.Table{table(my("utf8mb4_bin"), my("utf8mb4_0900_ai_ci"))}, true},
		{"unique key ci -> bin -> ci (reverted inside the window)", irbackup.BackupKindIncremental, before, []*ir.Table{
			table(my("utf8mb4_bin"), my("utf8mb4_0900_ai_ci")), table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_0900_ai_ci")),
		}, true},
		{"postgres key ci -> database default", irbackup.BackupKindIncremental, pgBefore, []*ir.Table{table(ir.Text{Determinism: ir.CollationDeterministic}, ir.Text{})}, true},
		{"no collation recorded for a character key (VStream): conservative", irbackup.BackupKindIncremental, before, []*ir.Table{table(ir.Varchar{Length: 20}, my("utf8mb4_0900_ai_ci"))}, true},
		{"control: non-key column", irbackup.BackupKindIncremental, before, []*ir.Table{table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_bin"))}, false},
		{"control: neither side records a collation", irbackup.BackupKindIncremental, bare, []*ir.Table{table(ir.Varchar{Length: 20}, ir.Varchar{Length: 20})}, false},
		{"control: unchanged", irbackup.BackupKindIncremental, before, []*ir.Table{table(my("utf8mb4_0900_ai_ci"), my("utf8mb4_0900_ai_ci"))}, false},
		{"control: full manifest", irbackup.BackupKindFull, before, []*ir.Table{table(my("utf8mb4_bin"), my("utf8mb4_0900_ai_ci"))}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			const start = irbackup.FormatVersionInjectiveChunkAAD
			m := &irbackup.Manifest{Kind: c.kind, FormatVersion: start}
			for _, h := range c.history {
				js, err := ir.MarshalTable(h)
				if err != nil {
					t.Fatal(err)
				}
				m.SchemaHistory = append(m.SchemaHistory, &irbackup.SchemaHistoryEntry{Schema: "s", Table: "kd", TableJSON: js})
			}
			// The delta the window's END shape would produce: none for the
			// reverted cell, so only the history can stamp it.
			stampKeyCollationHistory(m, c.before)
			if got := m.FormatVersion >= irbackup.FormatVersionKeyCollationDelta; got != c.stamped {
				t.Errorf("FormatVersion = %d; stamped = %v, want %v", m.FormatVersion, got, c.stamped)
			}
		})
	}
}

// TestCDCSegmentCommitsStampFromHistory is the roster for the history-based
// Bug 304 stamp (review 2026-10-12 #2, finding 2): every non-test function
// under internal/pipeline that SEALS a manifest — assigns `….BackupID =
// irbackup.ComputeBackupID(…)` — must call stampKeyCollationHistory, unless
// it is named below with a reason. It reaches what the delta-writer roster
// above cannot: a commit that writes no delta (the drain) and a window whose
// key change the END diff cannot see (A → B → A). The universe is derived
// from the AST; the floor (two CDC commit sites today: the `backup stream`
// rollover and `backup incremental`) keeps a blind walk from passing. It
// does NOT reach a commit that seals through some other spelling (a helper
// returning the id, as lineage's chain builders do for tests) — those are
// not production commit paths today.
func TestCDCSegmentCommitsStampFromHistory(t *testing.T) {
	exempt := map[string]string{
		"backup/backup.go:Run": "a FULL backup: no CDC window, no SchemaHistory, and StampKeyCollationDelta is a no-op for a full",
	}
	fset := token.NewFileSet()
	commits := 0
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			seals, stamps := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch v := n.(type) {
				case *ast.AssignStmt:
					for i, lhs := range v.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || sel.Sel.Name != "BackupID" || i >= len(v.Rhs) {
							continue
						}
						if call, ok := v.Rhs[i].(*ast.CallExpr); ok {
							if fs, ok := call.Fun.(*ast.SelectorExpr); ok && fs.Sel.Name == "ComputeBackupID" {
								seals = true
							}
						}
					}
				case *ast.CallExpr:
					if id, ok := v.Fun.(*ast.Ident); ok && id.Name == "stampKeyCollationHistory" {
						stamps = true
					}
				}
				return true
			})
			if !seals {
				continue
			}
			key := filepath.ToSlash(path) + ":" + fn.Name.Name
			if _, ok := exempt[key]; ok {
				continue
			}
			commits++
			if !stamps {
				t.Errorf("%s: %s seals a manifest without stampKeyCollationHistory — a window whose rows follow a key "+
					"collation change its end diff cannot see restores merged on a pre-v0.159.1 binary (Bug 304; "+
					"exempt it here with a reason if it seals no CDC segment)", fset.Position(fn.Pos()), fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if commits < 2 {
		t.Fatalf("found %d CDC commit site(s); want at least 2 (runRollover, IncrementalBackup.Run) — the walk is blind", commits)
	}
}
