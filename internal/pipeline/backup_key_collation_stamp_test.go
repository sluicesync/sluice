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
