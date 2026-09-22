// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// nullInsertOutcome is what the driver DID when a NULL was inserted into a
// table's primary-key column — the measured fact every cell below is graded
// against, so the expectation is observed on the real engine rather than
// derived from the documentation the fix cites.
type nullInsertOutcome string

const (
	// outcomeAssigned: SQLite auto-assigned a value — the rowid alias.
	outcomeAssigned nullInsertOutcome = "auto-assigned"
	// outcomeStoredNULL: the row landed with a NULL key (SQLite's legacy
	// tolerance of NULL in a non-alias PRIMARY KEY of a rowid table).
	outcomeStoredNULL nullInsertOutcome = "stored NULL"
	// outcomeNotNullRefused: the insert failed NOT NULL (a WITHOUT ROWID
	// table's PK, or an explicit NOT NULL).
	outcomeNotNullRefused nullInsertOutcome = "NOT NULL refused"
)

// measureNullInsert creates a fresh file, runs ddl, inserts a NULL key and
// reports what happened.
func measureNullInsert(t *testing.T, ddl string) nullInsertOutcome {
	t.Helper()
	path := filepath.Join(t.TempDir(), "m.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		t.Fatalf("ddl %q: %v", ddl, err)
	}
	return measureNullInsertOn(t, db, "t")
}

// measureNullInsertOn inserts a NULL key into an existing table `name`
// carrying columns (id, v) and classifies the outcome.
func measureNullInsertOn(t *testing.T, db *sql.DB, name string) nullInsertOutcome {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `INSERT INTO `+quoteIdent(name)+` (id, v) VALUES (NULL, 'x')`)
	switch {
	case err == nil:
	case strings.Contains(err.Error(), "NOT NULL"):
		return outcomeNotNullRefused
	default:
		t.Fatalf("insert NULL into %s: unexpected error %v", name, err)
	}
	var id sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT id FROM `+quoteIdent(name)+` WHERE v = 'x'`).Scan(&id); err != nil {
		t.Fatalf("read back %s: %v", name, err)
	}
	if id.Valid {
		return outcomeAssigned
	}
	return outcomeStoredNULL
}

// rowidAliasCell is one (spelling × constraint form × rowid/WITHOUT ROWID)
// cell of the matrix.
type rowidAliasCell struct {
	name string
	ddl  string
}

// rowidAliasMatrix enumerates the class the fix reaches: every integer
// spelling SQLite's docs say does or does not alias × the three ways a
// single-column PK can be declared × rowid vs WITHOUT ROWID, plus the
// AUTOINCREMENT keyword (which is only legal on the one aliasing spelling).
// The matrix carries NO expected value — each cell is measured.
func rowidAliasMatrix() []rowidAliasCell {
	spellings := []string{"INTEGER", "integer", "INT", "BIGINT", "INTEGER(10)", "MEDIUMINT"}
	forms := []struct{ name, tmpl string }{
		{"column-constraint PK", "CREATE TABLE t(id %s PRIMARY KEY, v TEXT)%s"},
		{"column-constraint PK DESC", "CREATE TABLE t(id %s PRIMARY KEY DESC, v TEXT)%s"},
		{"table-constraint PK", "CREATE TABLE t(id %s, v TEXT, PRIMARY KEY(id))%s"},
	}
	storages := []struct{ name, suffix string }{
		{"rowid", ""},
		{"WITHOUT ROWID", " WITHOUT ROWID"},
	}
	cells := make([]rowidAliasCell, 0, len(spellings)*len(forms)*len(storages)+1)
	for _, sp := range spellings {
		for _, f := range forms {
			for _, st := range storages {
				cells = append(cells, rowidAliasCell{
					name: fmt.Sprintf("%s/%s/%s", sp, f.name, st.name),
					ddl:  fmt.Sprintf(f.tmpl, sp, st.suffix),
				})
			}
		}
	}
	cells = append(cells, rowidAliasCell{
		name: "INTEGER/AUTOINCREMENT keyword/rowid",
		ddl:  "CREATE TABLE t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
	})
	return cells
}

// TestRowidAlias_ReaderAndWriterMatchTheDriver is the GC-4 class pin. For
// every cell it (1) MEASURES whether the source auto-assigns by inserting a
// NULL key, (2) asserts the reader's Integer.AutoIncrement equals that
// measurement, and (3) emits the read IR through the SQLite writer into a
// fresh file and asserts the TARGET's measured behaviour equals the source's
// — auto-assigns iff the source did, stores NULL iff the source did, refuses
// NOT NULL iff the source did. (3) is the writer sibling of the same class:
// before the fix the writer spelled every integer PK `INTEGER PRIMARY KEY`,
// so a `BIGINT PRIMARY KEY` source landed on a target that invents keys.
//
// Both halves rest on the catalog fact the reader now uses — SQLite
// materialises an origin='pk' index exactly when the key is NOT the rowid
// alias — and this matrix is that fact's check: if the invariant broke for a
// cell, the reader's verdict would diverge from the measured outcome here.
func TestRowidAlias_ReaderAndWriterMatchTheDriver(t *testing.T) {
	eng := Engine{}
	ctx := context.Background()
	var assigned, notAssigned int
	for _, cell := range rowidAliasMatrix() {
		t.Run(cell.name, func(t *testing.T) {
			sourceOutcome := measureNullInsert(t, cell.ddl)
			if sourceOutcome == outcomeAssigned {
				assigned++
			} else {
				notAssigned++
			}

			// (2) the reader's verdict equals the measurement.
			src := seedDB(t, cell.ddl)
			sr, err := eng.OpenSchemaReader(ctx, src)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer func() { _ = sr.(*SchemaReader).Close() }()
			schema, err := sr.ReadSchema(ctx)
			if err != nil {
				t.Fatalf("ReadSchema: %v", err)
			}
			tbl := tableByName(schema, "t")
			if tbl == nil {
				t.Fatal("table t missing")
			}
			id := columnByName(tbl, "id")
			if id == nil {
				t.Fatal("column id missing")
			}
			iv, ok := id.Type.(ir.Integer)
			if !ok {
				t.Fatalf("id type = %#v; want ir.Integer", id.Type)
			}
			if want := sourceOutcome == outcomeAssigned; iv.AutoIncrement != want {
				t.Errorf("%s: reader AutoIncrement = %v; driver measured %q on NULL insert",
					cell.ddl, iv.AutoIncrement, sourceOutcome)
			}

			// (3) the writer's target behaves as the source did.
			dst := filepath.Join(t.TempDir(), "out.db")
			sw, err := eng.OpenSchemaWriter(ctx, dst)
			if err != nil {
				t.Fatalf("OpenSchemaWriter: %v", err)
			}
			if err := sw.CreateTablesWithoutConstraints(ctx, &ir.Schema{Tables: []*ir.Table{tbl}}); err != nil {
				t.Fatalf("CreateTablesWithoutConstraints: %v", err)
			}
			_ = sw.(*SchemaWriter).Close()
			db, err := sql.Open("sqlite", dst)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() { _ = db.Close() }()
			if targetOutcome := measureNullInsertOn(t, db, "t"); targetOutcome != sourceOutcome {
				t.Errorf("%s: target measured %q on NULL insert; source measured %q — the writer "+
					"changed whether the key auto-assigns", cell.ddl, targetOutcome, sourceOutcome)
			}
		})
	}
	// Anti-vacuity: the matrix must have exercised BOTH verdicts, or a reader
	// that always answered one way would pass.
	if assigned == 0 || notAssigned == 0 {
		t.Fatalf("matrix measured assigned=%d not-assigned=%d; both classes must be present", assigned, notAssigned)
	}
}

// TestEmitTableDef_NonAliasIntegerPKSpelling pins the writer wart by shape:
// a sole integer PK WITHOUT AutoIncrement is spelled [nonAliasIntegerPKType]
// with a table-level PRIMARY KEY clause, and one WITH it stays the inline
// `INTEGER PRIMARY KEY` rowid alias.
func TestEmitTableDef_NonAliasIntegerPKSpelling(t *testing.T) {
	mk := func(auto bool) *ir.Table {
		return &ir.Table{
			Name: "t",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: auto}, Nullable: false},
				{Name: "v", Type: ir.Text{}, Nullable: true},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
		}
	}
	alias, err := emitTableDef(mk(true))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(alias, `"id" INTEGER PRIMARY KEY`) || strings.Contains(alias, `PRIMARY KEY ("id")`) {
		t.Errorf("auto-assigning PK must be the inline rowid alias:\n%s", alias)
	}
	plain, err := emitTableDef(mk(false))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plain, `"id" `+nonAliasIntegerPKType+` NOT NULL`) || !strings.Contains(plain, `PRIMARY KEY ("id")`) {
		t.Errorf("non-auto-assigning PK must be spelled %s with a table-level PRIMARY KEY:\n%s", nonAliasIntegerPKType, plain)
	}
	if strings.Contains(plain, "INTEGER PRIMARY KEY") {
		t.Errorf("non-auto-assigning PK must not become the rowid alias:\n%s", plain)
	}
}
