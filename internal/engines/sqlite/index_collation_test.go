// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// caseVariantOutcome is what the driver DID when the case-variant of an
// already-present key value was inserted: refused (a NOCASE key) or
// accepted (a BINARY key). Measured, never reasoned (GC-5).
type caseVariantOutcome string

const (
	outcomeRefusedVariant  caseVariantOutcome = "refused case-variant"
	outcomeAcceptedVariant caseVariantOutcome = "accepted case-variant"
)

// measureCaseVariant inserts 'a@x' then 'A@X' into table.col (extra columns
// via fill) and classifies the second insert.
func measureCaseVariant(t *testing.T, db *sql.DB, insertTmpl string) caseVariantOutcome {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, strings.ReplaceAll(insertTmpl, "?", "'a@x'")); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := db.ExecContext(ctx, strings.ReplaceAll(insertTmpl, "?", "'A@X'"))
	switch {
	case err == nil:
		return outcomeAcceptedVariant
	case strings.Contains(err.Error(), "UNIQUE"):
		return outcomeRefusedVariant
	default:
		t.Fatalf("second insert: unexpected error %v", err)
		return ""
	}
}

// indexCollationCell is one shape of the GC-5 matrix: the DDL, the key the
// case-variant probe targets, the index the reader must expose it on, and
// the per-column attributes it must carry.
type indexCollationCell struct {
	name      string
	ddl       []string
	insert    string // template with ? for the probed value
	index     string // "" = the PRIMARY KEY
	wantCols  []ir.IndexColumn
	uniqueKey bool
}

var indexCollationMatrix = []indexCollationCell{
	{
		name:      "inline UNIQUE on a NOCASE column",
		ddl:       []string{`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT COLLATE NOCASE UNIQUE, v TEXT)`},
		insert:    `INSERT INTO t (email, v) VALUES (?, 'x')`,
		index:     "t_email_key",
		wantCols:  []ir.IndexColumn{{Column: "email", Collation: "NOCASE", CollationDialect: sqliteDialect}},
		uniqueKey: true,
	},
	{
		name:      "table-level composite UNIQUE with an indexed-column COLLATE",
		ddl:       []string{`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT, v TEXT, UNIQUE (email COLLATE NOCASE, v))`},
		insert:    `INSERT INTO t (email, v) VALUES (?, 'same')`,
		index:     "t_email_v_key",
		wantCols:  []ir.IndexColumn{{Column: "email", Collation: "NOCASE", CollationDialect: sqliteDialect}, {Column: "v"}},
		uniqueKey: true,
	},
	{
		name:      "TEXT PRIMARY KEY on a NOCASE column (rowid table)",
		ddl:       []string{`CREATE TABLE t (email TEXT COLLATE NOCASE PRIMARY KEY, v TEXT)`},
		insert:    `INSERT INTO t (email, v) VALUES (?, 'x')`,
		wantCols:  []ir.IndexColumn{{Column: "email", Collation: "NOCASE", CollationDialect: sqliteDialect}},
		uniqueKey: true,
	},
	{
		name:      "TEXT PRIMARY KEY on a NOCASE column (WITHOUT ROWID)",
		ddl:       []string{`CREATE TABLE t (email TEXT COLLATE NOCASE PRIMARY KEY, v TEXT) WITHOUT ROWID`},
		insert:    `INSERT INTO t (email, v) VALUES (?, 'x')`,
		wantCols:  []ir.IndexColumn{{Column: "email", Collation: "NOCASE", CollationDialect: sqliteDialect}},
		uniqueKey: true,
	},
	{
		name: "non-unique CREATE INDEX with DESC and a per-entry COLLATE",
		ddl: []string{
			`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT COLLATE RTRIM, v TEXT)`,
			`CREATE INDEX t_email_v_idx ON t (email DESC, v COLLATE NOCASE)`,
		},
		insert: `INSERT INTO t (email, v) VALUES (?, 'x')`,
		index:  "t_email_v_idx",
		wantCols: []ir.IndexColumn{
			{Column: "email", Desc: true, Collation: "RTRIM", CollationDialect: sqliteDialect},
			{Column: "v", Collation: "NOCASE", CollationDialect: sqliteDialect},
		},
	},
	{
		// The controls: BINARY — implicit or explicit — is the default and
		// must NOT be carried, so a schema with no COLLATE keeps its bytes.
		name:      "inline UNIQUE on a BINARY (default) column carries no collation",
		ddl:       []string{`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, v TEXT)`},
		insert:    `INSERT INTO t (email, v) VALUES (?, 'x')`,
		index:     "t_email_key",
		wantCols:  []ir.IndexColumn{{Column: "email"}},
		uniqueKey: true,
	},
	{
		name:      "inline UNIQUE with an explicit COLLATE BINARY carries no collation",
		ddl:       []string{`CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT COLLATE BINARY UNIQUE, v TEXT)`},
		insert:    `INSERT INTO t (email, v) VALUES (?, 'x')`,
		index:     "t_email_key",
		wantCols:  []ir.IndexColumn{{Column: "email"}},
		uniqueKey: true,
	},
}

// TestIndexCollation_ReaderAndWriterMatchTheDriver is the GC-5 class pin.
// For every cell it (1) MEASURES on the source whether a case-variant of a
// present key value is refused, (2) asserts the reader carries exactly the
// per-column collation/DESC the catalog reports, and (3) writes the read IR
// into a fresh file through the SQLite writer and asserts the TARGET measures
// the same outcome — a NOCASE unique refuses 'A@X' after 'a@x' on both sides,
// a BINARY one accepts it on both. Before the fix the reader carried no
// collation at all, so every target admitted the variant.
func TestIndexCollation_ReaderAndWriterMatchTheDriver(t *testing.T) {
	eng := Engine{}
	ctx := context.Background()
	var refused, accepted int
	for _, cell := range indexCollationMatrix {
		t.Run(cell.name, func(t *testing.T) {
			// (1) the source's own answer.
			probe, err := sql.Open("sqlite", seedDB(t, cell.ddl...))
			if err != nil {
				t.Fatal(err)
			}
			sourceOutcome := measureCaseVariant(t, probe, cell.insert)
			_ = probe.Close()
			if cell.uniqueKey && sourceOutcome == outcomeRefusedVariant {
				refused++
			} else {
				accepted++
			}

			// (2) the reader's carry.
			sr, err := eng.OpenSchemaReader(ctx, seedDB(t, cell.ddl...))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sr.(*SchemaReader).Close() }()
			schema, err := sr.ReadSchema(ctx)
			if err != nil {
				t.Fatalf("ReadSchema: %v", err)
			}
			tbl := tableByName(schema, "t")
			var got []ir.IndexColumn
			if cell.index == "" {
				got = tbl.PrimaryKey.Columns
			} else if idx := indexByName(tbl, cell.index); idx != nil {
				got = idx.Columns
			} else {
				t.Fatalf("index %q missing; have %+v", cell.index, tbl.Indexes)
			}
			if len(got) != len(cell.wantCols) {
				t.Fatalf("columns = %+v; want %+v", got, cell.wantCols)
			}
			for i := range got {
				if got[i] != cell.wantCols[i] {
					t.Errorf("column %d = %+v; want %+v", i, got[i], cell.wantCols[i])
				}
			}

			// (3) the writer's target behaves as the source did.
			dst := filepath.Join(t.TempDir(), "out.db")
			if err := eng.PreflightIndexes(schema); err != nil {
				t.Fatalf("SQLite target refused its own reader's output: %v", err)
			}
			sw, err := eng.OpenSchemaWriter(ctx, dst)
			if err != nil {
				t.Fatal(err)
			}
			if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
				t.Fatalf("CreateTablesWithoutConstraints: %v", err)
			}
			if err := sw.CreateIndexes(ctx, schema); err != nil {
				t.Fatalf("CreateIndexes: %v", err)
			}
			_ = sw.(*SchemaWriter).Close()
			db, err := sql.Open("sqlite", dst)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			if targetOutcome := measureCaseVariant(t, db, cell.insert); targetOutcome != sourceOutcome {
				t.Errorf("target measured %q; source measured %q — the writer changed which values the key treats as equal",
					targetOutcome, sourceOutcome)
			}
		})
	}
	if refused == 0 || accepted == 0 {
		t.Fatalf("matrix measured refused=%d accepted=%d; both classes must be present", refused, accepted)
	}
}

// TestIndexCollation_ForeignDialectRefusedOrWarned pins the SQLite target's
// side of the two-way policy for a collation it cannot enforce (one tagged
// with another engine's dialect): refused on a uniqueness-enforcing key at
// BOTH doors — the preflight and the emitter — with the same text, and
// WARN-dropped under the INDEX-COLLATION-DROPPED marker on a non-unique one.
func TestIndexCollation_ForeignDialectRefusedOrWarned(t *testing.T) {
	foreign := ir.IndexColumn{Column: "email", Collation: "und-x-icu", CollationDialect: "postgres"}
	mk := func(pk bool, idx *ir.Index) *ir.Table {
		tbl := &ir.Table{
			Name:    "t",
			Columns: []*ir.Column{{Name: "email", Type: ir.Text{Size: ir.TextLong}}},
		}
		if pk {
			tbl.PrimaryKey = &ir.Index{Unique: true, Columns: []ir.IndexColumn{foreign}}
		}
		if idx != nil {
			tbl.Indexes = []*ir.Index{idx}
		}
		return tbl
	}
	refusing := []struct {
		name string
		tbl  *ir.Table
		emit func(*ir.Table) error
	}{
		{"PRIMARY KEY", mk(true, nil), func(tb *ir.Table) error { _, err := emitTableDef(tb); return err }},
		{
			"UNIQUE index", mk(false, &ir.Index{Name: "t_email_key", Unique: true, Columns: []ir.IndexColumn{foreign}}),
			func(tb *ir.Table) error { _, err := emitCreateIndex(tb.Name, tb.Indexes[0]); return err },
		},
	}
	for _, c := range refusing {
		t.Run(c.name+" refused at both doors", func(t *testing.T) {
			pre := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{c.tbl}})
			emit := c.emit(c.tbl)
			for door, err := range map[string]error{"preflight": pre, "emitter": emit} {
				if err == nil || !strings.Contains(err.Error(), "und-x-icu") || !strings.Contains(err.Error(), "uniqueness") {
					t.Errorf("%s: err = %v; want a refusal naming the collation and the uniqueness argument", door, err)
				}
			}
		})
	}
	t.Run("non-unique index WARNs with the marker and is not refused", func(t *testing.T) {
		rec := captureWarns(t)
		tbl := mk(false, &ir.Index{Name: "t_email_idx", Columns: []ir.IndexColumn{foreign}})
		if err := (Engine{}).PreflightIndexes(&ir.Schema{Tables: []*ir.Table{tbl}}); err != nil {
			t.Fatalf("preflight refused a non-unique index: %v", err)
		}
		stmt, err := emitCreateIndex(tbl.Name, tbl.Indexes[0])
		if err != nil {
			t.Fatalf("emitter refused a non-unique index: %v", err)
		}
		if strings.Contains(stmt, "COLLATE") {
			t.Errorf("a foreign collation was interpolated: %s", stmt)
		}
		if !strings.Contains(strings.Join(rec.msgs, "\n"), "INDEX-COLLATION-DROPPED") {
			t.Errorf("no INDEX-COLLATION-DROPPED WARN; msgs=%v", rec.msgs)
		}
	})
	t.Run("a collation name that is not a bare identifier is refused, not interpolated", func(t *testing.T) {
		bad := ir.IndexColumn{Column: "email", Collation: `NOCASE"; DROP TABLE t; --`, CollationDialect: sqliteDialect}
		_, err := emitCreateIndex("t", &ir.Index{Name: "t_email_idx", Columns: []ir.IndexColumn{bad}})
		if err == nil || !strings.Contains(err.Error(), "bare identifier") {
			t.Errorf("err = %v; want the bare-identifier refusal", err)
		}
	})
}
