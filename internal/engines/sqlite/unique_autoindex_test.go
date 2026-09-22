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

// TestSQLiteUniqueAutoIndex_ReservedNameIsRefusedByTheDriver is the premise
// GC-22 rests on, driven rather than asserted: SQLite refuses to create an
// index whose name begins `sqlite_`, and IF NOT EXISTS does not suppress it.
// It also pins the shape of the catalog row the reader keys on — a UNIQUE
// constraint's auto-index is origin 'u' under exactly that reserved name.
func TestSQLiteUniqueAutoIndex_ReservedNameIsRefusedByTheDriver(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	name, origin := soleIndexListRow(t, db, "t")
	if origin != "u" || !strings.HasPrefix(name, reservedObjectNamePrefix) {
		t.Fatalf("index_list row = (%q, origin %q); want a `sqlite_`-prefixed origin 'u' auto-index", name, origin)
	}
	_, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS "sqlite_autoindex_other_1" ON t (email)`)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("driver accepted a sqlite_-prefixed index name (err=%v); the reader's carry and the "+
			"preflight door both rest on it being refused", err)
	}
}

// soleIndexListRow returns the (name, origin) of the one PRAGMA index_list row
// for table, fully consumed and closed before the caller issues more DDL.
func soleIndexListRow(t *testing.T, db *sql.DB, table string) (name, origin string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "PRAGMA index_list("+quotePragmaArg(table)+")")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var seq, unique, partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return name, origin
}

// TestRefuseReservedIndexName pins the door by shape, both directions.
func TestRefuseReservedIndexName(t *testing.T) {
	for _, name := range []string{"sqlite_autoindex_t_1", "SQLITE_stat1", "sqlite_x"} {
		if err := refuseReservedIndexName(name, "ctx"); err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("refuseReservedIndexName(%q) = %v; want a refusal naming the reserved prefix", name, err)
		}
	}
	for _, name := range []string{"t_email_key", "sqlite", "mysqlite_idx", "idx_sqlite_users"} {
		if err := refuseReservedIndexName(name, "ctx"); err != nil {
			t.Errorf("refuseReservedIndexName(%q) = %v; want nil (only the sqlite_ PREFIX is reserved)", name, err)
		}
	}
}

// TestSchemaReader_UniqueConstraintAutoIndex is the GC-22 reader pin on the
// real driver: an inline `UNIQUE`, a table-level `UNIQUE (a, b)`, and the
// same on a WITHOUT ROWID table are each carried as a constraint-backed
// unique index under the generated `<table>_<cols>_key` name — never under
// the reserved auto-index name — while a named CREATE UNIQUE INDEX (the
// control) is carried verbatim and NOT constraint-backed. The read schema
// then passes the SQLite target's preflight and is written to a fresh file
// whose uniqueness is ENFORCED (a duplicate is refused), which is the
// in-process form of the SQLite→SQLite migrate that used to fail after the
// copy.
func TestSchemaReader_UniqueConstraintAutoIndex(t *testing.T) {
	src := seedDB(
		t,
		`CREATE TABLE t (
			id    INTEGER PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			a     TEXT,
			b     TEXT,
			UNIQUE (a, b)
		)`,
		`CREATE UNIQUE INDEX t_named_uq ON t (b)`,
		`CREATE TABLE w (
			id    INTEGER PRIMARY KEY,
			code  TEXT NOT NULL UNIQUE
		) WITHOUT ROWID`,
		`INSERT INTO t (id, email, a, b) VALUES (1, 'x@example.com', 'a1', 'b1')`,
		`INSERT INTO w (id, code) VALUES (1, 'c1')`,
	)
	eng := Engine{}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, src)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}

	type want struct {
		cols    []string
		backed  bool
		present bool
	}
	wants := map[string]map[string]want{
		"t": {
			"t_email_key": {cols: []string{"email"}, backed: true},
			"t_a_b_key":   {cols: []string{"a", "b"}, backed: true},
			"t_named_uq":  {cols: []string{"b"}, backed: false},
		},
		"w": {
			"w_code_key": {cols: []string{"code"}, backed: true},
		},
	}
	for tableName, byName := range wants {
		tbl := tableByName(schema, tableName)
		if tbl == nil {
			t.Fatalf("table %s missing", tableName)
		}
		if len(tbl.Indexes) != len(byName) {
			t.Errorf("%s indexes = %d; want %d: %+v", tableName, len(tbl.Indexes), len(byName), tbl.Indexes)
		}
		for _, idx := range tbl.Indexes {
			if strings.HasPrefix(strings.ToLower(idx.Name), reservedObjectNamePrefix) {
				t.Errorf("%s carried the reserved auto-index name %q into the IR", tableName, idx.Name)
			}
			w, ok := byName[idx.Name]
			if !ok {
				t.Errorf("%s: unexpected index %q", tableName, idx.Name)
				continue
			}
			w.present = true
			byName[idx.Name] = w
			if !idx.Unique {
				t.Errorf("%s.%s: Unique = false; a UNIQUE constraint must never be relaxed", tableName, idx.Name)
			}
			if idx.ConstraintBacked != w.backed {
				t.Errorf("%s.%s: ConstraintBacked = %v; want %v", tableName, idx.Name, idx.ConstraintBacked, w.backed)
			}
			if idx.ConstraintNamed {
				t.Errorf("%s.%s: ConstraintNamed must stay false for a sluice-generated name", tableName, idx.Name)
			}
			got := make([]string, len(idx.Columns))
			for i, c := range idx.Columns {
				got[i] = c.Column
			}
			if strings.Join(got, ",") != strings.Join(w.cols, ",") {
				t.Errorf("%s.%s columns = %v; want %v", tableName, idx.Name, got, w.cols)
			}
		}
		for name, w := range byName {
			if !w.present {
				t.Errorf("%s: index %q missing; have %+v", tableName, name, tbl.Indexes)
			}
		}
	}

	// The SQLite target accepts the read schema BEFORE any data moves…
	if err := eng.PreflightIndexes(schema); err != nil {
		t.Fatalf("PreflightIndexes refused the reader's own output: %v", err)
	}
	// …and the written file enforces every uniqueness the source declared.
	dst := filepath.Join(t.TempDir(), "out.db")
	sw, err := eng.OpenSchemaWriter(ctx, dst)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := sw.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes on the reader's own output: %v (this is the GC-22 failure shape)", err)
	}
	_ = sw.(*SchemaWriter).Close()

	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	seed := []string{
		`INSERT INTO t (id, email, a, b) VALUES (1, 'x@example.com', 'a1', 'b1')`,
		`INSERT INTO w (id, code) VALUES (1, 'c1')`,
	}
	for _, s := range seed {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("seed target %q: %v", s, err)
		}
	}
	dups := []string{
		`INSERT INTO t (id, email, a, b) VALUES (2, 'x@example.com', 'a2', 'b2')`, // t_email_key
		`INSERT INTO t (id, email, a, b) VALUES (3, 'y@example.com', 'a1', 'b1')`, // t_a_b_key (and t_named_uq)
		`INSERT INTO t (id, email, a, b) VALUES (4, 'z@example.com', 'a9', 'b1')`, // t_named_uq
		`INSERT INTO w (id, code) VALUES (2, 'c1')`,                               // w_code_key
	}
	for _, s := range dups {
		if _, err := db.ExecContext(ctx, s); err == nil || !strings.Contains(err.Error(), "UNIQUE") {
			t.Errorf("target accepted a duplicate the source rejects: %q (err=%v)", s, err)
		}
	}
	// And a re-read of the target produces the same IR as the source did —
	// the generated name is stable across a round trip.
	sr2, err := eng.OpenSchemaReader(ctx, dst)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sr2.(*SchemaReader).Close() }()
	back, err := sr2.ReadSchema(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := indexByName(tableByName(back, "t"), "t_email_key"); got == nil || !got.Unique {
		t.Errorf("target re-read lost t_email_key: %+v", tableByName(back, "t").Indexes)
	}
}

// TestUniqueConstraintIndex_Name pins the generated name by shape.
func TestUniqueConstraintIndex_Name(t *testing.T) {
	idx := uniqueConstraintIndex("orders", []ir.IndexColumn{{Column: "tenant"}, {Column: "sku"}})
	if idx.Name != "orders_tenant_sku_key" || !idx.Unique || !idx.ConstraintBacked || idx.ConstraintNamed {
		t.Errorf("uniqueConstraintIndex = %+v; want orders_tenant_sku_key, Unique, ConstraintBacked, not ConstraintNamed", idx)
	}
}
