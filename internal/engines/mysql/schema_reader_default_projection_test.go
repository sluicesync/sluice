// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// catalogColumnRow is one information_schema.columns row as the SOURCE
// SERVER reports it — the raw (COLUMN_DEFAULT, EXTRA) surface form, which
// is where MariaDB and MySQL diverge for the same declared DDL — plus the
// IR default the row must translate to on either catalog reader.
type catalogColumnRow struct {
	name       string
	def        sql.NullString
	extra      string
	nullable   string
	dataType   string
	columnType string
	charMaxLen driver.Value // int64 or nil
	numPrec    driver.Value
	numScale   driver.Value
	dtPrec     driver.Value
	want       ir.DefaultValue
}

// bug286CatalogRows returns the Bug 286 shape matrix as flavor f's server
// reports it for the SAME six declared columns:
//
//	c1 VARCHAR(20) NULL
//	c2 VARCHAR(20) DEFAULT 'abc'
//	c3 INT DEFAULT 5
//	c4 DATETIME DEFAULT CURRENT_TIMESTAMP
//	c5 VARCHAR(20) NOT NULL DEFAULT ''
//	c6 VARCHAR(20) DEFAULT 'it''s'
//
// The MariaDB forms are the ground truth [translateMariaDBDefault]'s doc
// tabulates (measured on mariadb:11.4 by the v0.155.0 regression cycle,
// sluice-testing workspace/v1550/g1b.sh + g1c.sh): the bare keyword NULL
// for a defaultless nullable column, quoted strings WITH their quotes and
// SQL-standard quote doubling, and current_timestamp() with an EMPTY
// extra. The MySQL 8
// forms are SQL NULL, the bare value, and the CURRENT_TIMESTAMP keyword
// tagged DEFAULT_GENERATED. The want column is the parity target
// TestTranslateMariaDBDefault_ParityMatrix already pins per translator;
// here it is the INDEPENDENT expected value so the two readers cannot
// agree on a wrong answer.
func bug286CatalogRows(f Flavor) []catalogColumnRow {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	mdb := f == FlavorMariaDB
	pick := func(mariadb, mysql sql.NullString) sql.NullString {
		if mdb {
			return mariadb
		}
		return mysql
	}
	ctExtra := "DEFAULT_GENERATED"
	if mdb {
		ctExtra = ""
	}
	vc := func(name string, def sql.NullString, nullable string, want ir.DefaultValue) catalogColumnRow {
		return catalogColumnRow{
			name: name, def: def, nullable: nullable,
			dataType: "varchar", columnType: "varchar(20)", charMaxLen: int64(20),
			want: want,
		}
	}
	return []catalogColumnRow{
		vc("c1", pick(valid("NULL"), sql.NullString{}), "YES", ir.DefaultNone{}),
		vc("c2", pick(valid("'abc'"), valid("abc")), "YES", ir.DefaultLiteral{Value: "abc"}),
		{
			name: "c3", def: valid("5"), nullable: "YES",
			dataType: "int", columnType: "int(11)", numPrec: int64(10), numScale: int64(0),
			want: ir.DefaultLiteral{Value: "5"},
		},
		{
			name: "c4", def: pick(valid("current_timestamp()"), valid("CURRENT_TIMESTAMP")), extra: ctExtra, nullable: "YES",
			dataType: "datetime", columnType: "datetime", dtPrec: int64(0),
			want: ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP", Dialect: "mysql"},
		},
		vc("c5", pick(valid("''"), valid("")), "NO", ir.DefaultLiteral{Value: ""}),
		vc("c6", pick(valid("'it''s'"), valid("it's")), "YES", ir.DefaultLiteral{Value: "it's"}),
	}
}

// catalogFakeDriver serves ONE table's catalog rows to both catalog readers
// through the real database/sql path: the SchemaReader's per-database
// columnsQuery (the cold-start seed), loadTableSchema's per-table query
// (the CDC boundary projection) and the PRIMARY-key statistics lookup the
// latter chains. Any other query is a loud error, so a reader that grows a
// new catalog round-trip fails this pin visibly instead of scanning an
// empty result.
type catalogFakeDriver struct {
	rows []catalogColumnRow
}

type catalogFakeConn struct{ rows []catalogColumnRow }

func (d catalogFakeDriver) Open(string) (driver.Conn, error) {
	return catalogFakeConn(d), nil
}

func (catalogFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("catalog fake: Prepare not supported")
}
func (catalogFakeConn) Close() error { return nil }
func (catalogFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("catalog fake: Begin not supported")
}

func (c catalogFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "information_schema.statistics"):
		return &catalogFakeRows{cols: []string{"column_name"}, vals: [][]driver.Value{{"id"}}}, nil
	case strings.Contains(query, "information_schema.columns") && strings.Contains(query, "ORDER  BY table_name, ordinal_position"):
		// The SchemaReader's columnsQuery: table_name + ordinal + srs_id
		// on top of the per-table shape.
		out := &catalogFakeRows{cols: make([]string, 17)}
		for i, r := range c.rows {
			out.vals = append(out.vals, append([]driver.Value{"w", r.name, int64(i + 1)}, r.body(true)...))
		}
		return out, nil
	case strings.Contains(query, "information_schema.columns"):
		// loadTableSchema's per-table query.
		out := &catalogFakeRows{cols: make([]string, 14)}
		for _, r := range c.rows {
			out.vals = append(out.vals, append([]driver.Value{r.name}, r.body(false)...))
		}
		return out, nil
	}
	return nil, fmt.Errorf("catalog fake: unexpected query %q", query)
}

// body renders the row's columns from column_default onward in the
// readers' SELECT order; withSRID inserts the srs_id slot the
// SchemaReader's query carries between collation_name and column_type.
func (r catalogColumnRow) body(withSRID bool) []driver.Value {
	var def driver.Value
	if r.def.Valid {
		def = r.def.String
	}
	vals := []driver.Value{
		def, r.nullable, r.dataType,
		r.charMaxLen, r.numPrec, r.numScale, r.dtPrec,
		"utf8mb4", "utf8mb4_general_ci",
	}
	if withSRID {
		vals = append(vals, int64(0))
	}
	return append(vals, r.columnType, r.extra, "", "")
}

type catalogFakeRows struct {
	cols []string
	vals [][]driver.Value
	i    int
}

func (r *catalogFakeRows) Columns() []string { return r.cols }
func (r *catalogFakeRows) Close() error      { return nil }
func (r *catalogFakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.vals) {
		return io.EOF
	}
	copy(dest, r.vals[r.i])
	r.i++
	return nil
}

func newCatalogFakeDB(t *testing.T, name string, rows []catalogColumnRow) *sql.DB {
	t.Helper()
	sql.Register(name, catalogFakeDriver{rows: rows})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open catalog fake: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestLoadTableSchema_DefaultAgreesWithSeed_EveryBinlogFlavor is the Bug
// 286 pin, in the shape of GC-1's
// TestNormalizeForCDCComparison_Binlog_SeedAgreesWithBoundaryProjection
// one field over: for the SAME catalog row, the CDC boundary projection
// (loadTableSchema → projectTableIR — what the ADR-0091 forward-add-column
// intercept re-emits verbatim into the target's ADD COLUMN) must carry the
// IR default the cold-start seed (SchemaReader.populateColumns) carries,
// on every binlog flavor, and both must be the parity-matrix value.
//
// Before the fix loadTableSchema translated COLUMN_DEFAULT with the
// MySQL-convention [translateDefault] on every flavor under a comment
// that said the applier "uses Default for nothing it re-emits" — a
// premise the intercept had already expired — so on a MariaDB source a
// forwarded `ADD COLUMN c1 VARCHAR(20) NULL` landed on Postgres as
// DEFAULT 'NULL' (the four-character string), DEFAULT 'abc' as
// 'abc' with MariaDB's quote characters kept inside the literal, and
// DEFAULT CURRENT_TIMESTAMP as a string literal that
// bypassed the ADR-0058 §2a volatility door and killed the stream with a
// raw SQLSTATE 22007. The flavor roster is derived from the registry
// (binlogFlavors), so a new binlog flavor cannot escape the pin.
func TestLoadTableSchema_DefaultAgreesWithSeed_EveryBinlogFlavor(t *testing.T) {
	ctx := context.Background()
	for _, f := range binlogFlavors(t) {
		rows := bug286CatalogRows(f)
		db := newCatalogFakeDB(t, fmt.Sprintf("sluice-catalog-test-%s-%d", t.Name(), f), rows)

		// The cold-start seed.
		seed := map[string]*ir.Table{"w": {Name: "w"}}
		sr := &SchemaReader{db: db, schema: "src", flavor: f}
		if err := sr.populateColumns(ctx, seed); err != nil {
			t.Fatalf("%s: seed populateColumns: %v", f, err)
		}
		// The CDC boundary projection.
		tbl, err := loadTableSchema(ctx, db, "src", "w", f)
		if err != nil {
			t.Fatalf("%s: loadTableSchema: %v", f, err)
		}
		proj := projectTableIR(tbl)

		if len(seed["w"].Columns) != len(rows) || len(proj.Columns) != len(rows) {
			t.Fatalf("%s: seed=%d proj=%d columns; want %d each (the fake catalog is mis-shaped, not the readers)",
				f, len(seed["w"].Columns), len(proj.Columns), len(rows))
		}
		for i, row := range rows {
			seedDef, projDef := seed["w"].Columns[i].Default, proj.Columns[i].Default
			if !reflect.DeepEqual(seedDef, projDef) {
				t.Errorf("%s: column %s (COLUMN_DEFAULT=%q extra=%q): seed carries %#v, CDC boundary projection carries %#v — the intercept re-emits the projection's value (Bug 286)",
					f, row.name, row.def.String, row.extra, seedDef, projDef)
			}
			if !reflect.DeepEqual(projDef, row.want) {
				t.Errorf("%s: column %s (COLUMN_DEFAULT=%q extra=%q): projection default = %#v; want %#v",
					f, row.name, row.def.String, row.extra, projDef, row.want)
			}
		}
	}
}
