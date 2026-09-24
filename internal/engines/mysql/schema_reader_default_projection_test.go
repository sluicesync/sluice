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
	"regexp"
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

	// showCreate is the column's DEFAULT clause as SHOW CREATE TABLE
	// prints it — the authoritative bytes the NUL-truncation recovery
	// re-reads. Empty for a row that never triggers the recovery.
	showCreate string

	// probeHex is HEX(DEFAULT(col)) as MariaDB returns it — the true bytes
	// the GC-36 DEFAULT() probe re-reads. Empty for a row never probed.
	probeHex string

	// probeUTF8Hex is HEX(CONVERT(DEFAULT(col) USING utf8mb4)) — the true
	// text the GC-37 (h) probe re-reads for a character default that read
	// back with a '?'. Empty for a row never probed.
	probeUTF8Hex string
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

// gc37hTextCatalogRows returns the GC-37 (h) shapes as flavor f's server
// reports them: character-column literal defaults whose supplementary
// characters information_schema's utf8mb3 COLUMN_DEFAULT stores as '?',
// paired with the utf8mb4 DEFAULT() probe's true text, plus the controls
// that must NOT change — a genuine '?', BMP non-ASCII, a default with no
// '?' (never probed). Every COLUMN_DEFAULT below is what mysql:8.0.46 /
// mariadb:11.4.13 reported for the declared DDL in the comment (measured
// 2026-09-24); the probe hex is what both returned.
func gc37hTextCatalogRows(f Flavor) []catalogColumnRow {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	catalog := func(text string) sql.NullString {
		if f == FlavorMariaDB {
			return valid("'" + strings.ReplaceAll(text, "'", "''") + "'")
		}
		return valid(text)
	}
	col := func(name, dataType, columnType, text, probe, want string) catalogColumnRow {
		return catalogColumnRow{
			name: name, def: catalog(text), nullable: "YES",
			dataType: dataType, columnType: columnType,
			want: ir.DefaultLiteral{Value: want}, probeUTF8Hex: probe,
		}
	}
	rows := []catalogColumnRow{
		col("t1", "varchar", "varchar(20)", "?x", "F09F988078", "😀x"),             // DEFAULT '😀x'
		col("t2", "char", "char(4)", "?", "F09F9880", "😀"),                        // DEFAULT '😀'
		col("t3", "varchar", "varchar(20)", "?", "3F", "?"),                       // DEFAULT '?': a genuine '?'
		col("t4", "varchar", "varchar(20)", "???", "3FF09F98803F", "?😀?"),         // DEFAULT '?😀?': genuine '?' around a lost one
		col("t5", "varchar", "varchar(20)", "é?", "C3A9F09F9880", "é😀"),           // DEFAULT 'é😀': the BMP character survives
		col("t6", "varchar", "varchar(20)", "it's?", "69742773F09F9880", "it's😀"), // quote doubling on MariaDB
		col("t7", "enum", "enum('a','?b')", "?b", "3F62", "?b"),                   // ENUM DEFAULT '?b': a genuine '?' label
		{
			// DEFAULT 'é€': no '?', so never probed (the fake refuses a probe
			// of a column without probeUTF8Hex).
			name: "t8", def: catalog("é€"), nullable: "YES",
			dataType: "varchar", columnType: "varchar(10)", want: ir.DefaultLiteral{Value: "é€"},
		},
	}
	if f == FlavorMariaDB {
		// MariaDB takes a literal DEFAULT on TEXT, and stores one '?' per
		// BYTE of the lost character there (measured: TEXT DEFAULT '😀z'
		// → '????z').
		rows = append(rows, col("t9", "text", "text", "????z", "F09F98807A", "😀z"))
	}
	return rows
}

// gc29BinaryCatalogRows returns the GC-29 shapes: BINARY/VARBINARY literal
// defaults whose information_schema COLUMN_DEFAULT MySQL C-string-truncates
// at the first NUL byte (the table in binary_default_recovery.go's doc),
// paired with the SHOW CREATE clause that carries the true bytes, in both
// forms SHOW CREATE uses — the quoted escaped string (every byte < 0x80)
// and the hex literal (any byte >= 0x80) — plus a NUL-free control.
//
//	b1 BINARY(2)    DEFAULT 0x2700    → "0x27"     (well-formed but SHORT)
//	b2 BINARY(1)    DEFAULT 0x00      → "0x"       (empty)
//	b3 BINARY(3)    DEFAULT 0xFFEEDD  → "0xFFEEDD" (faithful)
//	b4 VARBINARY(4) DEFAULT 0xFF00    → "0xFF"     (well-formed but SHORT)
//	b5 BINARY(3)    DEFAULT 0xFF00AA  → "0xFF"     (SHORT mid-value; width padding cannot repair it)
//	b6 VARBINARY(4) DEFAULT 0x6100    → "0x61"     (VARBINARY in SHOW CREATE's quoted form)
//	b7 BINARY(4)    DEFAULT 0x00410042 → "0x"      (leading and interleaved NULs, quoted form)
//
// MariaDB escape-encodes NULs in a quoted COLUMN_DEFAULT instead of
// truncating (translateMariaDBDefault's doc), so the recovery never fires
// there; these rows are the MySQL-convention flavors' catalog surface.
func gc29BinaryCatalogRows() []catalogColumnRow {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	hexDef := func(h string) ir.DefaultValue { return ir.DefaultExpression{Expr: h, Dialect: hexLiteralDialect} }
	bin := func(name, dataType string, width int64, def, showCreate string, want ir.DefaultValue) catalogColumnRow {
		return catalogColumnRow{
			name: name, def: valid(def), nullable: "YES",
			dataType: dataType, columnType: fmt.Sprintf("%s(%d)", dataType, width), charMaxLen: width,
			want: want, showCreate: showCreate,
		}
	}
	return []catalogColumnRow{
		bin("b1", "binary", 2, "0x27", `'''\0'`, hexDef("0x2700")),
		bin("b2", "binary", 1, "0x", `'\0'`, hexDef("0x00")),
		bin("b3", "binary", 3, "0xFFEEDD", "0xFFEEDD", hexDef("0xFFEEDD")),
		bin("b4", "varbinary", 4, "0xFF", "0xFF00", hexDef("0xFF00")),
		bin("b5", "binary", 3, "0xFF", "0xFF00AA", hexDef("0xFF00AA")),
		bin("b6", "varbinary", 4, "0x61", `'a\0'`, hexDef("0x6100")),
		bin("b7", "binary", 4, "0x", `'\0A\0B'`, hexDef("0x00410042")),
	}
}

// gc36MariaDBBinaryCatalogRows returns the GC-36 shapes: MariaDB
// BINARY/VARBINARY literal defaults whose quoted COLUMN_DEFAULT the server
// stores with every non-UTF-8 byte replaced by '?', paired with the
// DEFAULT() probe's true bytes — the family matrix of {leading, mid,
// trailing, width-padding} NUL × bytes >= 0x80 × {BINARY, VARBINARY} ×
// the quote and backslash escapes, plus the controls whose catalog text is
// faithful (valid UTF-8 high bytes, a TRUE '?', pure ASCII) and the empty
// default that is never probed. Every COLUMN_DEFAULT below is the text
// mariadb:11.4.13 reported for the declared DDL in the comment (measured
// 2026-09-23; 10.6.28 and 10.11.19 identical for the rows measured there).
// want is the declared literal, width-padded for BINARY — the independent
// expected value, not the probe's echo.
func gc36MariaDBBinaryCatalogRows() []catalogColumnRow {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	hexDef := func(h string) ir.DefaultValue { return ir.DefaultExpression{Expr: h, Dialect: hexLiteralDialect} }
	bin := func(name, dataType string, width int64, catalog, probe, want string) catalogColumnRow {
		return catalogColumnRow{
			name: name, def: valid(catalog), nullable: "YES",
			dataType: dataType, columnType: fmt.Sprintf("%s(%d)", dataType, width), charMaxLen: width,
			want: hexDef(want), probeHex: probe,
		}
	}
	return []catalogColumnRow{
		bin("m1", "binary", 3, `'\0?\0'`, "00AB00", "0x00AB00"),                     // DEFAULT 0x00AB00: leading + trailing NUL
		bin("m2", "varbinary", 4, `'?\0?'`, "AB00CD", "0xAB00CD"),                   // DEFAULT 0xAB00CD: mid NUL
		bin("m3", "varbinary", 3, `'??\0'`, "FFFE00", "0xFFFE00"),                   // DEFAULT 0xFFFE00: trailing NUL
		bin("m4", "binary", 4, `'?\0\0\0'`, "FF000000", "0xFF000000"),               // DEFAULT 0xFF: width padding
		bin("m5", "varbinary", 4, `'''?'''`, "27FF27", "0x27FF27"),                  // DEFAULT 0x27FF27: quotes
		bin("m6", "binary", 3, `'\\?\\'`, "5C805C", "0x5C805C"),                     // DEFAULT 0x5C805C: backslashes
		bin("m7", "varbinary", 6, `'''?\\\0'`, "27FF5C00", "0x27FF5C00"),            // DEFAULT 0x27FF5C00: all three
		bin("m8", "varbinary", 4, `'????'`, "80818283", "0x80818283"),               // DEFAULT 0x80818283: every byte lost
		bin("m9", "varbinary", 8, `'????'`, "F09F9880", "0xF09F9880"),               // DEFAULT 0xF09F9880: 4-byte UTF-8 (utf8mb3 catalog)
		bin("m10", "varbinary", 4, "'\xDE\xAD'", "DEAD", "0xDEAD"),                  // DEFAULT 0xDEAD: valid UTF-8, faithful
		bin("m11", "varbinary", 4, `'??'`, "3F3F", "0x3F3F"),                        // DEFAULT 0x3F3F: a TRUE '?'
		bin("m12", "varbinary", 4, `'???'`, "3FAB3F", "0x3FAB3F"),                   // DEFAULT 0x3FAB3F: true '?' around a lost byte
		bin("m13", "varbinary", 6, "'\\n\\r\x1a\t?'", "0A0D1A09AB", "0x0A0D1A09AB"), // control bytes + a lost byte
		bin("m14", "binary", 4, `'ab\0\0'`, "61620000", "0x61620000"),               // DEFAULT 'ab': ASCII control
		{
			// DEFAULT '': no bytes to lose, never probed.
			name: "m15", def: valid("''"), nullable: "YES",
			dataType: "varbinary", columnType: "varbinary(4)", charMaxLen: int64(4),
			want: ir.DefaultLiteral{Value: ""},
		},
	}
}

// gc36MariaDB118BinaryCatalogRows is the same class as MariaDB 11.8+
// reports it: a lossless lowercase x'<hex>' literal (BINARY width-padded,
// measured on 11.8.9 and 12.3), which must land on the same IR as the
// probed ≤11.4 form and MySQL 8 — never probed, since nothing was lost.
func gc36MariaDB118BinaryCatalogRows() []catalogColumnRow {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	bin := func(name, dataType string, width int64, catalog, want string) catalogColumnRow {
		return catalogColumnRow{
			name: name, def: valid(catalog), nullable: "YES",
			dataType: dataType, columnType: fmt.Sprintf("%s(%d)", dataType, width), charMaxLen: width,
			want: ir.DefaultExpression{Expr: want, Dialect: hexLiteralDialect},
		}
	}
	return []catalogColumnRow{
		bin("x1", "binary", 3, "x'00ab00'", "0x00AB00"),        // DEFAULT 0x00AB00
		bin("x2", "varbinary", 4, "x'ab00cd'", "0xAB00CD"),     // DEFAULT 0xAB00CD
		bin("x3", "binary", 4, "x'ff000000'", "0xFF000000"),    // DEFAULT 0xFF: width padding
		bin("x4", "varbinary", 6, "x'27ff5c00'", "0x27FF5C00"), // DEFAULT 0x27FF5C00
		bin("x5", "varbinary", 4, "x'3f3f'", "0x3F3F"),         // DEFAULT 0x3F3F
		bin("x6", "binary", 4, "x'61620000'", "0x61620000"),    // DEFAULT 'ab'
	}
}

// showCreateTableW renders the SHOW CREATE TABLE text for the fake's one
// table: one indented line per column, the DEFAULT clause only where the
// row carries one, in the shape parseShowCreateColumnDefault reads.
func showCreateTableW(rows []catalogColumnRow) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE `w` (\n")
	for _, r := range rows {
		b.WriteString("  `" + r.name + "` " + r.columnType)
		if r.showCreate != "" {
			b.WriteString(" DEFAULT " + r.showCreate)
		}
		b.WriteString(",\n")
	}
	b.WriteString("  PRIMARY KEY (`id`)\n) ENGINE=InnoDB")
	return b.String()
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
	case strings.HasPrefix(query, "SHOW CREATE TABLE "):
		// The NUL-truncated binary-default recovery's authoritative re-read.
		return &catalogFakeRows{cols: []string{"Table", "Create Table"}, vals: [][]driver.Value{{"w", showCreateTableW(c.rows)}}}, nil
	case strings.HasPrefix(query, "SELECT HEX(CONVERT(DEFAULT("):
		// The GC-37 (h) text probe: the same shape, converted to utf8mb4.
		byName := map[string]catalogColumnRow{}
		for _, r := range c.rows {
			byName[r.name] = r
		}
		var vals []driver.Value
		for _, m := range probedColumnRE.FindAllStringSubmatch(query, -1) {
			r, ok := byName[m[1]]
			if !ok || r.probeUTF8Hex == "" {
				return nil, fmt.Errorf("catalog fake: utf8mb4 DEFAULT() probe of unexpected column %q", m[1])
			}
			vals = append(vals, r.probeUTF8Hex)
		}
		return &catalogFakeRows{cols: make([]string, len(vals)), vals: [][]driver.Value{vals}}, nil
	case strings.HasPrefix(query, "SELECT HEX(DEFAULT("):
		// The GC-36 MariaDB DEFAULT() probe: one row, one HEX per named
		// column, in the order the query names them.
		byName := map[string]catalogColumnRow{}
		for _, r := range c.rows {
			byName[r.name] = r
		}
		var vals []driver.Value
		for _, m := range probedColumnRE.FindAllStringSubmatch(query, -1) {
			r, ok := byName[m[1]]
			if !ok || r.probeHex == "" {
				return nil, fmt.Errorf("catalog fake: DEFAULT() probe of unexpected column %q", m[1])
			}
			vals = append(vals, r.probeHex)
		}
		return &catalogFakeRows{cols: make([]string, len(vals)), vals: [][]driver.Value{vals}}, nil
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

// probedColumnRE extracts each column name a DEFAULT() probe names.
var probedColumnRE = regexp.MustCompile("DEFAULT\\(`sluice_t`\\.`([^`]+)`\\)")

// body renders the row's columns from column_default onward in the
// readers' SELECT order; withSRID inserts the srs_id slot the
// SchemaReader's query carries between collation_name and column_type.
func (r catalogColumnRow) body(withSRID bool) []driver.Value {
	var def driver.Value
	if r.def.Valid {
		def = r.def.String
	}
	// information_schema reports a character set only for a character
	// column; a binary string, a number or a temporal reads NULL (the
	// readers' IFNULL → ""). The GC-37 (h) text recovery keys on it.
	charset, collation := "", ""
	switch r.dataType {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
		charset, collation = "utf8mb4", "utf8mb4_general_ci"
	}
	vals := []driver.Value{
		def, r.nullable, r.dataType,
		r.charMaxLen, r.numPrec, r.numScale, r.dtPrec,
		charset, collation,
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
//
// GC-29 is the same seed-vs-projection split one pass later: the seed ran
// the SHOW CREATE recovery for NUL-truncated BINARY/VARBINARY literal
// defaults and loadTableSchema did not, so a forwarded `ADD COLUMN b
// BINARY(2) DEFAULT 0x2700` projected the truncated `0x27` and the
// intercept re-emitted it. The gc29BinaryCatalogRows carry that shape on
// every MySQL-convention flavor.
//
// GC-36 is MariaDB's version of GC-29: its catalog stores every non-UTF-8
// byte of a quoted binary default as '?', so both readers must replace the
// catalog value from the DEFAULT() probe, or 0x00AB00 projects as 0x003F00.
// The gc36MariaDBBinaryCatalogRows carry that shape on the mariadb flavor.
func TestLoadTableSchema_DefaultAgreesWithSeed_EveryBinlogFlavor(t *testing.T) {
	ctx := context.Background()
	for _, f := range binlogFlavors(t) {
		rows := bug286CatalogRows(f)
		if f != FlavorMariaDB {
			rows = append(rows, gc29BinaryCatalogRows()...)
		} else {
			rows = append(rows, gc36MariaDBBinaryCatalogRows()...)
			rows = append(rows, gc36MariaDB118BinaryCatalogRows()...)
		}
		rows = append(rows, gc37hTextCatalogRows(f)...)
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
