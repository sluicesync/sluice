//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sample-mode row hash, family by family, on real MySQL.
//
// VERIFY-SAMPLE-COLUMN-ORDER's fix rewrote the per-column rendering the
// hash reads ([mysqlSampleToken]), and the rendering dispatches on a type
// FAMILY: the binary family is digested from its raw bytes, everything
// else is rendered through CAST(… AS CHAR) with a byte-digest fallback
// for a cast that comes back NULL. So per the Bug 74 rule the pin is
// every family, not one representative — each column below gets one row
// where ONLY that column is corrupted on the twin table, and every such
// row must hash differently, while a byte-identical twin must hash equal
// on every row.
//
// Independent expected value: the corruption is written by this file, per
// row, so which rows must differ is known before any hash is computed.
//
// The binary rows are the ones the old rendering could not see: under the
// utf8mb4 connection charset CAST(<invalid UTF-8> AS CHAR) is NULL, so
// GEOMETRY, BIT(8) 0xFF, and invalid-UTF-8 VARBINARY/BINARY/BLOB values
// all rendered '' and any two of them hashed equal. `fb` drives the CAST
// fallback directly by declaring a VARBINARY column to the hash as TEXT.
//
// The FLOAT rows pin the precision class: MySQL and MariaDB render a
// single-precision FLOAT through CAST(… AS CHAR) at 6 significant digits
// (1234567 and 1234568 both print as 1234570), so the token reads it
// through `(col * 1E0)` — a lossless promotion to DOUBLE, which prints
// shortest-round-trip — exactly as the row reader does. `fz` is the
// negative zero `* 1E0` keeps and `+ 0E0` would lose; `dp` holds DOUBLE to
// its last bit.
//
// The same matrix runs on MariaDB 11.4, minus two rows that are about the
// server rather than sluice: MariaDB stores a FLOAT -0 as 0 (so `fz` has
// nothing to corrupt), and its CAST of invalid UTF-8 substitutes '?'
// rather than returning NULL, so the CAST-is-NULL fallback `fb` drives is
// a MySQL-only backstop — on MariaDB the guarantee rests on the type
// mapping digesting every binary family, which the `vcb`/`tbin` rows (a
// CHARACTER SET binary VARCHAR and TEXT, i.e. bytes declared as text)
// hold to account.

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// sampleFamily is one column of the matrix: its DDL type, the value every
// row holds, and the value the corrupted twin holds on that family's row.
// mysqlOnly marks a row that is about a MySQL server behaviour (above).
type sampleFamily struct {
	col, ddl, base, corrupt string
	mysqlOnly               bool
}

var mysqlSampleFamilies = []sampleFamily{
	{col: "i", ddl: "INT", base: "7", corrupt: "8"},
	{col: "u", ddl: "BIGINT UNSIGNED", base: "18446744073709551615", corrupt: "18446744073709551614"},
	{col: "dcm", ddl: "DECIMAL(10,2)", base: "1.50", corrupt: "1.51"},
	{col: "f", ddl: "FLOAT", base: "1.5", corrupt: "1.25"},
	{col: "fp1", ddl: "FLOAT", base: "1234567", corrupt: "1234568"},
	{col: "fp2", ddl: "FLOAT", base: "0.1234567", corrupt: "0.1234568"},
	{col: "fz", ddl: "FLOAT", base: "-0e0", corrupt: "0", mysqlOnly: true},
	{col: "d", ddl: "DOUBLE", base: "0.1", corrupt: "0.2"},
	{col: "dp", ddl: "DOUBLE", base: "1", corrupt: "1.0000000000000002"},
	{col: "bo", ddl: "TINYINT(1)", base: "1", corrupt: "0"},
	{col: "ch", ddl: "CHAR(5)", base: "'ab'", corrupt: "'ac'"},
	{col: "vc", ddl: "VARCHAR(20)", base: "'hello'", corrupt: "'hellp'"},
	{col: "tx", ddl: "TEXT", base: "'text'", corrupt: "'texu'"},
	{col: "en", ddl: "ENUM('x','y')", base: "'x'", corrupt: "'y'"},
	{col: "st", ddl: "SET('p','q')", base: "'p'", corrupt: "'p,q'"},
	{col: "js", ddl: "JSON", base: `'{"a":1}'`, corrupt: `'{"a":2}'`},
	{col: "dt", ddl: "DATE", base: "'2024-01-02'", corrupt: "'2024-01-03'"},
	{col: "dtm", ddl: "DATETIME(6)", base: "'2024-01-02 03:04:05.123456'", corrupt: "'2024-01-02 03:04:05.123457'"},
	{col: "ts", ddl: "TIMESTAMP(6) NULL", base: "'2024-01-02 03:04:05.123456'", corrupt: "'2024-01-02 03:04:05.123457'"},
	{col: "tm", ddl: "TIME(6)", base: "'12:34:56.5'", corrupt: "'12:34:56.6'"},
	{col: "yr", ddl: "YEAR", base: "2024", corrupt: "2025"},
	{col: "bn", ddl: "BINARY(4)", base: "0xFFFFFFFF", corrupt: "0xFEFFFFFF"},
	{col: "vb", ddl: "VARBINARY(8)", base: "0x61FF62", corrupt: "0x61FE62"},
	{col: "vcb", ddl: "VARCHAR(8) CHARACTER SET binary", base: "0x61FF62", corrupt: "0x61FE62"},
	{col: "bl", ddl: "BLOB", base: "0xFF00", corrupt: "0xFE00"},
	{col: "tbin", ddl: "TEXT CHARACTER SET binary", base: "0xFF00", corrupt: "0xFE00"},
	{col: "bt", ddl: "BIT(8)", base: "b'11111111'", corrupt: "b'11111110'"},
	{col: "g", ddl: "GEOMETRY", base: "ST_GeomFromText('POINT(1 2)')", corrupt: "ST_GeomFromText('POINT(1 3)')"},
	{col: "fb", ddl: "VARBINARY(8)", base: "0x80", corrupt: "0x81", mysqlOnly: true},
}

// Rows past the family rows: the two encodings the old CONCAT_WS
// rendering collapsed, independent of type.
const (
	rowNullVsEmpty = 101 // t1 NULL on the source, '' on the twin
	rowSeparator   = 102 // ('a|', 'b') on the source, ('a', '|b') on the twin
)

func TestSampleRowHashes_FamilyMatrix(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vsfam")
	defer cleanup()
	runSampleFamilyMatrix(t, dsn, Engine{Flavor: FlavorVanilla}, mysqlSampleFamilies)
}

// TestSampleRowHashes_FamilyMatrix_MariaDB runs the same matrix on
// MariaDB 11.4, the LTS whose CAST of invalid UTF-8 substitutes '?'.
func TestSampleRowHashes_FamilyMatrix_MariaDB(t *testing.T) {
	dsn := newMariaDB(t, mariadb114Image, "vsfam")
	eng, ok := engines.Get("mariadb")
	if !ok {
		t.Fatal(`engine "mariadb" not registered`)
	}
	var families []sampleFamily
	for _, f := range mysqlSampleFamilies {
		if !f.mysqlOnly {
			families = append(families, f)
		}
	}
	runSampleFamilyMatrix(t, dsn, eng, families)
}

func runSampleFamilyMatrix(t *testing.T, dsn string, eng ir.Engine, families []sampleFamily) {
	t.Helper()
	cols := []string{"id"}
	defs := []string{"id BIGINT PRIMARY KEY"}
	for _, f := range families {
		cols = append(cols, f.col)
		defs = append(defs, f.col+" "+f.ddl)
	}
	cols = append(cols, "t1", "t2")
	defs = append(defs, "t1 TEXT", "t2 TEXT")

	insert := func(table string, id int, override map[string]string) string {
		vals := []string{fmt.Sprint(id)}
		for _, f := range families {
			v := f.base
			if o, ok := override[f.col]; ok {
				v = o
			}
			vals = append(vals, v)
		}
		for _, c := range []string{"t1", "t2"} {
			v := "'" + c + "'"
			if o, ok := override[c]; ok {
				v = o
			}
			vals = append(vals, v)
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n", table, strings.Join(cols, ", "), strings.Join(vals, ", "))
	}
	var script strings.Builder
	for _, table := range []string{"vs_src", "vs_same", "vs_bad"} {
		fmt.Fprintf(&script, "CREATE TABLE %s (%s);\n", table, strings.Join(defs, ", "))
	}
	for i, f := range families {
		id := i + 1
		script.WriteString(insert("vs_src", id, nil))
		script.WriteString(insert("vs_same", id, nil))
		script.WriteString(insert("vs_bad", id, map[string]string{f.col: f.corrupt}))
	}
	src := map[int]map[string]string{
		rowNullVsEmpty: {"t1": "NULL"},
		rowSeparator:   {"t1": "'a|'", "t2": "'b'"},
	}
	bad := map[int]map[string]string{
		rowNullVsEmpty: {"t1": "''"},
		rowSeparator:   {"t1": "'a'", "t2": "'|b'"},
	}
	for _, id := range []int{rowNullVsEmpty, rowSeparator} {
		script.WriteString(insert("vs_src", id, src[id]))
		script.WriteString(insert("vs_same", id, src[id]))
		script.WriteString(insert("vs_bad", id, bad[id]))
	}
	applyMySQL(t, dsn, script.String())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rdr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tables := map[string]*ir.Table{}
	for _, tb := range schema.Tables {
		// `fb` is a VARBINARY declared to the hash as TEXT, so its invalid
		// UTF-8 value takes the CAST-is-NULL fallback rather than the
		// binary-family branch.
		for _, c := range tb.Columns {
			if c.Name == "fb" {
				c.Type = ir.Text{}
			}
		}
		tables[tb.Name] = tb
	}
	plan := ir.SampleColumns{Key: []string{"id"}, Hashed: cols}
	sv := rdr.(ir.SampleVerifier)

	for _, algo := range []ir.HashAlgorithm{ir.HashMD5, ir.HashSHA256} {
		t.Run(algo.String(), func(t *testing.T) {
			hash := func(table string) map[string]string {
				rows, err := sv.SampleRowHashes(ctx, tables[table], plan, 1000, 42, algo)
				if err != nil {
					t.Fatalf("SampleRowHashes %s: %v", table, err)
				}
				out := make(map[string]string, len(rows))
				for _, r := range rows {
					out[r.PrimaryKey] = r.Hash
				}
				return out
			}
			srcH, sameH, badH := hash("vs_src"), hash("vs_same"), hash("vs_bad")
			want := len(families) + 2
			if len(srcH) != want || len(sameH) != want || len(badH) != want {
				t.Fatalf("sampled %d/%d/%d rows; want %d each", len(srcH), len(sameH), len(badH), want)
			}
			for pk, h := range srcH {
				if sameH[pk] != h {
					t.Errorf("row %s: byte-identical twin hashes differently — false mismatch", pk)
				}
			}
			label := func(pk string) string {
				switch pk {
				case fmt.Sprint(rowNullVsEmpty):
					return "NULL vs ''"
				case fmt.Sprint(rowSeparator):
					return "separator borrow"
				}
				var id int
				_, _ = fmt.Sscan(pk, &id)
				return families[id-1].ddl + " (" + families[id-1].col + ")"
			}
			for pk, h := range srcH {
				if badH[pk] == h {
					t.Errorf("row %s [%s]: corrupted value hashes EQUAL to the source — false clean", pk, label(pk))
				}
			}
		})
	}
}

// TestSampleRowHashes_PlanColumnNotOnTableRefused pins the loud half of
// the column contract: a plan naming a column the table lacks is an error,
// never a silently narrower hash.
func TestSampleRowHashes_PlanColumnNotOnTableRefused(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vsplan")
	defer cleanup()
	applyMySQL(t, dsn, "CREATE TABLE kd (id BIGINT PRIMARY KEY, b TEXT); INSERT INTO kd VALUES (1, 'x');")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rdr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	sv := rdr.(ir.SampleVerifier)
	_, err = sv.SampleRowHashes(ctx, schema.Tables[0], ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"b", "c"}}, 10, 1, ir.HashMD5)
	if err == nil || !strings.Contains(err.Error(), `column "c" is not on the table`) {
		t.Fatalf("err = %v; want the column-not-on-table refusal", err)
	}
}

// TestSampleRowHashes_BinaryKey pins the key half of the binary family: a
// BINARY key whose bytes are not valid UTF-8 casts to NULL under utf8mb4,
// which used to fail the scan of a single-column key (and, in a composite
// key, collapse distinct keys into one). The key now renders as hex.
func TestSampleRowHashes_BinaryKey(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "vsbinkey")
	defer cleanup()
	applyMySQL(t, dsn, "CREATE TABLE kb (id BINARY(4) PRIMARY KEY, v TEXT); INSERT INTO kb VALUES (0xFF000000, 'a'), (0xFE000000, 'b');")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rdr, err := Engine{Flavor: FlavorVanilla}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	rows, err := rdr.(ir.SampleVerifier).SampleRowHashes(ctx, schema.Tables[0],
		ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"id", "v"}}, 10, 1, ir.HashMD5)
	if err != nil {
		t.Fatalf("SampleRowHashes: %v", err)
	}
	if len(rows) != 2 || rows[0].PrimaryKey != "FE000000" || rows[1].PrimaryKey != "FF000000" {
		t.Fatalf("keys = %+v; want the two keys in hex", rows)
	}
}
